// =============================================================================
// Amatiz — Label Index
// =============================================================================
//
// Tracks unique label names and their values across all ingested log streams.
// Used by the Loki-compatible /labels and /label/{name}/values endpoints.
//
// Thread-safe: guarded by a mutex for concurrent reads/writes from the
// ingestion pipeline and HTTP handlers.
//
// Wire format (little-endian):
//   [4]  magic "LIDX"
//   [4]  label_count  u32
//   Repeated label_count times:
//     [2]  name_len     u16
//     [n]  name         [name_len]u8
//     [4]  value_count  u32
//     Repeated value_count times:
//       [2]  value_len  u16
//       [n]  value      [value_len]u8
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;

pub const LABEL_INDEX_MAGIC = [4]u8{ 'L', 'I', 'D', 'X' };

pub const LabelIndex = struct {
    allocator: Allocator,
    /// label_name → set of known values.
    index: std.StringHashMap(ValueSet),
    /// fingerprint("k1=v1\x1fk2=v2\x1f...") → owned label set.
    /// Tracks every unique label combination seen, so /series can return
    /// real {label=value, ...} records (not the Cartesian product of
    /// per-name values).
    series: std.StringHashMap([]LogEntry.Label),
    mutex: std.Thread.Mutex,

    const ValueSet = std.StringHashMap(void);

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(allocator: Allocator) LabelIndex {
        return .{
            .allocator = allocator,
            .index = std.StringHashMap(ValueSet).init(allocator),
            .series = std.StringHashMap([]LogEntry.Label).init(allocator),
            .mutex = .{},
        };
    }

    pub fn deinit(self: *LabelIndex) void {
        var it = self.index.iterator();
        while (it.next()) |entry| {
            // Free all value strings.
            var val_it = entry.value_ptr.iterator();
            while (val_it.next()) |val_entry| {
                self.allocator.free(val_entry.key_ptr.*);
            }
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.index.deinit();

        var sit = self.series.iterator();
        while (sit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            for (entry.value_ptr.*) |lbl| {
                self.allocator.free(lbl.key);
                self.allocator.free(lbl.value);
            }
            self.allocator.free(entry.value_ptr.*);
        }
        self.series.deinit();
    }

    // -----------------------------------------------------------------------
    // Indexing
    // -----------------------------------------------------------------------

    /// Record a label name-value pair. Thread-safe.
    pub fn trackLabel(self: *LabelIndex, name: []const u8, value: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.trackLabelLocked(name, value);
    }

    /// Record all labels from a log entry. Thread-safe.
    pub fn trackEntry(self: *LabelIndex, entry: *const LogEntry) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Always track "source" as a built-in label.
        self.trackLabelLocked("source", entry.source);

        for (entry.labels) |label| {
            self.trackLabelLocked(label.key, label.value);
        }

        self.trackSeriesLocked(entry) catch {};
    }

    /// Record the full label-set fingerprint for an entry. Idempotent.
    fn trackSeriesLocked(self: *LabelIndex, entry: *const LogEntry) !void {
        // Build sorted (key,value) list including the synthetic "source" label.
        var pairs = std.ArrayList(LogEntry.Label).init(self.allocator);
        defer pairs.deinit();

        try pairs.append(.{ .key = "source", .value = entry.source });
        for (entry.labels) |lbl| {
            if (std.mem.eql(u8, lbl.key, "source")) continue;
            try pairs.append(lbl);
        }
        std.mem.sort(LogEntry.Label, pairs.items, {}, struct {
            fn lt(_: void, a: LogEntry.Label, b: LogEntry.Label) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.lt);

        // Build fingerprint key.
        var fp = std.ArrayList(u8).init(self.allocator);
        errdefer fp.deinit();
        for (pairs.items, 0..) |lbl, i| {
            if (i > 0) try fp.append(0x1f);
            try fp.appendSlice(lbl.key);
            try fp.append('=');
            try fp.appendSlice(lbl.value);
        }
        const fp_slice = try fp.toOwnedSlice();
        errdefer self.allocator.free(fp_slice);

        if (self.series.contains(fp_slice)) {
            self.allocator.free(fp_slice);
            return;
        }

        // Deep-copy the label set so it outlives the source entry.
        const owned = try self.allocator.alloc(LogEntry.Label, pairs.items.len);
        var i: usize = 0;
        errdefer {
            var j: usize = 0;
            while (j < i) : (j += 1) {
                self.allocator.free(owned[j].key);
                self.allocator.free(owned[j].value);
            }
            self.allocator.free(owned);
        }
        while (i < pairs.items.len) : (i += 1) {
            owned[i] = .{
                .key = try self.allocator.dupe(u8, pairs.items[i].key),
                .value = try self.allocator.dupe(u8, pairs.items[i].value),
            };
        }

        try self.series.put(fp_slice, owned);
    }

    /// Return all known unique label sets. Caller owns the outer + inner slices.
    pub fn getSeries(self: *LabelIndex, allocator: Allocator) ![]const []const LogEntry.Label {
        self.mutex.lock();
        defer self.mutex.unlock();

        var out = std.ArrayList([]const LogEntry.Label).init(allocator);
        errdefer {
            for (out.items) |s| {
                for (s) |lbl| {
                    allocator.free(lbl.key);
                    allocator.free(lbl.value);
                }
                allocator.free(s);
            }
            out.deinit();
        }

        var it = self.series.iterator();
        while (it.next()) |entry| {
            const src = entry.value_ptr.*;
            const copy = try allocator.alloc(LogEntry.Label, src.len);
            var i: usize = 0;
            errdefer {
                var j: usize = 0;
                while (j < i) : (j += 1) {
                    allocator.free(copy[j].key);
                    allocator.free(copy[j].value);
                }
                allocator.free(copy);
            }
            while (i < src.len) : (i += 1) {
                copy[i] = .{
                    .key = try allocator.dupe(u8, src[i].key),
                    .value = try allocator.dupe(u8, src[i].value),
                };
            }
            try out.append(copy);
        }

        return out.toOwnedSlice();
    }

    fn trackLabelLocked(self: *LabelIndex, name: []const u8, value: []const u8) void {
        const gop = self.index.getOrPut(name) catch return;
        if (!gop.found_existing) {
            const owned_name = self.allocator.dupe(u8, name) catch return;
            gop.key_ptr.* = owned_name;
            gop.value_ptr.* = ValueSet.init(self.allocator);
        }

        var vs = gop.value_ptr;
        const vgop = vs.getOrPut(value) catch return;
        if (!vgop.found_existing) {
            const owned_value = self.allocator.dupe(u8, value) catch return;
            vgop.key_ptr.* = owned_value;
        }
    }

    // -----------------------------------------------------------------------
    // Querying
    // -----------------------------------------------------------------------

    /// Return all known label names. Caller owns the returned slice.
    pub fn getNames(self: *LabelIndex, allocator: Allocator) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (result.items) |item| allocator.free(item);
            result.deinit();
        }

        var it = self.index.iterator();
        while (it.next()) |entry| {
            try result.append(try allocator.dupe(u8, entry.key_ptr.*));
        }

        return result.toOwnedSlice();
    }

    /// Return all known values for a label name. Caller owns the returned slice.
    pub fn getValues(self: *LabelIndex, allocator: Allocator, name: []const u8) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (result.items) |item| allocator.free(item);
            result.deinit();
        }

        if (self.index.get(name)) |value_set| {
            var it = value_set.iterator();
            while (it.next()) |entry| {
                try result.append(try allocator.dupe(u8, entry.key_ptr.*));
            }
        }

        return result.toOwnedSlice();
    }

    /// Return total number of unique label names.
    pub fn nameCount(self: *LabelIndex) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.index.count();
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    pub fn saveToDir(self: *LabelIndex, dir: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/labels.idx", .{dir}) catch unreachable;

        std.fs.cwd().makePath(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        try writer.writeAll(&LABEL_INDEX_MAGIC);
        try writer.writeInt(u32, @intCast(self.index.count()), .little);

        var it = self.index.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            try writer.writeInt(u16, @intCast(name.len), .little);
            try writer.writeAll(name);

            const value_set = entry.value_ptr.*;
            try writer.writeInt(u32, @intCast(value_set.count()), .little);

            var val_it = value_set.iterator();
            while (val_it.next()) |val_entry| {
                const value = val_entry.key_ptr.*;
                try writer.writeInt(u16, @intCast(value.len), .little);
                try writer.writeAll(value);
            }
        }
    }

    pub fn loadFromDir(self: *LabelIndex, dir: []const u8) !void {
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/labels.idx", .{dir}) catch unreachable;

        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();
        const reader = file.reader();

        var magic: [4]u8 = undefined;
        try reader.readNoEof(&magic);
        if (!std.mem.eql(u8, &magic, &LABEL_INDEX_MAGIC)) return error.InvalidMagic;

        const label_count = try reader.readInt(u32, .little);
        var l: u32 = 0;
        while (l < label_count) : (l += 1) {
            const name_len = try reader.readInt(u16, .little);
            const name = try self.allocator.alloc(u8, name_len);
            errdefer self.allocator.free(name);
            try reader.readNoEof(name);

            var value_set = ValueSet.init(self.allocator);
            errdefer value_set.deinit();

            const value_count = try reader.readInt(u32, .little);
            var v: u32 = 0;
            while (v < value_count) : (v += 1) {
                const val_len = try reader.readInt(u16, .little);
                const val = try self.allocator.alloc(u8, val_len);
                errdefer self.allocator.free(val);
                try reader.readNoEof(val);
                try value_set.put(val, {});
            }

            try self.index.put(name, value_set);
        }
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "LabelIndex track and query" {
    const allocator = std.testing.allocator;
    var idx = LabelIndex.init(allocator);
    defer idx.deinit();

    idx.trackLabel("job", "myapp");
    idx.trackLabel("job", "myapp"); // duplicate — no-op
    idx.trackLabel("job", "otherapp");
    idx.trackLabel("level", "error");
    idx.trackLabel("level", "info");

    const names = try idx.getNames(allocator);
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqual(@as(usize, 2), names.len);

    const job_values = try idx.getValues(allocator, "job");
    defer {
        for (job_values) |v| allocator.free(v);
        allocator.free(job_values);
    }
    try std.testing.expectEqual(@as(usize, 2), job_values.len);
}

test "LabelIndex track entry" {
    const allocator = std.testing.allocator;
    var idx = LabelIndex.init(allocator);
    defer idx.deinit();

    const labels = [_]LogEntry.Label{
        .{ .key = "env", .value = "prod" },
    };

    const entry = LogEntry{
        .timestamp_ns = 1_000_000,
        .source = "/var/log/app.log",
        .message = "test",
        .raw_line = "test",
        .labels = &labels,
    };

    idx.trackEntry(&entry);

    // "source" should be automatically tracked.
    const source_vals = try idx.getValues(allocator, "source");
    defer {
        for (source_vals) |v| allocator.free(v);
        allocator.free(source_vals);
    }
    try std.testing.expect(source_vals.len >= 1);

    // "env" should also be tracked.
    const env_vals = try idx.getValues(allocator, "env");
    defer {
        for (env_vals) |v| allocator.free(v);
        allocator.free(env_vals);
    }
    try std.testing.expectEqual(@as(usize, 1), env_vals.len);
}

// =============================================================================
// Amatiz — Tailer positions file
// =============================================================================
//
// Persists the byte offset reached in each tailed file so we can resume
// without re-reading or skipping log lines after a restart.
//
// On disk format: a small flat JSON object mapping absolute path → offset:
//
//   { "/var/log/app.log": 1234567, "/var/log/sys.log": 9876 }
//
// Writes are serialized via a mutex and flushed atomically by writing to a
// `.tmp` file then renaming over the destination. If the positions file is
// missing or malformed at startup, an empty map is used and a fresh file is
// written on the next save.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Positions = struct {
    allocator: Allocator,
    path: []const u8, // owned; empty == persistence disabled
    map: std.StringHashMap(u64),
    mutex: std.Thread.Mutex = .{},

    pub fn init(allocator: Allocator, path: []const u8) !Positions {
        return .{
            .allocator = allocator,
            .path = try allocator.dupe(u8, path),
            .map = std.StringHashMap(u64).init(allocator),
        };
    }

    pub fn deinit(self: *Positions) void {
        var it = self.map.iterator();
        while (it.next()) |kv| self.allocator.free(kv.key_ptr.*);
        self.map.deinit();
        self.allocator.free(self.path);
    }

    /// True when persistence is enabled.
    pub fn enabled(self: *const Positions) bool {
        return self.path.len > 0;
    }

    /// Load positions from disk. Missing/invalid file = no-op.
    pub fn load(self: *Positions) void {
        if (!self.enabled()) return;
        const file = std.fs.cwd().openFile(self.path, .{}) catch return;
        defer file.close();
        const content = file.readToEndAlloc(self.allocator, 16 * 1024 * 1024) catch return;
        defer self.allocator.free(content);

        self.mutex.lock();
        defer self.mutex.unlock();

        var i: usize = 0;
        while (i < content.len) {
            // Find next "key".
            while (i < content.len and content[i] != '"') : (i += 1) {}
            if (i >= content.len) break;
            const ks = i + 1;
            var ke = ks;
            while (ke < content.len and content[ke] != '"') : (ke += 1) {
                if (content[ke] == '\\' and ke + 1 < content.len) ke += 1;
            }
            if (ke >= content.len) break;
            const key = content[ks..ke];

            // Skip ":" and whitespace.
            i = ke + 1;
            while (i < content.len and (content[i] == ' ' or content[i] == ':' or content[i] == '\t' or content[i] == '\n' or content[i] == '\r')) : (i += 1) {}
            const start = i;
            while (i < content.len and content[i] >= '0' and content[i] <= '9') : (i += 1) {}
            if (i <= start) continue;
            const offset = std.fmt.parseInt(u64, content[start..i], 10) catch continue;

            const owned_key = self.allocator.dupe(u8, key) catch continue;
            self.map.put(owned_key, offset) catch self.allocator.free(owned_key);
        }
    }

    pub fn get(self: *Positions, path: []const u8) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.map.get(path);
    }

    pub fn set(self: *Positions, path: []const u8, offset: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const gop = self.map.getOrPut(path) catch return;
        if (!gop.found_existing) {
            const dup = self.allocator.dupe(u8, path) catch {
                _ = self.map.remove(path);
                return;
            };
            gop.key_ptr.* = dup;
        }
        gop.value_ptr.* = offset;
    }

    /// Atomically persist the current map to disk.
    pub fn save(self: *Positions) void {
        if (!self.enabled()) return;
        self.mutex.lock();
        defer self.mutex.unlock();

        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        buf.append('{') catch return;

        var first = true;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (!first) buf.append(',') catch return;
            first = false;
            buf.append('"') catch return;
            for (kv.key_ptr.*) |c| {
                if (c == '"' or c == '\\') buf.append('\\') catch return;
                buf.append(c) catch return;
            }
            buf.appendSlice("\":") catch return;
            var num_buf: [32]u8 = undefined;
            const num = std.fmt.bufPrint(&num_buf, "{d}", .{kv.value_ptr.*}) catch return;
            buf.appendSlice(num) catch return;
        }
        buf.append('}') catch return;

        // Atomic write: write to .tmp then rename.
        var tmp_buf: [1024]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{self.path}) catch return;
        const dir = std.fs.path.dirname(self.path);
        if (dir) |d| std.fs.cwd().makePath(d) catch {};

        const tmp = std.fs.cwd().createFile(tmp_path, .{ .truncate = true }) catch return;
        defer tmp.close();
        _ = tmp.writeAll(buf.items) catch return;

        std.fs.cwd().rename(tmp_path, self.path) catch {};
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Positions roundtrip" {
    const allocator = std.testing.allocator;
    const path = "amatiz_test_positions.json";
    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    {
        var pos = try Positions.init(allocator, path);
        defer pos.deinit();
        pos.set("./logs/a.log", 12345);
        pos.set("./logs/b.log", 67890);
        pos.save();
    }

    var pos2 = try Positions.init(allocator, path);
    defer pos2.deinit();
    pos2.load();
    try std.testing.expectEqual(@as(?u64, 12345), pos2.get("./logs/a.log"));
    try std.testing.expectEqual(@as(?u64, 67890), pos2.get("./logs/b.log"));
    try std.testing.expectEqual(@as(?u64, null), pos2.get("./logs/missing.log"));
}

test "Positions disabled when path is empty" {
    const allocator = std.testing.allocator;
    var pos = try Positions.init(allocator, "");
    defer pos.deinit();
    try std.testing.expect(!pos.enabled());
    pos.set("/x", 1);
    pos.save(); // no-op
}

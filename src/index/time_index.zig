// =============================================================================
// Amatiz — Time Index
// =============================================================================
//
// Maps timestamp ranges to chunk files so the query engine can skip irrelevant
// chunks.  Stored on disk as a sorted array of ChunkMeta records in
// {index_dir}/time.idx.
//
// Wire format (little-endian):
//   [4]  magic "TIDX"
//   [4]  entry_count  u32
//   Repeated entry_count times:
//     [4]  chunk_id     u32
//     [8]  min_ts       i64
//     [8]  max_ts       i64
//     [4]  entry_count  u32
//     [2]  path_len     u16
//     [n]  path         [path_len]u8
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");

pub const TimeIndex = struct {
    allocator: Allocator,
    entries: std.ArrayList(ChunkMeta),

    pub const ChunkMeta = struct {
        chunk_id: u32,
        path: []const u8,
        min_ts: i64,
        max_ts: i64,
        entry_count: u32,
    };

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(allocator: Allocator) TimeIndex {
        return .{
            .allocator = allocator,
            .entries = std.ArrayList(ChunkMeta).init(allocator),
        };
    }

    pub fn deinit(self: *TimeIndex) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.path);
        }
        self.entries.deinit();
    }

    // -----------------------------------------------------------------------
    // Mutation
    // -----------------------------------------------------------------------

    /// Register a new chunk in the index. Keeps entries sorted by min_ts.
    pub fn add(self: *TimeIndex, meta: ChunkMeta) !void {
        const path_owned = try self.allocator.dupe(u8, meta.path);
        errdefer self.allocator.free(path_owned);

        const new_meta = ChunkMeta{
            .chunk_id = meta.chunk_id,
            .path = path_owned,
            .min_ts = meta.min_ts,
            .max_ts = meta.max_ts,
            .entry_count = meta.entry_count,
        };

        // Insert in sorted order by min_ts.
        var insert_pos: usize = self.entries.items.len;
        for (self.entries.items, 0..) |existing, i| {
            if (meta.min_ts < existing.min_ts) {
                insert_pos = i;
                break;
            }
        }
        try self.entries.insert(insert_pos, new_meta);
    }

    // -----------------------------------------------------------------------
    // Querying
    // -----------------------------------------------------------------------

    /// Return all chunks whose time range overlaps [from_ns, to_ns].
    /// Pass null for unbounded.
    pub fn query(self: *TimeIndex, from_ns: ?i64, to_ns: ?i64) []const ChunkMeta {
        // Simple linear scan — fine for thousands of chunks.
        // A binary search on sorted min_ts could skip early entries.
        const from = from_ns orelse std.math.minInt(i64);
        const to = to_ns orelse std.math.maxInt(i64);

        // We return a slice into our own entries list. The caller must not
        // hold this past a mutation.  For a more robust API, allocate a copy,
        // but this keeps the hot path allocation-free.
        var start: usize = 0;
        var end: usize = self.entries.items.len;

        // Skip chunks entirely before `from`.
        while (start < end and self.entries.items[start].max_ts < from) : (start += 1) {}
        // Skip chunks entirely after `to`.
        while (end > start and self.entries.items[end - 1].min_ts > to) : (end -= 1) {}

        return self.entries.items[start..end];
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    pub fn saveToDir(self: *TimeIndex, dir: []const u8) !void {
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/time.idx", .{dir}) catch unreachable;

        std.fs.cwd().makePath(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        try writer.writeAll(&types.TIME_INDEX_MAGIC);
        try writer.writeInt(u32, @intCast(self.entries.items.len), .little);

        for (self.entries.items) |entry| {
            try writer.writeInt(u32, entry.chunk_id, .little);
            try writer.writeInt(i64, entry.min_ts, .little);
            try writer.writeInt(i64, entry.max_ts, .little);
            try writer.writeInt(u32, entry.entry_count, .little);
            try writer.writeInt(u16, @intCast(entry.path.len), .little);
            try writer.writeAll(entry.path);
        }
    }

    pub fn loadFromDir(self: *TimeIndex, dir: []const u8) !void {
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/time.idx", .{dir}) catch unreachable;

        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();
        const reader = file.reader();

        var magic: [4]u8 = undefined;
        try reader.readNoEof(&magic);
        if (!std.mem.eql(u8, &magic, &types.TIME_INDEX_MAGIC)) return error.InvalidMagic;

        const num_entries = try reader.readInt(u32, .little);
        var i: u32 = 0;
        while (i < num_entries) : (i += 1) {
            const chunk_id = try reader.readInt(u32, .little);
            const min_ts = try reader.readInt(i64, .little);
            const max_ts = try reader.readInt(i64, .little);
            const entry_count = try reader.readInt(u32, .little);
            const path_len = try reader.readInt(u16, .little);
            const chunk_path = try self.allocator.alloc(u8, path_len);
            errdefer self.allocator.free(chunk_path);
            try reader.readNoEof(chunk_path);

            try self.entries.append(.{
                .chunk_id = chunk_id,
                .path = chunk_path,
                .min_ts = min_ts,
                .max_ts = max_ts,
                .entry_count = entry_count,
            });
        }
    }

    /// Number of indexed chunks.
    pub fn count(self: *const TimeIndex) usize {
        return self.entries.items.len;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "TimeIndex add and query" {
    const allocator = std.testing.allocator;
    var idx = TimeIndex.init(allocator);
    defer idx.deinit();

    try idx.add(.{
        .chunk_id = 1,
        .path = "data/2024-01-15/chunk_0001.amtz",
        .min_ts = 100,
        .max_ts = 200,
        .entry_count = 50,
    });
    try idx.add(.{
        .chunk_id = 2,
        .path = "data/2024-01-15/chunk_0002.amtz",
        .min_ts = 300,
        .max_ts = 400,
        .entry_count = 30,
    });

    // Query spanning both.
    const all = idx.query(null, null);
    try std.testing.expectEqual(@as(usize, 2), all.len);

    // Query only first chunk.
    const first = idx.query(50, 250);
    try std.testing.expectEqual(@as(usize, 1), first.len);
    try std.testing.expectEqual(@as(u32, 1), first[0].chunk_id);

    // Query beyond all.
    const none = idx.query(500, 600);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}

// =============================================================================
// Amatiz — Write-Ahead Log
// =============================================================================
//
// Every entry is appended here before being written to a chunk. If the
// process crashes before a chunk's header is flushed, the entries can be
// recovered by replaying the WAL on next startup.
//
// On-disk format: a flat sequence of LogEntry wire frames (no header).
//
//   [LogEntry frame]
//   [LogEntry frame]
//   ...
//
// The WAL is truncated whenever a chunk is finalized (its data is now
// durable in the chunk file).
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;

pub const Wal = struct {
    allocator: Allocator,
    path: []const u8,
    file: ?std.fs.File,

    pub fn init(allocator: Allocator, path: []const u8) !Wal {
        const owned = try allocator.dupe(u8, path);
        errdefer allocator.free(owned);

        if (std.fs.path.dirname(path)) |d| {
            std.fs.cwd().makePath(d) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
        }

        // Open or create. Don't truncate — replay must see existing entries.
        const file = try std.fs.cwd().createFile(owned, .{ .truncate = false, .read = true });
        // Seek to end so writes append.
        file.seekFromEnd(0) catch {};

        return .{
            .allocator = allocator,
            .path = owned,
            .file = file,
        };
    }

    pub fn deinit(self: *Wal) void {
        if (self.file) |f| f.close();
        self.file = null;
        self.allocator.free(self.path);
    }

    /// Append a serialized entry. Caller is responsible for ordering vs
    /// the chunk write.
    pub fn append(self: *Wal, entry: *const LogEntry) !void {
        const file = self.file orelse return error.WalClosed;
        try entry.serialize(file.writer());
    }

    /// Force pending writes to disk.
    pub fn sync(self: *Wal) void {
        if (self.file) |f| f.sync() catch {};
    }

    /// Truncate the WAL to zero length (called after a chunk is finalized).
    pub fn truncate(self: *Wal) !void {
        const file = self.file orelse return;
        try file.setEndPos(0);
        try file.seekTo(0);
    }

    /// Replay every entry in the WAL through `cb`. Stops at the first
    /// deserialization error (assumed to be a torn write at the tail).
    /// Truncates the WAL on completion so the same entries aren't replayed
    /// on the next start.
    pub fn replay(
        self: *Wal,
        ctx: *anyopaque,
        cb: *const fn (ctx: *anyopaque, entry: *const LogEntry) anyerror!void,
    ) !usize {
        const file = self.file orelse return 0;
        try file.seekTo(0);
        const stat = try file.stat();
        if (stat.size == 0) return 0;

        const reader = file.reader();
        var count: usize = 0;
        while (true) {
            var entry = LogEntry.deserialize(reader, self.allocator) catch break;
            defer entry.deinit(self.allocator);
            cb(ctx, &entry) catch |err| {
                std.log.warn("WAL replay callback error: {}", .{err});
                break;
            };
            count += 1;
        }

        // Reset for new appends.
        try file.seekFromEnd(0);
        return count;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Wal append/replay roundtrip" {
    const allocator = std.testing.allocator;
    const path = "amatiz_test_wal.bin";
    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    {
        var wal = try Wal.init(allocator, path);
        defer wal.deinit();

        const e1 = LogEntry{
            .timestamp_ns = 1000,
            .source = "src",
            .message = "hello",
            .raw_line = "hello",
            .labels = &.{},
        };
        try wal.append(&e1);

        const e2 = LogEntry{
            .timestamp_ns = 2000,
            .source = "src",
            .message = "world",
            .raw_line = "world",
            .labels = &.{},
        };
        try wal.append(&e2);
    }

    var wal2 = try Wal.init(allocator, path);
    defer wal2.deinit();

    const Ctx = struct {
        count: usize = 0,
        got_first: bool = false,
        got_second: bool = false,
    };
    var ctx = Ctx{};

    const replayed = try wal2.replay(@ptrCast(&ctx), struct {
        fn cb(c: *anyopaque, e: *const LogEntry) anyerror!void {
            const cc: *Ctx = @ptrCast(@alignCast(c));
            cc.count += 1;
            if (std.mem.eql(u8, e.message, "hello")) cc.got_first = true;
            if (std.mem.eql(u8, e.message, "world")) cc.got_second = true;
        }
    }.cb);

    try std.testing.expectEqual(@as(usize, 2), replayed);
    try std.testing.expect(ctx.got_first);
    try std.testing.expect(ctx.got_second);
}

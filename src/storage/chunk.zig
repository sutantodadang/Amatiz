// =============================================================================
// Amatiz — Chunk Manager
// =============================================================================
//
// A "chunk" is a single append-only file that stores serialised LogEntry
// records prefixed by a 32-byte header.  Once it reaches the configured size
// limit the storage engine finalises it (optionally compressing the payload)
// and opens a new chunk.
//
// On-disk layout:
//   [32 bytes] ChunkHeader  (see types.zig)
//   [N  bytes] Concatenated LogEntry wire frames
//
// Reads can happen concurrently with writes because we only ever append and
// the header is updated atomically after each flush (seek-back + write).
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;
const timestamp = @import("../utils/timestamp.zig");

pub const Chunk = struct {
    /// Absolute path of this chunk file.
    path: []const u8,
    /// Underlying file handle (null when the chunk is read-only / closed).
    file: ?std.fs.File,
    /// Number of entries written.
    entry_count: u32,
    /// Bytes of entry data written (excludes header). For compressed
    /// finalized chunks this is the compressed payload size.
    data_size: u64,
    /// Minimum timestamp seen.
    min_ts: i64,
    /// Maximum timestamp seen.
    max_ts: i64,
    /// Whether this chunk has been finalized (immutable).
    finalized: bool,
    /// Whether compression is applied on finalization.
    compressed: bool,
    /// Allocator used for path and any temp buffers.
    allocator: Allocator,
    /// Decompressed payload, only set for read-only compressed chunks.
    decompressed_buf: ?[]u8 = null,

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Create a brand-new chunk file, writing the initial header.
    pub fn create(allocator: Allocator, dir_path: []const u8, id: u32, compressed: bool) !Chunk {
        // Ensure directory exists.
        std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Build file name: chunk_NNNN.amtz
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "chunk_{d:0>4}.amtz", .{id}) catch unreachable;

        var path_buf: [512]u8 = undefined;
        const full_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, name }) catch unreachable;

        const path_owned = try allocator.dupe(u8, full_path);
        errdefer allocator.free(path_owned);

        const file = try std.fs.cwd().createFile(path_owned, .{
            .truncate = true,
            .read = true,
        });
        errdefer file.close();

        // Write initial header (all zeros for counts — updated on flush).
        const writer = file.writer();
        try types.writeChunkHeader(writer, .{}, 0, 0, 0);

        return .{
            .path = path_owned,
            .file = file,
            .entry_count = 0,
            .data_size = 0,
            .min_ts = std.math.maxInt(i64),
            .max_ts = std.math.minInt(i64),
            .finalized = false,
            .compressed = compressed,
            .allocator = allocator,
        };
    }

    /// Open an existing chunk in read-only mode (for querying).
    pub fn openReadOnly(allocator: Allocator, path: []const u8) !Chunk {
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();

        const header = try types.readChunkHeader(file.reader());

        const path_owned = try allocator.dupe(u8, path);
        errdefer allocator.free(path_owned);

        var decompressed: ?[]u8 = null;
        if (header.flags.compressed and header.data_size > 0) {
            // Read entire compressed payload then inflate into memory.
            const compressed_buf = try allocator.alloc(u8, header.data_size);
            defer allocator.free(compressed_buf);
            try file.reader().readNoEof(compressed_buf);

            var in_fbs = std.io.fixedBufferStream(@as([]const u8, compressed_buf));
            var out_buf = std.ArrayList(u8).init(allocator);
            errdefer out_buf.deinit();
            try std.compress.gzip.decompress(in_fbs.reader(), out_buf.writer());
            decompressed = try out_buf.toOwnedSlice();
        }

        return .{
            .path = path_owned,
            .file = file,
            .entry_count = header.entry_count,
            .data_size = header.data_size,
            .min_ts = std.math.maxInt(i64),
            .max_ts = std.math.minInt(i64),
            .finalized = header.flags.finalized,
            .compressed = header.flags.compressed,
            .allocator = allocator,
            .decompressed_buf = decompressed,
        };
    }

    // -----------------------------------------------------------------------
    // Writing
    // -----------------------------------------------------------------------

    /// Append a single log entry to this chunk. Returns number of bytes written.
    pub fn append(self: *Chunk, entry: *const LogEntry) !usize {
        if (self.finalized) return error.ChunkFinalized;
        const file = self.file orelse return error.ChunkNotOpen;

        const writer = file.writer();
        const size_before = self.data_size;
        try entry.serialize(writer);
        const written = entry.serializedSize();

        self.entry_count += 1;
        self.data_size += written;

        // Track timestamp range.
        if (entry.timestamp_ns < self.min_ts) self.min_ts = entry.timestamp_ns;
        if (entry.timestamp_ns > self.max_ts) self.max_ts = entry.timestamp_ns;

        _ = size_before;
        return written;
    }

    /// Append a batch of entries. Returns total bytes written.
    pub fn appendBatch(self: *Chunk, entries: []const LogEntry) !usize {
        var total: usize = 0;
        for (entries) |*entry| {
            total += try self.append(entry);
        }
        return total;
    }

    /// Flush the header to disk with current counts.
    pub fn flushHeader(self: *Chunk) !void {
        const file = self.file orelse return;
        try file.seekTo(0);
        try types.writeChunkHeader(
            file.writer(),
            .{ .compressed = self.compressed, .finalized = self.finalized },
            self.entry_count,
            self.data_size,
            0,
        );
        // Seek back to end for further appends.
        try file.seekFromEnd(0);
    }

    // -----------------------------------------------------------------------
    // Finalization
    // -----------------------------------------------------------------------

    /// Mark this chunk as immutable — no more writes allowed.
    /// Optionally compresses the payload.
    pub fn finalize(self: *Chunk) !void {
        if (self.finalized) return;

        if (self.compressed and self.data_size > 0) {
            try self.compressInPlace();
        }

        self.finalized = true;
        try self.flushHeader();

        // Close the file — from here on, the chunk is read-only.
        if (self.file) |f| {
            f.close();
            self.file = null;
        }
    }

    /// Read the existing payload, gzip-compress it, and rewrite the file.
    /// Updates `self.data_size` to the compressed size.
    fn compressInPlace(self: *Chunk) !void {
        const file = self.file orelse return error.ChunkNotOpen;

        // Read the existing uncompressed payload.
        try file.seekTo(types.CHUNK_HEADER_SIZE);
        const payload = try self.allocator.alloc(u8, self.data_size);
        defer self.allocator.free(payload);
        try file.reader().readNoEof(payload);

        // Compress to a memory buffer.
        var in_fbs = std.io.fixedBufferStream(@as([]const u8, payload));
        var out_buf = std.ArrayList(u8).init(self.allocator);
        defer out_buf.deinit();
        try std.compress.gzip.compress(in_fbs.reader(), out_buf.writer(), .{});

        // Rewrite payload region with compressed bytes.
        try file.seekTo(types.CHUNK_HEADER_SIZE);
        try file.writer().writeAll(out_buf.items);
        try file.setEndPos(types.CHUNK_HEADER_SIZE + out_buf.items.len);

        self.data_size = out_buf.items.len;
    }

    // -----------------------------------------------------------------------
    // Reading
    // -----------------------------------------------------------------------

    /// Iterator over entries in this chunk.
    pub const EntryIterator = union(enum) {
        file: struct {
            reader: std.fs.File.Reader,
            remaining: u32,
            allocator: Allocator,
        },
        mem: struct {
            fbs: std.io.FixedBufferStream([]const u8),
            remaining: u32,
            allocator: Allocator,
        },

        pub fn next(self: *EntryIterator) !?LogEntry {
            switch (self.*) {
                .file => |*f| {
                    if (f.remaining == 0) return null;
                    f.remaining -= 1;
                    return try LogEntry.deserialize(f.reader, f.allocator);
                },
                .mem => |*m| {
                    if (m.remaining == 0) return null;
                    m.remaining -= 1;
                    return try LogEntry.deserialize(m.fbs.reader(), m.allocator);
                },
            }
        }
    };

    /// Open an iterator over all entries. Caller must deinit each returned entry.
    pub fn iterator(self: *Chunk, allocator: Allocator) !EntryIterator {
        if (self.decompressed_buf) |buf| {
            return .{ .mem = .{
                .fbs = std.io.fixedBufferStream(@as([]const u8, buf)),
                .remaining = self.entry_count,
                .allocator = allocator,
            } };
        }
        const file = self.file orelse return error.ChunkNotOpen;
        // Seek past the header.
        try file.seekTo(types.CHUNK_HEADER_SIZE);
        return .{ .file = .{
            .reader = file.reader(),
            .remaining = self.entry_count,
            .allocator = allocator,
        } };
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    pub fn close(self: *Chunk) void {
        if (self.file) |f| {
            f.close();
            self.file = null;
        }
    }

    pub fn deinit(self: *Chunk) void {
        self.close();
        self.allocator.free(self.path);
        if (self.decompressed_buf) |b| {
            self.allocator.free(b);
            self.decompressed_buf = null;
        }
    }

    /// Total on-disk size: header + data.
    pub fn diskSize(self: *const Chunk) u64 {
        return types.CHUNK_HEADER_SIZE + self.data_size;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Chunk create, append, read round-trip" {
    const allocator = std.testing.allocator;
    const tmp_dir = "/tmp/amatiz_test_chunk";

    // Clean up from previous runs.
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var chunk = try Chunk.create(allocator, tmp_dir, 1, false);
    defer chunk.deinit();

    const entry = LogEntry{
        .timestamp_ns = 1_700_000_000_000_000_000,
        .source = "test.log",
        .message = "hello world",
        .raw_line = "hello world",
        .labels = &.{},
    };

    _ = try chunk.append(&entry);
    _ = try chunk.append(&entry);
    try chunk.flushHeader();

    try std.testing.expectEqual(@as(u32, 2), chunk.entry_count);
}

test "Chunk gzip compression round-trip" {
    const allocator = std.testing.allocator;
    const tmp_dir = "amatiz_test_chunk_gz";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var chunk = try Chunk.create(allocator, tmp_dir, 7, true);

    const e = LogEntry{
        .timestamp_ns = 42,
        .source = "compress.log",
        .message = "the quick brown fox jumps over the lazy dog",
        .raw_line = "the quick brown fox jumps over the lazy dog",
        .labels = &.{},
    };
    inline for (0..16) |_| {
        _ = try chunk.append(&e);
    }
    try chunk.flushHeader();
    const path_dup = try allocator.dupe(u8, chunk.path);
    defer allocator.free(path_dup);

    try chunk.finalize();
    chunk.deinit();

    var ro = try Chunk.openReadOnly(allocator, path_dup);
    defer ro.deinit();
    try std.testing.expect(ro.compressed);
    try std.testing.expectEqual(@as(u32, 16), ro.entry_count);

    var it = try ro.iterator(allocator);
    var seen: u32 = 0;
    while (try it.next()) |*entry_ptr| {
        var entry = entry_ptr.*;
        defer entry.deinit(allocator);
        try std.testing.expect(std.mem.eql(u8, entry.message, "the quick brown fox jumps over the lazy dog"));
        seen += 1;
    }
    try std.testing.expectEqual(@as(u32, 16), seen);
}

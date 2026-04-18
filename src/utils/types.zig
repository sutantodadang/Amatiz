// =============================================================================
// Amatiz — Shared Types
// =============================================================================
//
// Central data types used across all modules. Keeping them in one place avoids
// circular imports and makes the binary wire format the single source of truth.
//
// Wire format (little-endian):
//   [8]  timestamp_ns   i64
//   [2]  source_len     u16
//   [n]  source         [source_len]u8
//   [2]  labels_len     u16
//   [n]  labels         [labels_len]u8   "key=val,key=val"
//   [4]  message_len    u32
//   [n]  message        [message_len]u8
//
// Entry header overhead: 16 bytes fixed + variable payload.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// Log entry
// ---------------------------------------------------------------------------

/// A single log entry flowing through the system.
pub const LogEntry = struct {
    /// Nanoseconds since Unix epoch.
    timestamp_ns: i64,
    /// Source identifier — typically the originating file path.
    source: []const u8,
    /// Processed message content.
    message: []const u8,
    /// Original raw line from the source (may alias `message`).
    raw_line: []const u8,
    /// Key-value labels attached to this entry for filtering / grouping.
    labels: []const Label,
    /// Backing memory for deserialized label key/value slices. Null when labels
    /// are constructed in-memory (not deserialized).
    _labels_backing: ?[]const u8 = null,

    pub const Label = struct {
        key: []const u8,
        value: []const u8,
    };

    // -- Serialization -------------------------------------------------------

    /// Write the entry to `writer` in the binary wire format.
    pub fn serialize(self: *const LogEntry, writer: anytype) !void {
        try writer.writeInt(i64, self.timestamp_ns, .little);

        // Source
        try writer.writeInt(u16, @intCast(self.source.len), .little);
        try writer.writeAll(self.source);

        // Labels — encoded as "key=value,key=value"
        var labels_buf: [4096]u8 = undefined;
        var labels_len: u16 = 0;
        if (self.labels.len > 0) {
            var fbs = std.io.fixedBufferStream(&labels_buf);
            const lw = fbs.writer();
            for (self.labels, 0..) |label, i| {
                if (i > 0) lw.writeByte(',') catch unreachable;
                lw.writeAll(label.key) catch unreachable;
                lw.writeByte('=') catch unreachable;
                lw.writeAll(label.value) catch unreachable;
            }
            labels_len = @intCast(fbs.pos);
        }
        try writer.writeInt(u16, labels_len, .little);
        if (labels_len > 0) try writer.writeAll(labels_buf[0..labels_len]);

        // Message
        try writer.writeInt(u32, @intCast(self.message.len), .little);
        try writer.writeAll(self.message);
    }

    /// Deserialize one entry from `reader`. Caller owns all returned memory.
    pub fn deserialize(reader: anytype, allocator: Allocator) !LogEntry {
        const timestamp_ns = try reader.readInt(i64, .little);

        const source_len = try reader.readInt(u16, .little);
        const source = try allocator.alloc(u8, source_len);
        errdefer allocator.free(source);
        if (source_len > 0) try reader.readNoEof(source);

        const labels_len = try reader.readInt(u16, .little);
        var labels_raw: []u8 = &.{};
        if (labels_len > 0) {
            labels_raw = try allocator.alloc(u8, labels_len);
            errdefer allocator.free(labels_raw);
            try reader.readNoEof(labels_raw);
        }

        const message_len = try reader.readInt(u32, .little);
        const message = try allocator.alloc(u8, message_len);
        errdefer allocator.free(message);
        if (message_len > 0) try reader.readNoEof(message);

        // Parse labels from the CSV-like encoding.
        var labels_list = std.ArrayList(Label).init(allocator);
        errdefer labels_list.deinit();
        if (labels_len > 0) {
            var iter = std.mem.splitScalar(u8, labels_raw, ',');
            while (iter.next()) |pair| {
                if (std.mem.indexOfScalar(u8, pair, '=')) |eq| {
                    try labels_list.append(.{
                        .key = pair[0..eq],
                        .value = pair[eq + 1 ..],
                    });
                }
            }
        }

        return .{
            .timestamp_ns = timestamp_ns,
            .source = source,
            .message = message,
            .raw_line = message,
            .labels = try labels_list.toOwnedSlice(),
            ._labels_backing = if (labels_len > 0) labels_raw else null,
        };
    }

    /// Number of bytes this entry occupies on disk.
    pub fn serializedSize(self: *const LogEntry) usize {
        var lbl_size: usize = 0;
        for (self.labels, 0..) |label, i| {
            if (i > 0) lbl_size += 1;
            lbl_size += label.key.len + 1 + label.value.len;
        }
        return 8 + 2 + self.source.len + 2 + lbl_size + 4 + self.message.len;
    }

    /// Release all allocator-owned memory associated with a deserialized entry.
    pub fn deinit(self: *LogEntry, allocator: Allocator) void {
        allocator.free(self.source);
        if (self._labels_backing) |backing| {
            allocator.free(backing);
        }
        if (self.labels.len > 0) {
            allocator.free(self.labels);
        }
        allocator.free(self.message);
    }
};

// ---------------------------------------------------------------------------
// File-format constants
// ---------------------------------------------------------------------------

/// Chunk file magic bytes.
pub const CHUNK_MAGIC = [4]u8{ 'A', 'M', 'T', 'Z' };
/// Time index magic bytes.
pub const TIME_INDEX_MAGIC = [4]u8{ 'T', 'I', 'D', 'X' };
/// Keyword index magic bytes.
pub const KEYWORD_INDEX_MAGIC = [4]u8{ 'K', 'I', 'D', 'X' };
/// Current format version.
pub const FORMAT_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// Chunk header — 32 bytes on disk
// ---------------------------------------------------------------------------
// Written with explicit serialization (not memory-mapped struct) for
// portability across endianness and alignment.
//
// Layout:
//   [4]  magic
//   [2]  version
//   [2]  flags          bit 0 = compressed, bit 1 = finalized
//   [4]  entry_count
//   [4]  reserved
//   [8]  data_size      uncompressed payload bytes
//   [8]  compressed_size (0 when not compressed)
// ---------------------------------------------------------------------------

pub const CHUNK_HEADER_SIZE: usize = 32;

pub const ChunkFlags = struct {
    compressed: bool = false,
    finalized: bool = false,

    pub fn toBits(self: ChunkFlags) u16 {
        var bits: u16 = 0;
        if (self.compressed) bits |= 1;
        if (self.finalized) bits |= 2;
        return bits;
    }

    pub fn fromBits(bits: u16) ChunkFlags {
        return .{
            .compressed = (bits & 1) != 0,
            .finalized = (bits & 2) != 0,
        };
    }
};

/// Write a 32-byte chunk header.
pub fn writeChunkHeader(
    writer: anytype,
    flags: ChunkFlags,
    entry_count: u32,
    data_size: u64,
    compressed_size: u64,
) !void {
    try writer.writeAll(&CHUNK_MAGIC);
    try writer.writeInt(u16, FORMAT_VERSION, .little);
    try writer.writeInt(u16, flags.toBits(), .little);
    try writer.writeInt(u32, entry_count, .little);
    try writer.writeInt(u32, 0, .little); // reserved
    try writer.writeInt(u64, data_size, .little);
    try writer.writeInt(u64, compressed_size, .little);
}

/// Parsed chunk header.
pub const ChunkHeaderData = struct {
    version: u16,
    flags: ChunkFlags,
    entry_count: u32,
    data_size: u64,
    compressed_size: u64,
};

/// Read and validate a 32-byte chunk header.
pub fn readChunkHeader(reader: anytype) !ChunkHeaderData {
    var magic: [4]u8 = undefined;
    try reader.readNoEof(&magic);
    if (!std.mem.eql(u8, &magic, &CHUNK_MAGIC)) return error.InvalidMagic;

    const version = try reader.readInt(u16, .little);
    if (version != FORMAT_VERSION) return error.UnsupportedVersion;

    const flags_bits = try reader.readInt(u16, .little);
    const entry_count = try reader.readInt(u32, .little);
    _ = try reader.readInt(u32, .little); // reserved
    const data_size = try reader.readInt(u64, .little);
    const compressed_size = try reader.readInt(u64, .little);

    return .{
        .version = version,
        .flags = ChunkFlags.fromBits(flags_bits),
        .entry_count = entry_count,
        .data_size = data_size,
        .compressed_size = compressed_size,
    };
}

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Criteria for searching stored logs.
pub const QueryParams = struct {
    /// Optional keyword substring match (case-insensitive).
    keyword: ?[]const u8 = null,
    /// Start of time range (inclusive, nanos since epoch). null = unbounded.
    from_ns: ?i64 = null,
    /// End of time range (inclusive, nanos since epoch). null = unbounded.
    to_ns: ?i64 = null,
    /// Maximum number of results to return. 0 = unlimited.
    limit: usize = 1000,
};

/// A single query result row.
pub const QueryResult = struct {
    entry: LogEntry,
    chunk_path: []const u8,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "LogEntry round-trip serialization" {
    const allocator = std.testing.allocator;

    const labels = [_]LogEntry.Label{
        .{ .key = "env", .value = "prod" },
        .{ .key = "host", .value = "web-01" },
    };

    const original = LogEntry{
        .timestamp_ns = 1_700_000_000_000_000_000,
        .source = "/var/log/app.log",
        .message = "connection refused",
        .raw_line = "connection refused",
        .labels = &labels,
    };

    // Serialize
    var buf: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try original.serialize(fbs.writer());

    // Deserialize
    var read_stream = std.io.fixedBufferStream(fbs.getWritten());
    var decoded = try LogEntry.deserialize(read_stream.reader(), allocator);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(original.timestamp_ns, decoded.timestamp_ns);
    try std.testing.expectEqualStrings(original.source, decoded.source);
    try std.testing.expectEqualStrings(original.message, decoded.message);
    try std.testing.expectEqual(@as(usize, 2), decoded.labels.len);
}

test "ChunkFlags round-trip" {
    const flags = ChunkFlags{ .compressed = true, .finalized = false };
    const bits = flags.toBits();
    const recovered = ChunkFlags.fromBits(bits);
    try std.testing.expect(recovered.compressed);
    try std.testing.expect(!recovered.finalized);
}

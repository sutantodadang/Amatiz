// =============================================================================
// Amatiz — Keyword Index (Inverted Index)
// =============================================================================
//
// Maps words (case-folded) to the set of chunk IDs that contain them.
// Used by the query engine to skip chunks that cannot match a keyword search.
//
// Wire format (little-endian):
//   [4]  magic "KIDX"
//   [4]  term_count  u32
//   Repeated term_count times:
//     [2]  term_len       u16
//     [n]  term           [term_len]u8
//     [4]  posting_count  u32
//     Repeated posting_count times:
//       [4]  chunk_id  u32
//
// Terms are stored lowercase. Tokenisation splits on non-alphanumeric chars.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");

pub const KeywordIndex = struct {
    allocator: Allocator,
    /// term → list of chunk IDs.
    index: std.StringHashMap(std.ArrayList(u32)),

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(allocator: Allocator) KeywordIndex {
        return .{
            .allocator = allocator,
            .index = std.StringHashMap(std.ArrayList(u32)).init(allocator),
        };
    }

    pub fn deinit(self: *KeywordIndex) void {
        var it = self.index.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.index.deinit();
    }

    // -----------------------------------------------------------------------
    // Indexing
    // -----------------------------------------------------------------------

    /// Tokenize the message and record each word against the given chunk ID.
    pub fn indexMessage(self: *KeywordIndex, message: []const u8, chunk_id: u32) void {
        var start: usize = 0;
        var i: usize = 0;
        while (i < message.len) : (i += 1) {
            if (!isTokenChar(message[i])) {
                if (i > start) {
                    self.addTerm(message[start..i], chunk_id);
                }
                start = i + 1;
            }
        }
        // Last token.
        if (i > start) {
            self.addTerm(message[start..i], chunk_id);
        }
    }

    fn addTerm(self: *KeywordIndex, raw_term: []const u8, chunk_id: u32) void {
        if (raw_term.len < 2 or raw_term.len > 128) return; // Skip trivially short/long tokens.

        // Lowercase the term into a stack buffer.
        var lower_buf: [128]u8 = undefined;
        const len = @min(raw_term.len, 128);
        for (raw_term[0..len], 0..) |c, idx| {
            lower_buf[idx] = std.ascii.toLower(c);
        }
        const lower = lower_buf[0..len];

        const gop = self.index.getOrPut(lower) catch return;
        if (!gop.found_existing) {
            // We need to allocate an owned copy of the key.
            const owned = self.allocator.dupe(u8, lower) catch return;
            gop.key_ptr.* = owned;
            gop.value_ptr.* = std.ArrayList(u32).init(self.allocator);
        }

        var postings = gop.value_ptr;

        // Deduplicate: only add if the last entry differs.
        if (postings.items.len == 0 or postings.items[postings.items.len - 1] != chunk_id) {
            postings.append(chunk_id) catch return;
        }
    }

    fn isTokenChar(c: u8) bool {
        return std.ascii.isAlphanumeric(c) or c == '_' or c == '-';
    }

    // -----------------------------------------------------------------------
    // Lookup
    // -----------------------------------------------------------------------

    /// Return chunk IDs containing the given keyword (case-insensitive).
    pub fn lookup(self: *KeywordIndex, keyword: []const u8) []const u32 {
        var lower_buf: [128]u8 = undefined;
        const len = @min(keyword.len, 128);
        for (keyword[0..len], 0..) |c, idx| {
            lower_buf[idx] = std.ascii.toLower(c);
        }

        if (self.index.get(lower_buf[0..len])) |postings| {
            return postings.items;
        }
        return &.{};
    }

    /// Number of unique terms.
    pub fn termCount(self: *const KeywordIndex) usize {
        return self.index.count();
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    pub fn saveToDir(self: *KeywordIndex, dir: []const u8) !void {
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/keywords.idx", .{dir}) catch unreachable;

        std.fs.cwd().makePath(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        const writer = file.writer();

        try writer.writeAll(&types.KEYWORD_INDEX_MAGIC);
        try writer.writeInt(u32, @intCast(self.index.count()), .little);

        var it = self.index.iterator();
        while (it.next()) |entry| {
            const term = entry.key_ptr.*;
            const postings = entry.value_ptr.*.items;

            try writer.writeInt(u16, @intCast(term.len), .little);
            try writer.writeAll(term);
            try writer.writeInt(u32, @intCast(postings.len), .little);
            for (postings) |chunk_id| {
                try writer.writeInt(u32, chunk_id, .little);
            }
        }
    }

    pub fn loadFromDir(self: *KeywordIndex, dir: []const u8) !void {
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/keywords.idx", .{dir}) catch unreachable;

        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();
        const reader = file.reader();

        var magic: [4]u8 = undefined;
        try reader.readNoEof(&magic);
        if (!std.mem.eql(u8, &magic, &types.KEYWORD_INDEX_MAGIC)) return error.InvalidMagic;

        const term_count = try reader.readInt(u32, .little);
        var t: u32 = 0;
        while (t < term_count) : (t += 1) {
            const term_len = try reader.readInt(u16, .little);
            const term = try self.allocator.alloc(u8, term_len);
            errdefer self.allocator.free(term);
            try reader.readNoEof(term);

            const posting_count = try reader.readInt(u32, .little);
            var postings = std.ArrayList(u32).init(self.allocator);
            errdefer postings.deinit();

            var p: u32 = 0;
            while (p < posting_count) : (p += 1) {
                try postings.append(try reader.readInt(u32, .little));
            }

            try self.index.put(term, postings);
        }
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "KeywordIndex tokenize and lookup" {
    const allocator = std.testing.allocator;
    var idx = KeywordIndex.init(allocator);
    defer idx.deinit();

    idx.indexMessage("ERROR: connection refused to database", 1);
    idx.indexMessage("INFO: health check passed", 2);
    idx.indexMessage("ERROR: timeout waiting for response", 3);

    const error_chunks = idx.lookup("error");
    try std.testing.expect(error_chunks.len >= 2);

    const db_chunks = idx.lookup("database");
    try std.testing.expectEqual(@as(usize, 1), db_chunks.len);

    const missing = idx.lookup("nonexistent");
    try std.testing.expectEqual(@as(usize, 0), missing.len);
}

test "KeywordIndex case insensitive" {
    const allocator = std.testing.allocator;
    var idx = KeywordIndex.init(allocator);
    defer idx.deinit();

    idx.indexMessage("WARNING: disk full", 1);

    try std.testing.expect(idx.lookup("warning").len > 0);
    try std.testing.expect(idx.lookup("WARNING").len > 0);
    try std.testing.expect(idx.lookup("Warning").len > 0);
}

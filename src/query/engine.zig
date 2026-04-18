// =============================================================================
// Amatiz — Query Engine
// =============================================================================
//
// Executes time-range and keyword queries against the storage layer.
//
// Query flow:
//   1. Consult time index → candidate chunks overlapping the range.
//   2. If keyword present, consult keyword index → further filter chunks.
//   3. Open each candidate chunk and scan entries sequentially.
//   4. Apply predicate (time range + keyword substring match).
//   5. Collect up to `limit` matching entries.
//
// Chunk scanning uses sequential file I/O. For random-access patterns on
// very large data sets, mmap could be layered in later.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;
const QueryParams = types.QueryParams;
const Chunk = @import("../storage/chunk.zig").Chunk;
const StorageEngine = @import("../storage/engine.zig").StorageEngine;
const TimeIndex = @import("../index/time_index.zig").TimeIndex;
const logql = @import("../loki/logql.zig");

pub const QueryEngine = struct {
    allocator: Allocator,
    storage: *StorageEngine,

    pub fn init(allocator: Allocator, storage: *StorageEngine) QueryEngine {
        return .{
            .allocator = allocator,
            .storage = storage,
        };
    }

    // -----------------------------------------------------------------------
    // Query execution
    // -----------------------------------------------------------------------

    /// Execute a query and return matching log entries.
    /// Caller owns the returned slice and each entry's memory.
    pub fn execute(self: *QueryEngine, params: QueryParams) ![]LogEntry {
        var results = std.ArrayList(LogEntry).init(self.allocator);
        errdefer {
            for (results.items) |*entry| entry.deinit(self.allocator);
            results.deinit();
        }

        const limit = if (params.limit == 0) std.math.maxInt(usize) else params.limit;

        // Step 1: Get candidate chunks from time index.
        const candidates = self.storage.chunksForTimeRange(params.from_ns, params.to_ns);

        // Step 2: Filter by keyword index if applicable.
        var keyword_chunks: ?[]const u32 = null;
        if (params.keyword) |_| {
            keyword_chunks = self.storage.chunksForKeyword(params.keyword.?);
        }

        // Step 3: Scan each candidate chunk.
        for (candidates) |meta| {
            // If keyword index says this chunk doesn't contain the keyword, skip it.
            if (keyword_chunks) |kc| {
                if (!chunkInSet(meta.chunk_id, kc)) continue;
            }

            // Open chunk and iterate entries.
            self.scanChunk(meta.path, params, &results, limit) catch |err| {
                std.log.warn("failed to scan chunk {s}: {}", .{ meta.path, err });
                continue;
            };

            if (results.items.len >= limit) break;
        }

        // If no index-based candidates were found, fall back to full scan.
        if (candidates.len == 0) {
            try self.fullScan(params, &results, limit);
        }

        return results.toOwnedSlice();
    }

    /// Scan a single chunk file for matching entries.
    fn scanChunk(
        self: *QueryEngine,
        path: []const u8,
        params: QueryParams,
        results: *std.ArrayList(LogEntry),
        limit: usize,
    ) !void {
        var chunk = try Chunk.openReadOnly(self.allocator, path);
        defer chunk.deinit();

        var iter = try chunk.iterator(self.allocator);

        while (try iter.next()) |entry| {
            if (results.items.len >= limit) {
                // Free this entry since we won't keep it.
                var e = entry;
                e.deinit(self.allocator);
                break;
            }

            if (matchesQuery(&entry, params)) {
                try results.append(entry);
            } else {
                var e = entry;
                e.deinit(self.allocator);
            }
        }
    }

    /// Fall back: discover all chunk files and scan them.
    fn fullScan(
        self: *QueryEngine,
        params: QueryParams,
        results: *std.ArrayList(LogEntry),
        limit: usize,
    ) !void {
        const days = try self.storage.listDays(self.allocator);
        defer {
            for (days) |d| self.allocator.free(d);
            self.allocator.free(days);
        }

        for (days) |day| {
            // Quick date-level filtering.
            if (params.from_ns != null or params.to_ns != null) {
                if (!dayOverlaps(day, params.from_ns, params.to_ns)) continue;
            }

            const chunks = try self.storage.listChunksInDay(self.allocator, day);
            defer {
                for (chunks) |c| self.allocator.free(c);
                self.allocator.free(chunks);
            }

            for (chunks) |chunk_path| {
                self.scanChunk(chunk_path, params, results, limit) catch continue;
                if (results.items.len >= limit) return;
            }
        }
    }

    /// Free a slice of query results.
    pub fn freeResults(self: *QueryEngine, results: []LogEntry) void {
        for (results) |*entry| {
            entry.deinit(self.allocator);
        }
        self.allocator.free(results);
    }

    // -----------------------------------------------------------------------
    // LogQL execution — applies stream selector + line filters end-to-end.
    // -----------------------------------------------------------------------

    /// Execute a parsed LogQL expression against storage. Caller owns results.
    /// Combines time-range pruning (params.from_ns/to_ns/limit) with the full
    /// LogQL matcher + line-filter evaluation against each candidate entry.
    pub fn executeLogQL(
        self: *QueryEngine,
        expr: *const logql.LogQLExpr,
        params: QueryParams,
    ) ![]LogEntry {
        var results = std.ArrayList(LogEntry).init(self.allocator);
        errdefer {
            for (results.items) |*entry| entry.deinit(self.allocator);
            results.deinit();
        }

        const limit = if (params.limit == 0) std.math.maxInt(usize) else params.limit;
        const candidates = self.storage.chunksForTimeRange(params.from_ns, params.to_ns);

        for (candidates) |meta| {
            self.scanChunkLogQL(meta.path, expr, params, &results, limit) catch |err| {
                std.log.warn("LogQL chunk scan failed {s}: {}", .{ meta.path, err });
                continue;
            };
            if (results.items.len >= limit) break;
        }

        if (candidates.len == 0) {
            try self.fullScanLogQL(expr, params, &results, limit);
        }

        return results.toOwnedSlice();
    }

    fn scanChunkLogQL(
        self: *QueryEngine,
        path: []const u8,
        expr: *const logql.LogQLExpr,
        params: QueryParams,
        results: *std.ArrayList(LogEntry),
        limit: usize,
    ) !void {
        var chunk = try Chunk.openReadOnly(self.allocator, path);
        defer chunk.deinit();

        var iter = try chunk.iterator(self.allocator);

        while (try iter.next()) |entry| {
            if (results.items.len >= limit) {
                var e = entry;
                e.deinit(self.allocator);
                break;
            }

            if (entryMatchesLogQL(&entry, expr, params)) {
                try results.append(entry);
            } else {
                var e = entry;
                e.deinit(self.allocator);
            }
        }
    }

    fn fullScanLogQL(
        self: *QueryEngine,
        expr: *const logql.LogQLExpr,
        params: QueryParams,
        results: *std.ArrayList(LogEntry),
        limit: usize,
    ) !void {
        const days = try self.storage.listDays(self.allocator);
        defer {
            for (days) |d| self.allocator.free(d);
            self.allocator.free(days);
        }

        for (days) |day| {
            if (params.from_ns != null or params.to_ns != null) {
                if (!dayOverlaps(day, params.from_ns, params.to_ns)) continue;
            }

            const chunks = try self.storage.listChunksInDay(self.allocator, day);
            defer {
                for (chunks) |c| self.allocator.free(c);
                self.allocator.free(chunks);
            }

            for (chunks) |chunk_path| {
                self.scanChunkLogQL(chunk_path, expr, params, results, limit) catch continue;
                if (results.items.len >= limit) return;
            }
        }
    }

    /// Execute a metric LogQL query (rate / count_over_time, optionally summed
    /// by a label set) and return the result series.
    /// `step_seconds` is the resolution of returned samples; default 60s.
    pub fn executeMetric(
        self: *QueryEngine,
        expr: *const logql.LogQLExpr,
        params: QueryParams,
        step_seconds: u64,
    ) !MetricResult {
        std.debug.assert(expr.kind == .metric);

        const step = if (step_seconds == 0) 60 else step_seconds;
        const from_ns = params.from_ns orelse 0;
        const to_ns = params.to_ns orelse std.math.maxInt(i64);
        if (to_ns <= from_ns) return MetricResult{ .step_seconds = step, .series = &.{} };

        // Collect matching entries (no limit; metric queries scan everything).
        var matched = std.ArrayList(LogEntry).init(self.allocator);
        defer {
            for (matched.items) |*e| e.deinit(self.allocator);
            matched.deinit();
        }

        const inner_params = QueryParams{
            .from_ns = params.from_ns,
            .to_ns = params.to_ns,
            .keyword = null,
            .limit = 0,
        };
        const candidates = self.storage.chunksForTimeRange(params.from_ns, params.to_ns);
        for (candidates) |meta| {
            self.scanChunkLogQL(meta.path, expr, inner_params, &matched, std.math.maxInt(usize)) catch continue;
        }
        if (candidates.len == 0) {
            try self.fullScanLogQL(expr, inner_params, &matched, std.math.maxInt(usize));
        }

        // Bucket by (group_key, time_bucket).
        const total_buckets = blk: {
            const span_ns: i128 = @as(i128, to_ns) - @as(i128, from_ns);
            const step_ns: i128 = @as(i128, @intCast(step)) * std.time.ns_per_s;
            const n: usize = @intCast(@divTrunc(span_ns + step_ns - 1, step_ns));
            break :blk if (n == 0) 1 else n;
        };

        var series = std.StringHashMap(*MetricSeries).init(self.allocator);
        errdefer {
            var it = series.iterator();
            while (it.next()) |kv| {
                self.allocator.free(kv.key_ptr.*);
                kv.value_ptr.*.deinit(self.allocator);
                self.allocator.destroy(kv.value_ptr.*);
            }
            series.deinit();
        }

        for (matched.items) |entry| {
            const key = try buildGroupKey(self.allocator, &entry, expr.group_by);
            defer self.allocator.free(key);

            const gop = try series.getOrPut(key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try self.allocator.dupe(u8, key);
                const ms = try self.allocator.create(MetricSeries);
                ms.* = .{
                    .labels = try cloneGroupLabels(self.allocator, &entry, expr.group_by),
                    .counts = try self.allocator.alloc(u64, total_buckets),
                };
                @memset(ms.counts, 0);
                gop.value_ptr.* = ms;
            }
            const idx_i: i128 = @divTrunc(@as(i128, entry.timestamp_ns) - @as(i128, from_ns), @as(i128, @intCast(step)) * std.time.ns_per_s);
            if (idx_i < 0) continue;
            const idx: usize = @intCast(idx_i);
            if (idx >= total_buckets) continue;
            gop.value_ptr.*.counts[idx] += 1;
        }

        // Materialize result.
        var out_series = std.ArrayList(MetricResult.Series).init(self.allocator);
        errdefer {
            for (out_series.items) |*s| s.deinit(self.allocator);
            out_series.deinit();
        }
        var it = series.iterator();
        while (it.next()) |kv| {
            const ms = kv.value_ptr.*;
            var samples = std.ArrayList(MetricResult.Sample).init(self.allocator);
            errdefer samples.deinit();
            for (ms.counts, 0..) |c, i| {
                if (c == 0) continue;
                const ts_ns: i64 = from_ns + @as(i64, @intCast(i)) * @as(i64, @intCast(step)) * std.time.ns_per_s;
                const value: f64 = if (expr.metric_op == .rate)
                    @as(f64, @floatFromInt(c)) / @as(f64, @floatFromInt(expr.range_seconds))
                else
                    @as(f64, @floatFromInt(c));
                try samples.append(.{ .timestamp_ns = ts_ns, .value = value });
            }
            try out_series.append(.{
                .labels = ms.labels,
                .samples = try samples.toOwnedSlice(),
            });
            self.allocator.free(kv.key_ptr.*);
            self.allocator.free(ms.counts);
            self.allocator.destroy(ms);
        }
        series.deinit();

        return MetricResult{
            .step_seconds = step,
            .series = try out_series.toOwnedSlice(),
        };
    }
};

/// One sample series produced by `executeMetric`.
pub const MetricResult = struct {
    step_seconds: u64,
    series: []Series,

    pub const Sample = struct {
        timestamp_ns: i64,
        value: f64,
    };

    pub const Series = struct {
        labels: []types.LogEntry.Label,
        samples: []Sample,

        pub fn deinit(self: *Series, allocator: Allocator) void {
            for (self.labels) |lbl| {
                allocator.free(lbl.key);
                allocator.free(lbl.value);
            }
            allocator.free(self.labels);
            allocator.free(self.samples);
        }
    };

    pub fn deinit(self: *MetricResult, allocator: Allocator) void {
        for (self.series) |*s| s.deinit(allocator);
        allocator.free(self.series);
    }
};

const MetricSeries = struct {
    labels: []types.LogEntry.Label,
    counts: []u64,

    fn deinit(self: *MetricSeries, allocator: Allocator) void {
        for (self.labels) |lbl| {
            allocator.free(lbl.key);
            allocator.free(lbl.value);
        }
        allocator.free(self.labels);
        allocator.free(self.counts);
    }
};

fn buildGroupKey(allocator: Allocator, entry: *const LogEntry, group_by: []const []const u8) ![]u8 {
    if (group_by.len == 0) return allocator.dupe(u8, "");
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    for (group_by) |name| {
        try buf.appendSlice(name);
        try buf.append('=');
        const v = lookupLabel(entry, name) orelse "";
        try buf.appendSlice(v);
        try buf.append(0);
    }
    return buf.toOwnedSlice();
}

fn cloneGroupLabels(allocator: Allocator, entry: *const LogEntry, group_by: []const []const u8) ![]types.LogEntry.Label {
    var labels = try allocator.alloc(types.LogEntry.Label, group_by.len);
    var i: usize = 0;
    errdefer {
        for (labels[0..i]) |lbl| {
            allocator.free(lbl.key);
            allocator.free(lbl.value);
        }
        allocator.free(labels);
    }
    while (i < group_by.len) : (i += 1) {
        const v = lookupLabel(entry, group_by[i]) orelse "";
        labels[i] = .{
            .key = try allocator.dupe(u8, group_by[i]),
            .value = try allocator.dupe(u8, v),
        };
    }
    return labels;
}

fn lookupLabel(entry: *const LogEntry, name: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, name, "source") or std.mem.eql(u8, name, "filename")) return entry.source;
    for (entry.labels) |lbl| {
        if (std.mem.eql(u8, lbl.key, name)) return lbl.value;
    }
    return null;
}

// ---------------------------------------------------------------------------
// LogQL entry-level evaluation
// ---------------------------------------------------------------------------

/// Evaluate one entry against a LogQL expression and the surrounding query
/// params (time range, etc.). Synthesizes a `source` and `filename` label so
/// users can write `{source="..."}` / `{filename="..."}` even without the
/// ingestion pipeline attaching them explicitly.
pub fn entryMatchesLogQL(
    entry: *const LogEntry,
    expr: *const logql.LogQLExpr,
    params: QueryParams,
) bool {
    if (params.from_ns) |from| {
        if (entry.timestamp_ns < from) return false;
    }
    if (params.to_ns) |to| {
        if (entry.timestamp_ns > to) return false;
    }

    var stack: [32]logql.LabelPair = undefined;
    var n: usize = 0;
    if (n < stack.len) {
        stack[n] = .{ .name = "source", .value = entry.source };
        n += 1;
    }
    if (n < stack.len) {
        stack[n] = .{ .name = "filename", .value = entry.source };
        n += 1;
    }
    var has_tenant = false;
    for (entry.labels) |lbl| {
        if (n >= stack.len) break;
        stack[n] = .{ .name = lbl.key, .value = lbl.value };
        if (std.mem.eql(u8, lbl.key, "tenant")) has_tenant = true;
        n += 1;
    }

    if (!has_tenant and n < stack.len) {
        stack[n] = .{ .name = "tenant", .value = "default" };
        n += 1;
    }

    if (!expr.matchesLabels(stack[0..n])) return false;
    if (!expr.matchesLine(entry.message)) return false;

    return true;
}

// ---------------------------------------------------------------------------
// Matching predicates
// ---------------------------------------------------------------------------

fn matchesQuery(entry: *const LogEntry, params: QueryParams) bool {
    // Time range filter.
    if (params.from_ns) |from| {
        if (entry.timestamp_ns < from) return false;
    }
    if (params.to_ns) |to| {
        if (entry.timestamp_ns > to) return false;
    }

    // Keyword substring match (case-insensitive).
    if (params.keyword) |keyword| {
        if (!containsInsensitive(entry.message, keyword)) return false;
    }

    return true;
}

/// Case-insensitive substring search.
fn containsInsensitive(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;

    const end = haystack.len - needle.len + 1;
    var i: usize = 0;
    while (i < end) : (i += 1) {
        var matched = true;
        for (needle, 0..) |nc, j| {
            if (std.ascii.toLower(haystack[i + j]) != std.ascii.toLower(nc)) {
                matched = false;
                break;
            }
        }
        if (matched) return true;
    }
    return false;
}

/// Check if a chunk_id is in a sorted-ish list.
fn chunkInSet(id: u32, set: []const u32) bool {
    for (set) |s| {
        if (s == id) return true;
    }
    return false;
}

/// Quick check: does a "YYYY-MM-DD" day string overlap the query time range?
fn dayOverlaps(day: []const u8, from_ns: ?i64, to_ns: ?i64) bool {
    const ts_mod = @import("../utils/timestamp.zig");
    const day_start = ts_mod.parseDate(day) catch return true; // If we can't parse, include it.
    const day_end = day_start + std.time.ns_per_day - 1;

    if (from_ns) |from| {
        if (day_end < from) return false;
    }
    if (to_ns) |to| {
        if (day_start > to) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "containsInsensitive" {
    try std.testing.expect(containsInsensitive("Hello World", "world"));
    try std.testing.expect(containsInsensitive("ERROR: connection refused", "error"));
    try std.testing.expect(!containsInsensitive("Hello World", "xyz"));
    try std.testing.expect(containsInsensitive("test", ""));
}

test "matchesQuery time range" {
    const entry = LogEntry{
        .timestamp_ns = 1000,
        .source = "test",
        .message = "hello",
        .raw_line = "hello",
        .labels = &.{},
    };

    try std.testing.expect(matchesQuery(&entry, .{ .from_ns = 500, .to_ns = 1500 }));
    try std.testing.expect(!matchesQuery(&entry, .{ .from_ns = 1500, .to_ns = 2000 }));
    try std.testing.expect(matchesQuery(&entry, .{}));
}

test "matchesQuery keyword" {
    const entry = LogEntry{
        .timestamp_ns = 1000,
        .source = "test",
        .message = "ERROR: database timeout",
        .raw_line = "ERROR: database timeout",
        .labels = &.{},
    };

    try std.testing.expect(matchesQuery(&entry, .{ .keyword = "database" }));
    try std.testing.expect(matchesQuery(&entry, .{ .keyword = "ERROR" }));
    try std.testing.expect(!matchesQuery(&entry, .{ .keyword = "success" }));
}

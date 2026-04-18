// =============================================================================
// Amatiz — Loki-Compatible JSON Response Formatting
// =============================================================================
//
// Formats query results in the Grafana Loki response format so Grafana can
// natively consume them. Supports both "streams" and "matrix" result types.
//
// Query response format:
//   {
//     "status": "success",
//     "data": {
//       "resultType": "streams",
//       "result": [
//         {
//           "stream": { "label1": "value1" },
//           "values": [
//             ["<timestamp_ns>", "<log_line>"],
//             ...
//           ]
//         }
//       ],
//       "stats": { ... }
//     }
//   }
//
// Labels response:
//   { "status": "success", "data": ["label1", "label2"] }
//
// Label values response:
//   { "status": "success", "data": ["value1", "value2"] }
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;

// ---------------------------------------------------------------------------
// Query responses — streams result type
// ---------------------------------------------------------------------------

/// A single log stream in a Loki response (unique label set + values).
pub const StreamResult = struct {
    labels: []const LogEntry.Label,
    /// Array of (timestamp_ns, log_line) pairs.
    values: []const StreamValue,
};

pub const StreamValue = struct {
    timestamp_ns: i64,
    line: []const u8,
};

/// Format a streams query response into a Loki-compatible JSON buffer.
pub fn formatStreamsResponse(
    allocator: Allocator,
    streams: []const StreamResult,
) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"status\":\"success\",\"data\":{\"resultType\":\"streams\",\"result\":[");

    for (streams, 0..) |stream, si| {
        if (si > 0) try buf.append(',');

        // Stream object.
        try buf.appendSlice("{\"stream\":{");

        // Labels.
        for (stream.labels, 0..) |lbl, li| {
            if (li > 0) try buf.append(',');
            try buf.append('"');
            try appendEscaped(&buf, lbl.key);
            try buf.appendSlice("\":\"");
            try appendEscaped(&buf, lbl.value);
            try buf.append('"');
        }

        try buf.appendSlice("},\"values\":[");

        // Values.
        for (stream.values, 0..) |val, vi| {
            if (vi > 0) try buf.append(',');
            try buf.appendSlice("[\"");

            // Timestamp as string.
            var ts_buf: [32]u8 = undefined;
            const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{val.timestamp_ns}) catch unreachable;
            try buf.appendSlice(ts_str);

            try buf.appendSlice("\",\"");
            try appendEscaped(&buf, val.line);
            try buf.appendSlice("\"]");
        }

        try buf.appendSlice("]}");
    }

    try buf.appendSlice("],\"stats\":{\"summary\":{\"bytesProcessed\":0,\"linesProcessed\":0}}}}");

    return buf.toOwnedSlice();
}

/// Format a list of LogEntry results grouped by their full label set into
/// a Loki streams response. Two entries land in the same stream iff they have
/// the same {source, label1=v1, label2=v2, ...} fingerprint.
pub fn formatLogEntriesAsStreams(
    allocator: Allocator,
    entries: []const LogEntry,
) ![]u8 {
    // Each unique label-set fingerprint -> aggregated stream values + labels.
    var streams_map = std.StringHashMap(StreamAccum).init(allocator);
    defer {
        var it = streams_map.iterator();
        while (it.next()) |kv| {
            allocator.free(kv.key_ptr.*);
            kv.value_ptr.values.deinit();
            allocator.free(kv.value_ptr.labels);
        }
        streams_map.deinit();
    }

    for (entries) |entry| {
        // Build the stream's label set: synthesized "source" + entry labels.
        var label_buf = std.ArrayList(LogEntry.Label).init(allocator);
        defer label_buf.deinit();
        try label_buf.append(.{ .key = "source", .value = entry.source });
        for (entry.labels) |lbl| {
            // Skip duplicates of "source" if the pipeline already attached one.
            if (std.mem.eql(u8, lbl.key, "source")) continue;
            try label_buf.append(lbl);
        }

        // Sort labels by key for a deterministic fingerprint.
        std.mem.sort(LogEntry.Label, label_buf.items, {}, labelLessThan);

        const fp = try labelFingerprint(allocator, label_buf.items);

        const gop = try streams_map.getOrPut(fp);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .labels = try allocator.dupe(LogEntry.Label, label_buf.items),
                .values = std.ArrayList(StreamValue).init(allocator),
            };
        } else {
            // Already exists — free the duplicate fingerprint key.
            allocator.free(fp);
            gop.key_ptr.* = gop.key_ptr.*; // keep existing key
            // Restore the original key reference to avoid use-after-free.
            // (StringHashMap keeps the first-inserted key pointer, no action.)
        }

        try gop.value_ptr.values.append(.{
            .timestamp_ns = entry.timestamp_ns,
            .line = entry.message,
        });
    }

    // Build the StreamResult slice.
    var streams = std.ArrayList(StreamResult).init(allocator);
    defer streams.deinit();

    var it = streams_map.iterator();
    while (it.next()) |kv| {
        try streams.append(.{
            .labels = kv.value_ptr.labels,
            .values = kv.value_ptr.values.items,
        });
    }

    return formatStreamsResponse(allocator, streams.items);
}

const StreamAccum = struct {
    labels: []LogEntry.Label,
    values: std.ArrayList(StreamValue),
};

fn labelLessThan(_: void, a: LogEntry.Label, b: LogEntry.Label) bool {
    return std.mem.lessThan(u8, a.key, b.key);
}

/// Build a deterministic fingerprint string for a sorted label set.
/// Format: "k1=v1\x1fk2=v2\x1f...". Caller owns the returned slice.
fn labelFingerprint(allocator: Allocator, labels: []const LogEntry.Label) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    for (labels, 0..) |lbl, i| {
        if (i > 0) try buf.append(0x1f);
        try buf.appendSlice(lbl.key);
        try buf.append('=');
        try buf.appendSlice(lbl.value);
    }
    return buf.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Label responses
// ---------------------------------------------------------------------------

/// Format a list of label names as a Loki labels response.
pub fn formatLabelsResponse(allocator: Allocator, labels: []const []const u8) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"status\":\"success\",\"data\":[");

    for (labels, 0..) |label, i| {
        if (i > 0) try buf.append(',');
        try buf.append('"');
        try appendEscaped(&buf, label);
        try buf.append('"');
    }

    try buf.appendSlice("]}");
    return buf.toOwnedSlice();
}

/// Format a list of label values as a Loki label values response.
pub fn formatLabelValuesResponse(allocator: Allocator, values: []const []const u8) ![]u8 {
    // Same format as labels response.
    return formatLabelsResponse(allocator, values);
}

// ---------------------------------------------------------------------------
// Series response
// ---------------------------------------------------------------------------

/// Format series data (array of label sets).
pub fn formatSeriesResponse(
    allocator: Allocator,
    series: []const []const LogEntry.Label,
) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"status\":\"success\",\"data\":[");

    for (series, 0..) |label_set, si| {
        if (si > 0) try buf.append(',');
        try buf.append('{');
        for (label_set, 0..) |lbl, li| {
            if (li > 0) try buf.append(',');
            try buf.append('"');
            try appendEscaped(&buf, lbl.key);
            try buf.appendSlice("\":\"");
            try appendEscaped(&buf, lbl.value);
            try buf.append('"');
        }
        try buf.append('}');
    }

    try buf.appendSlice("]}");
    return buf.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Error response
// ---------------------------------------------------------------------------

/// Format a Loki-style error response.
pub fn formatErrorResponse(allocator: Allocator, message: []const u8) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"status\":\"error\",\"errorType\":\"bad_request\",\"error\":\"");
    try appendEscaped(&buf, message);
    try buf.appendSlice("\"}");

    return buf.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Matrix response (Prometheus-style metric query result)
// ---------------------------------------------------------------------------

const QueryEngineMod = @import("../query/engine.zig");

/// Format a metric query result in the Loki/Prometheus matrix shape:
///   { status, data: { resultType: "matrix", result: [ {metric:{}, values:[[ts,val]...]}, ... ] } }
pub fn formatMatrixResponse(allocator: Allocator, result: *const QueryEngineMod.MetricResult) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[");
    for (result.series, 0..) |series, si| {
        if (si > 0) try buf.append(',');
        try buf.appendSlice("{\"metric\":{");
        for (series.labels, 0..) |lbl, li| {
            if (li > 0) try buf.append(',');
            try buf.append('"');
            try appendEscaped(&buf, lbl.key);
            try buf.appendSlice("\":\"");
            try appendEscaped(&buf, lbl.value);
            try buf.append('"');
        }
        try buf.appendSlice("},\"values\":[");
        for (series.samples, 0..) |sample, sx| {
            if (sx > 0) try buf.append(',');
            try buf.append('[');
            // Prometheus matrix uses seconds as a float for the timestamp.
            const ts_seconds: f64 = @as(f64, @floatFromInt(sample.timestamp_ns)) / 1_000_000_000.0;
            try buf.writer().print("{d:.3},\"{d}\"", .{ ts_seconds, sample.value });
            try buf.append(']');
        }
        try buf.appendSlice("]}");
    }
    try buf.appendSlice("]}}");

    return buf.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Prometheus exposition format for /metrics
// ---------------------------------------------------------------------------

/// Format metrics in Prometheus text exposition format.
pub fn formatPrometheusMetrics(
    allocator: Allocator,
    total_entries: u64,
    total_bytes: u64,
    total_chunks: u32,
    uptime_s: u64,
    active_connections: u32,
) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    // Helper to write a single metric line.
    const metrics = [_]struct { name: []const u8, help: []const u8, metric_type: []const u8, value: u64 }{
        .{
            .name = "amatiz_log_entries_total",
            .help = "Total number of log entries ingested.",
            .metric_type = "counter",
            .value = total_entries,
        },
        .{
            .name = "amatiz_log_bytes_total",
            .help = "Total bytes of log data written.",
            .metric_type = "counter",
            .value = total_bytes,
        },
        .{
            .name = "amatiz_chunks_total",
            .help = "Total number of chunk files created.",
            .metric_type = "counter",
            .value = @intCast(total_chunks),
        },
        .{
            .name = "amatiz_uptime_seconds",
            .help = "Server uptime in seconds.",
            .metric_type = "gauge",
            .value = uptime_s,
        },
        .{
            .name = "amatiz_active_connections",
            .help = "Number of active HTTP connections.",
            .metric_type = "gauge",
            .value = @intCast(active_connections),
        },
    };

    for (metrics) |m| {
        var line_buf: [512]u8 = undefined;
        const help_line = std.fmt.bufPrint(&line_buf, "# HELP {s} {s}\n", .{ m.name, m.help }) catch continue;
        try buf.appendSlice(help_line);

        const type_line = std.fmt.bufPrint(&line_buf, "# TYPE {s} {s}\n", .{ m.name, m.metric_type }) catch continue;
        try buf.appendSlice(type_line);

        const value_line = std.fmt.bufPrint(&line_buf, "{s} {d}\n", .{ m.name, m.value }) catch continue;
        try buf.appendSlice(value_line);
    }

    return buf.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

fn appendEscaped(buf: *std.ArrayList(u8), s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice("\\\""),
            '\\' => try buf.appendSlice("\\\\"),
            '\n' => try buf.appendSlice("\\n"),
            '\r' => try buf.appendSlice("\\r"),
            '\t' => try buf.appendSlice("\\t"),
            else => {
                if (c < 0x20) {
                    try buf.appendSlice("\\u00");
                    const hex = "0123456789abcdef";
                    try buf.append(hex[c >> 4]);
                    try buf.append(hex[c & 0xf]);
                } else {
                    try buf.append(c);
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "formatLabelsResponse" {
    const allocator = std.testing.allocator;
    const labels = [_][]const u8{ "job", "level", "source" };
    const result = try formatLabelsResponse(allocator, &labels);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "\"success\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"job\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"level\"") != null);
}

test "formatPrometheusMetrics" {
    const allocator = std.testing.allocator;
    const result = try formatPrometheusMetrics(allocator, 1000, 50000, 5, 3600, 3);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "amatiz_log_entries_total 1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# HELP") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# TYPE") != null);
}

test "formatErrorResponse" {
    const allocator = std.testing.allocator;
    const result = try formatErrorResponse(allocator, "query parse error");
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "query parse error") != null);
}

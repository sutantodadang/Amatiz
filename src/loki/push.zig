// =============================================================================
// Amatiz — Loki Push API Handler
// =============================================================================
//
// Parses Grafana Loki push requests (JSON format) as sent by Promtail,
// Grafana Alloy, and other Loki-compatible log shippers.
//
// Push format (POST /loki/api/v1/push):
//   {
//     "streams": [
//       {
//         "stream": { "label1": "value1", "label2": "value2" },
//         "values": [
//           ["<nanosecond_timestamp>", "<log_line>"],
//           ["<nanosecond_timestamp>", "<log_line>"]
//         ]
//       }
//     ]
//   }
//
// Each stream is identified by its unique label set. Values are tuples of
// [timestamp_string, log_line]. The timestamp is nanoseconds since epoch
// as a string.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;
const ts = @import("../utils/timestamp.zig");

/// A parsed push request containing one or more log streams.
pub const PushRequest = struct {
    entries: []ParsedEntry,
    allocator: Allocator,

    pub const ParsedEntry = struct {
        timestamp_ns: i64,
        line: []const u8,
        labels: []const LogEntry.Label,
        source: []const u8,
    };

    pub fn deinit(self: *PushRequest) void {
        self.allocator.free(self.entries);
    }
};

/// Parse a Loki push JSON body into a list of log entries.
/// The returned entries reference slices within `body` — the caller must
/// keep `body` alive for the lifetime of the returned PushRequest.
pub fn parsePushBody(allocator: Allocator, body: []const u8) !PushRequest {
    var entries = std.ArrayList(PushRequest.ParsedEntry).init(allocator);
    errdefer entries.deinit();

    // Simple JSON parser for the Loki push format.
    // We look for "streams" array, then each stream has "stream" (labels)
    // and "values" (array of [timestamp, line] tuples).

    // Find "streams" array.
    const streams_start = findJsonKey(body, "streams") orelse return .{
        .entries = try entries.toOwnedSlice(),
        .allocator = allocator,
    };

    var pos = streams_start;

    // Skip to opening '[' of streams array.
    pos = skipToChar(body, pos, '[') orelse return .{
        .entries = try entries.toOwnedSlice(),
        .allocator = allocator,
    };
    pos += 1;

    // Parse each stream object.
    while (pos < body.len) {
        pos = skipWhitespace(body, pos);
        if (pos >= body.len) break;

        if (body[pos] == ']') break; // end of streams array
        if (body[pos] == ',') {
            pos += 1;
            continue;
        }

        if (body[pos] == '{') {
            // Parse stream object.
            pos += 1;

            // Collect labels and values positions.
            var labels_slice: []const u8 = "{}";
            var values_start: ?usize = null;

            // Scan the stream object for "stream" and "values" keys.
            var brace_depth: u32 = 1;
            const obj_start = pos;
            while (pos < body.len and brace_depth > 0) {
                if (body[pos] == '{') brace_depth += 1;
                if (body[pos] == '}') brace_depth -= 1;
                if (body[pos] == '"') {
                    pos = skipString(body, pos);
                    continue;
                }
                pos += 1;
            }
            const obj_end = pos;
            const obj_content = body[obj_start..obj_end];

            // Find "stream" sub-object within this stream object.
            if (findJsonKey(obj_content, "stream")) |stream_off| {
                if (skipToChar(obj_content, stream_off, '{')) |brace_start| {
                    if (findMatchingBrace(obj_content, brace_start)) |brace_end| {
                        labels_slice = obj_content[brace_start .. brace_end + 1];
                    }
                }
            }

            // Find "values" array within this stream object.
            if (findJsonKey(obj_content, "values")) |values_off| {
                values_start = values_off;
            }

            // Parse labels from the stream sub-object.
            var label_list = std.ArrayList(LogEntry.Label).init(allocator);
            defer label_list.deinit();
            parseLabelsFromJson(labels_slice, &label_list) catch {};
            const labels = label_list.items;

            // Build source from labels (use "job" label or first label).
            var source: []const u8 = "loki-push";
            for (labels) |lbl| {
                if (std.mem.eql(u8, lbl.key, "job") or std.mem.eql(u8, lbl.key, "filename")) {
                    source = lbl.value;
                    break;
                }
            }

            // Parse values.
            if (values_start) |vs| {
                if (skipToChar(obj_content, vs, '[')) |arr_start| {
                    var vp = arr_start + 1;
                    while (vp < obj_content.len) {
                        vp = skipWhitespace(obj_content, vp);
                        if (vp >= obj_content.len) break;
                        if (obj_content[vp] == ']') break;
                        if (obj_content[vp] == ',') {
                            vp += 1;
                            continue;
                        }

                        // Parse [timestamp, line] tuple.
                        if (obj_content[vp] == '[') {
                            vp += 1;
                            vp = skipWhitespace(obj_content, vp);

                            // Parse timestamp string.
                            var timestamp_ns: i64 = ts.nowNs();
                            if (vp < obj_content.len and obj_content[vp] == '"') {
                                const ts_start = vp + 1;
                                if (std.mem.indexOfPos(u8, obj_content, ts_start, "\"")) |ts_end| {
                                    const ts_str = obj_content[ts_start..ts_end];
                                    timestamp_ns = std.fmt.parseInt(i64, ts_str, 10) catch ts.nowNs();
                                    vp = ts_end + 1;
                                }
                            }

                            // Skip comma.
                            vp = skipWhitespace(obj_content, vp);
                            if (vp < obj_content.len and obj_content[vp] == ',') vp += 1;
                            vp = skipWhitespace(obj_content, vp);

                            // Parse log line string.
                            var line: []const u8 = "";
                            if (vp < obj_content.len and obj_content[vp] == '"') {
                                const line_start = vp + 1;
                                if (findUnescapedQuote(obj_content, line_start)) |line_end| {
                                    line = obj_content[line_start..line_end];
                                    vp = line_end + 1;
                                }
                            }

                            // Skip to closing ']' of tuple.
                            if (skipToChar(obj_content, vp, ']')) |bracket| {
                                vp = bracket + 1;
                            }

                            if (line.len > 0) {
                                try entries.append(.{
                                    .timestamp_ns = timestamp_ns,
                                    .line = line,
                                    .labels = labels,
                                    .source = source,
                                });
                            }
                        } else {
                            vp += 1;
                        }
                    }
                }
            }
        } else {
            pos += 1;
        }
    }

    return .{
        .entries = try entries.toOwnedSlice(),
        .allocator = allocator,
    };
}

// ---------------------------------------------------------------------------
// Label parsing
// ---------------------------------------------------------------------------

/// Parse labels from a JSON object string like {"key":"value","key2":"value2"}.
fn parseLabelsFromJson(json: []const u8, list: *std.ArrayList(LogEntry.Label)) !void {
    var pos: usize = 0;
    while (pos < json.len) {
        // Find key.
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const ks = q1 + 1;
            if (findUnescapedQuote(json, ks)) |q2| {
                const key = json[ks..q2];
                var vp = q2 + 1;

                // Skip colon and whitespace.
                while (vp < json.len and (json[vp] == ':' or json[vp] == ' ' or json[vp] == '\t')) : (vp += 1) {}

                // Parse value.
                if (vp < json.len and json[vp] == '"') {
                    const vs = vp + 1;
                    if (findUnescapedQuote(json, vs)) |ve| {
                        const value = json[vs..ve];
                        try list.append(.{ .key = key, .value = value });
                        pos = ve + 1;
                        continue;
                    }
                }
                pos = q2 + 1;
            } else break;
        } else break;
    }
}

// ---------------------------------------------------------------------------
// JSON scanning helpers
// ---------------------------------------------------------------------------

fn findJsonKey(json: []const u8, key: []const u8) ?usize {
    var pos: usize = 0;
    while (pos < json.len) {
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const ks = q1 + 1;
            if (std.mem.indexOfPos(u8, json, ks, "\"")) |q2| {
                if (std.mem.eql(u8, json[ks..q2], key)) {
                    return q2 + 1;
                }
                pos = q2 + 1;
            } else break;
        } else break;
    }
    return null;
}

fn skipToChar(buf: []const u8, start: usize, char: u8) ?usize {
    var pos = start;
    while (pos < buf.len) : (pos += 1) {
        if (buf[pos] == char) return pos;
    }
    return null;
}

fn skipWhitespace(buf: []const u8, start: usize) usize {
    var pos = start;
    while (pos < buf.len and (buf[pos] == ' ' or buf[pos] == '\t' or buf[pos] == '\n' or buf[pos] == '\r')) : (pos += 1) {}
    return pos;
}

fn skipString(buf: []const u8, start: usize) usize {
    if (start >= buf.len or buf[start] != '"') return start + 1;
    var pos = start + 1;
    while (pos < buf.len) {
        if (buf[pos] == '\\') {
            pos += 2;
            continue;
        }
        if (buf[pos] == '"') return pos + 1;
        pos += 1;
    }
    return pos;
}

fn findUnescapedQuote(buf: []const u8, start: usize) ?usize {
    var pos = start;
    while (pos < buf.len) {
        if (buf[pos] == '\\') {
            pos += 2;
            continue;
        }
        if (buf[pos] == '"') return pos;
        pos += 1;
    }
    return null;
}

fn findMatchingBrace(buf: []const u8, start: usize) ?usize {
    if (start >= buf.len or buf[start] != '{') return null;
    var depth: u32 = 0;
    var pos = start;
    while (pos < buf.len) {
        if (buf[pos] == '"') {
            pos = skipString(buf, pos);
            continue;
        }
        if (buf[pos] == '{') depth += 1;
        if (buf[pos] == '}') {
            depth -= 1;
            if (depth == 0) return pos;
        }
        pos += 1;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parsePushBody basic" {
    const allocator = std.testing.allocator;
    const body =
        \\{"streams":[{"stream":{"job":"myapp","level":"info"},"values":[["1700000000000000000","hello world"]]}]}
    ;

    var req = try parsePushBody(allocator, body);
    defer req.deinit();

    try std.testing.expect(req.entries.len >= 1);
}

test "parseLabelsFromJson" {
    const allocator = std.testing.allocator;
    var list = std.ArrayList(LogEntry.Label).init(allocator);
    defer list.deinit();

    try parseLabelsFromJson("{\"job\":\"myapp\",\"level\":\"info\"}", &list);

    try std.testing.expectEqual(@as(usize, 2), list.items.len);
    try std.testing.expectEqualStrings("job", list.items[0].key);
    try std.testing.expectEqualStrings("myapp", list.items[0].value);
}

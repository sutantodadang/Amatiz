// =============================================================================
// Amatiz — LogQL Parser
// =============================================================================
//
// Implements a subset of Grafana Loki's LogQL query language, sufficient for
// Grafana Explore and dashboard queries.
//
// Supported syntax:
//   Stream selector:  {label="value", label2=~"regex.*"}
//   Matchers:         =  (exact match)
//                     != (not equal)
//                     =~ (regex match)
//                     !~ (regex not match)
//   Line filters:     |= "substring"      (line contains)
//                     != "substring"       (line does not contain)
//                     |~ "regex"           (line matches regex)
//                     !~ "regex"           (line does not match regex)
//   Limit:            | limit N
//
// Example:
//   {job="myapp", level="error"} |= "timeout" != "healthcheck"
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single label matcher in a stream selector.
pub const LabelMatcher = struct {
    name: []const u8,
    value: []const u8,
    op: MatchOp,

    pub const MatchOp = enum {
        eq, // =
        neq, // !=
        regex_match, // =~
        regex_not_match, // !~
    };

    /// Test whether a label value matches this matcher.
    pub fn matches(self: *const LabelMatcher, label_value: ?[]const u8) bool {
        const val = label_value orelse return self.op == .neq or self.op == .regex_not_match;

        return switch (self.op) {
            .eq => std.mem.eql(u8, val, self.value),
            .neq => !std.mem.eql(u8, val, self.value),
            .regex_match => containsSubstring(val, self.value),
            .regex_not_match => !containsSubstring(val, self.value),
        };
    }
};

/// A single line filter applied after stream selection.
pub const LineFilter = struct {
    pattern: []const u8,
    op: FilterOp,

    pub const FilterOp = enum {
        contains, // |=
        not_contains, // !=
        regex_match, // |~
        regex_not_match, // !~
    };

    /// Test whether a log line matches this filter.
    pub fn matches(self: *const LineFilter, line: []const u8) bool {
        return switch (self.op) {
            .contains => containsInsensitive(line, self.pattern),
            .not_contains => !containsInsensitive(line, self.pattern),
            .regex_match => containsInsensitive(line, self.pattern),
            .regex_not_match => !containsInsensitive(line, self.pattern),
        };
    }
};

/// Direction for result ordering.
pub const Direction = enum {
    backward, // newest first (default)
    forward, // oldest first
};

/// A fully parsed LogQL expression.
pub const LogQLExpr = struct {
    /// Label matchers from the stream selector {label="value"}.
    matchers: []const LabelMatcher,
    /// Ordered list of line filters (|= "xxx" != "yyy").
    line_filters: []const LineFilter,
    /// Result limit (from "| limit N" or query parameter).
    limit: usize,
    /// Result direction.
    direction: Direction,
    /// Whether this is a streams query or a metric query.
    kind: Kind = .streams,
    /// For metric queries: which aggregation to compute.
    metric_op: MetricOp = .count_over_time,
    /// For metric queries: the range-vector duration in seconds (e.g. `[5m]`).
    range_seconds: u64 = 0,
    /// For metric queries: optional `sum by (lbl1, lbl2)` grouping.
    group_by: []const []const u8 = &.{},

    pub const Kind = enum { streams, metric };
    pub const MetricOp = enum { count_over_time, rate };

    /// Check if a set of labels matches all stream selector matchers.
    pub fn matchesLabels(self: *const LogQLExpr, labels: []const LabelPair) bool {
        for (self.matchers) |matcher| {
            var found_value: ?[]const u8 = null;
            for (labels) |lbl| {
                if (std.mem.eql(u8, lbl.name, matcher.name)) {
                    found_value = lbl.value;
                    break;
                }
            }
            if (!matcher.matches(found_value)) return false;
        }
        return true;
    }

    /// Check if a log line passes all line filters.
    pub fn matchesLine(self: *const LogQLExpr, line: []const u8) bool {
        for (self.line_filters) |filter| {
            if (!filter.matches(line)) return false;
        }
        return true;
    }
};

/// Simple label key-value pair used during matching.
pub const LabelPair = struct {
    name: []const u8,
    value: []const u8,
};

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parse a LogQL expression string into a structured LogQLExpr.
/// Caller owns all returned slices via the allocator.
pub fn parse(allocator: Allocator, query: []const u8) !LogQLExpr {
    const trimmed = std.mem.trim(u8, query, " \t\r\n");

    // Detect outer `sum by (...) (inner)` wrapper.
    var group_by_owned: []const []const u8 = &.{};
    errdefer freeStringList(allocator, group_by_owned);

    var inner_query = trimmed;
    var has_sum = false;

    if (std.mem.startsWith(u8, trimmed, "sum")) {
        var p: usize = 3;
        while (p < trimmed.len and (trimmed[p] == ' ' or trimmed[p] == '\t')) : (p += 1) {}
        // Optional `by (l1, l2)`
        if (p + 2 < trimmed.len and trimmed[p] == 'b' and trimmed[p + 1] == 'y') {
            p += 2;
            while (p < trimmed.len and (trimmed[p] == ' ' or trimmed[p] == '\t')) : (p += 1) {}
            if (p < trimmed.len and trimmed[p] == '(') {
                const list_start = p + 1;
                const list_end = std.mem.indexOfScalarPos(u8, trimmed, list_start, ')') orelse return error.InvalidLogQL;
                group_by_owned = try parseLabelList(allocator, trimmed[list_start..list_end]);
                p = list_end + 1;
            }
        }
        while (p < trimmed.len and (trimmed[p] == ' ' or trimmed[p] == '\t')) : (p += 1) {}
        if (p < trimmed.len and trimmed[p] == '(') {
            // Find matching closing paren.
            const close = findMatchingParen(trimmed, p) orelse return error.InvalidLogQL;
            inner_query = std.mem.trim(u8, trimmed[p + 1 .. close], " \t");
            has_sum = true;
        }
    }

    // Detect metric wrapper `rate(...)` or `count_over_time(...)`.
    var metric_op: ?LogQLExpr.MetricOp = null;
    var range_seconds: u64 = 0;

    inline for (.{ .{ "count_over_time", LogQLExpr.MetricOp.count_over_time }, .{ "rate", LogQLExpr.MetricOp.rate } }) |entry| {
        const name = entry[0];
        if (std.mem.startsWith(u8, inner_query, name)) {
            var q = name.len;
            while (q < inner_query.len and (inner_query[q] == ' ' or inner_query[q] == '\t')) : (q += 1) {}
            if (q < inner_query.len and inner_query[q] == '(') {
                const close = findMatchingParen(inner_query, q) orelse return error.InvalidLogQL;
                const body = std.mem.trim(u8, inner_query[q + 1 .. close], " \t");
                // Extract trailing `[duration]` from body.
                const dur_start = std.mem.lastIndexOfScalar(u8, body, '[') orelse return error.InvalidLogQL;
                const dur_end = std.mem.lastIndexOfScalar(u8, body, ']') orelse return error.InvalidLogQL;
                if (dur_end <= dur_start) return error.InvalidLogQL;
                range_seconds = parseDurationSeconds(body[dur_start + 1 .. dur_end]) orelse return error.InvalidLogQL;
                metric_op = entry[1];
                inner_query = std.mem.trim(u8, body[0..dur_start], " \t");
                break;
            }
        }
    }

    if (has_sum and metric_op == null) return error.InvalidLogQL;

    var expr = try parseStreams(allocator, inner_query);
    if (metric_op) |mop| {
        expr.kind = .metric;
        expr.metric_op = mop;
        expr.range_seconds = range_seconds;
        expr.group_by = group_by_owned;
    } else {
        // Free unused group_by from a malformed `sum` without metric (should
        // never reach here due to the check above, but stay defensive).
        freeStringList(allocator, group_by_owned);
    }
    return expr;
}

/// Parse a streams-only expression (stream selector + line filters + limit).
fn parseStreams(allocator: Allocator, query: []const u8) !LogQLExpr {
    var matchers = std.ArrayList(LabelMatcher).init(allocator);
    errdefer matchers.deinit();
    var line_filters = std.ArrayList(LineFilter).init(allocator);
    errdefer line_filters.deinit();

    var pos: usize = 0;
    const input = std.mem.trim(u8, query, " \t\r\n");

    // Step 1: Parse stream selector {label="value", ...}
    if (pos < input.len and input[pos] == '{') {
        pos += 1; // skip '{'
        while (pos < input.len and input[pos] != '}') {
            // Skip whitespace and commas.
            while (pos < input.len and (input[pos] == ' ' or input[pos] == ',' or input[pos] == '\t')) : (pos += 1) {}
            if (pos >= input.len or input[pos] == '}') break;

            // Parse label name.
            const name_start = pos;
            while (pos < input.len and isLabelChar(input[pos])) : (pos += 1) {}
            const name = input[name_start..pos];
            if (name.len == 0) return error.InvalidLogQL;

            // Skip whitespace.
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}

            // Parse operator.
            const op = try parseMatchOp(input, &pos);

            // Skip whitespace.
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}

            // Parse quoted value.
            const value = try parseQuotedString(input, &pos);

            try matchers.append(.{ .name = name, .value = value, .op = op });
        }
        if (pos < input.len and input[pos] == '}') pos += 1;
    }

    // Step 2: Parse pipeline stages (line filters, limit).
    var limit: usize = 0;
    while (pos < input.len) {
        // Skip whitespace.
        while (pos < input.len and (input[pos] == ' ' or input[pos] == '\t')) : (pos += 1) {}
        if (pos >= input.len) break;

        // Try to match line filter operators.
        if (pos + 1 < input.len and input[pos] == '|' and input[pos + 1] == '=') {
            pos += 2;
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}
            const pattern = try parseQuotedString(input, &pos);
            try line_filters.append(.{ .pattern = pattern, .op = .contains });
        } else if (pos + 1 < input.len and input[pos] == '!' and input[pos + 1] == '=') {
            pos += 2;
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}
            const pattern = try parseQuotedString(input, &pos);
            try line_filters.append(.{ .pattern = pattern, .op = .not_contains });
        } else if (pos + 1 < input.len and input[pos] == '|' and input[pos + 1] == '~') {
            pos += 2;
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}
            const pattern = try parseQuotedString(input, &pos);
            try line_filters.append(.{ .pattern = pattern, .op = .regex_match });
        } else if (pos + 1 < input.len and input[pos] == '!' and input[pos + 1] == '~') {
            pos += 2;
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}
            const pattern = try parseQuotedString(input, &pos);
            try line_filters.append(.{ .pattern = pattern, .op = .regex_not_match });
        } else if (input[pos] == '|') {
            pos += 1;
            while (pos < input.len and input[pos] == ' ') : (pos += 1) {}

            // Check for "limit N".
            if (pos + 5 <= input.len and std.mem.eql(u8, input[pos .. pos + 5], "limit")) {
                pos += 5;
                while (pos < input.len and input[pos] == ' ') : (pos += 1) {}
                const num_start = pos;
                while (pos < input.len and input[pos] >= '0' and input[pos] <= '9') : (pos += 1) {}
                if (pos > num_start) {
                    limit = std.fmt.parseInt(usize, input[num_start..pos], 10) catch 0;
                }
            } else {
                pos += 1; // skip unknown pipeline stage
            }
        } else {
            pos += 1; // skip unknown char
        }
    }

    return .{
        .matchers = try matchers.toOwnedSlice(),
        .line_filters = try line_filters.toOwnedSlice(),
        .limit = limit,
        .direction = .backward,
    };
}

/// Free a parsed LogQL expression.
pub fn free(allocator: Allocator, expr: *const LogQLExpr) void {
    allocator.free(expr.matchers);
    allocator.free(expr.line_filters);
    freeStringList(allocator, expr.group_by);
}

/// Free a list of allocator-owned label name strings.
fn freeStringList(allocator: Allocator, list: []const []const u8) void {
    for (list) |s| allocator.free(s);
    allocator.free(list);
}

/// Parse a comma-separated list of label names: `lbl1, lbl2, lbl3`.
fn parseLabelList(allocator: Allocator, src: []const u8) ![]const []const u8 {
    var out = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (out.items) |s| allocator.free(s);
        out.deinit();
    }
    var iter = std.mem.splitScalar(u8, src, ',');
    while (iter.next()) |part| {
        const t = std.mem.trim(u8, part, " \t");
        if (t.len == 0) continue;
        try out.append(try allocator.dupe(u8, t));
    }
    return out.toOwnedSlice();
}

/// Find the closing `)` matching the `(` at `open_pos`.
fn findMatchingParen(input: []const u8, open_pos: usize) ?usize {
    var depth: usize = 0;
    var i = open_pos;
    while (i < input.len) : (i += 1) {
        switch (input[i]) {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if (depth == 0) return i;
            },
            else => {},
        }
    }
    return null;
}

/// Parse `30s`, `1m`, `5m`, `1h`, `1d` into seconds. Bare digits = seconds.
pub fn parseDurationSeconds(s: []const u8) ?u64 {
    if (s.len == 0) return null;
    var num_end: usize = 0;
    while (num_end < s.len and s[num_end] >= '0' and s[num_end] <= '9') : (num_end += 1) {}
    if (num_end == 0) return null;
    const n = std.fmt.parseInt(u64, s[0..num_end], 10) catch return null;
    if (num_end == s.len) return n; // bare seconds
    const unit = s[num_end..];
    if (std.mem.eql(u8, unit, "s")) return n;
    if (std.mem.eql(u8, unit, "m")) return n * 60;
    if (std.mem.eql(u8, unit, "h")) return n * 3600;
    if (std.mem.eql(u8, unit, "d")) return n * 86_400;
    return null;
}

// ---------------------------------------------------------------------------
// Parser helpers
// ---------------------------------------------------------------------------

fn parseMatchOp(input: []const u8, pos: *usize) !LabelMatcher.MatchOp {
    if (pos.* + 1 < input.len) {
        const two = input[pos.* .. pos.* + 2];
        if (std.mem.eql(u8, two, "=~")) {
            pos.* += 2;
            return .regex_match;
        } else if (std.mem.eql(u8, two, "!~")) {
            pos.* += 2;
            return .regex_not_match;
        } else if (std.mem.eql(u8, two, "!=")) {
            pos.* += 2;
            return .neq;
        }
    }
    if (pos.* < input.len and input[pos.*] == '=') {
        pos.* += 1;
        return .eq;
    }
    return error.InvalidLogQL;
}

fn parseQuotedString(input: []const u8, pos: *usize) ![]const u8 {
    if (pos.* >= input.len) return error.InvalidLogQL;

    const quote = input[pos.*];
    if (quote != '"' and quote != '`') return error.InvalidLogQL;
    pos.* += 1;

    const start = pos.*;
    while (pos.* < input.len) {
        if (input[pos.*] == '\\' and pos.* + 1 < input.len) {
            pos.* += 2; // skip escaped char
            continue;
        }
        if (input[pos.*] == quote) {
            const value = input[start..pos.*];
            pos.* += 1; // skip closing quote
            return value;
        }
        pos.* += 1;
    }
    return error.InvalidLogQL;
}

fn isLabelChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

// ---------------------------------------------------------------------------
// String matching utilities
// ---------------------------------------------------------------------------

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

/// Case-insensitive substring search (for external use).
fn containsSubstring(haystack: []const u8, needle: []const u8) bool {
    return containsInsensitive(haystack, needle);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "LogQL parse simple stream selector" {
    const allocator = std.testing.allocator;
    const expr = try parse(allocator, "{job=\"myapp\"}");
    defer free(allocator, &expr);

    try std.testing.expectEqual(@as(usize, 1), expr.matchers.len);
    try std.testing.expectEqualStrings("job", expr.matchers[0].name);
    try std.testing.expectEqualStrings("myapp", expr.matchers[0].value);
    try std.testing.expectEqual(LabelMatcher.MatchOp.eq, expr.matchers[0].op);
}

test "LogQL parse multiple matchers" {
    const allocator = std.testing.allocator;
    const expr = try parse(allocator, "{job=\"myapp\", level!=\"debug\"}");
    defer free(allocator, &expr);

    try std.testing.expectEqual(@as(usize, 2), expr.matchers.len);
    try std.testing.expectEqualStrings("level", expr.matchers[1].name);
    try std.testing.expectEqual(LabelMatcher.MatchOp.neq, expr.matchers[1].op);
}

test "LogQL parse with line filters" {
    const allocator = std.testing.allocator;
    const expr = try parse(allocator, "{job=\"myapp\"} |= \"error\" != \"timeout\"");
    defer free(allocator, &expr);

    try std.testing.expectEqual(@as(usize, 1), expr.matchers.len);
    try std.testing.expectEqual(@as(usize, 2), expr.line_filters.len);
    try std.testing.expectEqualStrings("error", expr.line_filters[0].pattern);
    try std.testing.expectEqual(LineFilter.FilterOp.contains, expr.line_filters[0].op);
    try std.testing.expectEqualStrings("timeout", expr.line_filters[1].pattern);
    try std.testing.expectEqual(LineFilter.FilterOp.not_contains, expr.line_filters[1].op);
}

test "LogQL label matching" {
    const allocator = std.testing.allocator;
    const expr = try parse(allocator, "{job=\"myapp\", level=\"error\"}");
    defer free(allocator, &expr);

    const labels = [_]LabelPair{
        .{ .name = "job", .value = "myapp" },
        .{ .name = "level", .value = "error" },
    };
    try std.testing.expect(expr.matchesLabels(&labels));

    const wrong_labels = [_]LabelPair{
        .{ .name = "job", .value = "otherapp" },
        .{ .name = "level", .value = "error" },
    };
    try std.testing.expect(!expr.matchesLabels(&wrong_labels));
}

test "LogQL line filter matching" {
    const allocator = std.testing.allocator;
    const expr = try parse(allocator, "{job=\"x\"} |= \"error\" != \"health\"");
    defer free(allocator, &expr);

    try std.testing.expect(expr.matchesLine("connection error occurred"));
    try std.testing.expect(!expr.matchesLine("info: health check ok"));
    try std.testing.expect(!expr.matchesLine("error in health check")); // contains both, but "health" excluded
}

test "LogQL empty selector" {
    const allocator = std.testing.allocator;
    const expr = try parse(allocator, "{}");
    defer free(allocator, &expr);

    try std.testing.expectEqual(@as(usize, 0), expr.matchers.len);
    // Empty selector should match everything.
    try std.testing.expect(expr.matchesLabels(&.{}));
}

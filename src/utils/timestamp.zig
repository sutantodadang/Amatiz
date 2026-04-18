// =============================================================================
// Amatiz — Timestamp Utilities
// =============================================================================
//
// Helpers for timestamp parsing, formatting, and date-directory naming.
// All timestamps internally are i64 nanoseconds since Unix epoch.
// =============================================================================

const std = @import("std");

/// Convert a nanosecond timestamp to a YYYY-MM-DD string for directory names.
pub fn toDayString(timestamp_ns: i64, buf: *[10]u8) void {
    const secs: u64 = @intCast(@divTrunc(timestamp_ns, std.time.ns_per_s));
    const epoch_secs = std.time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = epoch_secs.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    _ = std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{
        year_day.year,
        @as(u8, @intFromEnum(month_day.month)),
        @as(u8, month_day.day_index) + 1,
    }) catch unreachable;
}

/// Return current wall-clock time as nanoseconds since epoch.
pub fn nowNs() i64 {
    return @intCast(std.time.nanoTimestamp());
}

/// Return current wall-clock time as milliseconds since epoch.
pub fn nowMs() i64 {
    return @intCast(@divTrunc(std.time.nanoTimestamp(), std.time.ns_per_ms));
}

/// Parse an ISO-8601 date string "YYYY-MM-DD" into nanos-since-epoch (start of day UTC).
pub fn parseDate(date_str: []const u8) !i64 {
    if (date_str.len < 10) return error.InvalidDateFormat;

    const year = try std.fmt.parseInt(u16, date_str[0..4], 10);
    const month = try std.fmt.parseInt(u8, date_str[5..7], 10);
    const day = try std.fmt.parseInt(u8, date_str[8..10], 10);

    if (month < 1 or month > 12 or day < 1 or day > 31) return error.InvalidDateValue;

    // Days from epoch (1970-01-01).
    var total_days: i64 = 0;
    var y: u16 = 1970;
    while (y < year) : (y += 1) {
        total_days += if (isLeapYear(y)) @as(i64, 366) else 365;
    }
    const month_days = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        total_days += month_days[m - 1];
        if (m == 2 and isLeapYear(year)) total_days += 1;
    }
    total_days += day - 1;

    return total_days * std.time.ns_per_day;
}

/// Parse an ISO-8601 datetime "YYYY-MM-DDTHH:MM:SS" into nanos.
pub fn parseDatetime(dt: []const u8) !i64 {
    const day_ns = try parseDate(dt[0..10]);
    if (dt.len < 19) return day_ns;

    const hour = try std.fmt.parseInt(u8, dt[11..13], 10);
    const minute = try std.fmt.parseInt(u8, dt[14..16], 10);
    const second = try std.fmt.parseInt(u8, dt[17..19], 10);

    return day_ns +
        @as(i64, hour) * std.time.ns_per_hour +
        @as(i64, minute) * std.time.ns_per_min +
        @as(i64, second) * std.time.ns_per_s;
}

/// Format nanosecond timestamp as ISO-8601 "YYYY-MM-DDTHH:MM:SS.sssZ".
pub fn formatIso8601(timestamp_ns: i64, buf: *[24]u8) void {
    const abs: u64 = @intCast(if (timestamp_ns < 0) -timestamp_ns else timestamp_ns);
    const total_secs = abs / std.time.ns_per_s;
    const millis = (abs % std.time.ns_per_s) / std.time.ns_per_ms;

    const epoch_secs = std.time.epoch.EpochSeconds{ .secs = total_secs };
    const epoch_day = epoch_secs.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_secs = epoch_secs.getDaySeconds();

    _ = std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year_day.year,
        @as(u8, @intFromEnum(month_day.month)),
        @as(u8, month_day.day_index) + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
        @as(u32, @intCast(millis)),
    }) catch unreachable;
}

fn isLeapYear(year: u16) bool {
    if (year % 4 != 0) return false;
    if (year % 100 != 0) return true;
    return year % 400 == 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "toDayString" {
    // 2024-01-15 00:00:00 UTC  ~= 1705276800 seconds
    const ts: i64 = 1705276800 * std.time.ns_per_s;
    var buf: [10]u8 = undefined;
    toDayString(ts, &buf);
    try std.testing.expectEqualStrings("2024-01-15", &buf);
}

test "parseDate round-trip" {
    const ns = try parseDate("2024-06-15");
    var buf: [10]u8 = undefined;
    toDayString(ns, &buf);
    try std.testing.expectEqualStrings("2024-06-15", &buf);
}

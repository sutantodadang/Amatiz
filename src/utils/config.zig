// =============================================================================
// Amatiz — Configuration
// =============================================================================
//
// Loads settings from a JSON config file with sensible defaults. Every field
// has a default so Amatiz works out-of-the-box without any config file.
//
// Config search order:
//   1. Path passed via --config CLI flag
//   2. ./amatiz.json   (CWD)
//   3. /etc/amatiz/config.json
//   4. Built-in defaults
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const LogEntry = @import("types.zig").LogEntry;

/// One Promtail-style scrape config: a named job with paths to tail and the
/// static labels to attach to every entry from those paths.
///
/// Example JSON:
///   {
///     "job":    "varlogs",
///     "paths":  ["./logs/app.log", "./logs/sys.log"],
///     "labels": { "env": "prod", "service": "api" }
///   }
pub const ScrapeConfig = struct {
    job: []const u8,
    paths: []const []const u8,
    labels: []const LogEntry.Label,
};

/// Amatiz runtime configuration.
pub const Config = struct {
    // -- Storage ---------------------------------------------------------------
    data_dir: []const u8 = "./data",
    index_dir: []const u8 = "./index",
    chunk_max_bytes: u64 = 10 * 1024 * 1024,
    compression_enabled: bool = true,

    // -- Ingestion -------------------------------------------------------------
    /// Legacy flat list of files to tail. Empty = none.
    /// New deployments should prefer `scrape_configs`.
    watch_files: []const []const u8 = &.{},
    /// Promtail-style scrape configs.
    scrape_configs: []const ScrapeConfig = &.{},
    batch_size: u32 = 256,
    flush_interval_ms: u64 = 1000,
    tail_poll_ms: u64 = 250,
    /// File path used by the tailer to persist read offsets across restarts.
    /// Empty disables persistence.
    positions_path: []const u8 = "",

    // -- HTTP ------------------------------------------------------------------
    http_host: []const u8 = "0.0.0.0",
    http_port: u16 = 3100,
    max_connections: u32 = 128,

    // -- Query -----------------------------------------------------------------
    default_query_limit: usize = 1000,

    // -- General ---------------------------------------------------------------
    log_level: []const u8 = "info",

    /// Internal arena owning every dynamically allocated string/array on this
    /// Config. `null` for the all-defaults instance.
    arena: ?*std.heap.ArenaAllocator = null,

    pub fn deinit(self: *Config) void {
        if (self.arena) |a| {
            const child = a.child_allocator;
            a.deinit();
            child.destroy(a);
            self.arena = null;
        }
    }
};

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

pub fn load(allocator: Allocator, explicit_path: ?[]const u8) !Config {
    const candidates = [_][]const u8{
        "amatiz.json",
        "/etc/amatiz/config.json",
    };

    if (explicit_path) |path| {
        return loadFromFile(allocator, path) catch |err| {
            std.log.err("failed to load config from {s}: {}", .{ path, err });
            return err;
        };
    }

    for (candidates) |path| {
        if (loadFromFile(allocator, path)) |cfg| {
            std.log.info("loaded config from {s}", .{path});
            return cfg;
        } else |_| {}
    }

    std.log.info("no config file found — using built-in defaults", .{});
    return Config{};
}

fn loadFromFile(allocator: Allocator, path: []const u8) !Config {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const stat = try file.stat();
    if (stat.size > 1024 * 1024) return error.ConfigTooLarge;

    const content = try file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(content);

    return parseJsonOwned(allocator, content);
}

/// Parse JSON into a Config that owns its memory via an arena.
pub fn parseJsonOwned(allocator: Allocator, content: []const u8) !Config {
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer {
        arena.deinit();
        allocator.destroy(arena);
    }
    const arena_alloc = arena.allocator();

    var cfg = Config{};
    cfg.arena = arena;

    if (extractString(content, "data_dir")) |v| cfg.data_dir = try arena_alloc.dupe(u8, v);
    if (extractString(content, "index_dir")) |v| cfg.index_dir = try arena_alloc.dupe(u8, v);
    if (extractString(content, "http_host")) |v| cfg.http_host = try arena_alloc.dupe(u8, v);
    if (extractString(content, "log_level")) |v| cfg.log_level = try arena_alloc.dupe(u8, v);
    if (extractString(content, "positions_path")) |v| cfg.positions_path = try arena_alloc.dupe(u8, v);

    if (extractInt(content, "chunk_max_bytes")) |v| cfg.chunk_max_bytes = @intCast(v);
    if (extractInt(content, "batch_size")) |v| cfg.batch_size = @intCast(v);
    if (extractInt(content, "flush_interval_ms")) |v| cfg.flush_interval_ms = @intCast(v);
    if (extractInt(content, "tail_poll_ms")) |v| cfg.tail_poll_ms = @intCast(v);
    if (extractInt(content, "http_port")) |v| cfg.http_port = @intCast(v);
    if (extractInt(content, "max_connections")) |v| cfg.max_connections = @intCast(v);
    if (extractInt(content, "default_query_limit")) |v| cfg.default_query_limit = @intCast(v);

    if (extractBool(content, "compression_enabled")) |v| cfg.compression_enabled = v;

    cfg.watch_files = try parseStringArray(arena_alloc, content, "watch_files");
    cfg.scrape_configs = try parseScrapeConfigs(arena_alloc, content);

    return cfg;
}

/// Backward-compat thin wrapper used by the existing flat-fields test.
fn parseJson(content: []const u8) !Config {
    var cfg = Config{};
    cfg.data_dir = extractString(content, "data_dir") orelse cfg.data_dir;
    cfg.index_dir = extractString(content, "index_dir") orelse cfg.index_dir;
    cfg.http_host = extractString(content, "http_host") orelse cfg.http_host;
    cfg.log_level = extractString(content, "log_level") orelse cfg.log_level;

    if (extractInt(content, "chunk_max_bytes")) |v| cfg.chunk_max_bytes = @intCast(v);
    if (extractInt(content, "batch_size")) |v| cfg.batch_size = @intCast(v);
    if (extractInt(content, "flush_interval_ms")) |v| cfg.flush_interval_ms = @intCast(v);
    if (extractInt(content, "tail_poll_ms")) |v| cfg.tail_poll_ms = @intCast(v);
    if (extractInt(content, "http_port")) |v| cfg.http_port = @intCast(v);
    if (extractInt(content, "max_connections")) |v| cfg.max_connections = @intCast(v);
    if (extractInt(content, "default_query_limit")) |v| cfg.default_query_limit = @intCast(v);

    if (extractBool(content, "compression_enabled")) |v| cfg.compression_enabled = v;
    return cfg;
}

// ---------------------------------------------------------------------------
// Tiny JSON helpers — flat objects only.
// ---------------------------------------------------------------------------

fn extractString(json: []const u8, key: []const u8) ?[]const u8 {
    var pos: usize = 0;
    while (pos < json.len) {
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const after_q1 = q1 + 1;
            if (std.mem.indexOfPos(u8, json, after_q1, "\"")) |q2| {
                const found_key = json[after_q1..q2];
                if (std.mem.eql(u8, found_key, key)) {
                    var vp = q2 + 1;
                    while (vp < json.len and (json[vp] == ' ' or json[vp] == ':' or json[vp] == '\t')) : (vp += 1) {}
                    if (vp < json.len and json[vp] == '"') {
                        const vs = vp + 1;
                        if (std.mem.indexOfPos(u8, json, vs, "\"")) |ve| {
                            return json[vs..ve];
                        }
                    }
                }
                pos = q2 + 1;
            } else break;
        } else break;
    }
    return null;
}

fn extractInt(json: []const u8, key: []const u8) ?i64 {
    var pos: usize = 0;
    while (pos < json.len) {
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const after_q1 = q1 + 1;
            if (std.mem.indexOfPos(u8, json, after_q1, "\"")) |q2| {
                const found_key = json[after_q1..q2];
                if (std.mem.eql(u8, found_key, key)) {
                    var vp = q2 + 1;
                    while (vp < json.len and (json[vp] == ' ' or json[vp] == ':' or json[vp] == '\t' or json[vp] == '\n' or json[vp] == '\r')) : (vp += 1) {}
                    const start = vp;
                    while (vp < json.len and ((json[vp] >= '0' and json[vp] <= '9') or json[vp] == '-')) : (vp += 1) {}
                    if (vp > start) {
                        return std.fmt.parseInt(i64, json[start..vp], 10) catch null;
                    }
                }
                pos = q2 + 1;
            } else break;
        } else break;
    }
    return null;
}

fn extractBool(json: []const u8, key: []const u8) ?bool {
    var pos: usize = 0;
    while (pos < json.len) {
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const after_q1 = q1 + 1;
            if (std.mem.indexOfPos(u8, json, after_q1, "\"")) |q2| {
                const found_key = json[after_q1..q2];
                if (std.mem.eql(u8, found_key, key)) {
                    var vp = q2 + 1;
                    while (vp < json.len and (json[vp] == ' ' or json[vp] == ':' or json[vp] == '\t')) : (vp += 1) {}
                    if (vp + 4 <= json.len and std.mem.eql(u8, json[vp .. vp + 4], "true")) return true;
                    if (vp + 5 <= json.len and std.mem.eql(u8, json[vp .. vp + 5], "false")) return false;
                }
                pos = q2 + 1;
            } else break;
        } else break;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Array / nested object helpers (for scrape_configs)
// ---------------------------------------------------------------------------

fn locateValue(json: []const u8, key: []const u8) ?usize {
    var pos: usize = 0;
    while (pos < json.len) {
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const after_q1 = q1 + 1;
            if (std.mem.indexOfPos(u8, json, after_q1, "\"")) |q2| {
                const found = json[after_q1..q2];
                if (std.mem.eql(u8, found, key)) {
                    var vp = q2 + 1;
                    while (vp < json.len and (json[vp] == ' ' or json[vp] == ':' or json[vp] == '\t' or json[vp] == '\n' or json[vp] == '\r')) : (vp += 1) {}
                    return vp;
                }
                pos = q2 + 1;
            } else break;
        } else break;
    }
    return null;
}

fn matchBalanced(json: []const u8, start: usize, open: u8, close: u8) ?usize {
    if (start >= json.len or json[start] != open) return null;
    var depth: usize = 0;
    var i: usize = start;
    while (i < json.len) : (i += 1) {
        const c = json[i];
        if (c == '"') {
            i += 1;
            while (i < json.len) : (i += 1) {
                if (json[i] == '\\' and i + 1 < json.len) {
                    i += 1;
                    continue;
                }
                if (json[i] == '"') break;
            }
            if (i >= json.len) return null;
        } else if (c == open) {
            depth += 1;
        } else if (c == close) {
            depth -= 1;
            if (depth == 0) return i;
        }
    }
    return null;
}

fn parseStringArray(arena: Allocator, json: []const u8, key: []const u8) ![]const []const u8 {
    const after = locateValue(json, key) orelse return &.{};
    if (after >= json.len or json[after] != '[') return &.{};
    const close = matchBalanced(json, after, '[', ']') orelse return &.{};

    var out = std.ArrayList([]const u8).init(arena);
    errdefer out.deinit();

    var i = after + 1;
    while (i < close) {
        while (i < close and (json[i] == ' ' or json[i] == ',' or json[i] == '\n' or json[i] == '\r' or json[i] == '\t')) : (i += 1) {}
        if (i >= close) break;
        if (json[i] != '"') {
            i += 1;
            continue;
        }
        const vs = i + 1;
        var ve = vs;
        while (ve < close and json[ve] != '"') : (ve += 1) {
            if (json[ve] == '\\' and ve + 1 < close) ve += 1;
        }
        try out.append(try arena.dupe(u8, json[vs..ve]));
        i = ve + 1;
    }

    return out.toOwnedSlice();
}

fn parseLabelObject(arena: Allocator, json: []const u8, key: []const u8) ![]const LogEntry.Label {
    const after = locateValue(json, key) orelse return &.{};
    if (after >= json.len or json[after] != '{') return &.{};
    const close = matchBalanced(json, after, '{', '}') orelse return &.{};

    var out = std.ArrayList(LogEntry.Label).init(arena);
    errdefer out.deinit();

    var i = after + 1;
    while (i < close) {
        while (i < close and json[i] != '"') : (i += 1) {}
        if (i >= close) break;
        const ks = i + 1;
        var ke = ks;
        while (ke < close and json[ke] != '"') : (ke += 1) {
            if (json[ke] == '\\' and ke + 1 < close) ke += 1;
        }
        if (ke >= close) break;
        const k = json[ks..ke];

        i = ke + 1;
        while (i < close and (json[i] == ' ' or json[i] == ':' or json[i] == '\t' or json[i] == '\n' or json[i] == '\r')) : (i += 1) {}
        if (i >= close or json[i] != '"') break;
        const vs = i + 1;
        var ve = vs;
        while (ve < close and json[ve] != '"') : (ve += 1) {
            if (json[ve] == '\\' and ve + 1 < close) ve += 1;
        }
        if (ve >= close) break;
        const v = json[vs..ve];

        try out.append(.{
            .key = try arena.dupe(u8, k),
            .value = try arena.dupe(u8, v),
        });
        i = ve + 1;
    }

    return out.toOwnedSlice();
}

fn parseScrapeConfigs(arena: Allocator, json: []const u8) ![]const ScrapeConfig {
    const after = locateValue(json, "scrape_configs") orelse return &.{};
    if (after >= json.len or json[after] != '[') return &.{};
    const close = matchBalanced(json, after, '[', ']') orelse return &.{};

    var out = std.ArrayList(ScrapeConfig).init(arena);
    errdefer out.deinit();

    var i = after + 1;
    while (i < close) {
        while (i < close and json[i] != '{') : (i += 1) {}
        if (i >= close) break;
        const obj_close = matchBalanced(json, i, '{', '}') orelse break;
        const obj = json[i .. obj_close + 1];

        const job = extractString(obj, "job") orelse "default";
        const paths = try parseStringArray(arena, obj, "paths");
        const labels = try parseLabelObject(arena, obj, "labels");

        try out.append(.{
            .job = try arena.dupe(u8, job),
            .paths = paths,
            .labels = labels,
        });

        i = obj_close + 1;
    }

    return out.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "default config has sane values" {
    const cfg = Config{};
    try std.testing.expectEqual(@as(u16, 3100), cfg.http_port);
    try std.testing.expectEqual(@as(u64, 10 * 1024 * 1024), cfg.chunk_max_bytes);
}

test "parseJson extracts known fields" {
    const json =
        \\{
        \\  "data_dir": "/tmp/amatiz",
        \\  "http_port": 9090,
        \\  "compression_enabled": false
        \\}
    ;
    const cfg = try parseJson(json);
    try std.testing.expectEqualStrings("/tmp/amatiz", cfg.data_dir);
    try std.testing.expectEqual(@as(u16, 9090), cfg.http_port);
    try std.testing.expect(!cfg.compression_enabled);
}

test "parseJsonOwned parses scrape_configs" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "data_dir": "/tmp/x",
        \\  "scrape_configs": [
        \\    {
        \\      "job": "varlogs",
        \\      "paths": ["./logs/a.log", "./logs/b.log"],
        \\      "labels": { "env": "prod", "svc": "api" }
        \\    },
        \\    {
        \\      "job": "audit",
        \\      "paths": ["./logs/audit.log"],
        \\      "labels": {}
        \\    }
        \\  ]
        \\}
    ;
    var cfg = try parseJsonOwned(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("/tmp/x", cfg.data_dir);
    try std.testing.expectEqual(@as(usize, 2), cfg.scrape_configs.len);
    try std.testing.expectEqualStrings("varlogs", cfg.scrape_configs[0].job);
    try std.testing.expectEqual(@as(usize, 2), cfg.scrape_configs[0].paths.len);
    try std.testing.expectEqualStrings("./logs/a.log", cfg.scrape_configs[0].paths[0]);
    try std.testing.expectEqual(@as(usize, 2), cfg.scrape_configs[0].labels.len);
    try std.testing.expectEqualStrings("env", cfg.scrape_configs[0].labels[0].key);
    try std.testing.expectEqualStrings("prod", cfg.scrape_configs[0].labels[0].value);
    try std.testing.expectEqual(@as(usize, 0), cfg.scrape_configs[1].labels.len);
}

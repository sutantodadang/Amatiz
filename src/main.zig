// =============================================================================
// Amatiz — Main Entry Point
// =============================================================================
//
// Wires modules together, parses CLI, and dispatches to the appropriate
// command handler.  This is the only file with `pub fn main()`.
// =============================================================================

const std = @import("std");
const builtin = @import("builtin");

// -- Module imports ----------------------------------------------------------
const config_mod = @import("utils/config.zig");
const Config = config_mod.Config;
const types = @import("utils/types.zig");
const LogEntry = types.LogEntry;
const ts = @import("utils/timestamp.zig");
const BufferPool = @import("utils/buffer_pool.zig").BufferPool;
const StorageEngine = @import("storage/engine.zig").StorageEngine;
const Chunk = @import("storage/chunk.zig").Chunk;
const TimeIndex = @import("index/time_index.zig").TimeIndex;
const KeywordIndex = @import("index/keyword_index.zig").KeywordIndex;
const Pipeline = @import("ingest/pipeline.zig").Pipeline;
const Tailer = @import("ingest/tailer.zig").Tailer;
const Positions = @import("ingest/positions.zig").Positions;
const QueryEngine = @import("query/engine.zig").QueryEngine;
const HttpServer = @import("transport/server.zig").HttpServer;
const cli = @import("cli/args.zig");

const VERSION = "0.1.0";

pub fn main() !void {
    // Use a general-purpose allocator with safety checks in debug mode.
    var gpa: std.heap.GeneralPurposeAllocator(.{
        .enable_memory_limit = true,
    }) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Memory limit: 100 MiB target as per spec.
    gpa.requested_memory_limit = 100 * 1024 * 1024;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    const parsed = cli.parse(&args);

    switch (parsed.command) {
        .serve => try cmdServe(allocator, parsed),
        .ingest => try cmdIngest(allocator, parsed),
        .query => try cmdQuery(allocator, parsed),
        .tail => try cmdTail(allocator, parsed),
        .version => try cmdVersion(),
        .help => try cli.printUsage(std.io.getStdOut().writer()),
    }
}

// ===========================================================================
// Command: serve
// ===========================================================================

fn cmdServe(allocator: std.mem.Allocator, parsed: cli.ParsedArgs) !void {
    var cfg = try config_mod.load(allocator, parsed.config_path);
    defer cfg.deinit();

    const data_dir = parsed.data_dir orelse cfg.data_dir;
    const index_dir = parsed.index_dir orelse cfg.index_dir;
    const port = parsed.port orelse cfg.http_port;
    const host = parsed.host orelse cfg.http_host;

    var storage = try StorageEngine.init(
        allocator,
        data_dir,
        index_dir,
        cfg.chunk_max_bytes,
        cfg.compression_enabled,
    );
    defer storage.deinit();

    storage.enableWal() catch |err| {
        std.log.warn("WAL not available: {}", .{err});
    };

    var pipeline = Pipeline.init(allocator, &storage, cfg.batch_size, cfg.flush_interval_ms);
    defer pipeline.deinit();

    var tailer = Tailer.init(allocator, &pipeline, cfg.tail_poll_ms);
    defer tailer.deinit();

    // Set up positions file (defaults to <index_dir>/positions.json when not
    // explicitly configured).
    var positions_buf: [1024]u8 = undefined;
    const positions_path = if (cfg.positions_path.len > 0)
        cfg.positions_path
    else
        try std.fmt.bufPrint(&positions_buf, "{s}/positions.json", .{index_dir});

    var positions = try Positions.init(allocator, positions_path);
    defer positions.deinit();
    positions.load();
    tailer.setPositions(&positions);

    // Wire scrape configs: each path → its label set, then start tailing.
    for (cfg.scrape_configs) |sc| {
        for (sc.paths) |file_path| {
            // Build the per-source label set: configured labels + auto-attached
            // {job, filename} (so Loki queries `{job="x"}` work without users
            // having to hand-add them).
            var labels = std.ArrayList(LogEntry.Label).init(allocator);
            errdefer labels.deinit();
            try labels.append(.{ .key = "job", .value = sc.job });
            try labels.append(.{ .key = "filename", .value = file_path });
            for (sc.labels) |lbl| try labels.append(lbl);
            const owned = try labels.toOwnedSlice();
            // Note: `owned` is leaked at process exit on purpose — pipeline
            // borrows the slice and the OS reclaims memory on shutdown.
            try pipeline.setSourceLabels(file_path, owned);

            tailer.tailFile(file_path) catch |err| {
                std.log.warn("could not tail {s}: {}", .{ file_path, err });
            };
        }
    }

    // Legacy flat watch_files (no labels).
    for (cfg.watch_files) |file_path| {
        tailer.tailFile(file_path) catch |err| {
            std.log.warn("could not tail {s}: {}", .{ file_path, err });
        };
    }

    var server = HttpServer.init(
        allocator,
        &storage,
        &pipeline,
        host,
        port,
        cfg.max_connections,
    );

    const stdout = std.io.getStdOut().writer();
    try stdout.print(
        \\
        \\ ╔══════════════════════════════════════╗
        \\ ║          Amatiz v{s}              ║
        \\ ║   High-performance log aggregator    ║
        \\ ╚══════════════════════════════════════╝
        \\
        \\ → HTTP:      http://{s}:{d}
        \\ → Data:      {s}
        \\ → Index:     {s}
        \\ → Positions: {s}
        \\
    , .{ VERSION, host, port, data_dir, index_dir, positions_path });

    try server.serve();
}

// ===========================================================================
// Command: ingest
// ===========================================================================

fn cmdIngest(allocator: std.mem.Allocator, parsed: cli.ParsedArgs) !void {
    const file_path = parsed.ingest_file orelse {
        std.log.err("missing file path. Usage: amatiz ingest <file>", .{});
        return error.MissingArgument;
    };

    var cfg = try config_mod.load(allocator, parsed.config_path);
    defer cfg.deinit();
    const data_dir = parsed.data_dir orelse cfg.data_dir;
    const index_dir = parsed.index_dir orelse cfg.index_dir;

    var storage = try StorageEngine.init(
        allocator,
        data_dir,
        index_dir,
        cfg.chunk_max_bytes,
        cfg.compression_enabled,
    );
    defer storage.deinit();

    storage.enableWal() catch |err| {
        std.log.warn("WAL not available: {}", .{err});
    };

    var pipeline = Pipeline.init(allocator, &storage, cfg.batch_size, cfg.flush_interval_ms);
    defer pipeline.deinit();

    var tailer = Tailer.init(allocator, &pipeline, cfg.tail_poll_ms);
    defer tailer.deinit();

    const stdout = std.io.getStdOut().writer();
    try stdout.print("amatiz: tailing {s} → {s}\n", .{ file_path, data_dir });
    try stdout.print("press Ctrl+C to stop\n", .{});

    try tailer.tailFile(file_path);

    // Block until interrupted (Ctrl+C).
    while (true) {
        std.time.sleep(1 * std.time.ns_per_s);

        // Print periodic stats.
        const metrics = storage.getMetrics();
        try stdout.print("\r  entries: {d}  bytes: {d}  chunks: {d}    ", .{
            metrics.entries,
            metrics.bytes,
            metrics.chunks,
        });
    }
}

// ===========================================================================
// Command: query
// ===========================================================================

fn cmdQuery(allocator: std.mem.Allocator, parsed: cli.ParsedArgs) !void {
    var cfg = try config_mod.load(allocator, parsed.config_path);
    defer cfg.deinit();
    const data_dir = parsed.data_dir orelse cfg.data_dir;
    const index_dir = parsed.index_dir orelse cfg.index_dir;

    var storage = try StorageEngine.init(
        allocator,
        data_dir,
        index_dir,
        cfg.chunk_max_bytes,
        cfg.compression_enabled,
    );
    defer storage.deinit();

    var qe = QueryEngine.init(allocator, &storage);

    var params = types.QueryParams{
        .keyword = parsed.query_keyword,
        .limit = parsed.query_limit orelse cfg.default_query_limit,
    };

    if (parsed.query_from) |from_str| {
        params.from_ns = ts.parseDatetime(from_str) catch ts.parseDate(from_str) catch null;
    }
    if (parsed.query_to) |to_str| {
        params.to_ns = ts.parseDatetime(to_str) catch ts.parseDate(to_str) catch null;
    }

    const results = try qe.execute(params);
    defer qe.freeResults(results);

    const stdout = std.io.getStdOut().writer();
    try stdout.print("found {d} results\n", .{results.len});
    try stdout.print("{s:—<80}\n", .{""});

    for (results) |entry| {
        // Format timestamp.
        var ts_buf: [24]u8 = undefined;
        ts.formatIso8601(entry.timestamp_ns, &ts_buf);
        try stdout.print("[{s}] {s}: {s}\n", .{ &ts_buf, entry.source, entry.message });
    }

    try stdout.print("{s:—<80}\n", .{""});
    try stdout.print("{d} results\n", .{results.len});
}

// ===========================================================================
// Command: tail
// ===========================================================================

fn cmdTail(allocator: std.mem.Allocator, parsed: cli.ParsedArgs) !void {
    const host = parsed.tail_host orelse "127.0.0.1";
    const port = parsed.tail_port orelse 3100;

    const stdout = std.io.getStdOut().writer();
    try stdout.print("connecting to {s}:{d}/tail ...\n", .{ host, port });

    const address = try std.net.Address.parseIp(host, port);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Send HTTP GET request.
    const request = "GET /tail HTTP/1.1\r\nHost: " ++ "amatiz" ++ "\r\nAccept: text/event-stream\r\n\r\n";
    _ = try stream.write(request);

    // Read and display the stream.
    var buf: [8192]u8 = undefined;
    var skip_headers = true;

    while (true) {
        const n = stream.read(&buf) catch |err| {
            try stdout.print("\nconnection closed: {}\n", .{err});
            break;
        };
        if (n == 0) {
            try stdout.print("\nconnection closed by server\n", .{});
            break;
        }

        const data = buf[0..n];

        // Skip HTTP response headers.
        if (skip_headers) {
            if (std.mem.indexOf(u8, data, "\r\n\r\n")) |end| {
                skip_headers = false;
                if (end + 4 < n) {
                    try stdout.writeAll(data[end + 4 ..]);
                }
            }
            continue;
        }

        try stdout.writeAll(data);
    }

    _ = allocator;
}

// ===========================================================================
// Command: version
// ===========================================================================

fn cmdVersion() !void {
    const stdout = std.io.getStdOut().writer();
    try stdout.print("amatiz v{s}\n", .{VERSION});
    try stdout.print("zig {}\n", .{builtin.zig_version});
    try stdout.print("target: {s}-{s}\n", .{
        @tagName(builtin.cpu.arch),
        @tagName(builtin.os.tag),
    });
}

// ===========================================================================
// Tests — pull in all modules so `zig build test` exercises everything.
// ===========================================================================

test {
    // Force the test runner to analyze and test all referenced modules.
    _ = @import("utils/types.zig");
    _ = @import("utils/config.zig");
    _ = @import("utils/buffer_pool.zig");
    _ = @import("utils/timestamp.zig");
    _ = @import("storage/chunk.zig");
    _ = @import("storage/engine.zig");
    _ = @import("index/time_index.zig");
    _ = @import("index/keyword_index.zig");
    _ = @import("index/label_index.zig");
    _ = @import("ingest/pipeline.zig");
    _ = @import("ingest/tailer.zig");
    _ = @import("ingest/positions.zig");
    _ = @import("storage/wal.zig");
    _ = @import("query/engine.zig");
    _ = @import("transport/server.zig");
    _ = @import("cli/args.zig");
    _ = @import("loki/logql.zig");
    _ = @import("loki/push.zig");
    _ = @import("loki/responses.zig");
}

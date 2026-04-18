// =============================================================================
// Amatiz — HTTP Transport Server
// =============================================================================
//
// Lightweight HTTP/1.1 server built on raw TCP (std.net).
//
// Endpoints:
//   POST /ingest     — receive JSON log entries
//   GET  /query      — search logs by time & keyword
//   GET  /tail       — live-stream new log entries (chunked transfer)
//   GET  /metrics    — basic stats (entries, bytes, uptime)
//   GET  /health     — health check
//
// Concurrency: one OS thread per connection, capped at max_connections.
// Connections are short-lived except /tail which stays open.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;
const QueryParams = types.QueryParams;
const StorageEngine = @import("../storage/engine.zig").StorageEngine;
const Pipeline = @import("../ingest/pipeline.zig").Pipeline;
const QueryEngine = @import("../query/engine.zig").QueryEngine;
const ts = @import("../utils/timestamp.zig");
const logql = @import("../loki/logql.zig");
const push_mod = @import("../loki/push.zig");
const responses = @import("../loki/responses.zig");

const ui_html = @embedFile("../ui/index.html");

pub const HttpServer = struct {
    allocator: Allocator,
    storage: *StorageEngine,
    pipeline: *Pipeline,
    query_engine: QueryEngine,
    host: []const u8,
    port: u16,
    max_connections: u32,
    active_connections: std.atomic.Value(u32),
    should_stop: std.atomic.Value(bool),
    start_time_ns: i64,

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(
        allocator: Allocator,
        storage: *StorageEngine,
        pipeline: *Pipeline,
        host: []const u8,
        port: u16,
        max_connections: u32,
    ) HttpServer {
        return .{
            .allocator = allocator,
            .storage = storage,
            .pipeline = pipeline,
            .query_engine = QueryEngine.init(allocator, storage),
            .host = host,
            .port = port,
            .max_connections = max_connections,
            .active_connections = std.atomic.Value(u32).init(0),
            .should_stop = std.atomic.Value(bool).init(false),
            .start_time_ns = ts.nowNs(),
        };
    }

    /// Start the server. Blocks until stop() is called.
    pub fn serve(self: *HttpServer) !void {
        const address = try std.net.Address.parseIp(self.host, self.port);
        var server = try address.listen(.{
            .reuse_address = true,
        });
        defer server.deinit();

        std.log.info("HTTP server listening on {s}:{d}", .{ self.host, self.port });

        while (!self.should_stop.load(.acquire)) {
            // Accept with a timeout so we can check should_stop.
            const conn = server.accept() catch |err| {
                if (err == error.WouldBlock) {
                    std.time.sleep(10 * std.time.ns_per_ms);
                    continue;
                }
                std.log.err("accept error: {}", .{err});
                continue;
            };

            // Enforce connection limit.
            if (self.active_connections.load(.acquire) >= self.max_connections) {
                sendError(conn.stream, 503, "Too many connections") catch {};
                conn.stream.close();
                continue;
            }

            _ = self.active_connections.fetchAdd(1, .release);

            // Spawn handler thread.
            const ctx = self.allocator.create(ConnContext) catch {
                _ = self.active_connections.fetchSub(1, .release);
                conn.stream.close();
                continue;
            };
            ctx.* = .{
                .server = self,
                .stream = conn.stream,
            };

            _ = std.Thread.spawn(.{}, handleConnection, .{ctx}) catch {
                self.allocator.destroy(ctx);
                _ = self.active_connections.fetchSub(1, .release);
                conn.stream.close();
            };
        }
    }

    pub fn stop(self: *HttpServer) void {
        self.should_stop.store(true, .release);
    }

    // -----------------------------------------------------------------------
    // Connection handling
    // -----------------------------------------------------------------------

    const ConnContext = struct {
        server: *HttpServer,
        stream: std.net.Stream,
    };

    fn handleConnection(ctx: *ConnContext) void {
        defer {
            ctx.stream.close();
            _ = ctx.server.active_connections.fetchSub(1, .release);
            ctx.server.allocator.destroy(ctx);
        }

        handleRequest(ctx.server, ctx.stream) catch |err| {
            std.log.warn("request handling error: {}", .{err});
        };
    }

    fn handleRequest(self: *HttpServer, stream: std.net.Stream) !void {
        var buf: [8192]u8 = undefined;
        const n = try stream.read(&buf);
        if (n == 0) return;

        const request = buf[0..n];

        // Parse request line.
        const method_end = std.mem.indexOfScalar(u8, request, ' ') orelse return;
        const method = request[0..method_end];

        const path_start = method_end + 1;
        const path_end = std.mem.indexOfPos(u8, request, path_start, " ") orelse return;
        const full_path = request[path_start..path_end];

        // Split path and query string.
        var path: []const u8 = full_path;
        var query_string: []const u8 = "";
        if (std.mem.indexOfScalar(u8, full_path, '?')) |q_pos| {
            path = full_path[0..q_pos];
            query_string = full_path[q_pos + 1 ..];
        }

        // Find body (after \r\n\r\n).
        var body: []const u8 = "";
        var headers_blob: []const u8 = "";
        if (std.mem.indexOf(u8, request, "\r\n\r\n")) |header_end| {
            body = request[header_end + 4 ..];
            headers_blob = request[0..header_end];
        }

        // Resolve tenant ("X-Scope-OrgID"; default "default").
        const tenant = parseScopeOrgID(headers_blob);

        // Route.
        if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/ingest")) {
            try self.handleIngest(stream, body);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/query")) {
            try self.handleQuery(stream, query_string);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/tail")) {
            try self.handleTailDispatch(stream, headers_blob);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/metrics")) {
            try self.handleMetrics(stream);
            // -- Loki-compatible API routes -------------------------------------------
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/loki/api/v1/push")) {
            try self.handleLokiPush(stream, body, tenant);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/loki/api/v1/query_range")) {
            try self.handleLokiQueryRange(stream, query_string, tenant);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/loki/api/v1/query")) {
            try self.handleLokiQuery(stream, query_string, tenant);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/loki/api/v1/labels")) {
            try self.handleLokiLabels(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/loki/api/v1/label/")) {
            try self.handleLokiLabelValues(stream, path);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/loki/api/v1/series")) {
            try self.handleLokiSeries(stream, query_string, tenant);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/loki/api/v1/tail")) {
            try self.handleTailDispatch(stream, headers_blob);
        } else if (std.mem.eql(u8, method, "GET") and (std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, "/ui") or std.mem.eql(u8, path, "/ui/"))) {
            try sendResponseWithCors(stream, 200, "text/html; charset=utf-8", ui_html);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/ready")) {
            try sendResponse(stream, 200, "text/plain", "ready");
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/config")) {
            try sendResponse(stream, 200, "application/yaml", "---\n# amatiz config\n");
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/health")) {
            try sendResponse(stream, 200, "application/json", "{\"status\":\"ok\"}");
        } else {
            try sendError(stream, 404, "Not Found");
        }
    }

    // -----------------------------------------------------------------------
    // POST /ingest
    // -----------------------------------------------------------------------

    fn handleIngest(self: *HttpServer, stream: std.net.Stream, body: []const u8) !void {
        if (body.len == 0) {
            try sendError(stream, 400, "Empty body");
            return;
        }

        // Accept either a single JSON log line or an array of lines.
        // For simplicity, treat the body as a set of newline-delimited log lines.
        var iter = std.mem.splitScalar(u8, body, '\n');
        var count: u32 = 0;
        while (iter.next()) |line| {
            if (line.len == 0) continue;
            try self.pipeline.ingest("http", line);
            count += 1;
        }

        // Force flush.
        try self.pipeline.flush();

        var resp_buf: [128]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"ingested\":{d}}}", .{count}) catch unreachable;
        try sendResponse(stream, 200, "application/json", resp);
    }

    // -----------------------------------------------------------------------
    // GET /query?q=error&from=2024-01-01&to=2024-01-02&limit=100
    // -----------------------------------------------------------------------

    fn handleQuery(self: *HttpServer, stream: std.net.Stream, query_string: []const u8) !void {
        var params = QueryParams{};

        // Parse query string parameters.
        var qs_iter = std.mem.splitScalar(u8, query_string, '&');
        while (qs_iter.next()) |param| {
            if (std.mem.indexOfScalar(u8, param, '=')) |eq| {
                const key = param[0..eq];
                const value = param[eq + 1 ..];

                if (std.mem.eql(u8, key, "q") or std.mem.eql(u8, key, "keyword")) {
                    params.keyword = value;
                } else if (std.mem.eql(u8, key, "from")) {
                    params.from_ns = ts.parseDatetime(value) catch ts.parseDate(value) catch null;
                } else if (std.mem.eql(u8, key, "to")) {
                    params.to_ns = ts.parseDatetime(value) catch ts.parseDate(value) catch null;
                } else if (std.mem.eql(u8, key, "limit")) {
                    params.limit = std.fmt.parseInt(usize, value, 10) catch 1000;
                }
            }
        }

        const results = try self.query_engine.execute(params);
        defer self.query_engine.freeResults(results);

        // Build JSON response.
        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();

        try json_buf.appendSlice("{\"count\":");
        var count_buf: [32]u8 = undefined;
        const count_str = std.fmt.bufPrint(&count_buf, "{d}", .{results.len}) catch unreachable;
        try json_buf.appendSlice(count_str);
        try json_buf.appendSlice(",\"results\":[");

        for (results, 0..) |entry, i| {
            if (i > 0) try json_buf.append(',');
            try appendEntryJson(&json_buf, &entry);
        }

        try json_buf.appendSlice("]}");
        try sendResponse(stream, 200, "application/json", json_buf.items);
    }

    // -----------------------------------------------------------------------
    // GET /tail dispatcher — picks WebSocket vs SSE based on Upgrade header.
    // -----------------------------------------------------------------------

    fn handleTailDispatch(self: *HttpServer, stream: std.net.Stream, headers_blob: []const u8) !void {
        if (extractHeader(headers_blob, "Upgrade")) |upgrade| {
            if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, upgrade, " \t"), "websocket")) {
                if (extractHeader(headers_blob, "Sec-WebSocket-Key")) |raw_key| {
                    const key = std.mem.trim(u8, raw_key, " \t");
                    return self.handleWebSocketTail(stream, key);
                }
            }
        }
        return self.handleTail(stream);
    }

    // -----------------------------------------------------------------------
    // WebSocket tail (RFC 6455) — Grafana Explore live tail
    // -----------------------------------------------------------------------

    fn handleWebSocketTail(self: *HttpServer, stream: std.net.Stream, key: []const u8) !void {
        // Compute Sec-WebSocket-Accept = base64(SHA1(key + magic_guid)).
        const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        var concat_buf: [256]u8 = undefined;
        if (key.len + magic.len > concat_buf.len) {
            try sendError(stream, 400, "WebSocket key too long");
            return;
        }
        @memcpy(concat_buf[0..key.len], key);
        @memcpy(concat_buf[key.len .. key.len + magic.len], magic);

        var hash: [std.crypto.hash.Sha1.digest_length]u8 = undefined;
        std.crypto.hash.Sha1.hash(concat_buf[0 .. key.len + magic.len], &hash, .{});

        var accept_buf: [64]u8 = undefined;
        const accept = std.base64.standard.Encoder.encode(&accept_buf, &hash);

        var resp_buf: [256]u8 = undefined;
        const resp = try std.fmt.bufPrint(&resp_buf,
            "HTTP/1.1 101 Switching Protocols\r\n" ++
                "Upgrade: websocket\r\n" ++
                "Connection: Upgrade\r\n" ++
                "Sec-WebSocket-Accept: {s}\r\n\r\n",
            .{accept},
        );
        _ = try stream.write(resp);

        var ctx = WsTailContext{
            .stream = stream,
            .alive = std.atomic.Value(bool).init(true),
            .write_mu = .{},
            .allocator = self.allocator,
        };

        try self.storage.subscribe(.{
            .ctx = @ptrCast(&ctx),
            .notify = wsTailNotify,
        });
        defer self.storage.unsubscribe(@ptrCast(&ctx));

        // Idle loop. We don't read client frames here — clients typically only
        // send close/ping, and a closed socket will surface as a write error
        // on the next notify.
        while (ctx.alive.load(.acquire) and !self.should_stop.load(.acquire)) {
            std.time.sleep(500 * std.time.ns_per_ms);
        }
    }

    const WsTailContext = struct {
        stream: std.net.Stream,
        alive: std.atomic.Value(bool),
        write_mu: std.Thread.Mutex,
        allocator: Allocator,
    };

    fn wsTailNotify(ctx_raw: *anyopaque, entries: []const LogEntry) void {
        const ctx: *WsTailContext = @ptrCast(@alignCast(ctx_raw));
        if (!ctx.alive.load(.acquire)) return;

        for (entries) |entry| {
            // Build a Loki-tail-shaped JSON payload.
            var buf = std.ArrayList(u8).init(ctx.allocator);
            defer buf.deinit();
            const w = buf.writer();
            w.print("{{\"streams\":[{{\"stream\":{{\"source\":\"", .{}) catch continue;
            jsonEscape(w, entry.source) catch continue;
            w.writeAll("\"},\"values\":[[\"") catch continue;
            w.print("{d}", .{entry.timestamp_ns}) catch continue;
            w.writeAll("\",\"") catch continue;
            jsonEscape(w, entry.message) catch continue;
            w.writeAll("\"]]}]}") catch continue;

            ctx.write_mu.lock();
            defer ctx.write_mu.unlock();
            writeWsTextFrame(ctx.stream, buf.items) catch {
                ctx.alive.store(false, .release);
                return;
            };
        }
    }

    fn writeWsTextFrame(stream: std.net.Stream, payload: []const u8) !void {
        var header: [10]u8 = undefined;
        header[0] = 0x81; // FIN | opcode=1 (text)
        var hlen: usize = 1;
        if (payload.len <= 125) {
            header[1] = @intCast(payload.len);
            hlen = 2;
        } else if (payload.len <= 0xFFFF) {
            header[1] = 126;
            std.mem.writeInt(u16, header[2..4], @intCast(payload.len), .big);
            hlen = 4;
        } else {
            header[1] = 127;
            std.mem.writeInt(u64, header[2..10], payload.len, .big);
            hlen = 10;
        }
        _ = try stream.write(header[0..hlen]);
        _ = try stream.write(payload);
    }

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------

    fn handleTail(self: *HttpServer, stream: std.net.Stream) !void {
        // Send response headers for chunked transfer.
        const header =
            "HTTP/1.1 200 OK\r\n" ++
            "Content-Type: text/event-stream\r\n" ++
            "Transfer-Encoding: chunked\r\n" ++
            "Cache-Control: no-cache\r\n" ++
            "Connection: keep-alive\r\n" ++
            "\r\n";
        _ = try stream.write(header);

        // Register as a tail subscriber.
        var ctx = TailStreamContext{
            .stream = stream,
            .alive = std.atomic.Value(bool).init(true),
            .allocator = self.allocator,
        };

        try self.storage.subscribe(.{
            .ctx = @ptrCast(&ctx),
            .notify = tailNotify,
        });
        defer self.storage.unsubscribe(@ptrCast(&ctx));

        // Keep connection open until client disconnects.
        while (ctx.alive.load(.acquire) and !self.should_stop.load(.acquire)) {
            std.time.sleep(100 * std.time.ns_per_ms);

            // Send keep-alive (empty chunk comment).
            _ = stream.write("0\r\n\r\n") catch {
                // Broken pipe — client disconnected. We use the empty chunk
                // as a probe; actual disconnect triggers write error.
                break;
            };
        }
    }

    const TailStreamContext = struct {
        stream: std.net.Stream,
        alive: std.atomic.Value(bool),
        allocator: Allocator,
    };

    fn tailNotify(ctx_raw: *anyopaque, entries: []const LogEntry) void {
        const ctx: *TailStreamContext = @ptrCast(@alignCast(ctx_raw));

        for (entries) |*entry| {
            var buf: [8192]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            const w = fbs.writer();

            // Build SSE-style event data.
            w.print("data: {{\"ts\":{d},\"src\":\"", .{entry.timestamp_ns}) catch continue;
            w.writeAll(entry.source) catch continue;
            w.writeAll("\",\"msg\":\"") catch continue;
            // Escape quotes in message.
            for (entry.message) |c| {
                if (c == '"') {
                    w.writeAll("\\\"") catch continue;
                } else if (c == '\\') {
                    w.writeAll("\\\\") catch continue;
                } else {
                    w.writeByte(c) catch continue;
                }
            }
            w.writeAll("\"}") catch continue;
            w.writeAll("\n\n") catch continue;

            const data = fbs.getWritten();

            // Chunked encoding: size in hex + \r\n + data + \r\n.
            var chunk_header: [32]u8 = undefined;
            const ch_len = std.fmt.bufPrint(&chunk_header, "{x}\r\n", .{data.len}) catch continue;

            _ = ctx.stream.write(ch_len) catch {
                ctx.alive.store(false, .release);
                return;
            };
            _ = ctx.stream.write(data) catch {
                ctx.alive.store(false, .release);
                return;
            };
            _ = ctx.stream.write("\r\n") catch {
                ctx.alive.store(false, .release);
                return;
            };
        }
    }

    // -----------------------------------------------------------------------
    // GET /metrics (JSON format, legacy)
    // -----------------------------------------------------------------------

    fn handleMetrics(self: *HttpServer, stream: std.net.Stream) !void {
        const m = self.storage.getMetrics();
        const uptime_s: u64 = @intCast(@divTrunc(ts.nowNs() - self.start_time_ns, std.time.ns_per_s));

        // Prometheus exposition format for Grafana compatibility.
        const prom_body = responses.formatPrometheusMetrics(
            self.allocator,
            m.entries,
            m.bytes,
            m.chunks,
            uptime_s,
            self.active_connections.load(.acquire),
        ) catch {
            // Fall back to JSON.
            var buf: [512]u8 = undefined;
            const body = std.fmt.bufPrint(&buf,
                \\{{"total_entries":{d},"total_bytes":{d},"chunks":{d},"uptime_s":{d},"connections":{d}}}
            , .{
                m.entries,
                m.bytes,
                m.chunks,
                uptime_s,
                self.active_connections.load(.acquire),
            }) catch unreachable;
            try sendResponse(stream, 200, "application/json", body);
            return;
        };
        defer self.allocator.free(prom_body);
        try sendResponse(stream, 200, "text/plain; version=0.0.4; charset=utf-8", prom_body);
    }

    // -----------------------------------------------------------------------
    // POST /loki/api/v1/push — Loki-compatible log ingestion
    // -----------------------------------------------------------------------

    fn handleLokiPush(self: *HttpServer, stream: std.net.Stream, body: []const u8, tenant: []const u8) !void {
        if (body.len == 0) {
            const err_body = responses.formatErrorResponse(self.allocator, "empty request body") catch {
                try sendError(stream, 400, "Empty body");
                return;
            };
            defer self.allocator.free(err_body);
            try sendResponse(stream, 400, "application/json", err_body);
            return;
        }

        var req = push_mod.parsePushBody(self.allocator, body) catch {
            const err_body = responses.formatErrorResponse(self.allocator, "invalid push request JSON") catch {
                try sendError(stream, 400, "Invalid JSON");
                return;
            };
            defer self.allocator.free(err_body);
            try sendResponse(stream, 400, "application/json", err_body);
            return;
        };
        defer req.deinit();

        // Ingest each parsed entry through the pipeline, tagging with tenant.
        var count: u32 = 0;
        for (req.entries) |entry| {
            self.pipeline.ingestWithTenant(entry.source, entry.line, tenant) catch continue;
            count += 1;
        }

        // Force flush.
        self.pipeline.flush() catch {};

        // Loki push returns 204 No Content on success.
        try sendResponseNoBody(stream, 204);
    }

    // -----------------------------------------------------------------------
    // GET /loki/api/v1/query_range — LogQL range query
    // -----------------------------------------------------------------------

    fn handleLokiQueryRange(self: *HttpServer, stream: std.net.Stream, query_string: []const u8, tenant: []const u8) !void {
        // Parse query parameters: query, start, end, limit, direction, step.
        var logql_query: []const u8 = "{}";
        var from_ns: ?i64 = null;
        var to_ns: ?i64 = null;
        var limit: usize = 1000;
        var direction: logql.Direction = .backward;
        var step_seconds: u64 = 60;

        var qs_iter = std.mem.splitScalar(u8, query_string, '&');
        while (qs_iter.next()) |param| {
            if (std.mem.indexOfScalar(u8, param, '=')) |eq| {
                const key = param[0..eq];
                const value = param[eq + 1 ..];

                if (std.mem.eql(u8, key, "query")) {
                    const decoded = urlDecode(self.allocator, value) catch null;
                    logql_query = decoded orelse value;
                } else if (std.mem.eql(u8, key, "start")) {
                    from_ns = parseLokiTimestamp(value);
                } else if (std.mem.eql(u8, key, "end")) {
                    to_ns = parseLokiTimestamp(value);
                } else if (std.mem.eql(u8, key, "limit")) {
                    limit = std.fmt.parseInt(usize, value, 10) catch 1000;
                } else if (std.mem.eql(u8, key, "direction")) {
                    if (std.mem.eql(u8, value, "forward")) direction = .forward;
                } else if (std.mem.eql(u8, key, "step")) {
                    step_seconds = logql.parseDurationSeconds(value) orelse
                        (std.fmt.parseInt(u64, value, 10) catch 60);
                }
            }
        }

        // Parse the LogQL expression.
        var expr = logql.parse(self.allocator, logql_query) catch {
            const err_body = responses.formatErrorResponse(self.allocator, "invalid LogQL expression") catch {
                try sendError(stream, 400, "Invalid LogQL");
                return;
            };
            defer self.allocator.free(err_body);
            try sendResponse(stream, 400, "application/json", err_body);
            return;
        };
        defer logql.free(self.allocator, &expr);

        // Inject the tenant matcher so each request only sees its own data.
        try injectTenantMatcher(self.allocator, &expr, tenant);

        if (expr.limit > 0 and expr.limit < limit) limit = expr.limit;

        const params = QueryParams{
            .from_ns = from_ns,
            .to_ns = to_ns,
            .limit = limit,
        };

        _ = &direction; // direction handled at result-formatting time later

        // Metric query → Prometheus matrix response.
        if (expr.kind == .metric) {
            var mres = self.query_engine.executeMetric(&expr, params, step_seconds) catch |err| {
                std.log.warn("metric query failed: {}", .{err});
                const err_body = responses.formatErrorResponse(self.allocator, "metric query failed") catch {
                    try sendError(stream, 500, "Metric query failed");
                    return;
                };
                defer self.allocator.free(err_body);
                try sendResponse(stream, 500, "application/json", err_body);
                return;
            };
            defer mres.deinit(self.allocator);

            const resp_body = responses.formatMatrixResponse(self.allocator, &mres) catch {
                try sendError(stream, 500, "response formatting failed");
                return;
            };
            defer self.allocator.free(resp_body);
            try sendResponseWithCors(stream, 200, "application/json", resp_body);
            return;
        }

        // Streams query.
        const results = self.query_engine.executeLogQL(&expr, params) catch |err| {
            std.log.warn("query execution error: {}", .{err});
            const err_body = responses.formatErrorResponse(self.allocator, "query execution failed") catch {
                try sendError(stream, 500, "Query failed");
                return;
            };
            defer self.allocator.free(err_body);
            try sendResponse(stream, 500, "application/json", err_body);
            return;
        };
        defer self.query_engine.freeResults(results);

        // Format as Loki streams response.
        const resp_body = responses.formatLogEntriesAsStreams(self.allocator, results) catch {
            try sendError(stream, 500, "response formatting failed");
            return;
        };
        defer self.allocator.free(resp_body);

        try sendResponseWithCors(stream, 200, "application/json", resp_body);
    }

    // -----------------------------------------------------------------------
    // GET /loki/api/v1/query — LogQL instant query
    // -----------------------------------------------------------------------

    fn handleLokiQuery(self: *HttpServer, stream: std.net.Stream, query_string: []const u8, tenant: []const u8) !void {
        // Instant query reuses range query with a narrow time window.
        try self.handleLokiQueryRange(stream, query_string, tenant);
    }

    // -----------------------------------------------------------------------
    // GET /loki/api/v1/labels — List all label names
    // -----------------------------------------------------------------------

    fn handleLokiLabels(self: *HttpServer, stream: std.net.Stream) !void {
        const names = self.storage.getLabelNames(self.allocator) catch |err| {
            std.log.warn("label names query error: {}", .{err});
            try sendError(stream, 500, "Failed to fetch labels");
            return;
        };
        defer {
            for (names) |n| self.allocator.free(n);
            self.allocator.free(names);
        }

        const body = responses.formatLabelsResponse(self.allocator, names) catch {
            try sendError(stream, 500, "Failed to format labels");
            return;
        };
        defer self.allocator.free(body);

        try sendResponseWithCors(stream, 200, "application/json", body);
    }

    // -----------------------------------------------------------------------
    // GET /loki/api/v1/label/{name}/values — List values for a label
    // -----------------------------------------------------------------------

    fn handleLokiLabelValues(self: *HttpServer, stream: std.net.Stream, path: []const u8) !void {
        // Extract label name from path: /loki/api/v1/label/{name}/values
        const prefix = "/loki/api/v1/label/";
        if (!std.mem.startsWith(u8, path, prefix)) {
            try sendError(stream, 404, "Not Found");
            return;
        }

        const rest = path[prefix.len..];
        const label_name = if (std.mem.indexOf(u8, rest, "/values")) |idx|
            rest[0..idx]
        else
            rest;

        if (label_name.len == 0) {
            try sendError(stream, 400, "Missing label name");
            return;
        }

        const values = self.storage.getLabelValues(self.allocator, label_name) catch |err| {
            std.log.warn("label values query error: {}", .{err});
            try sendError(stream, 500, "Failed to fetch label values");
            return;
        };
        defer {
            for (values) |v| self.allocator.free(v);
            self.allocator.free(values);
        }

        const body = responses.formatLabelValuesResponse(self.allocator, values) catch {
            try sendError(stream, 500, "Failed to format label values");
            return;
        };
        defer self.allocator.free(body);

        try sendResponseWithCors(stream, 200, "application/json", body);
    }

    // -----------------------------------------------------------------------
    // GET /loki/api/v1/series — Find series matching label selectors
    // -----------------------------------------------------------------------

    fn handleLokiSeries(self: *HttpServer, stream: std.net.Stream, query_string: []const u8, tenant: []const u8) !void {
        _ = query_string;

        const series = self.storage.getSeries(self.allocator) catch {
            try sendError(stream, 500, "Failed to fetch series");
            return;
        };
        defer {
            for (series) |s| {
                for (s) |lbl| {
                    self.allocator.free(lbl.key);
                    self.allocator.free(lbl.value);
                }
                self.allocator.free(s);
            }
            self.allocator.free(series);
        }

        // Filter to series belonging to this tenant.
        var filtered = std.ArrayList([]const LogEntry.Label).init(self.allocator);
        defer filtered.deinit();
        for (series) |s| {
            for (s) |lbl| {
                if (std.mem.eql(u8, lbl.key, "tenant") and std.mem.eql(u8, lbl.value, tenant)) {
                    try filtered.append(s);
                    break;
                }
            }
        }

        const body = responses.formatSeriesResponse(self.allocator, filtered.items) catch {
            try sendError(stream, 500, "Failed to format series");
            return;
        };
        defer self.allocator.free(body);

        try sendResponseWithCors(stream, 200, "application/json", body);
    }

    // -----------------------------------------------------------------------
    // HTTP response helpers
    // -----------------------------------------------------------------------

    fn sendResponse(stream: std.net.Stream, status: u16, content_type: []const u8, body: []const u8) !void {
        var header_buf: [1024]u8 = undefined;
        const status_text = switch (status) {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            503 => "Service Unavailable",
            else => "Unknown",
        };

        const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {d} {s}\r\n" ++
            "Content-Type: {s}\r\n" ++
            "Content-Length: {d}\r\n" ++
            "Connection: close\r\n" ++
            "\r\n", .{ status, status_text, content_type, body.len }) catch unreachable;

        _ = try stream.write(header);
        if (body.len > 0) {
            _ = try stream.write(body);
        }
    }

    fn sendError(stream: std.net.Stream, status: u16, message: []const u8) !void {
        var buf: [256]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "{{\"error\":\"{s}\"}}", .{message}) catch unreachable;
        try sendResponse(stream, status, "application/json", body);
    }

    /// Send a response with CORS headers for Grafana browser access.
    fn sendResponseWithCors(stream: std.net.Stream, status: u16, content_type: []const u8, body: []const u8) !void {
        var header_buf: [2048]u8 = undefined;
        const status_text = switch (status) {
            200 => "OK",
            204 => "No Content",
            400 => "Bad Request",
            404 => "Not Found",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            else => "Unknown",
        };

        const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {d} {s}\r\n" ++
            "Content-Type: {s}\r\n" ++
            "Content-Length: {d}\r\n" ++
            "Access-Control-Allow-Origin: *\r\n" ++
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n" ++
            "Access-Control-Allow-Headers: Content-Type, X-Scope-OrgID\r\n" ++
            "Connection: close\r\n" ++
            "\r\n", .{ status, status_text, content_type, body.len }) catch unreachable;

        _ = try stream.write(header);
        if (body.len > 0) {
            _ = try stream.write(body);
        }
    }

    /// Send a 204 No Content response (for Loki push success).
    fn sendResponseNoBody(stream: std.net.Stream, status: u16) !void {
        const status_text = switch (status) {
            204 => "No Content",
            else => "OK",
        };
        var header_buf: [256]u8 = undefined;
        const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {d} {s}\r\n" ++
            "Access-Control-Allow-Origin: *\r\n" ++
            "Connection: close\r\n" ++
            "\r\n", .{ status, status_text }) catch unreachable;
        _ = try stream.write(header);
    }
};

// ---------------------------------------------------------------------------
// JSON formatting helper
// ---------------------------------------------------------------------------

fn appendEntryJson(buf: *std.ArrayList(u8), entry: *const LogEntry) !void {
    try buf.appendSlice("{\"timestamp\":");

    var num_buf: [32]u8 = undefined;
    const ts_str = std.fmt.bufPrint(&num_buf, "{d}", .{entry.timestamp_ns}) catch unreachable;
    try buf.appendSlice(ts_str);

    try buf.appendSlice(",\"source\":\"");
    try appendEscaped(buf, entry.source);

    try buf.appendSlice("\",\"message\":\"");
    try appendEscaped(buf, entry.message);

    try buf.appendSlice("\",\"labels\":{");
    for (entry.labels, 0..) |label, i| {
        if (i > 0) try buf.append(',');
        try buf.append('"');
        try appendEscaped(buf, label.key);
        try buf.appendSlice("\":\"");
        try appendEscaped(buf, label.value);
        try buf.append('"');
    }
    try buf.appendSlice("}}");
}

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
                    // Control character — skip or encode.
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
// Loki timestamp parser – handles nanosecond strings and Unix seconds
// ---------------------------------------------------------------------------

fn parseLokiTimestamp(value: []const u8) ?i64 {
    if (value.len == 0) return null;

    // Pure-digit string → nanoseconds (Grafana sends these).
    if (std.fmt.parseInt(i64, value, 10)) |ns| {
        // Heuristic: if the number is small it's probably seconds.
        if (ns < 1_000_000_000_000) {
            return ns * std.time.ns_per_s;
        }
        return ns;
    } else |_| {}

    // Floating-point seconds (e.g. "1700000000.123")
    if (std.fmt.parseFloat(f64, value)) |secs| {
        return @intFromFloat(secs * @as(f64, @floatFromInt(std.time.ns_per_s)));
    } else |_| {}

    return null;
}

// ---------------------------------------------------------------------------
// URL percent-decoding (minimal implementation for query params)
// ---------------------------------------------------------------------------

fn urlDecode(allocator: std.mem.Allocator, encoded: []const u8) ![]u8 {
    var out = std.ArrayList(u8).init(allocator);
    errdefer out.deinit();

    var i: usize = 0;
    while (i < encoded.len) {
        if (encoded[i] == '%' and i + 2 < encoded.len) {
            const hi = hexVal(encoded[i + 1]) orelse {
                try out.append(encoded[i]);
                i += 1;
                continue;
            };
            const lo = hexVal(encoded[i + 2]) orelse {
                try out.append(encoded[i]);
                i += 1;
                continue;
            };
            try out.append((hi << 4) | lo);
            i += 3;
        } else if (encoded[i] == '+') {
            try out.append(' ');
            i += 1;
        } else {
            try out.append(encoded[i]);
            i += 1;
        }
    }

    return out.toOwnedSlice();
}

fn hexVal(c: u8) ?u8 {
    if (c >= '0' and c <= '9') return c - '0';
    if (c >= 'a' and c <= 'f') return c - 'a' + 10;
    if (c >= 'A' and c <= 'F') return c - 'A' + 10;
    return null;
}

// ---------------------------------------------------------------------------
// Multi-tenancy helpers
// ---------------------------------------------------------------------------

/// Extract the `X-Scope-OrgID` header value from a raw HTTP request blob.
/// Returns `"default"` if the header is absent or empty.
fn parseScopeOrgID(headers_blob: []const u8) []const u8 {
    const v = extractHeader(headers_blob, "X-Scope-OrgID") orelse return "default";
    const trimmed = std.mem.trim(u8, v, " \t");
    if (trimmed.len == 0) return "default";
    for (trimmed) |c| {
        const ok = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '_' or c == '-' or c == '.';
        if (!ok) return "default";
    }
    return trimmed;
}

/// Find a header value by case-insensitive name. Returns the slice between
/// `:` and the trailing CRLF (untrimmed), or null if absent.
fn extractHeader(headers_blob: []const u8, name: []const u8) ?[]const u8 {
    var line_iter = std.mem.splitSequence(u8, headers_blob, "\r\n");
    _ = line_iter.next(); // skip request line
    while (line_iter.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const hname = std.mem.trim(u8, line[0..colon], " \t");
        if (hname.len != name.len) continue;
        if (!std.ascii.eqlIgnoreCase(hname, name)) continue;
        return line[colon + 1 ..];
    }
    return null;
}

/// Append a string to `writer` with JSON-string escaping for `"` and `\`.
fn jsonEscape(writer: anytype, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeByte(c),
        }
    }
}

/// Append a `tenant=<id>` matcher to a parsed LogQL expression so each
/// query is implicitly scoped to its tenant.
fn injectTenantMatcher(allocator: std.mem.Allocator, expr: *logql.LogQLExpr, tenant: []const u8) !void {
    const old = expr.matchers;
    const new = try allocator.alloc(logql.LabelMatcher, old.len + 1);
    @memcpy(new[0..old.len], old);
    new[old.len] = .{ .name = "tenant", .value = tenant, .op = .eq };
    allocator.free(old);
    expr.matchers = new;
}

// =============================================================================
// Amatiz — File Tailer
// =============================================================================
//
// Tails one or more files (like `tail -f`) using polling. Detects:
//   - New lines appended to the file.
//   - File truncation (log rotation via truncate).
//   - File replacement (log rotation via rename + create).
//
// Each tailed file runs in its own OS thread. The tailer feeds lines into the
// processing pipeline which batches and stores them.
//
// Polling is used instead of inotify/kqueue for maximum cross-platform
// compatibility (works on Linux, macOS, Windows, and in Docker containers
// with bind-mounted volumes).
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const Pipeline = @import("pipeline.zig").Pipeline;
const Positions = @import("positions.zig").Positions;

pub const Tailer = struct {
    allocator: Allocator,
    pipeline: *Pipeline,
    poll_interval_ns: u64,
    threads: std.ArrayList(std.Thread),
    should_stop: std.atomic.Value(bool),
    /// Optional persistent positions store. `null` = no resume across restarts.
    positions: ?*Positions,

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(allocator: Allocator, pipeline: *Pipeline, poll_ms: u64) Tailer {
        return .{
            .allocator = allocator,
            .pipeline = pipeline,
            .poll_interval_ns = poll_ms * std.time.ns_per_ms,
            .threads = std.ArrayList(std.Thread).init(allocator),
            .should_stop = std.atomic.Value(bool).init(false),
            .positions = null,
        };
    }

    /// Attach a positions store. The Tailer does not own it.
    pub fn setPositions(self: *Tailer, positions: *Positions) void {
        self.positions = positions;
    }

    pub fn deinit(self: *Tailer) void {
        self.stop();
        self.threads.deinit();
    }

    // -----------------------------------------------------------------------
    // Control
    // -----------------------------------------------------------------------

    /// Start tailing a file. Returns immediately; tailing runs in a background thread.
    pub fn tailFile(self: *Tailer, path: []const u8) !void {
        const ctx = try self.allocator.create(TailContext);
        ctx.* = .{
            .tailer = self,
            .path = try self.allocator.dupe(u8, path),
        };

        const thread = try std.Thread.spawn(.{}, tailThreadFn, .{ctx});
        try self.threads.append(thread);

        std.log.info("tailing file: {s}", .{path});
    }

    /// Signal all tail threads to stop and join them.
    pub fn stop(self: *Tailer) void {
        self.should_stop.store(true, .release);

        for (self.threads.items) |thread| {
            thread.join();
        }
        self.threads.clearRetainingCapacity();
    }

    // -----------------------------------------------------------------------
    // Thread context
    // -----------------------------------------------------------------------

    const TailContext = struct {
        tailer: *Tailer,
        path: []const u8,
    };

    fn tailThreadFn(ctx: *TailContext) void {
        const tailer = ctx.tailer;
        const path = ctx.path;
        defer {
            tailer.allocator.free(path);
            tailer.allocator.destroy(ctx);
        }

        tailLoop(tailer, path) catch |err| {
            std.log.err("tailer error for {s}: {}", .{ path, err });
        };
    }

    fn tailLoop(tailer: *Tailer, path: []const u8) !void {
        var read_buf: [64 * 1024]u8 = undefined;
        var line_buf = std.ArrayList(u8).init(tailer.allocator);
        defer line_buf.deinit();

        // Initial offset: prefer persisted position, else seek to current end.
        var last_size: u64 = blk: {
            if (tailer.positions) |p| {
                if (p.get(path)) |saved| break :blk saved;
            }
            if (std.fs.cwd().openFile(path, .{})) |file| {
                defer file.close();
                const stat = file.stat() catch break :blk 0;
                break :blk stat.size;
            } else |_| break :blk 0;
        };

        while (!tailer.should_stop.load(.acquire)) {
            // Try to open the file.
            const file = std.fs.cwd().openFile(path, .{}) catch {
                std.time.sleep(tailer.poll_interval_ns);
                continue;
            };
            defer file.close();

            const stat = file.stat() catch continue;

            // Detect truncation (log rotation).
            if (stat.size < last_size) {
                std.log.info("file truncated (rotation detected): {s}", .{path});
                last_size = 0;
            }

            if (stat.size <= last_size) {
                tailer.pipeline.tickFlush() catch {};
                std.time.sleep(tailer.poll_interval_ns);
                continue;
            }

            file.seekTo(last_size) catch continue;
            const bytes_to_read = stat.size - last_size;
            var total_read: u64 = 0;

            while (total_read < bytes_to_read) {
                const n = file.read(&read_buf) catch break;
                if (n == 0) break;
                total_read += n;

                var start: usize = 0;
                for (read_buf[0..n], 0..) |c, i| {
                    if (c == '\n') {
                        if (line_buf.items.len > 0) {
                            line_buf.appendSlice(read_buf[start..i]) catch continue;
                            processLine(tailer.pipeline, path, line_buf.items);
                            line_buf.clearRetainingCapacity();
                        } else {
                            const line = read_buf[start..i];
                            const clean = if (line.len > 0 and line[line.len - 1] == '\r')
                                line[0 .. line.len - 1]
                            else
                                line;
                            processLine(tailer.pipeline, path, clean);
                        }
                        start = i + 1;
                    }
                }

                if (start < n) {
                    line_buf.appendSlice(read_buf[start..n]) catch {};
                }
            }

            last_size = stat.size;

            // Persist read offset so a restart doesn't lose progress.
            if (tailer.positions) |p| {
                p.set(path, last_size);
                p.save();
            }

            tailer.pipeline.tickFlush() catch {};
            std.time.sleep(tailer.poll_interval_ns);
        }

        tailer.pipeline.flush() catch {};
        if (tailer.positions) |p| p.save();
    }

    fn processLine(pipeline: *Pipeline, source: []const u8, line: []const u8) void {
        if (line.len == 0) return;
        pipeline.ingest(source, line) catch |err| {
            std.log.err("pipeline ingest error: {}", .{err});
        };
    }
};

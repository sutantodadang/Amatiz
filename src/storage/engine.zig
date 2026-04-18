// =============================================================================
// Amatiz — Storage Engine
// =============================================================================
//
// Coordinates chunk lifecycle, directory layout, and index updates.
//
// Directory structure:
//   {data_dir}/
//     YYYY-MM-DD/
//       chunk_0001.amtz
//       chunk_0002.amtz
//       ...
//
//   {index_dir}/
//     time.idx        (binary time index)
//     keywords.idx    (binary keyword index)
//
// The engine is thread-safe: a mutex guards the active chunk and index state
// so multiple ingestion threads and the HTTP handler can operate concurrently.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;
const Chunk = @import("chunk.zig").Chunk;
const ts_util = @import("../utils/timestamp.zig");
const TimeIndex = @import("../index/time_index.zig").TimeIndex;
const KeywordIndex = @import("../index/keyword_index.zig").KeywordIndex;
const LabelIndex = @import("../index/label_index.zig").LabelIndex;
const Wal = @import("wal.zig").Wal;

pub const StorageEngine = struct {
    allocator: Allocator,
    data_dir: []const u8,
    index_dir: []const u8,
    chunk_max_bytes: u64,
    compression_enabled: bool,

    /// The currently active (writable) chunk.
    active_chunk: ?Chunk,
    /// Monotonically increasing chunk ID within a day directory.
    next_chunk_id: u32,
    /// Current day directory name (YYYY-MM-DD).
    current_day: [10]u8,

    /// Time index mapping timestamp ranges to chunks.
    time_index: TimeIndex,
    /// Keyword inverted index.
    keyword_index: KeywordIndex,

    /// Label name→values index (for Loki-compatible label discovery).
    label_index: LabelIndex,

    /// Protects active_chunk, next_chunk_id, current_day, and indices.
    mutex: std.Thread.Mutex,

    /// Running counters for basic metrics.
    total_entries: u64,
    total_bytes: u64,

    /// Optional write-ahead log. When set, every `writeBatch` is mirrored
    /// here before the chunk write so entries survive a crash before the
    /// chunk header is flushed. Truncated on chunk rotation.
    wal: ?Wal,
    /// True while replaying the WAL on startup — prevents the replayed
    /// entries from being re-appended to the WAL.
    replaying: bool,

    // -- Tail subscribers (for live streaming) --------------------------------
    /// Callbacks notified on every write. Protected by `mutex`.
    tail_subscribers: std.ArrayList(TailCallback),

    pub const TailCallback = struct {
        ctx: *anyopaque,
        notify: *const fn (ctx: *anyopaque, entries: []const LogEntry) void,
    };

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(
        allocator: Allocator,
        data_dir: []const u8,
        index_dir: []const u8,
        chunk_max_bytes: u64,
        compression_enabled: bool,
    ) !StorageEngine {
        // Ensure root dirs exist.
        std.fs.cwd().makePath(data_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        std.fs.cwd().makePath(index_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        var time_index = TimeIndex.init(allocator);
        time_index.loadFromDir(index_dir) catch {
            std.log.info("no existing time index found — starting fresh", .{});
        };

        var keyword_index = KeywordIndex.init(allocator);
        keyword_index.loadFromDir(index_dir) catch {
            std.log.info("no existing keyword index found — starting fresh", .{});
        };

        var label_index = LabelIndex.init(allocator);
        label_index.loadFromDir(index_dir) catch {
            std.log.info("no existing label index found — starting fresh", .{});
        };

        var day_buf: [10]u8 = undefined;
        ts_util.toDayString(ts_util.nowNs(), &day_buf);

        return .{
            .allocator = allocator,
            .data_dir = data_dir,
            .index_dir = index_dir,
            .chunk_max_bytes = chunk_max_bytes,
            .compression_enabled = compression_enabled,
            .active_chunk = null,
            .next_chunk_id = 1,
            .current_day = day_buf,
            .time_index = time_index,
            .keyword_index = keyword_index,
            .label_index = label_index,
            .mutex = .{},
            .total_entries = 0,
            .total_bytes = 0,
            .tail_subscribers = std.ArrayList(TailCallback).init(allocator),
            .wal = null,
            .replaying = false,
        };
    }

    /// Enable the write-ahead log. Opens (or creates) the WAL file at
    /// `<data_dir>/wal/current.wal` and replays any pending entries.
    pub fn enableWal(self: *StorageEngine) !void {
        var path_buf: [512]u8 = undefined;
        const wal_path = std.fmt.bufPrint(&path_buf, "{s}/wal/current.wal", .{self.data_dir}) catch unreachable;
        self.wal = try Wal.init(self.allocator, wal_path);

        // Replay any pending entries.
        self.replaying = true;
        defer self.replaying = false;

        const replayed = self.wal.?.replay(@ptrCast(self), struct {
            fn cb(ctx: *anyopaque, entry: *const LogEntry) anyerror!void {
                const eng: *StorageEngine = @ptrCast(@alignCast(ctx));
                // Mutex not held — replay runs single-threaded during init.
                try eng.writeEntryLocked(entry);
            }
        }.cb) catch |err| {
            std.log.warn("WAL replay failed: {}", .{err});
            return;
        };

        if (replayed > 0) {
            std.log.info("WAL replayed {d} entries", .{replayed});
            // Force the recovered chunk header to disk before truncating.
            if (self.active_chunk) |*c| c.flushHeader() catch {};
            self.wal.?.truncate() catch {};
        }
    }

    pub fn deinit(self: *StorageEngine) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.active_chunk) |*chunk| {
            chunk.flushHeader() catch {};
            chunk.deinit();
        }
        self.active_chunk = null;

        // Persist indices.
        self.time_index.saveToDir(self.index_dir) catch |err| {
            std.log.err("failed to save time index: {}", .{err});
        };
        self.keyword_index.saveToDir(self.index_dir) catch |err| {
            std.log.err("failed to save keyword index: {}", .{err});
        };
        self.label_index.saveToDir(self.index_dir) catch |err| {
            std.log.err("failed to save label index: {}", .{err});
        };

        self.time_index.deinit();
        self.keyword_index.deinit();
        self.label_index.deinit();
        self.tail_subscribers.deinit();

        if (self.wal) |*w| w.deinit();
        self.wal = null;
    }

    // -----------------------------------------------------------------------
    // Writing
    // -----------------------------------------------------------------------

    /// Write a batch of log entries to storage. Thread-safe.
    pub fn writeBatch(self: *StorageEngine, entries: []const LogEntry) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Append to WAL first so a crash before chunk-header flush is recoverable.
        if (!self.replaying) {
            if (self.wal) |*w| {
                for (entries) |*entry| {
                    w.append(entry) catch |err| {
                        std.log.warn("WAL append failed: {}", .{err});
                        break;
                    };
                }
                w.sync();
            }
        }

        for (entries) |*entry| {
            try self.writeEntryLocked(entry);
        }

        // Notify tail subscribers.
        for (self.tail_subscribers.items) |sub| {
            sub.notify(sub.ctx, entries);
        }
    }

    /// Write a single entry. Caller must hold the mutex.
    fn writeEntryLocked(self: *StorageEngine, entry: *const LogEntry) !void {
        // Check if we need to rotate to a new day directory.
        var day_buf: [10]u8 = undefined;
        ts_util.toDayString(entry.timestamp_ns, &day_buf);

        if (!std.mem.eql(u8, &day_buf, &self.current_day)) {
            // Day changed — finalize current chunk and reset.
            try self.rotateChunkLocked();
            self.current_day = day_buf;
            self.next_chunk_id = 1;
        }

        // Ensure we have an active chunk.
        if (self.active_chunk == null) {
            try self.openNewChunkLocked();
        }

        // Check size limit — rotate if needed.
        if (self.active_chunk.?.diskSize() >= self.chunk_max_bytes) {
            try self.rotateChunkLocked();
            try self.openNewChunkLocked();
        }

        const written = try self.active_chunk.?.append(entry);
        self.total_entries += 1;
        self.total_bytes += written;

        // Update keyword index with words from the message.
        self.keyword_index.indexMessage(entry.message, self.next_chunk_id - 1);

        // Track labels for Loki-compatible label discovery.
        self.label_index.trackEntry(entry);
    }

    /// Finalize the current active chunk and update indices.
    fn rotateChunkLocked(self: *StorageEngine) !void {
        if (self.active_chunk) |*chunk| {
            try chunk.finalize();

            // Register in time index.
            if (chunk.entry_count > 0) {
                try self.time_index.add(.{
                    .chunk_id = self.next_chunk_id - 1,
                    .path = chunk.path,
                    .min_ts = chunk.min_ts,
                    .max_ts = chunk.max_ts,
                    .entry_count = chunk.entry_count,
                });
            }

            chunk.deinit();
            self.active_chunk = null;

            // Persist indices periodically.
            self.time_index.saveToDir(self.index_dir) catch {};
            self.keyword_index.saveToDir(self.index_dir) catch {};
            self.label_index.saveToDir(self.index_dir) catch {};

            // The chunk's contents are now durable — clear the WAL.
            if (self.wal) |*w| w.truncate() catch {};
        }
    }

    /// Allocate and open a brand-new chunk in the current day directory.
    fn openNewChunkLocked(self: *StorageEngine) !void {
        var dir_buf: [512]u8 = undefined;
        const dir_path = std.fmt.bufPrint(&dir_buf, "{s}/{s}", .{
            self.data_dir,
            self.current_day,
        }) catch unreachable;

        // Scan existing chunks to find the next ID.
        const existing = self.scanChunksInDir(dir_path);
        if (existing > self.next_chunk_id) {
            self.next_chunk_id = existing;
        }

        self.active_chunk = try Chunk.create(
            self.allocator,
            dir_path,
            self.next_chunk_id,
            self.compression_enabled,
        );
        self.next_chunk_id += 1;
    }

    /// Count existing chunk files in a directory to avoid ID collisions.
    fn scanChunksInDir(self: *StorageEngine, dir_path: []const u8) u32 {
        _ = self;
        var max_id: u32 = 0;
        var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return 1;
        defer dir.close();

        var iter = dir.iterate();
        while (iter.next() catch null) |entry| {
            if (std.mem.startsWith(u8, entry.name, "chunk_") and
                std.mem.endsWith(u8, entry.name, ".amtz"))
            {
                // Extract ID from "chunk_NNNN.amtz".
                const id_str = entry.name[6 .. entry.name.len - 5];
                if (std.fmt.parseInt(u32, id_str, 10)) |id| {
                    if (id >= max_id) max_id = id + 1;
                } else |_| {}
            }
        }
        return if (max_id > 0) max_id else 1;
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Force-flush the active chunk header and indices to disk.
    pub fn flush(self: *StorageEngine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.active_chunk) |*chunk| {
            try chunk.flushHeader();
        }
        self.time_index.saveToDir(self.index_dir) catch {};
        self.keyword_index.saveToDir(self.index_dir) catch {};
        self.label_index.saveToDir(self.index_dir) catch {};
    }

    // -----------------------------------------------------------------------
    // Reading / discovery
    // -----------------------------------------------------------------------

    /// Return all chunk paths that may contain entries in the given time range.
    pub fn chunksForTimeRange(self: *StorageEngine, from_ns: ?i64, to_ns: ?i64) []const TimeIndex.ChunkMeta {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.time_index.query(from_ns, to_ns);
    }

    /// Return chunk IDs that contain the given keyword.
    pub fn chunksForKeyword(self: *StorageEngine, keyword: []const u8) []const u32 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.keyword_index.lookup(keyword);
    }

    /// List all day directories under data_dir.
    pub fn listDays(self: *StorageEngine, allocator: Allocator) ![][]const u8 {
        var result = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (result.items) |item| allocator.free(item);
            result.deinit();
        }

        var dir = std.fs.cwd().openDir(self.data_dir, .{ .iterate = true }) catch return result.toOwnedSlice();
        defer dir.close();

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .directory and entry.name.len == 10) {
                try result.append(try allocator.dupe(u8, entry.name));
            }
        }

        return result.toOwnedSlice();
    }

    /// List all chunk files in a specific day directory.
    pub fn listChunksInDay(self: *StorageEngine, allocator: Allocator, day: []const u8) ![][]const u8 {
        var result = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (result.items) |item| allocator.free(item);
            result.deinit();
        }

        var path_buf: [512]u8 = undefined;
        const dir_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ self.data_dir, day }) catch unreachable;

        var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return result.toOwnedSlice();
        defer dir.close();

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (std.mem.endsWith(u8, entry.name, ".amtz")) {
                var full_buf: [1024]u8 = undefined;
                const full_path = std.fmt.bufPrint(&full_buf, "{s}/{s}", .{ dir_path, entry.name }) catch continue;
                try result.append(try allocator.dupe(u8, full_path));
            }
        }

        return result.toOwnedSlice();
    }

    // -----------------------------------------------------------------------
    // Tail subscription
    // -----------------------------------------------------------------------

    pub fn subscribe(self: *StorageEngine, callback: TailCallback) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.tail_subscribers.append(callback);
    }

    pub fn unsubscribe(self: *StorageEngine, ctx: *anyopaque) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var i: usize = 0;
        while (i < self.tail_subscribers.items.len) {
            if (self.tail_subscribers.items[i].ctx == ctx) {
                _ = self.tail_subscribers.orderedRemove(i);
            } else {
                i += 1;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    pub fn getMetrics(self: *StorageEngine) struct { entries: u64, bytes: u64, chunks: u32 } {
        self.mutex.lock();
        defer self.mutex.unlock();
        return .{
            .entries = self.total_entries,
            .bytes = self.total_bytes,
            .chunks = self.next_chunk_id -| 1,
        };
    }

    // -----------------------------------------------------------------------
    // Label index access (for Loki-compatible endpoints)
    // -----------------------------------------------------------------------

    /// Return all known label names. Caller owns the returned slice.
    pub fn getLabelNames(self: *StorageEngine, allocator: Allocator) ![][]const u8 {
        return self.label_index.getNames(allocator);
    }

    /// Return all known values for a label. Caller owns the returned slice.
    pub fn getLabelValues(self: *StorageEngine, allocator: Allocator, name: []const u8) ![][]const u8 {
        return self.label_index.getValues(allocator, name);
    }

    /// Return every unique label set (series) we have observed.
    /// Caller owns both the outer slice and each inner Label slice's strings.
    pub fn getSeries(self: *StorageEngine, allocator: Allocator) ![]const []const LogEntry.Label {
        return self.label_index.getSeries(allocator);
    }
};

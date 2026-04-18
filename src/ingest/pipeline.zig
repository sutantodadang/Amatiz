// =============================================================================
// Amatiz — Processing Pipeline
// =============================================================================
//
// Each raw log line passes through a series of stages before reaching storage:
//
//   1. Parse   — extract structure (raw text or JSON)
//   2. Enrich  — attach timestamp, source, labels
//   3. Buffer  — batch entries for efficient writes
//   4. Store   — flush batch to the storage engine
//
// The pipeline is designed for zero-copy where possible: the raw line bytes
// are referenced directly until the entry is serialized to disk.
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../utils/types.zig");
const LogEntry = types.LogEntry;
const ts = @import("../utils/timestamp.zig");
const StorageEngine = @import("../storage/engine.zig").StorageEngine;

pub const Pipeline = struct {
    allocator: Allocator,
    storage: *StorageEngine,
    batch: std.ArrayList(LogEntry),
    batch_size: u32,
    flush_interval_ms: u64,
    last_flush_ns: i64,
    /// Default labels applied to every entry when no source-specific labels
    /// are configured.
    default_labels: []const LogEntry.Label,
    /// Per-source label sets (e.g. one per scrape config target). Looked up
    /// by source path. Slices are borrowed — caller (Config arena) owns memory.
    source_labels: std.StringHashMap([]const LogEntry.Label),
    /// Label arrays allocated per-entry (e.g. for tenant injection); freed on
    /// flush after `writeBatch` has serialized the entries.
    owned_label_arrays: std.ArrayList([]LogEntry.Label),
    /// Strings allocated per-entry (e.g. duped tenant id); freed on flush.
    owned_strings: std.ArrayList([]const u8),

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init(
        allocator: Allocator,
        storage: *StorageEngine,
        batch_size: u32,
        flush_interval_ms: u64,
    ) Pipeline {
        return .{
            .allocator = allocator,
            .storage = storage,
            .batch = std.ArrayList(LogEntry).init(allocator),
            .batch_size = batch_size,
            .flush_interval_ms = flush_interval_ms,
            .last_flush_ns = ts.nowNs(),
            .default_labels = &.{},
            .source_labels = std.StringHashMap([]const LogEntry.Label).init(allocator),
            .owned_label_arrays = std.ArrayList([]LogEntry.Label).init(allocator),
            .owned_strings = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *Pipeline) void {
        self.flush() catch {};
        self.batch.deinit();
        // Free duped source-key strings (values are borrowed from Config arena).
        var it = self.source_labels.iterator();
        while (it.next()) |kv| self.allocator.free(kv.key_ptr.*);
        self.source_labels.deinit();
        for (self.owned_label_arrays.items) |arr| self.allocator.free(arr);
        self.owned_label_arrays.deinit();
        for (self.owned_strings.items) |s| self.allocator.free(s);
        self.owned_strings.deinit();
    }

    /// Register a label set to apply to every entry whose `source` equals
    /// `source_path`. The `labels` slice is borrowed; the caller must keep it
    /// alive for the lifetime of the pipeline.
    pub fn setSourceLabels(
        self: *Pipeline,
        source_path: []const u8,
        labels: []const LogEntry.Label,
    ) !void {
        const gop = try self.source_labels.getOrPut(source_path);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, source_path);
        }
        gop.value_ptr.* = labels;
    }

    // -----------------------------------------------------------------------
    // Ingestion
    // -----------------------------------------------------------------------

    /// Process a single raw log line from a given source file.
    pub fn ingest(self: *Pipeline, source: []const u8, line: []const u8) !void {
        if (line.len == 0) return;

        var entry = self.parse(source, line);
        self.enrich(&entry);

        try self.batch.append(entry);

        // Flush when batch is full.
        if (self.batch.items.len >= self.batch_size) {
            try self.flush();
        }
    }

    /// Like `ingest` but injects an additional `tenant=<tenant_id>` label.
    /// The `tenant_id` is duped into the pipeline's allocator and freed on
    /// the next flush.
    pub fn ingestWithTenant(
        self: *Pipeline,
        source: []const u8,
        line: []const u8,
        tenant_id: []const u8,
    ) !void {
        if (line.len == 0) return;
        if (tenant_id.len == 0) return self.ingest(source, line);

        var entry = self.parse(source, line);
        self.enrich(&entry);

        const tenant_dup = try self.allocator.dupe(u8, tenant_id);
        errdefer self.allocator.free(tenant_dup);

        const base = entry.labels;
        const new_labels = try self.allocator.alloc(LogEntry.Label, base.len + 1);
        errdefer self.allocator.free(new_labels);
        @memcpy(new_labels[0..base.len], base);
        new_labels[base.len] = .{ .key = "tenant", .value = tenant_dup };

        try self.owned_strings.append(tenant_dup);
        try self.owned_label_arrays.append(new_labels);
        entry.labels = new_labels;

        try self.batch.append(entry);
        if (self.batch.items.len >= self.batch_size) {
            try self.flush();
        }
    }

    /// Process multiple lines at once (e.g. from a read buffer).
    pub fn ingestBatch(self: *Pipeline, source: []const u8, lines: []const []const u8) !void {
        for (lines) |line| {
            try self.ingest(source, line);
        }
    }

    /// Try to flush if the interval has elapsed. Called periodically.
    pub fn tickFlush(self: *Pipeline) !void {
        const now = ts.nowNs();
        const elapsed_ms: u64 = @intCast(@divTrunc(now - self.last_flush_ns, std.time.ns_per_ms));
        if (elapsed_ms >= self.flush_interval_ms and self.batch.items.len > 0) {
            try self.flush();
        }
    }

    // -----------------------------------------------------------------------
    // Pipeline stages
    // -----------------------------------------------------------------------

    /// Stage 1: Parse — attempt JSON extraction, fall back to raw text.
    fn parse(self: *Pipeline, source: []const u8, line: []const u8) LogEntry {
        _ = self;

        var entry = LogEntry{
            .timestamp_ns = 0,
            .source = source,
            .message = line,
            .raw_line = line,
            .labels = &.{},
        };

        // Try to detect JSON lines: starts with '{'.
        if (line.len > 0 and line[0] == '{') {
            // Attempt to extract "message" or "msg" field and a "timestamp"/"ts" field.
            if (extractJsonField(line, "message") orelse extractJsonField(line, "msg")) |msg| {
                entry.message = msg;
            }
            if (extractJsonField(line, "timestamp") orelse extractJsonField(line, "ts")) |ts_str| {
                entry.timestamp_ns = ts.parseDatetime(ts_str) catch 0;
            }
        }

        return entry;
    }

    /// Stage 2: Enrich — fill in missing timestamp, attach configured labels.
    /// Source-specific labels (registered via setSourceLabels) take priority
    /// over the global default_labels.
    fn enrich(self: *Pipeline, entry: *LogEntry) void {
        if (entry.timestamp_ns == 0) {
            entry.timestamp_ns = ts.nowNs();
        }
        if (self.source_labels.get(entry.source)) |labels| {
            entry.labels = labels;
        } else if (self.default_labels.len > 0) {
            entry.labels = self.default_labels;
        }
    }

    // -----------------------------------------------------------------------
    // Flushing
    // -----------------------------------------------------------------------

    /// Flush the current batch to the storage engine.
    pub fn flush(self: *Pipeline) !void {
        if (self.batch.items.len == 0) return;

        try self.storage.writeBatch(self.batch.items);
        self.batch.clearRetainingCapacity();

        for (self.owned_label_arrays.items) |arr| self.allocator.free(arr);
        self.owned_label_arrays.clearRetainingCapacity();
        for (self.owned_strings.items) |s| self.allocator.free(s);
        self.owned_strings.clearRetainingCapacity();

        self.last_flush_ns = ts.nowNs();
    }

    /// Number of entries waiting in the batch.
    pub fn pendingCount(self: *const Pipeline) usize {
        return self.batch.items.len;
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Very basic JSON field extraction without a full parser.
/// Finds "key": "value" and returns the value slice (pointing into `json`).
fn extractJsonField(json: []const u8, key: []const u8) ?[]const u8 {
    var pos: usize = 0;
    while (pos < json.len) {
        // Find the key.
        if (std.mem.indexOfPos(u8, json, pos, "\"")) |q1| {
            const ks = q1 + 1;
            if (std.mem.indexOfPos(u8, json, ks, "\"")) |q2| {
                const found = json[ks..q2];
                if (std.mem.eql(u8, found, key)) {
                    // Skip colon and whitespace.
                    var vp = q2 + 1;
                    while (vp < json.len and (json[vp] == ':' or json[vp] == ' ' or json[vp] == '\t')) : (vp += 1) {}
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "extractJsonField basic" {
    const json = "{\"message\": \"hello world\", \"level\": \"info\"}";
    const msg = extractJsonField(json, "message");
    try std.testing.expect(msg != null);
    try std.testing.expectEqualStrings("hello world", msg.?);

    const level = extractJsonField(json, "level");
    try std.testing.expect(level != null);
    try std.testing.expectEqualStrings("info", level.?);

    const missing = extractJsonField(json, "nope");
    try std.testing.expect(missing == null);
}

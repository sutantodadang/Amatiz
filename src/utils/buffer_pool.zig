// =============================================================================
// Amatiz — Buffer Pool
// =============================================================================
//
// Pre-allocated, reusable byte buffers to minimise heap churn on hot paths
// (log parsing, chunk writes, HTTP responses).  Thread-safe via a mutex
// around the free-list.
//
// Usage:
//   var pool = BufferPool.init(allocator, 4096, 32);
//   defer pool.deinit();
//   const buf = try pool.acquire();
//   defer pool.release(buf);
// =============================================================================

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BufferPool = struct {
    allocator: Allocator,
    buf_size: usize,
    free_list: std.ArrayList([]u8),
    mutex: std.Thread.Mutex,
    total_allocated: usize,
    max_pooled: usize,

    /// Create a new buffer pool.
    /// `buf_size`   — byte length of each buffer.
    /// `max_pooled` — maximum number of buffers kept in the free list
    ///                (excess buffers are freed immediately on release).
    pub fn init(allocator: Allocator, buf_size: usize, max_pooled: usize) BufferPool {
        return .{
            .allocator = allocator,
            .buf_size = buf_size,
            .free_list = std.ArrayList([]u8).init(allocator),
            .mutex = .{},
            .total_allocated = 0,
            .max_pooled = max_pooled,
        };
    }

    /// Destroy the pool and free every buffer still on the free list.
    pub fn deinit(self: *BufferPool) void {
        for (self.free_list.items) |buf| {
            self.allocator.free(buf);
        }
        self.free_list.deinit();
    }

    /// Obtain a buffer — either recycled from the pool or freshly allocated.
    pub fn acquire(self: *BufferPool) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.free_list.items.len > 0) {
            const buf = self.free_list.pop().?;
            @memset(buf, 0);
            return buf;
        }

        const buf = try self.allocator.alloc(u8, self.buf_size);
        self.total_allocated += 1;
        return buf;
    }

    /// Return a buffer to the pool for reuse.
    pub fn release(self: *BufferPool, buf: []u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.free_list.items.len >= self.max_pooled) {
            // Pool is full — free immediately to cap memory.
            self.allocator.free(buf);
            return;
        }

        self.free_list.append(buf) catch {
            self.allocator.free(buf);
        };
    }

    /// Current number of buffers sitting in the free list.
    pub fn available(self: *BufferPool) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.free_list.items.len;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "BufferPool acquire and release" {
    const allocator = std.testing.allocator;
    var pool = BufferPool.init(allocator, 64, 4);
    defer pool.deinit();

    const b1 = try pool.acquire();
    const b2 = try pool.acquire();
    try std.testing.expectEqual(@as(usize, 0), pool.available());

    pool.release(b1);
    try std.testing.expectEqual(@as(usize, 1), pool.available());

    pool.release(b2);
    try std.testing.expectEqual(@as(usize, 2), pool.available());

    // Re-acquire should recycle.
    const b3 = try pool.acquire();
    try std.testing.expectEqual(@as(usize, 1), pool.available());
    pool.release(b3);
}

test "BufferPool caps at max_pooled" {
    const allocator = std.testing.allocator;
    var pool = BufferPool.init(allocator, 32, 2);
    defer pool.deinit();

    const a = try pool.acquire();
    const b = try pool.acquire();
    const c = try pool.acquire();

    pool.release(a);
    pool.release(b);
    pool.release(c); // exceeds max_pooled=2, should free immediately

    try std.testing.expectEqual(@as(usize, 2), pool.available());
}

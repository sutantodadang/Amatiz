const std = @import("std");

/// Build configuration for Amatiz - a high-performance observability system.
///
/// Targets:
///   zig build          - Build debug binary
///   zig build -Doptimize=ReleaseFast - Build optimized binary
///   zig build -Doptimize=ReleaseSmall - Build size-optimized binary (Docker)
///   zig build run      - Build and run
///   zig build test     - Run unit tests
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ---------------------------------------------------------------------------
    // Main executable
    // ---------------------------------------------------------------------------
    const exe = b.addExecutable(.{
        .name = "amatiz",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Strip debug info for release builds to minimize binary size
    if (optimize != .Debug) {
        exe.root_module.strip = true;
    }

    b.installArtifact(exe);

    // ---------------------------------------------------------------------------
    // Run step
    // ---------------------------------------------------------------------------
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    // Forward CLI arguments: zig build run -- serve --port 8080
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run amatiz");
    run_step.dependOn(&run_cmd.step);

    // ---------------------------------------------------------------------------
    // Unit tests
    // ---------------------------------------------------------------------------
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}

// =============================================================================
// Amatiz — CLI Argument Parser
// =============================================================================
//
// Commands:
//   amatiz serve   [--config path] [--port N] [--data-dir path]
//   amatiz ingest  <file> [--config path] [--data-dir path]
//   amatiz query   <keyword> [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--limit N]
//   amatiz tail    [--host addr] [--port N]
//   amatiz version
//   amatiz help
// =============================================================================

const std = @import("std");

pub const Command = enum {
    serve,
    ingest,
    query,
    tail,
    version,
    help,
};

pub const ParsedArgs = struct {
    command: Command = .help,

    // -- Common ---------------------------------------------------------------
    config_path: ?[]const u8 = null,
    data_dir: ?[]const u8 = null,
    index_dir: ?[]const u8 = null,

    // -- serve ----------------------------------------------------------------
    port: ?u16 = null,
    host: ?[]const u8 = null,

    // -- ingest ---------------------------------------------------------------
    ingest_file: ?[]const u8 = null,

    // -- query ----------------------------------------------------------------
    query_keyword: ?[]const u8 = null,
    query_from: ?[]const u8 = null,
    query_to: ?[]const u8 = null,
    query_limit: ?usize = null,

    // -- tail -----------------------------------------------------------------
    tail_host: ?[]const u8 = null,
    tail_port: ?u16 = null,
};

pub fn parse(args_iter: anytype) ParsedArgs {
    var result = ParsedArgs{};

    // Skip the program name.
    _ = args_iter.next();

    // First positional arg is the command.
    const cmd_str = args_iter.next() orelse return result;
    result.command = parseCommand(cmd_str);

    // For ingest and query, the next positional arg is the target.
    switch (result.command) {
        .ingest => {
            if (args_iter.next()) |arg| {
                if (arg.len > 0 and arg[0] != '-') {
                    result.ingest_file = arg;
                }
            }
        },
        .query => {
            if (args_iter.next()) |arg| {
                if (arg.len > 0 and arg[0] != '-') {
                    result.query_keyword = arg;
                }
            }
        },
        else => {},
    }

    // Parse remaining flags.
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--config")) {
            result.config_path = args_iter.next();
        } else if (std.mem.eql(u8, arg, "--data-dir")) {
            result.data_dir = args_iter.next();
        } else if (std.mem.eql(u8, arg, "--index-dir")) {
            result.index_dir = args_iter.next();
        } else if (std.mem.eql(u8, arg, "--port")) {
            if (args_iter.next()) |v| {
                result.port = std.fmt.parseInt(u16, v, 10) catch null;
                result.tail_port = result.port;
            }
        } else if (std.mem.eql(u8, arg, "--host")) {
            result.host = args_iter.next();
            result.tail_host = result.host;
        } else if (std.mem.eql(u8, arg, "--from")) {
            result.query_from = args_iter.next();
        } else if (std.mem.eql(u8, arg, "--to")) {
            result.query_to = args_iter.next();
        } else if (std.mem.eql(u8, arg, "--limit")) {
            if (args_iter.next()) |v| {
                result.query_limit = std.fmt.parseInt(usize, v, 10) catch null;
            }
        }
    }

    return result;
}

fn parseCommand(s: []const u8) Command {
    if (std.mem.eql(u8, s, "serve")) return .serve;
    if (std.mem.eql(u8, s, "ingest")) return .ingest;
    if (std.mem.eql(u8, s, "query")) return .query;
    if (std.mem.eql(u8, s, "tail")) return .tail;
    if (std.mem.eql(u8, s, "version") or std.mem.eql(u8, s, "--version") or std.mem.eql(u8, s, "-v")) return .version;
    if (std.mem.eql(u8, s, "help") or std.mem.eql(u8, s, "--help") or std.mem.eql(u8, s, "-h")) return .help;
    return .help;
}

pub fn printUsage(writer: anytype) !void {
    try writer.writeAll(
        \\
        \\  █████╗ ███╗   ███╗ █████╗ ████████╗██╗███████╗
        \\ ██╔══██╗████╗ ████║██╔══██╗╚══██╔══╝██║╚══███╔╝
        \\ ███████║██╔████╔██║███████║   ██║   ██║  ███╔╝
        \\ ██╔══██║██║╚██╔╝██║██╔══██║   ██║   ██║ ███╔╝
        \\ ██║  ██║██║ ╚═╝ ██║██║  ██║   ██║   ██║███████╗
        \\ ╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚══════╝
        \\
        \\ High-performance observability system
        \\
        \\ USAGE:
        \\   amatiz <command> [options]
        \\
        \\ COMMANDS:
        \\   serve                  Start HTTP server and tail configured files
        \\   ingest <file>          Tail a file and store logs
        \\   query  <keyword>       Search stored logs
        \\   tail                   Live-stream logs from a running server
        \\   version                Show version
        \\   help                   Show this help
        \\
        \\ GLOBAL OPTIONS:
        \\   --config <path>        Path to config file (default: amatiz.json)
        \\   --data-dir <path>      Override data directory
        \\   --index-dir <path>     Override index directory
        \\
        \\ SERVE OPTIONS:
        \\   --host <addr>          Bind address (default: 0.0.0.0)
        \\   --port <port>          Bind port (default: 3100)
        \\
        \\ QUERY OPTIONS:
        \\   --from <date>          Start date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
        \\   --to <date>            End date
        \\   --limit <N>            Max results (default: 1000)
        \\
        \\ TAIL OPTIONS:
        \\   --host <addr>          Server address (default: 127.0.0.1)
        \\   --port <port>          Server port (default: 3100)
        \\
        \\ EXAMPLES:
        \\   amatiz serve --port 8080
        \\   amatiz ingest /var/log/syslog
        \\   amatiz query "error" --from 2024-01-01 --limit 50
        \\   amatiz tail --port 8080
        \\
    );
}

# Amatiz

High-performance, single-binary observability system written in Zig. Think Promtail + Loki, but lighter.

[![CI](https://github.com/OWNER/amatiz/actions/workflows/ci.yml/badge.svg)](https://github.com/OWNER/amatiz/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)

- **Single binary** — no runtime dependencies, no JVM, no Go runtime
- **Low memory** — targets < 100 MB working set
- **Fast startup** — sub-second cold start
- **Docker-friendly** — scratch-based image, ~5 MB

## Quick Start

```bash
# Build
zig build

# Ingest a file
./zig-out/bin/amatiz ingest /var/log/syslog

# Start server
./zig-out/bin/amatiz serve --port 3100

# Query logs
./zig-out/bin/amatiz query "error" --from 2024-01-01 --limit 50

# Live tail from running server
./zig-out/bin/amatiz tail --port 3100
```

## Web UI

Browse to **`http://localhost:3100/`** for a built-in single-page UI:

* LogQL query input with time-range picker (`now`, `now-1h`, ISO, or raw
  nanosecond timestamps).
* Result table for stream queries; matrix view for metric queries
  (`rate`, `count_over_time`, `sum by (...)`).
* **Live tail** button — opens a WebSocket to `/loki/api/v1/tail` and
  prepends new entries as they arrive.
* Tenant selector (sets `X-Scope-OrgID` on every request).

The UI is a single embedded HTML file (`src/ui/index.html`) with no external
dependencies — it ships inside the binary.

## Using Grafana

Amatiz speaks the Loki HTTP API, so Grafana can use it as a Loki data source
without any plugin:

1. In Grafana: **Connections → Data sources → Add data source → Loki**.
2. Set **URL** to `http://localhost:3100` (or wherever Amatiz listens).
3. Optional: under **HTTP Headers**, add `X-Scope-OrgID: <your-tenant>`.
4. Click **Save & test**. Grafana's *Explore* view, dashboards, and
   live-tail mode all work against Amatiz.

The metric query subset (`rate`, `count_over_time`, `sum by (lbl) (...)`) is
sufficient to back simple Grafana time-series panels.

## Architecture

```
┌─────────────┐    ┌────────────┐    ┌──────────────┐
│  File Tailer │───→│  Pipeline   │───→│    Storage    │
│  (per file)  │    │  parse →    │    │  Engine      │
└─────────────┘    │  enrich →   │    │  ┌─────────┐ │
                   │  batch →    │    │  │ Chunks  │ │
┌─────────────┐    │  store      │    │  │ (10MB)  │ │
│  HTTP API   │───→│             │    │  └─────────┘ │
│  POST /ingest│   └────────────┘    │  ┌─────────┐ │
└─────────────┘                      │  │ Time Idx│ │
                                     │  └─────────┘ │
┌─────────────┐    ┌────────────┐    │  ┌─────────┐ │
│  HTTP API   │───→│   Query    │───→│  │ Kw Idx  │ │
│  GET /query  │   │   Engine   │    │  └─────────┘ │
└─────────────┘    └────────────┘    └──────────────┘
```

## Data Layout

```
data/
  2024-01-15/
    chunk_0001.amtz    # append-only, immutable once full
    chunk_0002.amtz
  2024-01-16/
    chunk_0001.amtz

index/
  time.idx             # maps timestamp ranges → chunks
  keywords.idx         # inverted index: word → chunk IDs
```

## HTTP API

### Native endpoints

| Endpoint      | Method | Description                          |
|--------------|--------|--------------------------------------|
| `/`, `/ui`   | GET    | Embedded single-page web UI          |
| `/ingest`    | POST   | Receive log lines (newline-delimited) |
| `/query`     | GET    | Search logs by keyword & time range  |
| `/tail`      | GET    | Live-stream new entries (SSE or WebSocket) |
| `/metrics`   | GET    | System stats (entries, bytes, uptime) |
| `/health`    | GET    | Health check                         |
| `/ready`     | GET    | Readiness probe                      |

### Loki-compatible endpoints

Amatiz speaks the [Grafana Loki HTTP API](https://grafana.com/docs/loki/latest/reference/api/),
so it can be selected as a Loki data source in Grafana directly.

| Endpoint                            | Method | Description                                  |
|------------------------------------|--------|----------------------------------------------|
| `/loki/api/v1/push`                | POST   | Promtail/Loki JSON push (streams + values)   |
| `/loki/api/v1/query`               | GET    | LogQL instant query                          |
| `/loki/api/v1/query_range`         | GET    | LogQL range query (logs **and** metrics)     |
| `/loki/api/v1/labels`              | GET    | List known label names                       |
| `/loki/api/v1/label/{name}/values` | GET    | List values for a label                      |
| `/loki/api/v1/series`              | GET    | List label sets (filtered by tenant)         |
| `/loki/api/v1/tail`                | GET    | Live tail — RFC 6455 WebSocket, falls back to SSE |

### LogQL support

Stream selectors and line filters:

```logql
{job="myapp", level="error"} |= "timeout" != "healthcheck" | limit 100
```

Operators: `=`, `!=`, `=~`, `!~` (regex operators are substring fallbacks — see
*Known limitations* below).

Metric queries (Prometheus-style matrix response):

```logql
count_over_time({job="myapp"} |= "error" [5m])
rate({job="myapp"}[1m])
sum by (level) (count_over_time({job="myapp"}[1m]))
```

The `step` query parameter (e.g. `step=60s`) controls bucket resolution.

### Multi-tenancy

Every request may carry an `X-Scope-OrgID` header. When present, ingested
entries are tagged with a `tenant=<id>` label, and every query is implicitly
scoped to the tenant via an injected `tenant="<id>"` matcher. Requests without
the header are mapped to tenant `default`.

> Tenants are **logically isolated by label**, not physically separated on
> disk. See *Known limitations*.

### Native query parameters (`/query`)

| Param   | Example                  | Description                |
|---------|--------------------------|----------------------------|
| `q`     | `q=error`                | Keyword substring search   |
| `from`  | `from=2024-01-15`        | Start date (inclusive)     |
| `to`    | `to=2024-01-16`          | End date (inclusive)       |
| `limit` | `limit=100`              | Max results (default 1000) |

### Examples

```bash
# Ingest logs via HTTP
curl -X POST http://localhost:3100/ingest -d 'error: connection refused
warning: high latency detected
info: health check passed'

# Query for errors
curl "http://localhost:3100/query?q=error&from=2024-01-15&limit=10"

# Live tail
curl -N http://localhost:3100/tail

# Loki LogQL range query
curl --get http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={job="myapp"} |= "error"' \
  --data-urlencode 'start=1700000000000000000' \
  --data-urlencode 'end=1700003600000000000'

# Loki metric query (rate of errors per second over last 5 minutes)
curl --get http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query=rate({job="myapp"} |= "error" [5m])' \
  --data-urlencode 'step=60s'

# Push as a specific tenant
curl -X POST http://localhost:3100/loki/api/v1/push \
  -H 'X-Scope-OrgID: team-a' \
  -H 'Content-Type: application/json' \
  -d '{"streams":[{"stream":{"job":"myapp"},"values":[["1700000000000000000","hello"]]}]}'

# Check metrics
curl http://localhost:3100/metrics
```

## Promtail-style scrape configs

In addition to the legacy flat `watch_files: [...]` list, the config now
accepts Promtail-shaped `scrape_configs`:

```json
{
  "scrape_configs": [
    {
      "paths": ["/var/log/myapp/*.log"],
      "labels": { "job": "myapp", "env": "prod" }
    }
  ]
}
```

Each tailed file automatically gets `filename=<path>` and `job=<configured>`
labels. The tailer persists its position to `<index_dir>/positions.json` on
every flush so restarts resume from where they left off (Promtail behaviour).

## Durability

* **WAL.** Every entry is appended to `<data_dir>/wal/current.wal` before
  being added to the in-memory batch. The WAL is replayed on startup and
  truncated each time a chunk is flushed and indexed.
* **gzip-compressed chunks.** When `compression_enabled` is true (default),
  finalized chunk payloads are rewritten in gzip form. Readers detect the
  flag and decompress transparently.
* **Tailer positions.** See above.

## Known limitations

* **Multi-tenancy is label-based**, not storage-isolated. All tenants share
  the same `data_dir`, WAL, and indexes; isolation is enforced by injecting a
  `tenant` matcher into every query. Suitable for trusted multi-team setups.
* **Regex operators (`=~`, `|~`) are substring matches**, not real regex.
* **Push body must be JSON** — Promtail's default snappy/protobuf body is not
  yet decoded. Configure Promtail with `client.batchwait` and JSON, or use
  Amatiz's own `/ingest` endpoint.
* **No object-storage backend** — chunks live on the local filesystem.
* **No service discovery** (Kubernetes / Consul / etc.).


## Configuration

Create `amatiz.json` in the working directory or pass `--config path`:

```json
{
  "data_dir": "./data",
  "index_dir": "./index",
  "chunk_max_bytes": 10485760,
  "compression_enabled": false,
  "batch_size": 256,
  "flush_interval_ms": 1000,
  "tail_poll_ms": 250,
  "http_host": "0.0.0.0",
  "http_port": 3100,
  "max_connections": 128,
  "default_query_limit": 1000,
  "log_level": "info"
}
```

All fields are optional. Missing fields use defaults shown above.

## Docker

```bash
# Build image
docker build -t amatiz .

# Run with persistent data
docker run -d \
  --name amatiz \
  -p 3100:3100 \
  -v ./data:/data \
  -v ./index:/index \
  amatiz serve

# Ingest from a mounted log file
docker run -d \
  -v /var/log:/logs:ro \
  -v ./data:/data \
  amatiz ingest /logs/syslog
```

## Building

```bash
# Debug build
zig build

# Optimised build
zig build -Doptimize=ReleaseFast

# Size-optimised (for Docker)
zig build -Doptimize=ReleaseSmall

# Cross-compile for Linux (from any host)
zig build -Dtarget=x86_64-linux-musl -Doptimize=ReleaseSmall

# Run tests
zig build test
```

## Project Structure

```
src/
  main.zig              Entry point, command dispatch
  cli/
    args.zig            CLI argument parser
  ingest/
    tailer.zig          File tailing with rotation detection
    pipeline.zig        Log processing pipeline (parse → enrich → batch → store)
  transport/
    server.zig          HTTP/1.1 server (TCP-based)
  storage/
    engine.zig          Storage coordinator (chunk lifecycle, indices)
    chunk.zig           Append-only chunk file management
  index/
    time_index.zig      Timestamp range → chunk mapping
    keyword_index.zig   Inverted index for keyword search
  query/
    engine.zig          Query execution (index lookup → chunk scan → filter)
  utils/
    types.zig           Shared types (LogEntry, wire format, constants)
    config.zig          JSON configuration loader
    buffer_pool.zig     Thread-safe reusable buffer pool
    timestamp.zig       Timestamp parsing and formatting
```

## Design Decisions

**Why Zig?** Zero-overhead abstractions, explicit allocators, comptime generics, cross-compilation to any target. No GC pauses, no runtime overhead, predictable performance.

**Why polling for file tailing?** inotify (Linux) and kqueue (macOS) don't work reliably across Docker bind mounts and NFS. Polling at 250ms is imperceptible for log aggregation and works everywhere.

**Why binary chunk format?** Sequential append-only writes are the fastest I/O pattern on any storage. Binary format minimizes bytes-per-entry overhead (16 bytes fixed header vs ~50+ for JSON framing).

**Why per-day directories?** Natural data lifecycle management. Old logs can be deleted by removing a date directory. Time-range queries skip entire directories without reading any files.

**Why no async I/O framework?** For a log aggregator, the bottleneck is disk I/O and network, not CPU. Thread-per-connection is simple, debuggable, and sufficient for the connection counts this system targets (< 128 concurrent).

## Releases

CI is wired up via `.github/workflows/ci.yml`:

* **Pull requests** run `zig build test` only — no image is published.
* **Tags matching `v*.*.*`** (e.g. `v0.1.0`) run the tests and then publish a
  multi-arch (`linux/amd64` + `linux/arm64`) Docker image, tagged with:
  * the full semver (`0.1.0`), `major.minor` (`0.1`), and `major` (`0`)
  * the commit short SHA (`sha-abc1234`)
  * `latest`

To enable Docker Hub publishing, add two repository secrets:

| Secret              | Value                                |
|---------------------|--------------------------------------|
| `DOCKERHUB_USERNAME`| Your Docker Hub username             |
| `DOCKERHUB_TOKEN`   | Access token from *Account → Security* |

The image is published as `${DOCKERHUB_USERNAME}/amatiz`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).


# 3fs-rs

A Rust rewrite of [3FS (Fire-Flyer File System)](https://github.com/deepseek-ai/3FS) — DeepSeek's high-performance distributed file system designed for AI training workloads.

## Status: Work in Progress

This project is an incremental rewrite of 3FS from C++ (~122K lines, 833 files) to idiomatic Rust. The rewrite is organized into 7 phases, building bottom-up by dependency order.

| Phase | Description | Crates | Status |
|-------|-------------|--------|--------|
| 1 | Foundation types, utilities, serialization, config, logging, monitoring, memory | 9 crates | **Complete** |
| 2 | Networking (TCP/RDMA), key-value store, application framework | 6 crates | **Complete** |
| 3 | Service framework, protocol definitions, stubs | 3 crates | **Complete** |
| 4 | Core services (chunk engine, storage, metadata, management) | 5 crates | **Complete** |
| 5 | Client, CLI, FUSE mount | 3 crates | **Complete** |
| 6 | Utilities (trash cleaner, analytics, monitor collector) | 3 crates | **Complete** |
| 7 | Server binaries and integration | 7 binaries | **Complete** |

### Current Stats

- **38 crates** (31 library crates + 7 binary crates)
- **~37,000 lines** of Rust across 130+ source files
- **385+ tests** passing across all phases

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Binary Targets                        │
│  meta-server  storage-server  mgmtd-server  fuse-mount  │
│  admin        monitor-collector  simple-example          │
├─────────────────────────────────────────────────────────┤
│                  Service Layer                           │
│  meta-service  storage-service  mgmtd-service           │
│  core-service  chunk-engine                              │
├─────────────────────────────────────────────────────────┤
│               Protocol & Framework                       │
│  proto  service-derive  stubs  app  cli  client  fuse   │
├─────────────────────────────────────────────────────────┤
│              Infrastructure Layer                         │
│  net  net-tcp  net-rdma  kv  kv-backends  fdb           │
├─────────────────────────────────────────────────────────┤
│                 Foundation (Phase 1)                      │
│  types  utils  serde  serde-derive  config              │
│  config-derive  logging  monitor  memory                 │
└─────────────────────────────────────────────────────────┘
```

## Crate Overview

### Phase 1 — Foundation

| Crate | Description |
|-------|-------------|
| `hf3fs-types` | StatusCode (~280 codes), Status/Result types, StrongType IDs, Address, Duration, UtcTime |
| `hf3fs-utils` | LruCache, ObjectPool, WorkStealingQueue, varint encoding, MurmurHash3, ZSTD compression, Semaphore, BackgroundRunner |
| `hf3fs-serde-derive` | `#[derive(WireSerialize, WireDeserialize)]` proc macros |
| `hf3fs-serde` | Wire serialization traits and implementations — little-endian, length-prefixed, CRC32C checksums |
| `hf3fs-config-derive` | `#[derive(Config)]` proc macro with hot-update, section grouping, min/max validation |
| `hf3fs-config` | Config trait, ConfigManager with ArcSwap, TOML parsing, hot-reload support |
| `hf3fs-logging` | tracing-based structured logging with rotating file appender and JSON format |
| `hf3fs-monitor` | Counter, Gauge, Histogram metrics with MetricsRegistry and LogReporter |
| `hf3fs-memory` | Feature-gated jemalloc/mimalloc global allocator wrappers |

### Phases 2–4 — Infrastructure & Services

| Crate | Description |
|-------|-------------|
| `hf3fs-net` / `hf3fs-net-tcp` / `hf3fs-net-rdma` | Networking layer with TCP and RDMA transports |
| `hf3fs-kv` / `hf3fs-fdb` | Key-value abstraction with FoundationDB backend |
| `hf3fs-app` | Application lifecycle framework (TwoPhaseApplication pattern) |
| `hf3fs-service-derive` | `#[hf3fs_service]` proc macro for RPC service definitions |
| `hf3fs-proto` | 100+ protocol message types for meta, storage, and management |
| `hf3fs-stubs` | Client-side stubs and mock implementations for all services |
| `hf3fs-meta-service` | Metadata service with inode management, directory entries, path resolution |
| `hf3fs-storage-service` | Storage service for chunk read/write/remove operations |
| `hf3fs-mgmtd-service` | Management daemon service with heartbeat and cluster routing |

### Phases 5–6 — Client & Utilities

| Crate | Description |
|-------|-------------|
| `hf3fs-client` | Client library with routing, retry logic, meta/storage/mgmtd clients |
| `hf3fs-cli` | Admin CLI with config, cluster, storage, and meta subcommands |
| `hf3fs-fuse` | FUSE filesystem with 25 operations, inode/handle tables |
| `hf3fs-analytics` | Event types, collector trait, structured trace logging |
| `hf3fs-monitor-collector` | Metrics aggregator and exporter framework |
| `hf3fs-trash-cleaner` | Trash item parsing, scanner, and cleanup logic |
| `hf3fs-lib` | Public API re-exports, USRBIO types, C FFI bindings |

### Phase 7 — Binaries

| Binary | Description |
|--------|-------------|
| `hf3fs-meta-server` | Metadata server binary |
| `hf3fs-storage-server` | Storage server binary |
| `hf3fs-mgmtd-server` | Management daemon binary |
| `hf3fs-fuse-mount` | FUSE mount binary |
| `hf3fs-admin` | Admin CLI binary with subcommands |
| `hf3fs-monitor-collector-bin` | Monitor collector binary |
| `hf3fs-simple-example` | Example/demo binary |

## Building

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Build in release mode
cargo build --workspace --release
```

### Requirements

- Rust 1.85.0+
- For RDMA support: `libibverbs-dev` (Linux only)
- For FoundationDB backend: `libfdb` client library

## Design Decisions

- **Idiomatic Rust**: Not a line-by-line translation. Uses Rust idioms — `Result<T, E>`, traits, enums, ownership — while preserving 3FS's architecture and wire compatibility.
- **Proc macros**: Custom derive macros (`WireSerialize`, `WireDeserialize`, `Config`) replace C++ template metaprogramming.
- **Wire compatibility**: The binary serialization format matches the original C++ implementation for interoperability.
- **Workspace organization**: Each C++ module maps to a dedicated Rust crate with explicit dependencies.

## Original Project

This is a rewrite of [deepseek-ai/3FS](https://github.com/deepseek-ai/3FS), licensed under MIT. 3FS is a high-performance distributed file system that uses disaggregated architecture with RDMA networking and targets AI/ML training and checkpointing workloads.

## License

MIT — same as the original 3FS project.

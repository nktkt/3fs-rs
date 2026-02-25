# 3fs-rs

A Rust rewrite of [3FS (Fire-Flyer File System)](https://github.com/deepseek-ai/3FS) — DeepSeek's high-performance distributed file system designed for AI training workloads.

## Status: Work in Progress

This project is an incremental rewrite of 3FS from C++ (~122K lines, 833 files) to idiomatic Rust. The rewrite is organized into 7 phases, building bottom-up by dependency order.

| Phase | Description | Crates | Status |
|-------|-------------|--------|--------|
| 1 | Foundation types, utilities, serialization, config, logging, monitoring, memory | 9 crates | **Complete** (126 tests passing) |
| 2 | Networking (TCP/RDMA), key-value store, application framework | 6 crates | In Progress |
| 3 | Service framework, protocol definitions, stubs | 3 crates | In Progress |
| 4 | Core services (chunk engine, storage, metadata, management) | 5 crates | In Progress |
| 5 | Client, CLI, FUSE mount | 3 crates | Scaffolded |
| 6 | Utilities (trash cleaner, analytics, monitor collector) | 3 crates | Scaffolded |
| 7 | Server binaries and integration | 7 binaries | Scaffolded |

### Current Stats

- **38 crates** (31 library crates + 7 binary crates)
- **~13,400 lines** of Rust across 93 source files
- **126+ tests** passing (Phase 1 fully green)

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

## Phase 1 Crates (Complete)

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

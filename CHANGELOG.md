# Changelog

> **Maintained fork:** [structured-world/coordinode-fjall](https://github.com/structured-world/coordinode-fjall)
> of [fjall-rs/fjall](https://github.com/fjall-rs/fjall) by [Structured World Foundation](https://sw.foundation).
> Published on [crates.io](https://crates.io/crates/coordinode-fjall).
> Fork tags use `v`-prefix (`v4.0.0`); upstream uses bare tags (`3.1.1`).

## [Unreleased]

## [4.1.0](https://github.com/structured-world/coordinode-fjall/compare/v4.0.0...v4.1.0) - 2026-03-24

### Added

- db_bench tool + benchmark CI workflow + gh-pages dashboard ([#46](https://github.com/structured-world/coordinode-fjall/pull/46))

### Documentation

- add codecov, benchmarks, deps.rs, license badges to README ([#54](https://github.com/structured-world/coordinode-fjall/pull/54))

### Fixed

- *(oracle)* reclaim committed_txns entries on every commit ([#51](https://github.com/structured-world/coordinode-fjall/pull/51))

## [4.0.0] — Fork Epoch (2026-03-23)

First release of `coordinode-fjall` — maintained fork of [fjall-rs/fjall](https://github.com/fjall-rs/fjall) v3.1.1.
Published to [crates.io](https://crates.io/crates/coordinode-fjall). All changes since upstream v3.1.1.

### Added

- Merge operator API for commutative LSM operations (#34)
- Online backup via checkpointing (#31)
- Snapshot (non-serializable) reads in OptimisticTxDatabase (#22)
- JournalWriter trait for pluggable WAL (#28)
- Custom sequence number generators (#4)
- Switch to coordinode-lsm-tree v4.0 from crates.io (package alias, zero code changes)
- Zstd + encryption feature passthrough to lsm-tree

### Fixed

- Prevent deadlock in DatabaseInner::drop under write pressure (#1)
- Ingested data invisible after reopen (#2)
- Flush starvation under sustained write pressure
- Journal checksum verified on raw bytes, not re-serialized data (#21)
- Worker thread counter underflow race on early exit
- Resolve 50 clippy errors (indexing_slicing, unwrap_used, expect_used) (#36)
- Journal underflow guard before payload offset subtraction
- Worker flush recovery limited to worker #0 to avoid channel flooding
- Adapt to CompactionResult return type (lsm-tree #103)
- 30+ additional correctness and safety fixes

### Performance

- Write group commit pipeline for concurrent writers (#32)
- Optimized journal encoding for single-item writes (#3)
- HashingWriter for zero-alloc checksum verification
- Write SingleItem directly from borrowed slices

### Refactored

- Journal batch reader reuses HashingWriter, HashingSink for checksum
- Extract JournalWriter trait for Raft WAL integration
- Table-driven assertions in ingest reopen tests
- Transaction helpers: mark_key_read, bind keyspace once in for_update
- Tighten visibility and use #[expect] with reasons on all suppressions
- 15+ additional cleanup refactors

### Testing

- Benchmark infrastructure and Hermitage SSI test suite (#23)
- Regression test for ingested data invisible after reopen (#2)
- Snapshot and serializable read isolation verification
- SingleItem error recovery path coverage
- Doc test fix: replace todo!() with None (#25)

## 3.1.0

- [feat] Implemented support for compaction filters (custom logic during compactions)
- [msrv] Reduced MSRV to 1.90

## 3.0.0

- [feat] Implemented new block format in `lsm-tree`
- [feat] Bookkeep LSM-tree changes (flushes, compactions) in `Version` history
- [feat] Prefix truncation inside data & index blocks
- [feat] Allow unpinning filter blocks
- [feat] Implemented partitioned filters
- [feat] Allow calling bulk ingestion on non-empty keyspaces
- [feat] Introduced level-based configuration policies for most configuration parameters
- [feat] Journal compression for large values
- [feat] Database locking using the new Rust file locking API
- [feat] Rewritten key-value separation to run during compactions, instead of dedicated GC runs
- [feat] Full file checksums to allow fast database corruption checks (in the future)
- [feat] Checksum check on block & blob reads
- [api] Make Ingestion API more flexible
- [feat] Shortening eligible sequence numbers when compacting into the last level to save disk space
- [api] Change constructor to `Database::builder` instead of `Config::new`
- [api] Changed naming of keyspace->database, and partition->keyspace
- [api] Change transaction feature flags to be separate structs, `OptimisticTxDatabase` and `SingleWriterTxDatabase`
- [api] Changed snapshot error type, fixes #156
- [api] Unified transactions read operations and snapshots with `Readable` trait
- [api] Guard API for iterator values
- [api] Removed old garbage collection APIs
- [api] `metrics` feature flag for cache hit rates etc. (will be exposed in the future)
- [api] Change `bytes` feature flag to `bytes_1` to pin its version
- [api] Make read operations in optimistic write transactions non-mut
- [fix] Consider blob files in FIFO compaction size limit, fixes #133
- [perf] Use a single hash per key for filters, instead of two
- [perf] Improve leveled compaction scoring
- [perf] Improve leveled compaction picking to use less hashing and heap allocations
- [perf] Use `quick-cache` for file descriptor caching
- [perf] Promote levels immediately to L6 to get rid of tombstones easily
- [perf] Rewritten maintenance task bookkeeping, and write stalling mechanisms to be less aggressive
- [perf] Allow `lsm-tree` flushes to merge multiple sealed memtables into L0, if necessary
- [perf] Skip heap allocation in blob memtable inserts
- [perf] Skip compression when rewriting compressed blob files
- [msrv] Increased MSRV to **1.91**
- [misc] Blob file descriptor caching
- [misc] Use Rust native `path::absolute`, removing `path-absolutize` dependency
- [misc] Remove `std-semaphore` dependency
- [misc] Remove `miniz` (will be replaced in the future)
- [misc] Use `byteorder-lite` as drop-in replacement for `byteorder`
- [refactor] Changed background workers to be a single thread pool
- [internal] Store keyspace configurations in a meta keyspace, instead of individual binary config files
- [internal] Use `sfa` for most file scaffolding in `lsm-tree`

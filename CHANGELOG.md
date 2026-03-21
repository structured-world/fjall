# Changelog

> **Maintained fork** of [fjall-rs/fjall](https://github.com/fjall-rs/fjall) by [Structured World Foundation](https://sw.foundation) for the CoordiNode database engine.
> Fork epoch starts at **v4.0.0** — upstream tags use no prefix (`3.1.1`), fork tags use `v`-prefix (`v4.0.0`).

## 4.0.0

Fork epoch release. Aligned with lsm-tree V4 disk format (based on pre-4.0.0 git revision; see Cargo.toml for exact commit).

- [feat] Custom sequence number generators (`SequenceNumberGenerator` trait)
- [feat] Snapshot (non-serializable) reads in `OptimisticTxDatabase`
- [perf] Optimized journal encoding for single-item writes (zero-alloc path)
- [perf] `HashingWriter` for zero-alloc checksum verification in journal
- [fix] Deadlock in `DatabaseInner::drop` under write pressure
- [fix] Flush starvation under sustained write pressure
- [fix] Worker thread counter underflow race on early exit
- [fix] Journal checksum verified on raw bytes, not re-serialized data
- [fix] Journal underflow guard before payload offset subtraction
- [fix] Ingested data invisible after reopen
- [fix] Worker flush recovery limited to worker #0 to avoid channel flooding
- [refactor] Journal batch reader reuses `HashingWriter`, `HashingSink` for checksum
- [refactor] Table-driven assertions in ingest reopen tests
- [refactor] Transaction helpers: `mark_key_read`, bind keyspace once in `for_update`
- [ci] CoordiNode CI with multi-OS matrix, Copilot review, upstream monitor
- [ci] Automated changelog and releases via release-plz
- [ci] Dependabot for cargo and actions dependencies
- [deps] Bumped lz4_flex from 0.11 to 0.13

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

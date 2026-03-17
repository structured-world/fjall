---
applyTo: "**/*.rs"
---

# Rust Code Review Instructions

## Review Priority (HIGH → LOW)

Focus review effort on real bugs, not cosmetics. Stop after finding issues in higher tiers — do not pad reviews with low-priority nitpicks.

### Tier 1 — Logic Bugs and Correctness (MUST flag)
- Data corruption: wrong key encoding, incorrect key range boundaries, off-by-one in segment range checks
- Transaction isolation: snapshot reads seeing uncommitted data, write-write conflicts not detected
- Compaction/flush ordering: levels merged in wrong order, tombstones dropped before reaching all overlapping segments
- Sequence number monotonicity: non-monotonic sequence numbers assigned to writes, stale reads from broken ordering
- Incorrect merge/batch semantics: partial batch application, merge operator applied in wrong order
- Missing validation: unchecked segment metadata, unvalidated key/value sizes from disk
- Resource leaks: unclosed file handles, leaked temporary files, missing cleanup on drop
- Concurrency: data races, lock ordering violations, shared mutable state without sync
- Error swallowing: `let _ = fallible_call()` silently dropping errors that affect correctness
- Integer overflow/truncation on size calculations, block offsets, or segment counters

### Tier 2 — Safety and Crash Recovery (MUST flag)
- `unsafe` without `// SAFETY:` invariant explanation
- `unwrap()`/`expect()` on disk I/O, file operations, or deserialization (must use `Result` propagation)
- Crash safety: partial writes not protected by WAL, missing fsync before metadata update, wrong fsync ordering (data must be fsynced before the manifest that references it)
- WAL consistency: entries readable after crash, incomplete entries detectable and skippable
- File atomicity: metadata files updated in place instead of write-to-temp + rename
- Hardcoded secrets, credentials, or private URLs

### Tier 3 — API Design and Robustness (flag if clear improvement)
- Public API missing `#[must_use]` on builder-style methods returning `Self`, `Option`, or custom result-like types (`Result` itself is already `#[must_use]`)
- `pub` visibility where `pub(crate)` suffices
- Missing `Send + Sync` bounds on types used across threads
- `Clone` on large types where a reference would work
- Unnecessary allocation: `Vec` where an iterator would suffice, cloning keys for lookup

### Tier 4 — Style (ONLY flag if misleading or confusing)
- Variable/function names that actively mislead about behavior
- Dead code (unused functions, unreachable branches)

## DO NOT Flag (Explicit Exclusions)

These are not actionable review findings. Do not raise them:

- **Comment wording vs code behavior**: If a comment says "flush when memtable is full" but the threshold is a byte count, the intent is clear. Do not suggest rewording comments to match implementation details. Comments describe intent and context, not repeat the code.
- **Comment precision**: "returns the value" when it technically returns `Result<Option<Value>>` — the comment conveys meaning, not the type signature.
- **Magic numbers with context**: `4096` for block size, `10` for L0 threshold in a test with an explanatory comment or assertion message. Do not suggest named constants when the value is used once with sufficient context.
- **Minor naming preferences**: `opts` vs `options`, `cf` vs `column_family`, `kv` vs `key_value` — these are team style, not bugs.
- **Import ordering**: Grouping or ordering of import statements is style, not a finding. (Unused imports ARE actionable — they trigger `-D warnings` in CI.)
- **Test code style**: Tests prioritize readability and explicitness over DRY. Repeated setup code in tests is acceptable.
- **`#[allow(clippy::...)]` in existing upstream code**: Respect existing suppressions with justification. New code in this fork should use `#[expect(clippy::...)]` per the Rust-Specific Standards section.
- **Partition/keyspace naming in tests**: Names like `"default"`, `"test"`, `"data"` in test code are fine — they exist for clarity, not production naming standards.
- **Compaction/flush thresholds in tests**: Specific numeric values for segment size, L0 threshold, or memtable size in tests are chosen for test speed and determinism, not production tuning.

## Scope Rules

- **Review ONLY code within the PR's diff.** Do not suggest inline fixes for unchanged lines.
- For issues **outside the diff**, suggest opening a separate issue.
- **Read the PR description.** If it lists known limitations or deferred items, do not re-flag them.
- This fork has **multiple feature branches in parallel**. A fix that seems missing in one PR may already exist in another. Check the PR body for cross-references.

## Rust-Specific Standards

- Prefer `#[expect(lint)]` over `#[allow(lint)]` — `#[expect]` warns when suppression becomes unnecessary
- `TryFrom`/`TryInto` for fallible conversions; `as` casts need justification
- No `unwrap()` / `expect()` on I/O paths — use `?` propagation
- `expect()` is acceptable for programmer invariants (e.g., lock poisoning, `const` construction) with reason
- Code must pass `cargo clippy --all-features -- -D warnings`

## Testing Standards

- Test naming: `fn <what>_<condition>_<expected>()` or `fn test_<scenario>()`
- No mocks for storage — use real on-disk files via `tempfile::tempdir()`
- Crash recovery tests: write data, simulate crash (drop without flush/close), reopen and verify WAL replay
- Integration tests that require special setup use `#[ignore = "reason"]`
- Prefer `assert_eq!` with message over bare `assert!` for better failure output
- Hardcoded values in tests are fine when accompanied by explanatory comments or assertion messages

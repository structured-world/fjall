// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::{JournalWriter, PersistMode};
use crate::{batch::item::Item as BatchItem, keyspace::InternalKeyspaceId};
use lsm_tree::{CompressionType, SeqNo, ValueType};
use std::path::PathBuf;

/// A no-op journal writer that discards all writes.
///
/// Use this when durability is handled by an external system such as
/// a Raft consensus WAL. All write operations succeed immediately
/// without performing any I/O.
///
/// Recovery always returns an empty result since the external WAL
/// is responsible for replaying committed state.
pub struct NoopWriter;

impl JournalWriter for NoopWriter {
    fn write_raw(
        &mut self,
        _keyspace_id: InternalKeyspaceId,
        _key: &[u8],
        _value: &[u8],
        _value_type: ValueType,
        _seqno: SeqNo,
    ) -> crate::Result<usize> {
        Ok(0)
    }

    fn write_batch(&mut self, _items: &[BatchItem], _seqno: SeqNo) -> crate::Result<usize> {
        Ok(0)
    }

    fn write_clear(
        &mut self,
        _keyspace_id: InternalKeyspaceId,
        _seqno: SeqNo,
    ) -> crate::Result<usize> {
        Ok(0)
    }

    fn persist(&mut self, _mode: PersistMode) -> std::io::Result<()> {
        Ok(())
    }

    /// Always returns 0 — prevents journal rotation (`worker_pool` rotates when `pos() > 64M`).
    fn pos(&mut self) -> crate::Result<u64> {
        Ok(0)
    }

    /// Always returns 0 — reports no disk usage for journal space accounting.
    fn len(&self) -> crate::Result<u64> {
        Ok(0)
    }

    fn rotate(&mut self) -> crate::Result<(PathBuf, PathBuf)> {
        // NoopWriter has no files — rotation should never be triggered
        // because pos() always returns 0, which is below the rotation threshold
        Err(crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "noop journal does not support rotation",
        )))
    }

    fn set_compression(&mut self, _comp: CompressionType, _threshold: usize) {}

    fn path(&self) -> Option<PathBuf> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn noop_write_raw_returns_zero() {
        let mut w = NoopWriter;
        let n = w.write_raw(0, b"key", b"val", ValueType::Value, 1);
        assert_eq!(n.ok(), Some(0));
    }

    #[test]
    fn noop_write_batch_returns_zero() {
        let mut w = NoopWriter;
        assert_eq!(w.write_batch(&[], 0).ok(), Some(0));
    }

    #[test]
    fn noop_write_clear_returns_zero() {
        let mut w = NoopWriter;
        assert_eq!(w.write_clear(0, 0).ok(), Some(0));
    }

    #[test]
    fn noop_persist_succeeds() {
        let mut w = NoopWriter;
        assert!(w.persist(PersistMode::SyncAll).is_ok());
        assert!(w.persist(PersistMode::SyncData).is_ok());
        assert!(w.persist(PersistMode::Buffer).is_ok());
    }

    #[test]
    fn noop_pos_and_len_zero() {
        let mut w = NoopWriter;
        assert_eq!(w.pos().ok(), Some(0));
        assert_eq!(w.len().ok(), Some(0));
    }

    #[test]
    fn noop_rotate_returns_error() {
        let mut w = NoopWriter;
        assert!(w.rotate().is_err());
    }

    #[test]
    fn noop_set_compression_is_harmless() {
        let mut w = NoopWriter;
        w.set_compression(CompressionType::None, 0);
        w.set_compression(CompressionType::Lz4, 4096);
    }

    #[test]
    fn noop_path_is_none() {
        let w = NoopWriter;
        assert!(w.path().is_none());
    }
}

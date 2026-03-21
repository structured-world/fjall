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

    fn pos(&mut self) -> crate::Result<u64> {
        Ok(0)
    }

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

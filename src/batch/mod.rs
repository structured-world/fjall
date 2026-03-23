// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod item;

use crate::{write_group::WriteOp, Database, Keyspace, PersistMode};
use item::Item;
use lsm_tree::{AbstractTree, UserKey, UserValue, ValueType};
use std::collections::HashSet;

/// An atomic write batch
///
/// Allows atomically writing across keyspaces inside the [`Database`].
pub struct WriteBatch {
    pub(crate) data: Vec<Item>,
    db: Database,
    durability: Option<PersistMode>,
}

impl WriteBatch {
    /// Initializes a new write batch.
    ///
    /// This function is called by [`Database::batch`].
    pub(crate) fn new(db: Database) -> Self {
        Self {
            data: Vec::new(),
            db,
            durability: None,
        }
    }

    /// Initializes a new write batch with preallocated capacity.
    ///
    /// ### Note
    ///
    /// "Capacity" refers to the number of batch item slots, not their size in memory.
    #[must_use]
    pub fn with_capacity(db: Database, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            db,
            durability: None,
        }
    }

    /// Gets the number of batched items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no batches items (yet).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Inserts a key-value pair into the batch.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(&mut self, p: &Keyspace, key: K, value: V) {
        self.data
            .push(Item::new(p.clone(), key, value, ValueType::Value));
    }

    /// Removes a key-value pair.
    pub fn remove<K: Into<UserKey>>(&mut self, p: &Keyspace, key: K) {
        self.data
            .push(Item::new(p.clone(), key, vec![], ValueType::Tombstone));
    }

    /// Adds a weak tombstone marker for a key.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    #[doc(hidden)]
    pub fn remove_weak<K: Into<UserKey>>(&mut self, p: &Keyspace, key: K) {
        self.data
            .push(Item::new(p.clone(), key, vec![], ValueType::WeakTombstone));
    }

    /// Adds a merge operand for a key.
    ///
    /// The operand is lazily combined with the existing value during reads
    /// and compaction, using the merge operator registered on the keyspace.
    ///
    /// All items in a batch share a single sequence number at commit time,
    /// so multiple merge operands for the same key may not all be preserved.
    /// Use separate batches or `Keyspace::merge` directly when every
    /// operand must be retained.
    pub fn merge<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        p: &Keyspace,
        key: K,
        operand: V,
    ) {
        self.data
            .push(Item::new(p.clone(), key, operand, ValueType::MergeOperand));
    }

    /// Commits the batch to the [`Database`] atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[expect(
        clippy::missing_panics_doc,
        reason = "unreachable! for programmer invariant"
    )]
    pub fn commit(mut self) -> crate::Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let items = std::mem::take(&mut self.data);

        // Validate merge operands BEFORE journaling — reject early to avoid
        // partial journal+memtable state that requires aborted() cleanup.
        for item in &items {
            if item.value_type == ValueType::MergeOperand
                && item.keyspace.config.merge_operator.is_none()
            {
                return Err(crate::Error::MissingMergeOperator);
            }
        }

        log::trace!("batch: Submitting {} items to write group", items.len());

        let (batch_seqno, op) = self.db.supervisor.write_group.submit(
            WriteOp::Batch { items },
            self.durability,
            &self.db.supervisor.journal,
            &self.db.supervisor.seqno,
            &self.db.is_poisoned,
            &self.db.supervisor.pending_watermark,
        )?;

        let WriteOp::Batch { items } = op else {
            unreachable!("submitted Batch, must get Batch back");
        };

        // TODO: maybe we can use a stack alloc hashset/vec here, such as smallset
        #[expect(clippy::mutable_key_type)]
        let mut keyspaces_with_possible_stall = HashSet::new();

        #[expect(clippy::expect_used)]
        let keyspaces = self
            .db
            .supervisor
            .keyspaces
            .read()
            .expect("lock is poisoned");

        let mut batch_size = 0u64;

        log::trace!("Applying batch (size={}) to memtable(s)", items.len());

        for item in items {
            // TODO: need a better, generic write op
            let (item_size, _) = match item.value_type {
                ValueType::Value => item.keyspace.tree.insert(item.key, item.value, batch_seqno),
                ValueType::Tombstone => item.keyspace.tree.remove(item.key, batch_seqno),
                ValueType::WeakTombstone => item.keyspace.tree.remove_weak(item.key, batch_seqno),
                // NOTE: merge_operator validated in the pre-journal check above
                ValueType::MergeOperand => {
                    item.keyspace.tree.merge(item.key, item.value, batch_seqno)
                }
                ValueType::Indirection => unreachable!(),
            };

            batch_size += item_size;

            // IMPORTANT: Clone the handle, because we don't want to keep the keyspaces lock open
            keyspaces_with_possible_stall.insert(item.keyspace.clone());
        }

        self.db.supervisor.pending_watermark.applied(batch_seqno);

        log::trace!("batch: Write group commit done");

        drop(keyspaces);

        // IMPORTANT: Add batch size to current write buffer size
        // Otherwise write buffer growth is unbounded when using batches
        self.db.supervisor.write_buffer_size.allocate(batch_size);

        // Check each affected keyspace for write stall/halt
        for keyspace in &keyspaces_with_possible_stall {
            let memtable_size = keyspace.tree.active_memtable().size();
            keyspace.check_memtable_rotate(memtable_size);
            keyspace.local_backpressure();
        }

        Ok(())
    }
}

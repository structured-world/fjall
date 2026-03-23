// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::SharedSequenceNumberGenerator;

use crate::{
    db::Keyspaces,
    flush::manager::FlushManager,
    journal::{manager::JournalManager, Journal},
    snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager,
    write_group::{PendingWatermark, WriteGroup},
};
use std::sync::{Arc, Mutex, RwLock};

pub struct SupervisorInner {
    pub db_config: crate::Config,

    /// Dictionary of all keyspaces
    #[doc(hidden)]
    pub keyspaces: Arc<RwLock<Keyspaces>>,

    pub(crate) write_buffer_size: WriteBufferManager,
    pub(crate) flush_manager: FlushManager,

    pub seqno: SharedSequenceNumberGenerator,

    pub snapshot_tracker: SnapshotTracker,

    pub(crate) journal: Arc<Journal>,

    /// Group commit pipeline — amortizes journal I/O across concurrent writers
    pub(crate) write_group: WriteGroup,

    /// Ordered watermark — ensures MVCC visibility advances only through
    /// consecutively-applied seqnos (prevents out-of-order publish after group commit)
    pub(crate) pending_watermark: PendingWatermark,

    /// Tracks journal size and garbage collects sealed journals when possible
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,

    #[expect(
        dead_code,
        reason = "backpressure_lock reserved for future write throttling"
    )]
    pub(crate) backpressure_lock: Mutex<()>,
}

#[derive(Clone)]
pub struct Supervisor(Arc<SupervisorInner>);

impl std::ops::Deref for Supervisor {
    type Target = SupervisorInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Supervisor {
    pub fn new(inner: SupervisorInner) -> Self {
        Self(Arc::new(inner))
    }
}

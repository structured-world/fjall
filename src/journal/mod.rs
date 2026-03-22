// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod batch_reader;
pub mod entry;
pub mod error;
pub mod manager;
pub mod noop;
pub mod reader;
mod recovery;
pub mod writer;

#[cfg(test)]
mod test;

use self::writer::{JournalWriter, PersistMode};
use crate::file::fsync_directory;
use batch_reader::JournalBatchReader;
use lsm_tree::CompressionType;
use reader::JournalReader;
use recovery::{recover_journals, RecoveryResult};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, MutexGuard,
    },
};
use writer::Writer;

/// Type alias for the mutex guard returned by [`Journal::get_writer`].
pub type JournalWriterGuard<'a> = MutexGuard<'a, Box<dyn JournalWriter>>;

/// The journal (write-ahead log) ensures crash safety by recording
/// mutations before they are applied to memtables.
///
/// The journal wraps a [`JournalWriter`] behind a mutex to serialize
/// all write operations. The default file-based writer can be replaced
/// with a custom implementation (e.g., [`noop::NoopWriter`] for Raft
/// integration) via [`crate::JournalMode`].
pub struct Journal {
    writer: Mutex<Box<dyn JournalWriter>>,

    /// Set to `true` when opening in noop mode and leftover `.jnl` files
    /// are detected from a prior file-based run.
    leftover_detected: AtomicBool,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.path() {
            Some(path) => write!(f, "{}", path.display()),
            None => write!(f, "<noop journal>"),
        }
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        log::trace!("Dropping journal, trying to flush");

        match self.persist(PersistMode::SyncAll) {
            Ok(()) => {
                log::trace!("Flushed journal successfully");
            }
            Err(e) => {
                log::error!("Flush error on drop: {e:?}");
            }
        }

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl Journal {
    /// Creates a journal wrapping a custom [`JournalWriter`] implementation.
    pub fn with_writer(writer: Box<dyn JournalWriter>) -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            writer: Mutex::new(writer),
            leftover_detected: AtomicBool::new(false),
        }
    }

    /// Creates a no-op journal that discards all writes.
    ///
    /// Use when an external system (e.g., Raft WAL) handles durability.
    pub fn noop() -> Self {
        Self::with_writer(Box::new(noop::NoopWriter))
    }

    /// Marks that leftover `.jnl` files were found during noop mode open.
    pub(crate) fn set_leftover_detected(&self) {
        self.leftover_detected.store(true, Ordering::Relaxed);
    }

    /// Returns `true` if leftover `.jnl` files were detected during open.
    #[doc(hidden)]
    #[must_use]
    #[expect(dead_code, reason = "used in db_test via supervisor.journal")]
    pub fn leftover_detected(&self) -> bool {
        self.leftover_detected.load(Ordering::Relaxed)
    }

    /// Sets compression on the underlying writer.
    pub fn with_compression(self, comp: CompressionType, threshold: usize) -> Self {
        {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let mut writer = self.writer.lock().expect("lock is poisoned");
            writer.set_compression(comp, threshold);
        }
        self
    }

    fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(Self {
            writer: Mutex::new(Box::new(Writer::from_file(path)?)),
            leftover_detected: AtomicBool::new(false),
        })
    }

    /// Creates a new file-based journal at the given path.
    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        log::trace!("Creating new journal at {}", path.display());

        #[expect(
            clippy::expect_used,
            reason = "journal path always has a parent directory"
        )]
        let folder = path.parent().expect("parent should exist");

        std::fs::create_dir_all(folder).inspect_err(|e| {
            log::error!(
                "Failed to create journal folder at {}: {e:?}",
                path.display(),
            );
        })?;

        let writer = Writer::create_new(path)?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(folder)?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(Self {
            writer: Mutex::new(Box::new(writer)),
            leftover_detected: AtomicBool::new(false),
        })
    }

    /// Hands out write access for the journal.
    pub(crate) fn get_writer(&self) -> JournalWriterGuard<'_> {
        #[expect(clippy::expect_used)]
        self.writer.lock().expect("lock is poisoned")
    }

    /// Returns the journal file path, if backed by a file.
    ///
    /// Note: `Journal` is not re-exported from `lib.rs` — this is crate-internal API.
    /// The `Option` return handles noop journals which have no backing file.
    pub fn path(&self) -> Option<PathBuf> {
        self.get_writer().path()
    }

    /// Returns a reader for recovering batches from the journal file.
    ///
    /// Returns `None` for non-file-based journals (e.g., noop).
    pub fn get_reader(&self) -> crate::Result<Option<JournalBatchReader>> {
        let Some(path) = self.path() else {
            return Ok(None);
        };
        let raw_reader = JournalReader::new(path)?;
        Ok(Some(JournalBatchReader::new(raw_reader)))
    }

    /// Persists the journal.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        let mut journal_writer = self.get_writer();
        journal_writer.persist(mode).map_err(Into::into)
    }

    /// Recovers file-based journals from the given path.
    pub fn recover<P: AsRef<Path>>(
        path: P,
        compression: CompressionType,
        compression_threshold: usize,
    ) -> crate::Result<RecoveryResult> {
        recover_journals(path, compression, compression_threshold)
    }
}

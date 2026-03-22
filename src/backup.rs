// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::{
        fsync_directory, KEYSPACES_FOLDER, LOCK_FILE, LSM_CURRENT_VERSION_MARKER, VERSION_MARKER,
    },
    Database,
};
use lsm_tree::AbstractTree;
use std::path::{Path, PathBuf};

/// Tries to hard-link `src` to `dst`, falling back to a durable copy if
/// hard-linking fails (e.g., cross-device backup).
fn link_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(e) => {
            log::debug!(
                "Hard-link failed for {} -> {} ({e}), falling back to copy",
                src.display(),
                dst.display(),
            );
            copy_and_fsync(src, dst)
        }
    }
}

/// Copies `src` to `dst` and fsyncs the destination file to ensure durability.
///
/// Uses explicit `File::open` + `std::io::copy` instead of `std::fs::copy`
/// (`CopyFileExW`) because the latter fails on Windows when the source file
/// is held open for writing (e.g., the active journal file).
fn copy_and_fsync(src: &Path, dst: &Path) -> std::io::Result<()> {
    let mut reader = std::fs::File::open(src)?;
    let mut writer = std::fs::File::create(dst)?;
    std::io::copy(&mut reader, &mut writer)?;
    writer.sync_all()
}

/// Returns `parent` of `path`, normalizing empty parent (from relative paths
/// like `"backup"`) to `"."` so that `create_dir_all` and `fsync_directory`
/// always receive a valid path.
fn effective_parent(path: &Path) -> PathBuf {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// LSM-tree on-disk layout:
///   `<keyspace>/current`       — manifest pointer (references active version)
///   `<keyspace>/v<N>`          — version snapshots
///   `<keyspace>/tables/<id>`   — SST segment files (immutable)
///   `<keyspace>/blobs/<id>`    — blob files for KV separation (immutable)
///
/// Copies an LSM-tree's on-disk files into `dst_dir` by enumerating the source
/// directories directly:
/// - SST and blob files are hard-linked (immutable, zero-copy safe)
/// - Version files and manifest are copied and fsynced (small, mutable metadata)
///
/// Using directory enumeration (rather than `tree.current_version()`) avoids a
/// race with compaction: the manifest (`current`) and linked tables are always
/// from the same on-disk state. Any orphaned tables in the backup are cleaned
/// up by recovery.
fn backup_tree(src_dir: &Path, dst_dir: &Path) -> crate::Result<()> {
    // Copy manifest (LSM_CURRENT_VERSION_MARKER file). This is required for
    // keyspace recovery: recovery deletes keyspaces without a `current` marker,
    // so backing up without it would produce an incomplete backup.
    let manifest_src = src_dir.join(LSM_CURRENT_VERSION_MARKER);
    if manifest_src.try_exists()? {
        copy_and_fsync(&manifest_src, &dst_dir.join(LSM_CURRENT_VERSION_MARKER))?;
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "missing manifest file `current` in keyspace directory {}",
                src_dir.display()
            ),
        )
        .into());
    }

    // Copy and fsync version files (v0, v1, v2, ...)
    for entry in std::fs::read_dir(src_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with('v') && entry.file_type()?.is_file() {
            copy_and_fsync(&entry.path(), &dst_dir.join(&name))?;
        }
    }

    // Hard-link all SST files from tables/ directory.
    // Linking the full directory contents (rather than a version snapshot)
    // ensures consistency with the copied manifest — any extra orphaned
    // tables are harmless and cleaned up by recovery.
    let tables_src = src_dir.join("tables");
    let tables_dst = dst_dir.join("tables");
    std::fs::create_dir_all(&tables_dst)?;

    if tables_src.try_exists()? {
        for entry in std::fs::read_dir(&tables_src)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                link_or_copy(&entry.path(), &tables_dst.join(entry.file_name()))?;
            }
        }
    }

    fsync_directory(&tables_dst)?;

    // Hard-link all blob files from blobs/ directory (if it exists)
    let blobs_src = src_dir.join("blobs");
    if blobs_src.try_exists()? {
        let blobs_dst = dst_dir.join("blobs");
        std::fs::create_dir_all(&blobs_dst)?;

        for entry in std::fs::read_dir(&blobs_src)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                link_or_copy(&entry.path(), &blobs_dst.join(entry.file_name()))?;
            }
        }

        fsync_directory(&blobs_dst)?;
    }

    fsync_directory(dst_dir)?;

    Ok(())
}

/// Copies a keyspace's LSM-tree into the backup destination.
fn backup_keyspace(src_path: &Path, keyspaces_dst: &Path) -> crate::Result<()> {
    #[expect(
        clippy::expect_used,
        reason = "keyspace paths always have a directory name"
    )]
    let dir_name = src_path
        .file_name()
        .expect("keyspace path should have dir name");
    let dst = keyspaces_dst.join(dir_name);

    std::fs::create_dir_all(&dst)?;
    backup_tree(src_path, &dst)
}

impl Database {
    /// Creates a consistent online backup of the database at the given path.
    ///
    /// The backup is crash-safe and can be opened as a regular database.
    /// Writes continue during backup — only the journal is briefly locked
    /// while the active journal file is copied.
    ///
    /// LSM segment files (SSTs, blobs) are immutable and are hard-linked into
    /// the backup directory for O(1) per-file cost. If hard-linking fails
    /// (e.g., cross-device), files are copied instead.
    ///
    /// # Consistency model
    ///
    /// The backup captures a consistent point-in-time of the WAL (journal),
    /// then copies SST/blob segments and metadata without holding the write
    /// lock. Concurrent writes after the WAL snapshot go into a new journal
    /// and are NOT included in the backup. Recovery replays the backed-up WAL
    /// on top of the SST segments, producing a consistent state via LSM
    /// sequence numbers (duplicate entries are resolved by highest seqno).
    ///
    /// # Noop journal mode
    ///
    /// When using [`JournalMode::Noop`](crate::JournalMode::Noop), no journal
    /// files are copied. The external WAL system (e.g., Raft) is responsible
    /// for its own backup of unflushed data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The destination path already exists
    /// - An I/O error occurs during the backup
    ///
    /// On error, the partially-created backup directory is not cleaned up;
    /// the caller should remove it if needed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let backup_folder = tempfile::tempdir()?;
    /// # let db = Database::builder(&folder).open()?;
    /// # let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
    /// items.insert("key", "value")?;
    ///
    /// let backup_path = backup_folder.path().join("backup");
    /// db.backup_to(&backup_path)?;
    ///
    /// // The backup can be opened as a regular database
    /// drop(db);
    /// let restored = Database::builder(&backup_path).open()?;
    /// let items = restored.keyspace("my_items", KeyspaceCreateOptions::default)?;
    /// assert_eq!(&*items.get("key")?.unwrap(), b"value");
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    /// # Panics
    ///
    /// May panic if internal invariants are violated: lock poisoning on
    /// internal synchronization primitives, or unexpected failures when
    /// deriving internal filesystem paths.
    pub fn backup_to<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let path = path.as_ref();

        log::info!("Starting online backup to {}", path.display());

        // Atomically create the destination directory; fail if it already exists.
        // Using create_dir (not create_dir_all) avoids the TOCTOU race between
        // try_exists() and directory creation.
        let parent = effective_parent(path);
        std::fs::create_dir_all(&parent)?;

        match std::fs::create_dir(path) {
            Ok(()) => {
                // Ensure the directory entry is durable by fsyncing the parent.
                fsync_directory(&parent)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("backup destination already exists: {}", path.display()),
                )));
            }
            Err(e) => {
                return Err(crate::Error::Io(e));
            }
        }

        // Create destination keyspace directory
        let keyspaces_dst = path.join(KEYSPACES_FOLDER);
        std::fs::create_dir(&keyspaces_dst)?;

        self.backup_journals(path)?;

        // Backup meta keyspace (keyspace 0)
        let meta_path = self.meta_keyspace.tree().tree_config().path.clone();
        backup_keyspace(&meta_path, &keyspaces_dst)?;

        // Backup all user keyspaces.
        // Clone Keyspace handles (Arc) under the read lock to keep them alive
        // during backup — this prevents a concurrent delete_keyspace() from
        // dropping the last handle and cleaning up the directory mid-copy.
        // The read lock is released before I/O so Database::keyspace() (which
        // needs a write lock) is not blocked.
        let keyspace_handles: Vec<crate::Keyspace> = {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let keyspaces = self.supervisor.keyspaces.read().expect("lock is poisoned");
            keyspaces.values().cloned().collect()
        };

        for ks in &keyspace_handles {
            let ks_path = ks.tree.tree_config().path.clone();
            backup_keyspace(&ks_path, &keyspaces_dst)?;
            log::debug!("Backed up keyspace {:?}", ks.name);
        }

        // Copy and fsync the database version marker
        copy_and_fsync(
            &self.config.path.join(VERSION_MARKER),
            &path.join(VERSION_MARKER),
        )?;

        // Create the lock file (recovery expects it to exist)
        std::fs::File::create_new(path.join(LOCK_FILE))?.sync_all()?;

        // Fsync all directories to ensure durability
        fsync_directory(&keyspaces_dst)?;
        fsync_directory(path)?;

        log::info!("Online backup completed successfully at {}", path.display());

        Ok(())
    }

    /// Copies all journal files (active + sealed) into the backup directory.
    ///
    /// Holds the journal writer lock only while persisting and copying the
    /// active journal; sealed journals are copied while holding a read lock
    /// on the journal manager. The file-based journal is pre-allocated up to
    /// 64 MiB; the full file (including unused pre-allocated space) is copied.
    fn backup_journals(&self, backup_path: &Path) -> crate::Result<()> {
        {
            let mut journal_writer = self.supervisor.journal.get_writer();

            // Flush and fsync the active journal
            journal_writer.persist(crate::PersistMode::SyncAll)?;

            // Hold a read lock on the journal manager while copying sealed
            // journals, so maintenance (which takes a write lock) cannot delete
            // them mid-copy. Acquire the read lock while the writer lock is
            // still held to prevent a WAL rotation from adding a post-cut
            // journal to the sealed set.
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let journal_manager_guard = self
                .supervisor
                .journal_manager
                .read()
                .expect("lock is poisoned");
            let sealed_paths = journal_manager_guard.sealed_journal_paths();

            // Copy active journal file (if file-based) while still holding the
            // writer lock to keep the cut point stable.
            if let Some(active_path) = journal_writer.path() {
                #[expect(
                    clippy::expect_used,
                    reason = "journal file paths always have a file name component"
                )]
                let filename = active_path
                    .file_name()
                    .expect("journal path should have file name");

                copy_and_fsync(&active_path, &backup_path.join(filename))?;
            }

            // Release the writer lock; new writes can resume while we copy
            // sealed journals. The journal_manager read lock remains held.
            drop(journal_writer);

            // Copy sealed journal files. The paths were captured under the
            // writer lock so the set is frozen at the WAL cut point. Sealed
            // journals are immutable — they cannot be modified, only deleted
            // by maintenance. Maintenance requires a write lock on
            // journal_manager; we hold a read lock on the manager, so
            // maintenance cannot delete the snapshotted sealed journals until
            // this loop completes.
            for sealed_path in &sealed_paths {
                #[expect(
                    clippy::expect_used,
                    reason = "sealed journal paths always have a file name component"
                )]
                let filename = sealed_path
                    .file_name()
                    .expect("sealed journal path should have file name");

                copy_and_fsync(sealed_path, &backup_path.join(filename))?;
            }
        }

        log::debug!("Journal files copied. Hard-linking segments...");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn backup_tree_missing_manifest_returns_error() -> std::io::Result<()> {
        let src = tempfile::tempdir()?;
        let dst = tempfile::tempdir()?;
        let dst_path = dst.path().join("out");
        std::fs::create_dir_all(&dst_path)?;

        // src has no "current" file → backup_tree should return NotFound
        let result = backup_tree(src.path(), &dst_path);
        let err = match result {
            Err(err) => err,
            Ok(()) => panic!("backup_tree should fail without a manifest"),
        };
        assert!(
            matches!(&err, crate::Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
            "expected NotFound for missing manifest, got: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn copy_and_fsync_round_trip() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("src.dat");
        let dst = dir.path().join("dst.dat");

        let data = b"hello backup";
        std::fs::write(&src, data)?;

        copy_and_fsync(&src, &dst)?;

        assert_eq!(std::fs::read(&dst)?, data);
        Ok(())
    }

    #[test]
    fn effective_parent_normalizes_empty() {
        // Relative path like "backup" has parent Some("")
        assert_eq!(effective_parent(Path::new("backup")), PathBuf::from("."));

        // Absolute path returns actual parent
        let p = effective_parent(Path::new("/tmp/backup"));
        assert_eq!(p, PathBuf::from("/tmp"));

        // Nested relative path returns parent
        let p = effective_parent(Path::new("a/b/c"));
        assert_eq!(p, PathBuf::from("a/b"));
    }
}

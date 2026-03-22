// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::{fsync_directory, KEYSPACES_FOLDER, LOCK_FILE, VERSION_MARKER},
    Database,
};
use lsm_tree::AbstractTree;
use std::path::Path;

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

/// LSM-tree on-disk layout:
///   `<keyspace>/current`       — manifest pointer (references active version)
///   `<keyspace>/v<N>`          — version snapshots
///   `<keyspace>/tables/<id>`   — SST segment files (immutable)
///   `<keyspace>/blobs/<id>`    — blob files for KV separation (immutable)
///
/// Copies an LSM-tree's on-disk files into `dst_dir`:
/// - SST and blob files are hard-linked (immutable, zero-copy safe)
/// - Version files and manifest are copied and fsynced (small, mutable metadata)
///
/// The manifest (`current`) and version files are copied BEFORE snapshotting
/// the tree version to ensure the manifest references a version ≤ the one
/// we use for table enumeration. Any extra tables linked from a newer
/// in-memory version are harmless (recovery cleans up orphaned tables).
fn backup_tree(tree: &lsm_tree::AnyTree, dst_dir: &Path) -> crate::Result<()> {
    let src_dir = &tree.tree_config().path;

    // Copy manifest and version files FIRST, before current_version().
    // This guarantees the on-disk `current` file references a version ≤
    // the one we snapshot next, so all tables it references will be
    // included in our hard-link set. If compaction creates a newer version
    // between the copy and the snapshot, we link extra tables (which recovery
    // cleans up as orphans) rather than missing tables (which would be fatal).

    // Copy manifest ("current" file). This is required for keyspace recovery:
    // recovery deletes keyspaces without a `current` marker, so backing up
    // without it would produce an incomplete backup.
    let manifest_src = src_dir.join("current");
    if manifest_src.try_exists()? {
        copy_and_fsync(&manifest_src, &dst_dir.join("current"))?;
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

    // NOW snapshot the version — guaranteed ≥ the manifest we just copied
    let version = tree.current_version();

    // Create tables/ subdirectory and hard-link SST files
    let tables_dst = dst_dir.join("tables");
    std::fs::create_dir_all(&tables_dst)?;

    for table in version.iter_tables() {
        let src = &*table.path;

        #[expect(
            clippy::expect_used,
            reason = "SST file paths always have a file name component"
        )]
        let filename = src.file_name().expect("SST path should have file name");
        link_or_copy(src, &tables_dst.join(filename))?;
    }

    fsync_directory(&tables_dst)?;

    // Create blobs/ subdirectory and hard-link blob files (if any)
    if version.blob_file_count() > 0 {
        let blobs_dst = dst_dir.join("blobs");
        std::fs::create_dir_all(&blobs_dst)?;

        for blob_file in version.blob_files.iter() {
            let src = blob_file.path();

            #[expect(
                clippy::expect_used,
                reason = "blob file paths always have a file name component"
            )]
            let filename = src.file_name().expect("blob path should have file name");
            link_or_copy(src, &blobs_dst.join(filename))?;
        }

        fsync_directory(&blobs_dst)?;
    }

    fsync_directory(dst_dir)?;

    Ok(())
}

/// Copies a keyspace's LSM-tree into the backup destination.
fn backup_keyspace(tree: &lsm_tree::AnyTree, keyspaces_dst: &Path) -> crate::Result<()> {
    let src = &tree.tree_config().path;

    #[expect(
        clippy::expect_used,
        reason = "keyspace paths always have a directory name"
    )]
    let dir_name = src.file_name().expect("keyspace path should have dir name");
    let dst = keyspaces_dst.join(dir_name);

    std::fs::create_dir_all(&dst)?;
    backup_tree(tree, &dst)
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
    #[expect(
        clippy::missing_panics_doc,
        reason = "panics only if internal RwLock is poisoned"
    )]
    pub fn backup_to<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let path = path.as_ref();

        log::info!("Starting online backup to {}", path.display());

        // Atomically create the destination directory; fail if it already exists.
        // Using create_dir (not create_dir_all) avoids the TOCTOU race between
        // try_exists() and directory creation.
        // path.parent() returns Some("") for relative paths like "backup";
        // skip create_dir_all/fsync on empty parent (equivalent to current dir).
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }

        match std::fs::create_dir(path) {
            Ok(()) => {
                // Ensure the directory entry is durable by fsyncing the parent.
                if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    fsync_directory(parent)?;
                }
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
        backup_keyspace(self.meta_keyspace.tree(), &keyspaces_dst)?;

        // Backup all user keyspaces.
        // Clone handles under the read lock, then release it before doing I/O
        // so that Database::keyspace() (which needs a write lock) is not blocked.
        let keyspace_handles: Vec<_> = {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let keyspaces = self.supervisor.keyspaces.read().expect("lock is poisoned");
            keyspaces
                .values()
                .map(|ks| (ks.tree.clone(), ks.name.clone()))
                .collect()
        };

        for (tree, name) in &keyspace_handles {
            backup_keyspace(tree, &keyspaces_dst)?;
            log::debug!("Backed up keyspace {name:?}");
        }

        // Copy and fsync the database version marker
        copy_and_fsync(
            &self.config.path.join(VERSION_MARKER),
            &path.join(VERSION_MARKER),
        )
        .inspect_err(|e| {
            log::error!("Failed to copy version marker during backup: {e:?}");
        })?;

        // Create the lock file (recovery expects it to exist)
        {
            let lock = std::fs::File::create_new(path.join(LOCK_FILE)).inspect_err(|e| {
                log::error!("Failed to create lock file in backup: {e:?}");
            })?;
            lock.sync_all()?;
        }

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
        // Scope the journal writer lock to just persist + active journal copy.
        // Sealed journals are immutable and protected by the journal_manager
        // read lock, so the writer lock is not needed for them.
        {
            let mut journal_writer = self.supervisor.journal.get_writer();

            // Flush and fsync the active journal
            journal_writer
                .persist(crate::PersistMode::SyncAll)
                .map_err(|e| {
                    log::error!("Failed to persist journal during backup: {e:?}");
                    e
                })?;

            // Copy active journal file (if file-based)
            if let Some(active_path) = journal_writer.path() {
                #[expect(
                    clippy::expect_used,
                    reason = "journal file paths always have a file name component"
                )]
                let filename = active_path
                    .file_name()
                    .expect("journal path should have file name");

                copy_and_fsync(&active_path, &backup_path.join(filename)).inspect_err(|e| {
                    log::error!(
                        "Failed to copy active journal {} during backup: {e:?}",
                        active_path.display(),
                    );
                })?;
            }

            // `journal_writer` is dropped here, releasing the writer lock.
            // Writes can resume while sealed journals are being copied below.
        }

        // Copy sealed journal files while holding a read lock on the journal manager.
        // The lock is intentionally held for the entire copy duration: unlike the
        // keyspaces map (where we can clone handles and release), sealed journal
        // files can be deleted by JournalManager::maintenance() which acquires a
        // write lock. Releasing the read lock before copying would allow maintenance
        // to delete a journal file between listing and copying.
        {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let journal_manager = self
                .supervisor
                .journal_manager
                .read()
                .expect("lock is poisoned");

            let sealed_paths = journal_manager.sealed_journal_paths();

            for sealed_path in &sealed_paths {
                #[expect(
                    clippy::expect_used,
                    reason = "sealed journal paths always have a file name component"
                )]
                let filename = sealed_path
                    .file_name()
                    .expect("sealed journal path should have file name");

                copy_and_fsync(sealed_path, &backup_path.join(filename)).inspect_err(|e| {
                    log::error!(
                        "Failed to copy sealed journal {} during backup: {e:?}",
                        sealed_path.display(),
                    );
                })?;
            }
        }

        log::debug!("Journal files copied. Hard-linking segments...");

        Ok(())
    }
}

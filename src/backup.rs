// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::{fsync_directory, KEYSPACES_FOLDER, LOCK_FILE, VERSION_MARKER},
    Database,
};
use lsm_tree::AbstractTree;
use std::path::Path;

/// Tries to hard-link `src` to `dst`, falling back to `fs::copy` if hard-linking
/// fails (e.g., cross-device backup).
fn link_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(e) => {
            log::debug!(
                "Hard-link failed for {} -> {} ({e}), falling back to copy",
                src.display(),
                dst.display(),
            );
            std::fs::copy(src, dst)?;
            Ok(())
        }
    }
}

/// LSM-tree on-disk layout:
///   `<keyspace>/current`       — manifest pointer (references active version)
///   `<keyspace>/v<N>`          — version snapshots
///   `<keyspace>/tables/<id>`   — SST segment files (immutable)
///   `<keyspace>/blobs/<id>`    — blob files for KV separation (immutable)
///
/// Copies an LSM-tree's on-disk files into `dst_dir`:
/// - SST and blob files are hard-linked (immutable, zero-copy safe)
/// - Version files and manifest are copied (small, mutable metadata)
fn backup_tree(tree: &lsm_tree::AnyTree, dst_dir: &Path) -> crate::Result<()> {
    let src_dir = &tree.tree_config().path;
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

    // Copy version files (v0, v1, v2, ...) — small metadata, always copy
    for entry in std::fs::read_dir(src_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with('v') && entry.file_type()?.is_file() {
            std::fs::copy(entry.path(), dst_dir.join(&name))?;
        }
    }

    // Copy manifest ("current" file)
    let manifest_src = src_dir.join("current");
    if manifest_src.try_exists()? {
        std::fs::copy(&manifest_src, dst_dir.join("current"))?;
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
    /// while its files are copied.
    ///
    /// LSM segment files (SSTs, blobs) are immutable and are hard-linked into
    /// the backup directory for O(1) per-file cost. If hard-linking fails
    /// (e.g., cross-device), files are copied instead.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The destination path already exists
    /// - An I/O error occurs during the backup
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

        // Destination must not exist (prevent accidental overwrites)
        if path.try_exists()? {
            return Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("backup destination already exists: {}", path.display()),
            )));
        }

        // Create destination directory structure
        std::fs::create_dir_all(path)?;
        let keyspaces_dst = path.join(KEYSPACES_FOLDER);
        std::fs::create_dir_all(&keyspaces_dst)?;

        self.backup_journals(path)?;

        // Backup meta keyspace (keyspace 0)
        backup_keyspace(self.meta_keyspace.tree(), &keyspaces_dst)?;

        // Backup all user keyspaces
        {
            #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
            let keyspaces = self.supervisor.keyspaces.read().expect("lock is poisoned");

            for keyspace in keyspaces.values() {
                backup_keyspace(&keyspace.tree, &keyspaces_dst)?;
                log::debug!("Backed up keyspace {:?}", keyspace.name);
            }
        }

        // Copy the database version marker
        std::fs::copy(
            self.config.path.join(VERSION_MARKER),
            path.join(VERSION_MARKER),
        )
        .inspect_err(|e| {
            log::error!("Failed to copy version marker during backup: {e:?}");
        })?;

        // Create the lock file (recovery expects it to exist)
        std::fs::File::create_new(path.join(LOCK_FILE)).inspect_err(|e| {
            log::error!("Failed to create lock file in backup: {e:?}");
        })?;

        // Fsync all directories to ensure durability
        fsync_directory(&keyspaces_dst)?;
        fsync_directory(path)?;

        log::info!("Online backup completed successfully at {}", path.display());

        Ok(())
    }

    /// Copies all journal files (active + sealed) into the backup directory.
    ///
    /// Holds the journal lock for the duration — this is the write-pause window.
    fn backup_journals(&self, backup_path: &Path) -> crate::Result<()> {
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

            std::fs::copy(&active_path, backup_path.join(filename)).inspect_err(|e| {
                log::error!(
                    "Failed to copy active journal {} during backup: {e:?}",
                    active_path.display(),
                );
            })?;
        }

        // Copy sealed journal files
        #[expect(clippy::expect_used, reason = "poisoned lock is unrecoverable")]
        let sealed_paths = self
            .supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .sealed_journal_paths();

        for sealed_path in &sealed_paths {
            #[expect(
                clippy::expect_used,
                reason = "sealed journal paths always have a file name component"
            )]
            let filename = sealed_path
                .file_name()
                .expect("sealed journal path should have file name");

            std::fs::copy(sealed_path, backup_path.join(filename)).inspect_err(|e| {
                log::error!(
                    "Failed to copy sealed journal {} during backup: {e:?}",
                    sealed_path.display(),
                );
            })?;
        }

        log::debug!("Journal files copied, lock released. Hard-linking segments...");

        Ok(())
    }
}

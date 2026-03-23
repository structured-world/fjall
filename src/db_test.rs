use crate::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test_log::test]
fn clear_recover_sealed() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a")?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);

        tree.rotate_memtable_and_wait()?;
        assert!(tree.is_empty()?);
        db.supervisor.journal.get_writer().rotate()?;

        tree.insert("b", "a")?;
        assert!(tree.contains_key("b")?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert!(!tree.contains_key("a")?);
        assert!(tree.contains_key("b")?);
    }

    Ok(())
}

// TODO: investigate: flaky on macOS???
#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
#[ignore = "restore"]
fn whitebox_db_drop() -> crate::Result<()> {
    use crate::Database;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        drop(tree);
        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let _tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        let _tree2 = db.keyspace("different", Default::default)?;
        assert_eq!(7, crate::drop::load_drop_counter());
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
#[ignore = "restore"]
fn whitebox_db_drop_2() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("tree", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("tree1", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree2.insert("b", "b")?;

        tree.rotate_memtable_and_wait()?;
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[test]
pub fn test_exotic_keyspace_names() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;

    for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
        let tree = db.keyspace(name, KeyspaceCreateOptions::default)?;
        tree.insert("a", "a")?;
        assert_eq!(1, tree.len()?);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes())?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_order() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree.rotate_memtable()?;

        tree.insert("a", "b")?;
        tree.rotate_memtable()?;

        tree.insert("a", "c")?;
        tree.rotate_memtable()?;
    }

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(b"c", &*tree.get("a")?.unwrap());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_blob() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes().repeat(1_024))?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_000)
        })?;
        let tree2 = db.keyspace("default2", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());
        assert_eq!(i, tree2.len()?.try_into().unwrap());

        let mut batch = db.batch();
        batch.insert(&tree, i.to_be_bytes(), i.to_be_bytes());
        batch.insert(&tree2, i.to_be_bytes(), i.to_be_bytes().repeat(1_024));
        batch.commit()?;

        assert_eq!(i + 1, tree.len()?.try_into().unwrap());
        assert_eq!(i + 1, tree2.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
fn noop_journal_create_and_write() -> crate::Result<()> {
    use crate::{Database, JournalMode, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder)
            .journal_mode(JournalMode::Noop)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        // Writes succeed without journal I/O
        tree.insert("a", "hello")?;
        tree.insert("b", "world")?;

        // Data is readable within the session
        assert_eq!(tree.get("a")?.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(tree.get("b")?.as_deref(), Some(b"world".as_slice()));
        assert_eq!(tree.len()?, 2);

        // No .jnl files created (scan recursively to cover nested dirs)
        fn count_jnl_files(dir: &std::path::Path) -> std::io::Result<usize> {
            let mut count = 0;
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    count += count_jnl_files(&path)?;
                } else if path
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("jnl"))
                {
                    count += 1;
                }
            }
            Ok(count)
        }
        assert_eq!(
            count_jnl_files(folder.path())?,
            0,
            "noop journal should not create .jnl files"
        );

        // get_reader returns None for noop journal
        assert!(db.supervisor.journal.get_reader()?.is_none());

        // journal_count still works (reports sealed journals only + 1)
        assert_eq!(db.journal_count(), 1);

        // journal_disk_space reports 0
        assert_eq!(db.journal_disk_space()?, 0);

        // No leftover .jnl from fresh noop open
        assert!(!db.supervisor.journal.leftover_detected());
    }

    // Re-open with noop — recovery succeeds (no journal to replay)
    {
        let db = Database::builder(&folder)
            .journal_mode(JournalMode::Noop)
            .open()?;

        // Recreate keyspace; data durability with noop journal is unspecified
        // here (it may or may not survive depending on flush behavior).
        let _tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        // WAL-specific invariants: still no journal to read or disk space used.
        assert!(db.supervisor.journal.get_reader()?.is_none());
        assert_eq!(db.journal_disk_space()?, 0);
    }

    Ok(())
}

#[test]
fn noop_journal_mode_switch_detects_leftover_jnl() -> crate::Result<()> {
    use crate::{Database, JournalMode, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    // First: create DB with file-based journal (creates .jnl files)
    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        tree.insert("x", "y")?;
    }

    // Verify .jnl file exists
    let has_jnl = std::fs::read_dir(folder.path())?.flatten().any(|e| {
        e.path()
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("jnl"))
    });
    assert!(has_jnl, "file-based journal should have created .jnl");

    // Re-open with noop mode — should warn about leftover .jnl but succeed
    {
        let db = Database::builder(&folder)
            .journal_mode(JournalMode::Noop)
            .open()?;

        // DB opens successfully despite leftover .jnl files
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        tree.insert("a", "b")?;
        assert!(db.supervisor.journal.get_reader()?.is_none());

        // Verify leftover .jnl detection actually ran
        assert!(
            db.supervisor.journal.leftover_detected(),
            "noop open should detect leftover .jnl files from prior file-based run"
        );
    }

    Ok(())
}

#[test]
fn remove_weak_when_key_exists_deletes_key() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "value")?;
    assert!(tree.contains_key("a")?);

    tree.remove_weak("a")?;
    assert!(!tree.contains_key("a")?);

    Ok(())
}

#[test]
fn clear_when_keys_present_removes_all() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "1")?;
    tree.insert("b", "2")?;
    assert_eq!(tree.len()?, 2);

    tree.clear()?;
    assert!(tree.is_empty()?);

    Ok(())
}

#[test]
fn batch_when_committed_insert_and_remove_atomic() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "1")?;

    let mut batch = db.batch();
    batch.insert(&tree, "b", "2");
    batch.insert(&tree, "c", "3");
    batch.remove(&tree, "a");
    batch.commit()?;

    assert!(!tree.contains_key("a")?);
    assert!(tree.contains_key("b")?);
    assert!(tree.contains_key("c")?);

    Ok(())
}

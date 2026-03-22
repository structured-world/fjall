use fjall::{
    Database, JournalMode, KeyspaceCreateOptions, KvSeparationOptions, OptimisticTxDatabase,
    PersistMode, SingleWriterTxDatabase,
};
use std::sync::{atomic::AtomicBool, Arc};
use test_log::test;

#[test]
fn backup_basic() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

    for i in 0..100u64 {
        items.insert(i.to_be_bytes(), format!("value-{i}"))?;
    }
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    // Open backup and verify data
    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("my_items", KeyspaceCreateOptions::default)?;

    for i in 0..100u64 {
        let val = items.get(i.to_be_bytes())?.expect("key should exist");
        assert_eq!(&*val, format!("value-{i}").as_bytes());
    }
    assert_eq!(items.len()?, 100);

    Ok(())
}

#[test]
fn backup_multiple_keyspaces() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let ks_a = db.keyspace("alpha", KeyspaceCreateOptions::default)?;
    let ks_b = db.keyspace("beta", KeyspaceCreateOptions::default)?;

    for i in 0..50u64 {
        ks_a.insert(i.to_be_bytes(), b"alpha")?;
        ks_b.insert(i.to_be_bytes(), b"beta")?;
    }
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(ks_a);
    drop(ks_b);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let ks_a = restored.keyspace("alpha", KeyspaceCreateOptions::default)?;
    let ks_b = restored.keyspace("beta", KeyspaceCreateOptions::default)?;

    assert_eq!(ks_a.len()?, 50);
    assert_eq!(ks_b.len()?, 50);

    for i in 0..50u64 {
        assert_eq!(&*ks_a.get(i.to_be_bytes())?.unwrap(), b"alpha");
        assert_eq!(&*ks_b.get(i.to_be_bytes())?.unwrap(), b"beta");
    }

    Ok(())
}

#[test]
fn backup_empty_database() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    db.backup_to(&backup_path)?;
    drop(db);

    // Open backup — should work even with no keyspaces
    let restored = Database::builder(&backup_path).open()?;
    assert_eq!(restored.keyspace_count(), 0);

    Ok(())
}

#[test]
fn backup_destination_already_exists() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    // backup.path() already exists
    let backup_path = backup.path().to_path_buf();

    let db = Database::builder(&folder).open()?;
    let result = db.backup_to(&backup_path);

    let err = result.expect_err("backup_to should fail when destination directory already exists");
    assert!(
        matches!(&err, fjall::Error::Io(e) if e.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists, got: {err:?}",
    );

    Ok(())
}

#[test]
fn backup_with_flushed_segments() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    // Write enough data to trigger a memtable flush
    for i in 0..1000u64 {
        items.insert(i.to_be_bytes(), vec![b'x'; 1024])?;
    }
    // Force flush to create SST files
    items.rotate_memtable_and_wait()?;
    db.persist(PersistMode::SyncAll)?;

    // Write more data into the active memtable (not yet flushed)
    for i in 1000..1100u64 {
        items.insert(i.to_be_bytes(), b"recent")?;
    }
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;

    // Flushed data
    let expected = vec![b'x'; 1024];
    for i in 0..1000u64 {
        let val = items
            .get(i.to_be_bytes())?
            .expect("flushed key should exist");
        assert_eq!(&*val, expected.as_slice());
    }

    // Memtable data (recovered from journal)
    for i in 1000..1100u64 {
        let val = items
            .get(i.to_be_bytes())?
            .expect("memtable key should exist");
        assert_eq!(&*val, b"recent");
    }

    Ok(())
}

#[test]
fn backup_with_blob_kv_separation() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("blobs", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(KvSeparationOptions::default()))
    })?;

    // Write large values to trigger blob separation
    for i in 0..50u64 {
        items.insert(i.to_be_bytes(), vec![b'v'; 8192])?;
    }
    items.rotate_memtable_and_wait()?;
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("blobs", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(KvSeparationOptions::default()))
    })?;

    for i in 0..50u64 {
        let val = items.get(i.to_be_bytes())?.expect("blob key should exist");
        assert_eq!(val.len(), 8192);
    }

    Ok(())
}

#[test]
fn backup_single_writer_tx() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = SingleWriterTxDatabase::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    {
        let mut tx = db.write_tx();
        tx.insert(&items, "a", "1");
        tx.insert(&items, "b", "2");
        tx.commit()?;
    }
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;
    assert_eq!(&*items.get("a")?.unwrap(), b"1");
    assert_eq!(&*items.get("b")?.unwrap(), b"2");

    Ok(())
}

#[test]
fn backup_optimistic_tx() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    {
        let mut tx = db.write_tx()?;
        tx.insert(&items, "x", "10");
        tx.insert(&items, "y", "20");
        tx.commit()?;
    }
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;
    assert_eq!(&*items.get("x")?.unwrap(), b"10");
    assert_eq!(&*items.get("y")?.unwrap(), b"20");

    Ok(())
}

#[test]
fn backup_with_deletes() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    items.insert("keep", "yes")?;
    items.insert("delete_me", "no")?;
    items.remove("delete_me")?;
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;
    assert_eq!(&*items.get("keep")?.unwrap(), b"yes");
    assert!(items.get("delete_me")?.is_none());

    Ok(())
}

#[test]
fn backup_with_sealed_journals() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    // Write + rotate multiple times to create sealed journals
    for batch in 0..3u64 {
        for i in 0..10u64 {
            let key = (batch * 10 + i).to_be_bytes();
            items.insert(key, format!("batch-{batch}"))?;
        }
        items.rotate_memtable_and_wait()?;
    }

    // Write more data into the active journal (not yet flushed)
    for i in 30..35u64 {
        items.insert(i.to_be_bytes(), b"unflushed")?;
    }
    db.persist(PersistMode::SyncAll)?;
    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;
    assert_eq!(items.len()?, 35);

    for i in 0..30u64 {
        assert!(
            items.get(i.to_be_bytes())?.is_some(),
            "flushed key {i} missing"
        );
    }
    for i in 30..35u64 {
        assert_eq!(
            &*items.get(i.to_be_bytes())?.expect("unflushed key missing"),
            b"unflushed",
        );
    }

    Ok(())
}

#[test]
fn backup_to_nested_nonexistent_path() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    // Parent directories don't exist yet — backup_to should create them
    let backup_path = backup.path().join("a").join("b").join("c").join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;
    items.insert("k", "v")?;
    db.persist(PersistMode::SyncAll)?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;
    assert_eq!(&*items.get("k")?.unwrap(), b"v");

    Ok(())
}

#[test]
fn backup_noop_journal() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder)
        .journal_mode(JournalMode::Noop)
        .open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    // With noop journal, data is only durable after flush to segments
    items.insert("a", "1")?;
    items.rotate_memtable_and_wait()?;

    db.backup_to(&backup_path)?;
    drop(items);
    drop(db);

    // Restore — flushed data should be present, no journal files expected
    let restored = Database::builder(&backup_path)
        .journal_mode(JournalMode::Noop)
        .open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;
    assert_eq!(&*items.get("a")?.unwrap(), b"1");

    Ok(())
}

#[test]
fn backup_twice_to_same_path_fails() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;

    // First backup succeeds
    db.backup_to(&backup_path)?;

    // Second backup to same path fails with AlreadyExists
    let err = db
        .backup_to(&backup_path)
        .expect_err("second backup to same path should fail");
    assert!(
        matches!(&err, fjall::Error::Io(e) if e.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists, got: {err:?}",
    );

    Ok(())
}

#[test]
fn backup_under_concurrent_writes() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let backup_path = backup.path().join("backup");

    let db = Database::builder(&folder).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    // Pre-populate with some data and flush to create SSTs
    for i in 0..100u64 {
        items.insert(i.to_be_bytes(), b"initial")?;
    }
    items.rotate_memtable_and_wait()?;
    db.persist(PersistMode::SyncAll)?;

    // Spawn concurrent writer threads that insert continuously.
    // Writer threads only *explicitly* call `insert` (no direct rotate/flush
    // calls). Any memtable rotation/flush is driven internally by `insert`
    // itself. This avoids explicit concurrent flush operations in test code,
    // which can conflict with hard-link creation on Windows.
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    for thread_id in 0..3u64 {
        let items_clone = items.clone();
        let stop_clone = stop.clone();

        handles.push(std::thread::spawn(move || {
            let mut counter = 0u64;
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("t{thread_id}-{counter}");
                items_clone.insert(key.as_bytes(), b"concurrent").ok();
                counter += 1;

                if counter > 500 {
                    break;
                }
            }
        }));
    }

    // Run backup while writers are active
    db.backup_to(&backup_path)?;

    // Stop writers and wait — unwrap join to surface thread panics
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().expect("writer thread panicked");
    }

    drop(items);
    drop(db);

    // Verify backup opens and contains at least the pre-populated data
    let restored = Database::builder(&backup_path).open()?;
    let items = restored.keyspace("items", KeyspaceCreateOptions::default)?;

    for i in 0..100u64 {
        assert!(
            items.get(i.to_be_bytes())?.is_some(),
            "pre-populated key {i} missing from backup",
        );
    }

    Ok(())
}

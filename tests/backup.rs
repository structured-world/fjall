use fjall::{
    Database, KeyspaceCreateOptions, KvSeparationOptions, OptimisticTxDatabase, PersistMode,
    SingleWriterTxDatabase,
};
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

    assert!(result.is_err());

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
    for i in 0..1000u64 {
        let val = items
            .get(i.to_be_bytes())?
            .expect("flushed key should exist");
        assert_eq!(&*val, vec![b'x'; 1024]);
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

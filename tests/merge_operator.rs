use fjall::{Database, KeyspaceCreateOptions, MergeOperator, SingleWriterTxDatabase};
use lsm_tree::{get_tmp_folder, UserValue};
use std::sync::Arc;
use test_log::test;

/// A simple merge operator that concatenates operands with ","
struct ConcatMerge;

impl MergeOperator for ConcatMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut result = base_value.unwrap_or(b"").to_vec();
        for op in operands {
            if !result.is_empty() {
                result.push(b',');
            }
            result.extend_from_slice(op);
        }
        Ok(result.into())
    }
}

/// A merge operator that sums u64 values encoded as little-endian bytes
struct SumMerge;

impl MergeOperator for SumMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut sum = match base_value {
            Some(v) if v.len() == 8 => u64::from_le_bytes(v.try_into().expect("8 bytes")),
            _ => 0,
        };
        for op in operands {
            if op.len() == 8 {
                sum += u64::from_le_bytes((*op).try_into().expect("8 bytes"));
            }
        }
        Ok(sum.to_le_bytes().to_vec().into())
    }
}

fn concat_assigner() -> Arc<dyn Fn(&str) -> Option<Arc<dyn MergeOperator>> + Send + Sync> {
    Arc::new(|name| match name {
        "items" => Some(Arc::new(ConcatMerge)),
        _ => None,
    })
}

fn concat_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default().with_merge_operator(Some(Arc::new(ConcatMerge)))
}

#[test]
fn merge_basic_keyspace() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = Database::builder(&folder)
        .with_merge_operator_assigner(concat_assigner())
        .open()?;

    let tree = db.keyspace("items", concat_opts)?;

    tree.merge("key1", "a")?;
    tree.merge("key1", "b")?;
    tree.merge("key1", "c")?;

    let val = tree.get("key1")?.expect("should exist");
    assert_eq!(val.as_ref(), b"a,b,c");

    Ok(())
}

#[test]
fn merge_with_existing_value() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = Database::builder(&folder)
        .with_merge_operator_assigner(concat_assigner())
        .open()?;

    let tree = db.keyspace("items", concat_opts)?;

    tree.insert("key1", "base")?;
    tree.merge("key1", "x")?;
    tree.merge("key1", "y")?;

    let val = tree.get("key1")?.expect("should exist");
    assert_eq!(val.as_ref(), b"base,x,y");

    Ok(())
}

#[test]
fn merge_missing_operator_returns_error() -> fjall::Result<()> {
    let folder = get_tmp_folder();
    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("no_op", KeyspaceCreateOptions::default)?;

    let err = tree.merge("key", "val").unwrap_err();
    assert!(
        matches!(err, fjall::Error::MissingMergeOperator),
        "expected MissingMergeOperator, got {err:?}",
    );

    Ok(())
}

#[test]
fn merge_in_write_batch() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = Database::builder(&folder)
        .with_merge_operator_assigner(Arc::new(|name| match name {
            "items" => Some(Arc::new(SumMerge)),
            _ => None,
        }))
        .open()?;

    let tree = db.keyspace("items", || {
        KeyspaceCreateOptions::default().with_merge_operator(Some(Arc::new(SumMerge)))
    })?;

    // Batch merge on different keys (same-key merges in a single batch
    // share one seqno, so the last operand wins)
    let mut batch = db.batch();
    batch.merge(&tree, "a", 10u64.to_le_bytes());
    batch.merge(&tree, "b", 20u64.to_le_bytes());
    batch.commit()?;

    let val_a = tree.get("a")?.expect("should exist");
    let sum_a = u64::from_le_bytes(val_a.as_ref().try_into().expect("8 bytes"));
    assert_eq!(sum_a, 10);

    let val_b = tree.get("b")?.expect("should exist");
    let sum_b = u64::from_le_bytes(val_b.as_ref().try_into().expect("8 bytes"));
    assert_eq!(sum_b, 20);

    Ok(())
}

#[test]
fn merge_recovery_reinstalls_operator() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    // Write merge operands
    {
        let db = Database::builder(&folder)
            .with_merge_operator_assigner(concat_assigner())
            .open()?;

        let tree = db.keyspace("items", concat_opts)?;

        tree.merge("key1", "a")?;
        tree.merge("key1", "b")?;
    }

    // Reopen — merge operator must be reinstalled via assigner
    {
        let db = Database::builder(&folder)
            .with_merge_operator_assigner(concat_assigner())
            .open()?;

        // Reopen with default options — merge operator is restored solely
        // via the assigner, proving the recovery path works.
        let tree = db.keyspace("items", KeyspaceCreateOptions::default)?;

        let val = tree.get("key1")?.expect("should exist after recovery");
        assert_eq!(val.as_ref(), b"a,b");
    }

    Ok(())
}

#[test]
fn merge_single_writer_tx() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = SingleWriterTxDatabase::builder(&folder)
        .with_merge_operator_assigner(concat_assigner())
        .open()?;

    let tree = db.keyspace("items", concat_opts)?;

    tree.merge("key1", "a")?;
    tree.merge("key1", "b")?;

    let val = tree.get("key1")?.expect("should exist");
    assert_eq!(val.as_ref(), b"a,b");

    Ok(())
}

#[test]
fn merge_write_tx() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = SingleWriterTxDatabase::builder(&folder)
        .with_merge_operator_assigner(concat_assigner())
        .open()?;

    let tree = db.keyspace("items", concat_opts)?;

    // Merge different keys in a single transaction
    let mut tx = db.write_tx();
    tx.merge(&tree, "key1", "x")?;
    tx.merge(&tree, "key2", "y")?;
    tx.commit()?;

    let val1 = tree.get("key1")?.expect("should exist");
    assert_eq!(val1.as_ref(), b"x");

    let val2 = tree.get("key2")?.expect("should exist");
    assert_eq!(val2.as_ref(), b"y");

    Ok(())
}

#[test]
fn merge_write_tx_missing_operator() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = SingleWriterTxDatabase::builder(&folder).open()?;

    let tree = db.keyspace("no_op", KeyspaceCreateOptions::default)?;

    let err = tree.merge("key", "val").unwrap_err();
    assert!(matches!(err, fjall::Error::MissingMergeOperator));

    let mut tx = db.write_tx();
    let err = tx.merge(&tree, "key", "val").unwrap_err();
    assert!(matches!(err, fjall::Error::MissingMergeOperator));

    Ok(())
}

#[test]
fn merge_multiple_keyspaces_selective_operator() -> fjall::Result<()> {
    let folder = get_tmp_folder();

    let db = Database::builder(&folder)
        .with_merge_operator_assigner(Arc::new(|name| match name {
            "concat_ks" => Some(Arc::new(ConcatMerge)),
            "sum_ks" => Some(Arc::new(SumMerge)),
            _ => None,
        }))
        .open()?;

    let concat_tree = db.keyspace("concat_ks", || {
        KeyspaceCreateOptions::default().with_merge_operator(Some(Arc::new(ConcatMerge)))
    })?;

    let sum_tree = db.keyspace("sum_ks", || {
        KeyspaceCreateOptions::default().with_merge_operator(Some(Arc::new(SumMerge)))
    })?;

    concat_tree.merge("k", "hello")?;
    concat_tree.merge("k", "world")?;

    sum_tree.insert("c", &100u64.to_le_bytes())?;
    sum_tree.merge("c", 42u64.to_le_bytes())?;

    let concat_val = concat_tree.get("k")?.expect("exists");
    assert_eq!(concat_val.as_ref(), b"hello,world");

    let sum_val = sum_tree.get("c")?.expect("exists");
    let sum = u64::from_le_bytes(sum_val.as_ref().try_into().expect("8 bytes"));
    assert_eq!(sum, 142);

    Ok(())
}

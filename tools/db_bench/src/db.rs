use crate::config::BenchConfig;
use fjall::Keyspace;

/// Prefill a keyspace with sequential keys for read benchmarks.
pub fn prefill_sequential(keyspace: &Keyspace, config: &BenchConfig) -> fjall::Result<()> {
    for i in 0..config.num {
        let key = make_sequential_key(i, config.key_size);
        let value = make_value(config.value_size);
        keyspace.insert(key, value)?;
    }

    eprintln!(
        "Prefilled {} keys ({} bytes/entry)",
        config.num,
        config.entry_size(),
    );

    Ok(())
}

/// Create a sequential key from a u64 index, padded or truncated to key_size.
///
/// For key_size >= 8: full BE u64 + zero-padding.
/// For key_size < 8: trailing (least-significant) bytes so small indices
/// produce distinct keys.
#[inline]
pub fn make_sequential_key(index: u64, key_size: usize) -> Vec<u8> {
    let mut key = vec![0u8; key_size];
    fill_sequential_key(&mut key, index);
    key
}

/// Write a sequential key into an existing buffer (zero-alloc variant).
#[inline]
pub fn fill_sequential_key(buf: &mut [u8], index: u64) {
    let key_size = buf.len();
    // --key_size validated in main; debug_assert avoids per-op overhead in release.
    debug_assert!(key_size > 0, "key_size must be > 0");
    let be_bytes = index.to_be_bytes();

    if key_size >= 8 {
        buf[..8].copy_from_slice(&be_bytes);
        buf[8..].fill(0);
    } else {
        debug_assert!(
            index < (1u64 << (key_size * 8)),
            "index {index} exceeds unique key space for key_size {key_size}"
        );
        buf.copy_from_slice(&be_bytes[8 - key_size..]);
    }
}

/// Create a random key of the given size.
#[inline]
pub fn make_random_key(key_size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut key = vec![0u8; key_size];
    rand::rng().fill(&mut key[..]);
    key
}

/// Create a deterministic value of the given size.
#[inline]
pub fn make_value(value_size: usize) -> Vec<u8> {
    vec![0x42u8; value_size]
}

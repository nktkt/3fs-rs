//! In-memory KV store backed by a `BTreeMap`.
//!
//! This provides a fully functional [`KvEngine`] implementation suitable for
//! testing and lightweight use cases. All data lives in memory behind a
//! `parking_lot::RwLock`.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use hf3fs_kv::{
    GetRangeResult, KeySelector, KeyValue, KvEngine, ReadOnlyTransaction, ReadWriteTransaction,
    Versionstamp,
};
use hf3fs_types::Result;

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// In-memory KV engine using a shared `BTreeMap`.
#[derive(Clone)]
pub struct MemDbEngine {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    version: Arc<AtomicI64>,
}

impl MemDbEngine {
    /// Create a new, empty in-memory database.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            version: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Return the number of keys currently stored.
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Return whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }
}

impl Default for MemDbEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for MemDbEngine {
    type RoTxn = MemDbReadOnlyTxn;
    type RwTxn = MemDbReadWriteTxn;

    fn create_readonly_transaction(&self) -> Self::RoTxn {
        // Take a snapshot of the data at the current version.
        let snapshot = self.data.read().clone();
        let read_version = self.version.load(Ordering::SeqCst);
        MemDbReadOnlyTxn {
            snapshot,
            read_version,
        }
    }

    fn create_readwrite_transaction(&self) -> Self::RwTxn {
        let snapshot = self.data.read().clone();
        let read_version = self.version.load(Ordering::SeqCst);
        MemDbReadWriteTxn {
            ro: MemDbReadOnlyTxn {
                snapshot,
                read_version,
            },
            pending_writes: Vec::new(),
            pending_deletes: Vec::new(),
            data: Arc::clone(&self.data),
            version: Arc::clone(&self.version),
            committed_version: -1,
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: range collection
// ---------------------------------------------------------------------------

/// Collect key-value pairs from a BTreeMap snapshot according to `begin` / `end`
/// key selectors and a limit.
fn collect_range(
    map: &BTreeMap<Vec<u8>, Vec<u8>>,
    begin: &KeySelector,
    end: &KeySelector,
    limit: i32,
) -> GetRangeResult {
    let start_bound = if begin.inclusive {
        Bound::Included(begin.key.clone())
    } else {
        Bound::Excluded(begin.key.clone())
    };

    let end_bound = if end.inclusive {
        Bound::Included(end.key.clone())
    } else {
        Bound::Excluded(end.key.clone())
    };

    let limit = limit.max(0) as usize;
    let mut kvs = Vec::new();
    let mut has_more = false;

    for (k, v) in map.range((start_bound, end_bound)) {
        if kvs.len() >= limit {
            has_more = true;
            break;
        }
        kvs.push(KeyValue {
            key: k.clone(),
            value: v.clone(),
        });
    }

    GetRangeResult { kvs, has_more }
}

// ---------------------------------------------------------------------------
// Read-only transaction
// ---------------------------------------------------------------------------

/// Read-only transaction operating on a point-in-time snapshot.
pub struct MemDbReadOnlyTxn {
    snapshot: BTreeMap<Vec<u8>, Vec<u8>>,
    read_version: i64,
}

#[async_trait]
impl ReadOnlyTransaction for MemDbReadOnlyTxn {
    fn set_read_version(&mut self, version: i64) {
        self.read_version = version;
    }

    async fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key).cloned())
    }

    async fn snapshot_get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limit: i32,
    ) -> Result<GetRangeResult> {
        Ok(collect_range(&self.snapshot, begin, end, limit))
    }

    async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limit: i32,
    ) -> Result<GetRangeResult> {
        self.snapshot_get_range(begin, end, limit).await
    }

    async fn cancel(&mut self) -> Result<()> {
        Ok(())
    }

    fn reset(&mut self) {
        // Clear snapshot to release memory; the txn is no longer usable after reset.
        self.snapshot.clear();
    }
}

// ---------------------------------------------------------------------------
// Read-write transaction
// ---------------------------------------------------------------------------

/// Read-write transaction that buffers writes and applies them atomically on
/// commit.
pub struct MemDbReadWriteTxn {
    /// The underlying read-only snapshot for reads.
    ro: MemDbReadOnlyTxn,
    /// Buffered writes: (key, value).
    pending_writes: Vec<(Vec<u8>, Vec<u8>)>,
    /// Buffered deletes.
    pending_deletes: Vec<Vec<u8>>,
    /// Reference to the shared data store for committing.
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    /// Shared version counter.
    version: Arc<AtomicI64>,
    /// Version assigned after a successful commit; -1 if not yet committed.
    committed_version: i64,
}

#[async_trait]
impl ReadOnlyTransaction for MemDbReadWriteTxn {
    fn set_read_version(&mut self, version: i64) {
        self.ro.set_read_version(version);
    }

    async fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ro.snapshot_get(key).await
    }

    async fn snapshot_get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limit: i32,
    ) -> Result<GetRangeResult> {
        self.ro.snapshot_get_range(begin, end, limit).await
    }

    async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limit: i32,
    ) -> Result<GetRangeResult> {
        self.ro.get_range(begin, end, limit).await
    }

    async fn cancel(&mut self) -> Result<()> {
        self.pending_writes.clear();
        self.pending_deletes.clear();
        self.ro.cancel().await
    }

    fn reset(&mut self) {
        self.pending_writes.clear();
        self.pending_deletes.clear();
        self.ro.reset();
    }
}

#[async_trait]
impl ReadWriteTransaction for MemDbReadWriteTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.pending_writes
            .push((key.to_vec(), value.to_vec()));
        Ok(())
    }

    async fn clear(&mut self, key: &[u8]) -> Result<()> {
        self.pending_deletes.push(key.to_vec());
        Ok(())
    }

    async fn add_read_conflict(&mut self, _key: &[u8]) -> Result<()> {
        // In-memory store does not track read conflicts.
        Ok(())
    }

    async fn add_read_conflict_range(&mut self, _begin: &[u8], _end: &[u8]) -> Result<()> {
        // In-memory store does not track read conflict ranges.
        Ok(())
    }

    async fn set_versionstamped_key(
        &mut self,
        key: &[u8],
        offset: u32,
        value: &[u8],
    ) -> Result<()> {
        // For the in-memory backend we stamp a monotonic version into the key at
        // `offset`.  The versionstamp is 10 bytes derived from the current
        // version counter.
        let ver = self.version.load(Ordering::SeqCst) + 1;
        let stamp = version_to_stamp(ver);
        let mut stamped_key = key.to_vec();
        let off = offset as usize;
        if off + 10 <= stamped_key.len() {
            stamped_key[off..off + 10].copy_from_slice(&stamp);
        }
        self.pending_writes.push((stamped_key, value.to_vec()));
        Ok(())
    }

    async fn set_versionstamped_value(
        &mut self,
        key: &[u8],
        value: &[u8],
        offset: u32,
    ) -> Result<()> {
        let ver = self.version.load(Ordering::SeqCst) + 1;
        let stamp = version_to_stamp(ver);
        let mut stamped_value = value.to_vec();
        let off = offset as usize;
        if off + 10 <= stamped_value.len() {
            stamped_value[off..off + 10].copy_from_slice(&stamp);
        }
        self.pending_writes
            .push((key.to_vec(), stamped_value));
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        // Apply all buffered mutations atomically under the write lock.
        let mut store = self.data.write();
        for key in self.pending_deletes.drain(..) {
            store.remove(&key);
        }
        for (key, value) in self.pending_writes.drain(..) {
            store.insert(key, value);
        }
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        self.committed_version = new_version;
        Ok(())
    }

    fn get_committed_version(&self) -> i64 {
        self.committed_version
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a monotonic i64 version to a 10-byte versionstamp.
///
/// The first 8 bytes are the big-endian encoding of the version; the last 2
/// bytes are zero (batch number).
fn version_to_stamp(version: i64) -> Versionstamp {
    let mut stamp = [0u8; 10];
    stamp[..8].copy_from_slice(&version.to_be_bytes());
    stamp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_kv::{key_after, prefix_list_end_key};

    // -- basic get / set ---------------------------------------------------

    #[tokio::test]
    async fn test_basic_set_and_get() {
        let engine = MemDbEngine::new();
        assert!(engine.is_empty());

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(engine.len(), 2);

        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(ro.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(ro.get(b"key3").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_overwrite_key() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"k", b"v1").await.unwrap();
        txn.commit().await.unwrap();

        let mut txn2 = engine.create_readwrite_transaction();
        txn2.set(b"k", b"v2").await.unwrap();
        txn2.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"k").await.unwrap(), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_clear_key() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"a", b"1").await.unwrap();
        txn.set(b"b", b"2").await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(engine.len(), 2);

        let mut txn2 = engine.create_readwrite_transaction();
        txn2.clear(b"a").await.unwrap();
        txn2.commit().await.unwrap();
        assert_eq!(engine.len(), 1);

        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"a").await.unwrap(), None);
        assert_eq!(ro.get(b"b").await.unwrap(), Some(b"2".to_vec()));
    }

    // -- transaction isolation ---------------------------------------------

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let engine = MemDbEngine::new();

        // Write initial data.
        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"x", b"before").await.unwrap();
        txn.commit().await.unwrap();

        // Take a read-only snapshot.
        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"x").await.unwrap(), Some(b"before".to_vec()));

        // Write new data in a separate transaction.
        let mut txn2 = engine.create_readwrite_transaction();
        txn2.set(b"x", b"after").await.unwrap();
        txn2.commit().await.unwrap();

        // The snapshot should still see the old value.
        assert_eq!(ro.get(b"x").await.unwrap(), Some(b"before".to_vec()));

        // A new snapshot sees the updated value.
        let ro2 = engine.create_readonly_transaction();
        assert_eq!(ro2.get(b"x").await.unwrap(), Some(b"after".to_vec()));
    }

    #[tokio::test]
    async fn test_uncommitted_writes_not_visible() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"hidden", b"data").await.unwrap();
        // Do NOT commit.

        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"hidden").await.unwrap(), None);

        // Also not visible to another rw txn.
        let ro2 = engine.create_readwrite_transaction();
        assert_eq!(ro2.get(b"hidden").await.unwrap(), None);
    }

    // -- range queries -----------------------------------------------------

    #[tokio::test]
    async fn test_range_inclusive_both() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        for i in 0u8..10 {
            txn.set(&[i], &[i * 10]).await.unwrap();
        }
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(vec![2], true);
        let end = KeySelector::new(vec![5], true);
        let result = ro.get_range(&begin, &end, 100).await.unwrap();

        assert_eq!(result.kvs.len(), 4); // keys 2,3,4,5
        assert!(!result.has_more);
        assert_eq!(result.kvs[0].key, vec![2]);
        assert_eq!(result.kvs[3].key, vec![5]);
    }

    #[tokio::test]
    async fn test_range_exclusive_both() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        for i in 0u8..10 {
            txn.set(&[i], &[i]).await.unwrap();
        }
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(vec![2], false);
        let end = KeySelector::new(vec![5], false);
        let result = ro.get_range(&begin, &end, 100).await.unwrap();

        assert_eq!(result.kvs.len(), 2); // keys 3,4
        assert!(!result.has_more);
    }

    #[tokio::test]
    async fn test_range_with_limit() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        for i in 0u8..20 {
            txn.set(&[i], &[i]).await.unwrap();
        }
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(vec![0], true);
        let end = KeySelector::new(vec![19], true);
        let result = ro.get_range(&begin, &end, 5).await.unwrap();

        assert_eq!(result.kvs.len(), 5);
        assert!(result.has_more);
        assert_eq!(result.kvs[0].key, vec![0]);
        assert_eq!(result.kvs[4].key, vec![4]);
    }

    #[tokio::test]
    async fn test_range_empty() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"a", b"1").await.unwrap();
        txn.set(b"z", b"2").await.unwrap();
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        // Range that contains no keys.
        let begin = KeySelector::new(b"b".to_vec(), true);
        let end = KeySelector::new(b"c".to_vec(), true);
        let result = ro.get_range(&begin, &end, 100).await.unwrap();
        assert!(result.kvs.is_empty());
        assert!(!result.has_more);
    }

    // -- key_after and prefix range ----------------------------------------

    #[tokio::test]
    async fn test_key_after_range() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"foo", b"1").await.unwrap();
        txn.set(b"foo\x00", b"2").await.unwrap();
        txn.set(b"foo\x00\x01", b"3").await.unwrap();
        txn.set(b"fop", b"4").await.unwrap();
        txn.commit().await.unwrap();

        // key_after("foo") == "foo\0", use it as inclusive begin.
        let after = key_after(b"foo");
        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(after, true);
        let end = KeySelector::new(b"fop".to_vec(), false);
        let result = ro.get_range(&begin, &end, 100).await.unwrap();

        // Should get "foo\0" and "foo\0\x01", but NOT "foo" or "fop".
        assert_eq!(result.kvs.len(), 2);
        assert_eq!(result.kvs[0].key, b"foo\x00");
        assert_eq!(result.kvs[1].key, b"foo\x00\x01");
    }

    #[tokio::test]
    async fn test_prefix_range() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"pre:aa", b"1").await.unwrap();
        txn.set(b"pre:bb", b"2").await.unwrap();
        txn.set(b"pre:cc", b"3").await.unwrap();
        txn.set(b"prf:dd", b"4").await.unwrap(); // past the prefix
        txn.set(b"other", b"5").await.unwrap();
        txn.commit().await.unwrap();

        let prefix = b"pre:";
        let end_key = prefix_list_end_key(prefix);

        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(prefix.to_vec(), true);
        let end = KeySelector::new(end_key, false);
        let result = ro.get_range(&begin, &end, 100).await.unwrap();

        assert_eq!(result.kvs.len(), 3);
        assert_eq!(result.kvs[0].key, b"pre:aa");
        assert_eq!(result.kvs[1].key, b"pre:bb");
        assert_eq!(result.kvs[2].key, b"pre:cc");
    }

    // -- versioning --------------------------------------------------------

    #[tokio::test]
    async fn test_committed_version_increments() {
        let engine = MemDbEngine::new();

        let mut txn1 = engine.create_readwrite_transaction();
        txn1.set(b"a", b"1").await.unwrap();
        txn1.commit().await.unwrap();
        let v1 = txn1.get_committed_version();

        let mut txn2 = engine.create_readwrite_transaction();
        txn2.set(b"b", b"2").await.unwrap();
        txn2.commit().await.unwrap();
        let v2 = txn2.get_committed_version();

        assert!(v2 > v1);
    }

    #[tokio::test]
    async fn test_uncommitted_version_is_negative() {
        let engine = MemDbEngine::new();
        let txn = engine.create_readwrite_transaction();
        assert_eq!(txn.get_committed_version(), -1);
    }

    // -- cancel / reset ----------------------------------------------------

    #[tokio::test]
    async fn test_cancel_discards_writes() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"temp", b"data").await.unwrap();
        txn.cancel().await.unwrap();

        // After cancel the pending writes are gone; even committing should be a no-op.
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"temp").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_reset_clears_state() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"temp", b"data").await.unwrap();
        txn.reset();

        txn.commit().await.unwrap();
        assert!(engine.is_empty());
    }

    // -- concurrent reads --------------------------------------------------

    #[tokio::test]
    async fn test_multiple_concurrent_reads() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"shared", b"value").await.unwrap();
        txn.commit().await.unwrap();

        // Spawn several concurrent reads.
        let mut handles = Vec::new();
        for _ in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let ro = eng.create_readonly_transaction();
                ro.get(b"shared").await.unwrap()
            }));
        }

        for h in handles {
            let val = h.await.unwrap();
            assert_eq!(val, Some(b"value".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_concurrent_read_and_write() {
        let engine = MemDbEngine::new();

        // Initial write.
        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"counter", b"0").await.unwrap();
        txn.commit().await.unwrap();

        // Take a snapshot before concurrent writes.
        let snapshot = engine.create_readonly_transaction();

        // Perform a write in a background task.
        let eng = engine.clone();
        let write_handle = tokio::spawn(async move {
            let mut wt = eng.create_readwrite_transaction();
            wt.set(b"counter", b"1").await.unwrap();
            wt.commit().await.unwrap();
        });
        write_handle.await.unwrap();

        // Old snapshot still sees original value.
        assert_eq!(
            snapshot.get(b"counter").await.unwrap(),
            Some(b"0".to_vec())
        );

        // New snapshot sees updated value.
        let new_snap = engine.create_readonly_transaction();
        assert_eq!(
            new_snap.get(b"counter").await.unwrap(),
            Some(b"1".to_vec())
        );
    }

    // -- snapshot_get vs get -----------------------------------------------

    #[tokio::test]
    async fn test_snapshot_get_and_get_are_equivalent() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"s", b"v").await.unwrap();
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let a = ro.snapshot_get(b"s").await.unwrap();
        let b = ro.get(b"s").await.unwrap();
        assert_eq!(a, b);
    }

    // -- snapshot_get_range ------------------------------------------------

    #[tokio::test]
    async fn test_snapshot_get_range() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"m1", b"v1").await.unwrap();
        txn.set(b"m2", b"v2").await.unwrap();
        txn.set(b"m3", b"v3").await.unwrap();
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(b"m1".to_vec(), true);
        let end = KeySelector::new(b"m3".to_vec(), true);
        let result = ro.snapshot_get_range(&begin, &end, 10).await.unwrap();
        assert_eq!(result.kvs.len(), 3);
    }

    // -- versionstamped key / value ----------------------------------------

    #[tokio::test]
    async fn test_set_versionstamped_key() {
        let engine = MemDbEngine::new();

        // Build a key template with 10 zero bytes at offset 4.
        let mut key_template = b"pfx:".to_vec();
        key_template.extend_from_slice(&[0u8; 10]);
        key_template.extend_from_slice(b":sfx");

        let mut txn = engine.create_readwrite_transaction();
        txn.set_versionstamped_key(&key_template, 4, b"payload")
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // The stored key should have the version stamp in bytes 4..14.
        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(b"pfx:".to_vec(), true);
        let end_key = prefix_list_end_key(b"pfx:");
        let end = KeySelector::new(end_key, false);
        let result = ro.get_range(&begin, &end, 10).await.unwrap();
        assert_eq!(result.kvs.len(), 1);
        assert_eq!(&result.kvs[0].key[..4], b"pfx:");
        assert_eq!(&result.kvs[0].key[14..], b":sfx");
        assert_eq!(result.kvs[0].value, b"payload");
        // The stamp bytes should not all be zero (version > 0 when committing).
        assert_ne!(&result.kvs[0].key[4..14], &[0u8; 10]);
    }

    #[tokio::test]
    async fn test_set_versionstamped_value() {
        let engine = MemDbEngine::new();

        let mut val_template = b"hdr:".to_vec();
        val_template.extend_from_slice(&[0u8; 10]);

        let mut txn = engine.create_readwrite_transaction();
        txn.set_versionstamped_value(b"mykey", &val_template, 4)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let val = ro.get(b"mykey").await.unwrap().unwrap();
        assert_eq!(&val[..4], b"hdr:");
        assert_ne!(&val[4..14], &[0u8; 10]);
    }

    // -- engine default / clone --------------------------------------------

    #[tokio::test]
    async fn test_engine_default() {
        let engine = MemDbEngine::default();
        assert!(engine.is_empty());
    }

    #[tokio::test]
    async fn test_engine_clone_shares_state() {
        let engine = MemDbEngine::new();
        let engine2 = engine.clone();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"shared", b"yes").await.unwrap();
        txn.commit().await.unwrap();

        // The clone should see the committed data.
        let ro = engine2.create_readonly_transaction();
        assert_eq!(ro.get(b"shared").await.unwrap(), Some(b"yes".to_vec()));
    }

    // -- add_read_conflict (no-op but must not error) ----------------------

    #[tokio::test]
    async fn test_add_read_conflict_noop() {
        let engine = MemDbEngine::new();
        let mut txn = engine.create_readwrite_transaction();
        txn.add_read_conflict(b"whatever").await.unwrap();
        txn.add_read_conflict_range(b"a", b"z").await.unwrap();
        txn.commit().await.unwrap();
    }

    // -- multiple sets in single transaction are applied in order ----------

    #[tokio::test]
    async fn test_multiple_sets_last_wins() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"k", b"first").await.unwrap();
        txn.set(b"k", b"second").await.unwrap();
        txn.set(b"k", b"third").await.unwrap();
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        assert_eq!(ro.get(b"k").await.unwrap(), Some(b"third".to_vec()));
    }

    // -- set_read_version --------------------------------------------------

    #[tokio::test]
    async fn test_set_read_version() {
        let engine = MemDbEngine::new();
        let mut ro = engine.create_readonly_transaction();
        // Setting read version should not panic.
        ro.set_read_version(42);
    }

    // -- limit of 0 returns nothing ----------------------------------------

    #[tokio::test]
    async fn test_range_limit_zero() {
        let engine = MemDbEngine::new();

        let mut txn = engine.create_readwrite_transaction();
        txn.set(b"a", b"1").await.unwrap();
        txn.commit().await.unwrap();

        let ro = engine.create_readonly_transaction();
        let begin = KeySelector::new(b"a".to_vec(), true);
        let end = KeySelector::new(b"b".to_vec(), true);
        let result = ro.get_range(&begin, &end, 0).await.unwrap();
        assert!(result.kvs.is_empty());
        assert!(result.has_more);
    }
}

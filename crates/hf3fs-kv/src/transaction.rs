use async_trait::async_trait;
use hf3fs_types::Result;

/// Key-value pair.
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Key selector for range queries.
#[derive(Debug, Clone)]
pub struct KeySelector {
    pub key: Vec<u8>,
    pub inclusive: bool,
}

impl KeySelector {
    pub fn new(key: impl Into<Vec<u8>>, inclusive: bool) -> Self {
        Self {
            key: key.into(),
            inclusive,
        }
    }
}

/// Result of a range query.
pub struct GetRangeResult {
    pub kvs: Vec<KeyValue>,
    pub has_more: bool,
}

/// Versionstamp: 10-byte unique monotonic value per committed transaction.
pub type Versionstamp = [u8; 10];

/// Read-only transaction trait.
#[async_trait]
pub trait ReadOnlyTransaction: Send + Sync {
    fn set_read_version(&mut self, version: i64);

    async fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.snapshot_get(key).await
    }

    async fn snapshot_get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limit: i32,
    ) -> Result<GetRangeResult>;

    async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limit: i32,
    ) -> Result<GetRangeResult>;

    async fn cancel(&mut self) -> Result<()>;

    fn reset(&mut self);
}

/// Read-write transaction trait.
#[async_trait]
pub trait ReadWriteTransaction: ReadOnlyTransaction {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    async fn clear(&mut self, key: &[u8]) -> Result<()>;

    async fn add_read_conflict(&mut self, key: &[u8]) -> Result<()>;

    async fn add_read_conflict_range(&mut self, begin: &[u8], end: &[u8]) -> Result<()>;

    async fn set_versionstamped_key(
        &mut self,
        key: &[u8],
        offset: u32,
        value: &[u8],
    ) -> Result<()>;

    async fn set_versionstamped_value(
        &mut self,
        key: &[u8],
        value: &[u8],
        offset: u32,
    ) -> Result<()>;

    async fn commit(&mut self) -> Result<()>;

    fn get_committed_version(&self) -> i64;
}

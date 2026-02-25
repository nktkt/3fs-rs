use crate::transaction::{ReadOnlyTransaction, ReadWriteTransaction};

/// KV engine trait - creates transactions.
pub trait KvEngine: Send + Sync {
    type RoTxn: ReadOnlyTransaction;
    type RwTxn: ReadWriteTransaction;

    fn create_readonly_transaction(&self) -> Self::RoTxn;
    fn create_readwrite_transaction(&self) -> Self::RwTxn;
}

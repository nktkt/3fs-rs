use parking_lot::Mutex;
use std::hash::{Hash, Hasher};

/// Sharded data structure for reducing lock contention.
pub struct Shards<T> {
    shards: Vec<Mutex<T>>,
}

impl<T> Shards<T> {
    pub fn new(num_shards: usize, init: impl Fn() -> T) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Mutex::new(init()));
        }
        Self { shards }
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    pub fn shard<K: Hash>(&self, key: &K) -> parking_lot::MutexGuard<'_, T> {
        let idx = self.shard_index(key);
        self.shards[idx].lock()
    }

    pub fn shard_by_index(&self, index: usize) -> parking_lot::MutexGuard<'_, T> {
        self.shards[index % self.shards.len()].lock()
    }

    fn shard_index<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }

    pub fn for_each(&self, mut f: impl FnMut(&mut T)) {
        for shard in &self.shards {
            f(&mut shard.lock());
        }
    }
}

impl<T: Default> Shards<T> {
    pub fn with_default(num_shards: usize) -> Self {
        Self::new(num_shards, T::default)
    }
}

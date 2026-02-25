use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

/// A thread-safe object pool backed by a crossbeam `ArrayQueue`.
///
/// Objects can be returned to the pool when dropped via the `PoolGuard` wrapper.
pub struct ObjectPool<T> {
    inner: Arc<PoolInner<T>>,
}

struct PoolInner<T> {
    queue: ArrayQueue<T>,
    factory: Box<dyn Fn() -> T + Send + Sync>,
}

impl<T> ObjectPool<T> {
    pub fn new(capacity: usize, factory: impl Fn() -> T + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                queue: ArrayQueue::new(capacity),
                factory: Box::new(factory),
            }),
        }
    }

    /// Pre-populate the pool with objects.
    pub fn fill(&self, count: usize) {
        for _ in 0..count {
            let obj = (self.inner.factory)();
            let _ = self.inner.queue.push(obj);
        }
    }

    /// Acquire an object from the pool, or create a new one if the pool is empty.
    pub fn acquire(&self) -> PoolGuard<T> {
        let obj = self
            .inner
            .queue
            .pop()
            .unwrap_or_else(|| (self.inner.factory)());
        PoolGuard {
            value: Some(obj),
            pool: Arc::clone(&self.inner),
        }
    }

    /// Try to acquire an object without creating a new one. Returns `None` if pool is empty.
    pub fn try_acquire(&self) -> Option<PoolGuard<T>> {
        self.inner.queue.pop().map(|obj| PoolGuard {
            value: Some(obj),
            pool: Arc::clone(&self.inner),
        })
    }

    pub fn len(&self) -> usize {
        self.inner.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.queue.is_empty()
    }
}

impl<T> Clone for ObjectPool<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// RAII guard that returns the object to the pool on drop.
pub struct PoolGuard<T> {
    value: Option<T>,
    pool: Arc<PoolInner<T>>,
}

impl<T> std::ops::Deref for PoolGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().unwrap()
    }
}

impl<T> std::ops::DerefMut for PoolGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_mut().unwrap()
    }
}

impl<T> Drop for PoolGuard<T> {
    fn drop(&mut self) {
        if let Some(obj) = self.value.take() {
            // Best effort return to pool; if full, the object is simply dropped.
            let _ = self.pool.queue.push(obj);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_and_return() {
        let pool = ObjectPool::new(4, || Vec::<u8>::with_capacity(1024));
        pool.fill(2);
        assert_eq!(pool.len(), 2);

        {
            let mut buf = pool.acquire();
            buf.push(42);
            assert_eq!(buf[0], 42);
            assert_eq!(pool.len(), 1);
        }
        // returned to pool
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_acquire_when_empty() {
        let pool = ObjectPool::new(2, || 0i32);
        // pool is empty, acquire should create a new object
        let guard = pool.acquire();
        assert_eq!(*guard, 0);
    }

    #[test]
    fn test_try_acquire_empty() {
        let pool = ObjectPool::new(2, || 0i32);
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    fn test_clone_shares_pool() {
        let pool = ObjectPool::new(4, || 0i32);
        pool.fill(2);
        let pool2 = pool.clone();
        let _guard = pool2.acquire();
        assert_eq!(pool.len(), 1);
    }
}

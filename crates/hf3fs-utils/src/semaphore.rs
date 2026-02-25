use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore as TokioSemaphore};

/// A counting semaphore wrapping tokio's Semaphore.
#[derive(Clone)]
pub struct Semaphore {
    inner: Arc<TokioSemaphore>,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Self {
            inner: Arc::new(TokioSemaphore::new(permits)),
        }
    }

    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.inner.clone().acquire_owned().await.expect("semaphore closed")
    }

    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inner.clone().try_acquire_owned().ok()
    }

    pub fn available_permits(&self) -> usize {
        self.inner.available_permits()
    }
}

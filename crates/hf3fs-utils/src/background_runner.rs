use std::future::Future;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Spawns background tokio tasks with graceful shutdown support.
pub struct BackgroundRunner {
    shutdown_tx: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundRunner {
    pub fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self { shutdown_tx, handles: Vec::new() }
    }

    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub fn spawn<F, Fut>(&mut self, f: F)
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let rx = self.shutdown_tx.subscribe();
        self.handles.push(tokio::spawn(f(rx)));
    }

    pub fn spawn_periodic<F, Fut>(&mut self, interval: std::time::Duration, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut rx = self.shutdown_tx.subscribe();
        self.handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => { f().await; }
                    _ = rx.changed() => { break; }
                }
            }
        }));
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

impl Default for BackgroundRunner {
    fn default() -> Self { Self::new() }
}

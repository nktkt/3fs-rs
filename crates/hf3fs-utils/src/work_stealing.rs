use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::sync::Arc;

/// A work-stealing queue system wrapping crossbeam-deque.
///
/// One `Injector` for global task submission, plus per-worker local queues
/// with stealers for work stealing.
pub struct WorkStealingQueue<T> {
    injector: Arc<Injector<T>>,
    stealers: Vec<Stealer<T>>,
}

impl<T> WorkStealingQueue<T> {
    /// Create a new work-stealing system. Returns the shared queue and per-worker local queues.
    pub fn new(num_workers: usize) -> (Self, Vec<Worker<T>>) {
        let injector = Arc::new(Injector::new());
        let mut workers = Vec::with_capacity(num_workers);
        let mut stealers = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let w = Worker::new_fifo();
            stealers.push(w.stealer());
            workers.push(w);
        }

        (
            Self {
                injector,
                stealers,
            },
            workers,
        )
    }

    /// Push a task into the global injector queue.
    pub fn push(&self, task: T) {
        self.injector.push(task);
    }

    /// Try to steal a task. First tries the global injector, then steals from other workers.
    pub fn steal(&self) -> Option<T> {
        // Try global queue first.
        loop {
            match self.injector.steal() {
                Steal::Success(t) => return Some(t),
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }

        // Try stealing from other workers.
        for stealer in &self.stealers {
            loop {
                match stealer.steal() {
                    Steal::Success(t) => return Some(t),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
        }

        None
    }

    /// Check if the global injector is empty.
    pub fn is_empty(&self) -> bool {
        self.injector.is_empty()
    }

    /// Get the number of stealers (workers).
    pub fn num_workers(&self) -> usize {
        self.stealers.len()
    }
}

/// Helper: try to find work for a local worker. Checks the local queue first,
/// then the injector, then steals from peers.
pub fn find_task<T>(
    local: &Worker<T>,
    injector: &Injector<T>,
    stealers: &[Stealer<T>],
) -> Option<T> {
    // Check local queue first.
    if let Some(task) = local.pop() {
        return Some(task);
    }

    // Try global queue.
    loop {
        match injector.steal_batch_and_pop(local) {
            Steal::Success(t) => return Some(t),
            Steal::Empty => break,
            Steal::Retry => continue,
        }
    }

    // Try stealing from peers.
    for stealer in stealers {
        loop {
            match stealer.steal() {
                Steal::Success(t) => return Some(t),
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_steal() {
        let (queue, _workers) = WorkStealingQueue::new(2);
        queue.push(1);
        queue.push(2);
        queue.push(3);

        let mut results = Vec::new();
        while let Some(v) = queue.steal() {
            results.push(v);
        }
        results.sort();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[test]
    fn test_local_worker() {
        let (queue, workers) = WorkStealingQueue::new(2);
        let w = &workers[0];
        w.push(10);
        w.push(20);

        // Local pop returns items (order depends on FIFO/LIFO worker type).
        let first = w.pop();
        assert!(first == Some(10) || first == Some(20));

        // Push to global and steal.
        queue.push(30);
        let stolen = queue.steal();
        assert_eq!(stolen, Some(30));
    }

    #[test]
    fn test_find_task_helper() {
        let injector = Injector::new();
        let w1 = Worker::new_fifo();
        let w2 = Worker::new_fifo();
        let stealers = vec![w1.stealer(), w2.stealer()];

        w1.push(100);
        injector.push(200);

        // Should find local task first.
        let t = find_task(&w1, &injector, &stealers);
        assert_eq!(t, Some(100));

        // Now should find injector task.
        let t = find_task(&w1, &injector, &stealers);
        assert_eq!(t, Some(200));
    }
}

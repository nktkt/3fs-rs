//! Memory allocator wrappers for hf3fs.
//!
//! Use feature flags to select the allocator:
//! - `jemalloc`: Use jemalloc (recommended for production on Linux)
//! - `mimalloc`: Use mimalloc
//! - default: Use system allocator

#[cfg(feature = "jemalloc")]
mod jemalloc_alloc {
    use tikv_jemallocator::Jemalloc;

    #[global_allocator]
    static GLOBAL: Jemalloc = Jemalloc;

    pub fn allocated() -> usize {
        tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0)
    }

    pub fn resident() -> usize {
        tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0)
    }
}

#[cfg(feature = "mimalloc")]
mod mimalloc_alloc {
    use mimalloc::MiMalloc;

    #[global_allocator]
    static GLOBAL: MiMalloc = MiMalloc;

    pub fn allocated() -> usize {
        0 // mimalloc doesn't easily expose stats
    }

    pub fn resident() -> usize {
        0
    }
}

/// Get the number of bytes currently allocated.
pub fn allocated() -> usize {
    #[cfg(feature = "jemalloc")]
    { return jemalloc_alloc::allocated(); }

    #[cfg(feature = "mimalloc")]
    { return mimalloc_alloc::allocated(); }

    #[cfg(not(any(feature = "jemalloc", feature = "mimalloc")))]
    { 0 }
}

/// Get the resident set size in bytes.
pub fn resident() -> usize {
    #[cfg(feature = "jemalloc")]
    { return jemalloc_alloc::resident(); }

    #[cfg(feature = "mimalloc")]
    { return mimalloc_alloc::resident(); }

    #[cfg(not(any(feature = "jemalloc", feature = "mimalloc")))]
    { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats() {
        // Just ensure the functions don't panic
        let _ = allocated();
        let _ = resident();
    }
}

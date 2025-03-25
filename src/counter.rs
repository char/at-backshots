use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;

pub struct MonotonicCounter {
    key: &'static str,
    incr: AtomicU64,
}
impl MonotonicCounter {
    pub fn new(key: &'static str) -> Self {
        Self {
            key,
            incr: Default::default(),
        }
    }

    pub fn add(&self, n: u64) {
        self.incr.fetch_add(n, Ordering::Relaxed);
    }

    pub fn flush(&self, db: &rusqlite::Connection) -> Result<()> {
        let count_incr = self.incr.swap(0, Ordering::Relaxed);
        db.execute(
            "UPDATE counts SET count = count + ? WHERE key = ?",
            (count_incr, &self.key),
        )?;
        Ok(())
    }
}

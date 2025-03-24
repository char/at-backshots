use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard,
    },
};

use anyhow::Result;
use db::setup_db;
use rusqlite::Connection;

pub mod car;
pub mod mst;

pub mod data;
pub mod db;
pub mod http;
pub mod ingest;
pub mod lexicons;
pub mod storage;
pub mod tid;
pub mod zplc_client;

pub struct AppState {
    pub zplc_server: String,

    backlink_incr: AtomicU64,
    db: Mutex<rusqlite::Connection>,
}

impl AppState {
    pub fn new(data_dir: impl AsRef<Path>, zplc_server: String) -> Result<Self> {
        let _ = std::fs::create_dir_all(data_dir.as_ref());
        let db = Connection::open(data_dir.as_ref().join("./db"))?;
        setup_db(&db)?;

        Ok(Self {
            zplc_server,
            backlink_incr: AtomicU64::new(0),
            db: Mutex::new(db),
        })
    }

    pub fn db(&self) -> MutexGuard<'_, Connection> {
        self.db.lock().unwrap()
    }

    pub fn incr_backlink_count(&self, n: u64) -> Result<()> {
        self.backlink_incr.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    pub fn flush_backlink_count(&self, db: &Connection) -> Result<()> {
        let count_incr = self.backlink_incr.swap(0, Ordering::Relaxed);
        db.execute(
            "UPDATE counts SET count = count + ? WHERE key = 'backlinks'",
            [count_incr],
        )?;
        Ok(())
    }
}

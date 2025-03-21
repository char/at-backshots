use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::Result;
use rusqlite::Connection;

pub mod car;
pub mod mst;

pub mod data;
pub mod http;
pub mod ingest;
pub mod lexicons;
pub mod storage;
pub mod tid;
pub mod web;
pub mod zplc_client;

pub struct AppState {
    pub zplc_server: String,

    db: Mutex<rusqlite::Connection>,
}

impl AppState {
    pub fn new(data_dir: impl AsRef<Path>, zplc_server: String) -> Result<Self> {
        let _ = std::fs::create_dir_all(data_dir.as_ref());
        let db = Connection::open(data_dir.as_ref().join("./db"))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS counts (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            count INTEGER NOT NULL
        ) STRICT",
            (),
        )?;
        db.execute(
            "INSERT OR IGNORE INTO counts (key, count) VALUES ('backlinks', 0)",
            (),
        )?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS outline_rkeys (
            id INTEGER PRIMARY KEY,
            rkey TEXT UNIQUE NOT NULL
        ) STRICT",
            (),
        )?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS outline_dids (
            id INTEGER PRIMARY KEY,
            did TEXT UNIQUE NOT NULL
        ) STRICT",
            (),
        )?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY,
            collection TEXT UNIQUE NOT NULL
        ) STRICT",
            (),
        )?;

        Ok(Self {
            zplc_server,
            db: Mutex::new(db),
        })
    }

    pub fn db(&self) -> MutexGuard<'_, Connection> {
        self.db.try_lock().unwrap()
    }

    pub fn incr_backlink_count(&self, db: &MutexGuard<'_, Connection>, n: u64) -> Result<()> {
        db.execute(
            "UPDATE counts SET count = count + ? WHERE key = 'backlinks'",
            [n],
        )?;
        Ok(())
    }
}

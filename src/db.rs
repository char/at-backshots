use std::collections::HashMap;

use anyhow::Result;

pub type DbConnection = rusqlite::Connection;

pub fn setup_db(db: &DbConnection) -> Result<()> {
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
    db.execute(
        "CREATE TABLE IF NOT EXISTS data_stores (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    type TEXT NOT NULL -- 'live' | 'compacted'
) STRICT",
        (),
    )?;

    Ok(())
}

#[derive(Default)]
pub struct DbCaches {
    pub did: HashMap<String, u64>,
    pub collection: HashMap<String, u32>,
}

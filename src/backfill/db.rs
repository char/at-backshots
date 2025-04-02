use anyhow::Result;
use rusqlite::{fallible_iterator::FallibleIterator, Batch, Connection};

use crate::{
    data::did::{DID_FLAG_NON_STANDARD, DID_MASK},
    AppConfig,
};

pub fn open_backfill_db(cfg: &AppConfig) -> Result<Connection> {
    let backfill_db = Connection::open(cfg.data_dir.join("backfill.db"))?;
    backfill_db.pragma_update(None, "journal_mode", "WAL")?;
    backfill_db.pragma_update(None, "synchronous", "normal")?;
    let mut batch = Batch::new(&backfill_db, include_str!("./backfill_schema.sql"));
    while let Some(mut stmt) = batch.next()? {
        stmt.execute(())?;
    }
    Ok(backfill_db)
}

pub fn convert_did_from_db(did_id: i64) -> u64 {
    if did_id < 0 {
        return (-did_id as u64) | DID_FLAG_NON_STANDARD;
    }
    did_id as u64
}
pub fn convert_did_to_db(did: u64) -> i64 {
    if did & DID_FLAG_NON_STANDARD != 0 {
        return -((did & DID_MASK) as i64);
    }
    did as i64
}

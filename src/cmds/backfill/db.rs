use anyhow::Result;
use backshots::AppConfig;
use rusqlite::{fallible_iterator::FallibleIterator, Batch, Connection};

pub fn open_backfill_db(cfg: &AppConfig) -> Result<Connection> {
    let backfill_db = Connection::open(cfg.data_dir.join("backfill.db"))?;
    let mut batch = Batch::new(&backfill_db, include_str!("./db.sql"));
    while let Some(mut stmt) = batch.next()? {
        stmt.execute(())?;
    }
    Ok(backfill_db)
}

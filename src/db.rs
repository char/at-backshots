use std::collections::HashMap;

use anyhow::Result;

pub type DbConnection = rusqlite::Connection;

pub fn setup_db(db: &DbConnection) -> Result<()> {
    db.pragma_update(None, "journal_mode", "WAL")?;
    let mut batch = rusqlite::Batch::new(db, include_str!("./db_schema.sql"));
    while let Some(mut stmt) = rusqlite::fallible_iterator::FallibleIterator::next(&mut batch)? {
        stmt.execute(())?;
    }

    Ok(())
}

#[derive(Default)]
pub struct DbCaches {
    pub did: HashMap<String, u64>,
    pub collection: HashMap<String, u32>,
}

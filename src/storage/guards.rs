use std::ops::{Deref, DerefMut};

use anyhow::Result;

use crate::{db::DbConnection, AppContext};

use super::live::LiveStorageWriter;

pub struct LiveStorageWriterGuard {
    db: DbConnection,
    user_id: u64,
    pub store_id: u64,
    pub writer: LiveStorageWriter,
}

impl Deref for LiveStorageWriterGuard {
    type Target = LiveStorageWriter;
    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}
impl DerefMut for LiveStorageWriterGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}

impl LiveStorageWriterGuard {
    pub fn add_new(app: &AppContext) -> Result<()> {
        app.db.execute(
            "INSERT INTO data_stores (name, type) VALUES (strftime('%Y%m%d%H%M%S', 'now'), 'live')",
            (),
        )?;

        Ok(())
    }

    pub fn latest_id(app: &AppContext) -> Result<u64> {
        let id: u64 = app.db.query_row(
            "SELECT id, name FROM data_stores WHERE type = 'live' ORDER BY id DESC LIMIT 1",
            (),
            |r| r.get(0),
        )?;
        Ok(id)
    }

    pub fn latest(app: &AppContext) -> Result<Self> {
        let db = app.connect_to_db()?;

        let (id, name) = {
            let find_row = || {
                db.query_row(
                    "SELECT id, name FROM data_stores WHERE type = 'live' ORDER BY id DESC LIMIT 1",
                    (),
                    |r| {
                        let id: u64 = r.get(0)?;
                        let name: String = r.get(1)?;
                        Ok((id, name))
                    },
                )
            };

            match find_row() {
                Ok(r) => r,
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    Self::add_new(app)?;
                    find_row()?
                }
                Err(e) => return Err(e.into()),
            }
        };

        db.execute(
            "INSERT INTO data_store_users (data_store_id, node_id) VALUES (?, ?)",
            (id, app.node_id.to_string()),
        )?;
        let user_id = db.last_insert_rowid() as u64;

        let writer = LiveStorageWriter::new(app.data_dir.join("live/").join(name))?;

        Ok(Self {
            user_id,
            store_id: id,
            db,
            writer,
        })
    }
}

impl Drop for LiveStorageWriterGuard {
    fn drop(&mut self) {
        self.db
            .execute("DELETE FROM data_store_users WHERE id = ?", [self.user_id])
            .expect("failed to release LiveStorageWriter guard!");
    }
}

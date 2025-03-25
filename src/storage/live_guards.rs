use std::{
    fs::File,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use anyhow::Result;

use crate::AppContext;

use super::live::{LiveStorageReader, LiveStorageWriter};

pub struct LiveWriteHandle {
    pidfile: PathBuf,
    pub store_id: u64,
    pub writer: LiveStorageWriter,
}

impl Deref for LiveWriteHandle {
    type Target = LiveStorageWriter;
    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}
impl DerefMut for LiveWriteHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}

impl LiveWriteHandle {
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
        let (id, name) = {
            let find_row = || {
                app.db.query_row(
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

        let storage_dir = app.data_dir.join("live").join(name);
        let writer = LiveStorageWriter::new(&storage_dir)?;
        let pidfile = storage_dir.join(format!("{}.pid", std::process::id()));
        File::create_new(&pidfile)?;

        Ok(Self {
            pidfile,
            store_id: id,
            writer,
        })
    }
}

impl Drop for LiveWriteHandle {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.pidfile);
    }
}

pub struct LiveReadHandle {
    pidfile: PathBuf,
    pub reader: LiveStorageReader,
}
impl Deref for LiveReadHandle {
    type Target = LiveStorageReader;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}
impl DerefMut for LiveReadHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

impl LiveReadHandle {
    pub fn all(app: &AppContext) -> Result<Vec<u64>> {
        let mut statement = app
            .db
            .prepare("SELECT id FROM data_stores WHERE type = 'live' ORDER BY id ASC")?;
        let v = statement
            .query_map((), |row| row.get::<_, u64>(0))?
            .filter_map(Result::ok)
            .collect();
        Ok(v)
    }

    pub fn new(app: &AppContext, store_id: u64) -> Result<Self> {
        let db = app.connect_to_db()?;

        let name: String = db.query_row(
            "SELECT name FROM data_stores WHERE id = ? AND type = 'live'",
            [store_id],
            |row| row.get(0),
        )?;

        let storage_dir = app.data_dir.join("live").join(name);
        let reader = LiveStorageReader::new(&storage_dir)?;
        let pidfile = storage_dir.join(format!("{}.pid", std::process::id()));
        File::create_new(&pidfile)?;

        Ok(Self { pidfile, reader })
    }
}
impl Drop for LiveReadHandle {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.pidfile);
    }
}

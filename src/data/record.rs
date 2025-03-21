use anyhow::{Context, Result};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::{
    tid::{is_tid, s32decode, s32encode},
    AppState,
};

use super::{at_uri::parse_at_uri, did::Did};

// this is just an index into the collections table
pub type RecordCollection = u32;

pub const RKEY_FLAG_NOT_TID: u64 = 1 << 63;
pub const RKEY_DB_MASK: u64 = !RKEY_FLAG_NOT_TID;

#[derive(Clone, Copy, IntoBytes, FromBytes, Immutable, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C, packed)]
pub struct RecordId {
    pub rkey: u64,
    pub did_hi: u16,
    pub did_lo: u32,
    // u16::MAX signals that we need extended data
    pub collection: u16,
}

impl RecordId {
    pub fn did(&self) -> Did {
        ((self.did_hi as u64) << 32) | self.did_lo as u64
    }
    pub fn split_did(did: Did) -> (u16, u32) {
        (
            (((did >> 32) & u16::MAX as u64) as u16),
            ((did & u32::MAX as u64) as u32),
        )
    }

    pub fn new(did: u64, collection: u32, rkey: u64) -> Self {
        let (did_hi, did_lo) = Self::split_did(did);
        Self {
            rkey,
            collection: collection.try_into().unwrap_or(u16::MAX),
            did_hi,
            did_lo,
        }
    }
}

impl RecordId {
    pub async fn from_at_uri(app: &AppState, uri: &str) -> Result<Self> {
        let (repo, collection, rkey) = parse_at_uri(uri)?;
        Ok(RecordId::new(
            app.encode_did(repo).await?,
            app.encode_collection(collection)?,
            app.encode_rkey(rkey)?,
        ))
    }
}

impl std::fmt::Debug for RecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordId")
            // looks weird because ref to unaligned field
            .field("rkey", &u64::from(self.rkey))
            .field("collection", &u16::from(self.collection))
            .field("did", &self.did())
            .finish()
    }
}

impl AppState {
    pub fn resolve_rkey(&self, rkey_id: u64) -> Result<String> {
        if rkey_id & RKEY_FLAG_NOT_TID == 0 {
            return Ok(s32encode(rkey_id));
        }

        let rkey: String = self
            .db()
            .query_row(
                "SELECT rkey FROM outline_rkeys WHERE id = ?",
                [rkey_id & RKEY_DB_MASK],
                |row| row.get(0),
            )
            .context("could not find rkey in rkeys table")?;
        Ok(rkey)
    }

    pub fn encode_rkey(&self, rkey: &str) -> Result<u64> {
        if is_tid(rkey) {
            return Ok(s32decode(rkey));
        }

        let rkey_id: u64 = {
            let db = self.db();
            db.execute(
                "INSERT OR IGNORE INTO outline_rkeys (rkey) VALUES (?)",
                [rkey],
            )?;
            db.query_row(
                "SELECT id FROM outline_rkeys WHERE rkey = ?",
                [rkey],
                |row| row.get(0),
            )?
        };

        Ok(rkey_id | RKEY_FLAG_NOT_TID)
    }

    pub fn resolve_collection(&self, coll: RecordCollection) -> Result<String> {
        let collection: String = self
            .db()
            .query_row(
                "SELECT collection FROM collections WHERE id = ?",
                [coll],
                |row| row.get(0),
            )
            .context("could not find collection id in colls tree")?;

        Ok(collection)
    }

    pub fn encode_collection(&self, collection: &str) -> Result<RecordCollection> {
        let id: u32 = {
            let db = self.db();

            match db.query_row(
                "SELECT id FROM collections WHERE collection = ?",
                [collection],
                |row| row.get::<_, u32>(0),
            ) {
                Ok(coll) => Ok(coll),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    db.execute(
                        "INSERT OR IGNORE INTO collections (collection) VALUES (?)",
                        [collection],
                    )?;
                    Ok(db.last_insert_rowid() as u32)
                }
                Err(e) => Err(e),
            }
        }?;
        Ok(id)
    }
}

use anyhow::{Context, Result};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::{
    tid::{is_tid, s32decode, s32encode},
    AppContext,
};

use super::{at_uri::parse_at_uri, did::encode_did};

// this is just an index into the collections table
pub type RecordCollection = u32;

pub const RKEY_FLAG_NOT_TID: u64 = 1 << 63;
pub const RKEY_DB_MASK: u64 = !RKEY_FLAG_NOT_TID;

#[derive(Clone, Copy, IntoBytes, FromBytes, Immutable, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C, packed)]
pub struct RecordId {
    pub rkey: u64,
    pub collection: u32,
    pub did: u64,
    pub _flags: RecordIdFlags,
}

impl RecordId {
    pub fn new(did: u64, collection: u32, rkey: u64) -> Self {
        Self {
            rkey,
            collection,
            did,
            _flags: 0.into(),
        }
    }
}

impl RecordId {
    pub fn from_at_uri(app: &mut AppContext, uri: &str) -> Result<Self> {
        let (repo, collection, rkey) = parse_at_uri(uri)?;
        Ok(RecordId::new(
            encode_did(&mut *app, repo)?,
            encode_collection(&mut *app, collection)?,
            encode_rkey(&*app, rkey)?,
        ))
    }
}

impl std::fmt::Debug for RecordId {
    // we do a useless from() because otherwise we have refs to unaligned fields
    #[allow(clippy::useless_conversion)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordId")
            .field("rkey", &u64::from(self.rkey))
            .field("collection", &u32::from(self.collection))
            .field("did", &u64::from(self.did))
            .finish()
    }
}

pub fn encode_rkey(app: &AppContext, rkey: &str) -> Result<u64> {
    if is_tid(rkey) {
        return Ok(s32decode(rkey));
    }

    let rkey_id: u64 = {
        app.db.execute(
            "INSERT OR IGNORE INTO outline_rkeys (rkey) VALUES (?)",
            [rkey],
        )?;
        app.db.query_row(
            "SELECT id FROM outline_rkeys WHERE rkey = ?",
            [rkey],
            |row| row.get(0),
        )?
    };

    Ok(rkey_id | RKEY_FLAG_NOT_TID)
}

pub fn resolve_rkey(app: &AppContext, rkey_id: u64) -> Result<String> {
    if rkey_id & RKEY_FLAG_NOT_TID == 0 {
        return Ok(s32encode(rkey_id));
    }

    let rkey: String = app
        .db
        .query_row(
            "SELECT rkey FROM outline_rkeys WHERE id = ?",
            [rkey_id & RKEY_DB_MASK],
            |row| row.get(0),
        )
        .context("could not find rkey in rkeys table")?;
    Ok(rkey)
}

pub fn encode_collection(app: &mut AppContext, collection: &str) -> Result<RecordCollection> {
    if let Some(cached) = app.caches.collection.get(collection) {
        return Ok(*cached);
    }

    let id: u32 = match app.db.query_row(
        "SELECT id FROM collections WHERE collection = ?",
        [collection],
        |row| row.get::<_, u32>(0),
    ) {
        Ok(coll) => Ok(coll),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            app.db.execute(
                "INSERT OR IGNORE INTO collections (collection) VALUES (?)",
                [collection],
            )?;
            Ok(app.db.last_insert_rowid() as u32)
        }
        Err(e) => Err(e),
    }?;
    app.caches.collection.insert(collection.into(), id);
    Ok(id)
}

pub fn resolve_collection(app: &AppContext, coll: RecordCollection) -> Result<String> {
    let collection: String = app
        .db
        .query_row(
            "SELECT collection FROM collections WHERE id = ?",
            [coll],
            |row| row.get(0),
        )
        .context("could not find collection id in colls tree")?;

    Ok(collection)
}

#[derive(Clone, Copy, IntoBytes, FromBytes, Immutable)]
#[repr(transparent)]
pub struct RecordIdFlags(pub u32);
impl PartialEq for RecordIdFlags {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for RecordIdFlags {}
impl Ord for RecordIdFlags {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}
impl PartialOrd for RecordIdFlags {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl From<u32> for RecordIdFlags {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

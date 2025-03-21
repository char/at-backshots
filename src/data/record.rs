use anyhow::{Context, Result};
use std::borrow::Cow;
use zerocopy::{BigEndian, FromBytes, Immutable, IntoBytes, U16, U32};

use crate::AppState;

use super::{at_uri::parse_at_uri, did::Did};

// this is just an index into the collections table
pub type RecordCollection = u32;

// if 16th byte is 0, this is a nul-terminated string
// otherwise, this is a 120 bit index into the rkeys table
pub type RecordKey = [u8; 14];

#[derive(Clone, Copy, IntoBytes, FromBytes, Immutable, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C, packed)]
pub struct RecordId {
    pub rkey: [u8; 14],
    pub collection: U32<BigEndian>,
    pub did_hi: U16<BigEndian>,
    pub did_lo: U32<BigEndian>,
    // pub cid: CidV1Sha256,
}

impl RecordId {
    pub fn did(&self) -> Did {
        ((self.did_hi.get() as u64) << 32) | self.did_lo.get() as u64
    }
    pub fn split_did(did: Did) -> (u16, u32) {
        (
            (((did >> 32) & u16::MAX as u64) as u16),
            ((did & u32::MAX as u64) as u32),
        )
    }
}

impl RecordId {
    pub async fn from_at_uri(app: &AppState, uri: &str) -> Result<Self> {
        let (repo, collection, rkey) = parse_at_uri(uri)?;
        let did = app.encode_did(repo).await?;
        Ok(Self {
            rkey: app.encode_rkey(rkey)?,
            collection: U32::new(app.encode_collection(collection)?),
            did_lo: ((did & u32::MAX as u64) as u32).into(),
            did_hi: (((did >> 32) & u16::MAX as u64) as u16).into(),
        })
    }
}

impl std::fmt::Debug for RecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordId")
            .field(
                "rkey",
                &std::str::from_utf8(&self.rkey)
                    .as_deref()
                    .unwrap_or("<external>"),
            )
            .field("collection", &self.collection.get())
            .field("did", &self.did())
            .finish()
    }
}

impl AppState {
    pub fn resolve_rkey<'a>(&self, rkey: &'a RecordKey) -> Result<Cow<'a, str>> {
        if rkey[0] == 0 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&rkey[6..]);
            let rkey_id = u64::from_be_bytes(bytes);
            let rkey: String = self
                .db()
                .query_row(
                    "SELECT rkey FROM outline_rkeys WHERE id = ?",
                    [rkey_id],
                    |row| row.get(0),
                )
                .context("could not find rkey in rkeys table")?;
            return Ok(Cow::Owned(rkey));
        }

        let len = rkey.iter().position(|b| *b == 0).unwrap_or(rkey.len());
        let s = std::str::from_utf8(&rkey[..len]).context("rkey: failed to decode inline utf-8")?;
        Ok(Cow::Borrowed(s))
    }

    pub fn encode_rkey(&self, rkey: &str) -> Result<RecordKey> {
        // it isn't useful to reverse-index the rkeys, since they should be almost-globally-unique.

        if rkey.len() <= 13 && rkey.as_bytes()[0] != 0 {
            let mut bytes = [0u8; 14];
            bytes[0..rkey.len()].copy_from_slice(rkey.as_bytes());
            return Ok(bytes);
        }

        let rkey_id: u64 = {
            let db = self.db();
            db.execute("INSERT INTO outline_rkeys (rkey) VALUES (?)", [rkey])?;
            db.query_row(
                "SELECT id FROM outline_rkeys WHERE rkey = ?",
                [rkey],
                |row| row.get(0),
            )?
        };
        let mut bytes = [0u8; 14];
        bytes[6..].copy_from_slice(&rkey_id.to_be_bytes());

        Ok(bytes)
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

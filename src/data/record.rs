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
            let rkey = self
                .db_rkeys
                .get(rkey)?
                .context("could not find rkey in rkeys tree")?;
            let rkey =
                String::from_utf8(rkey.to_vec()).context("rkey: failed to decode stored utf-8")?;
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

        let bytes: Result<_, sled::transaction::TransactionError> =
            self.db_rkeys.transaction(|tx| {
                let counter = if let Some(counter) = tx.get([])? {
                    let mut bytes = [0u8; 16];
                    bytes.copy_from_slice(&counter[0..16]);
                    bytes[15] = 0;
                    u128::from_be_bytes(bytes) + 1
                } else {
                    0
                };

                let counter_bytes = counter.to_be_bytes();
                tx.insert(&[], &counter_bytes)?;
                let mut bytes = [0u8; 14];
                bytes.copy_from_slice(&counter_bytes[2..16]);
                assert_eq!(
                    bytes[0], 0,
                    "precondition: there should not be more than 2^104 rkeys"
                );
                Ok(bytes)
            });

        Ok(bytes?)
    }

    pub fn resolve_collection(&self, coll: RecordCollection) -> Result<String> {
        let collection = self
            .db_collections
            .get(coll.to_be_bytes())?
            .context("could not find collection id in colls tree")?;

        let collection =
            String::from_utf8(collection.to_vec()).context("collection: failed to decode utf-8")?;

        Ok(collection)
    }

    pub fn encode_collection(&self, collection: &str) -> Result<RecordCollection> {
        if let Some(collection) = self.db_collections_reverse.get(collection)? {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&collection[0..4]);
            return Ok(RecordCollection::from_be_bytes(bytes));
        }

        let counter: Result<_, sled::transaction::TransactionError> =
            self.db_collections.transaction(|tx| {
                let counter = if let Some(counter) = tx.get([])? {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&counter[0..8]);
                    u64::from_be_bytes(bytes) + 1
                } else {
                    0
                };

                tx.insert(&[], &counter.to_be_bytes())?;
                Ok(counter)
            });

        let counter = counter?;
        self.db_collections
            .insert(counter.to_be_bytes(), collection)?;
        self.db_collections_reverse
            .insert(collection, &counter.to_be_bytes())?;
        Ok(counter.try_into()?)
    }
}

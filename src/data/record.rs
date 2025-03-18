use anyhow::{Context, Result};
use std::borrow::Cow;

use super::did::Did;
use crate::AppState;

// this is just an index into the collections table
pub type RecordCollection = u64;

// if 16th byte is 0, this is a nul-terminated string
// otherwise, this is a 120 bit index into the rkeys table
pub type RecordKey = [u8; 16];

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct RecordId {
    pub rkey: [u8; 16],
    pub collection: u64,
    pub did: Did,
    // pub cid: CidV1Sha256,
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

        let s = std::str::from_utf8(rkey).context("rkey: failed to decode inline utf-8")?;
        Ok(Cow::Borrowed(s))
    }

    pub fn encode_rkey(&self, rkey: &str) -> Result<RecordKey> {
        // it isn't useful to reverse-index the rkeys, since they should be almost-globally-unique.

        // length check ensures final byte is 0
        if rkey.len() < 15 {
            let mut bytes = [0u8; 16];
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

                let bytes = counter.to_be_bytes();
                assert_eq!(
                    bytes[0], 0,
                    "precondition: there should not be more than 2^120 rkeys"
                );
                tx.insert(&[], &bytes)?;
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
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&collection[0..8]);
            return Ok(u64::from_be_bytes(bytes));
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
        Ok(counter)
    }
}

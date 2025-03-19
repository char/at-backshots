use anyhow::{Context, Result};

use crate::AppState;

pub type Did = u64;
pub const DID_MASK: u64 = 0x0000FFFFFFFFFFFF;
pub const ZPLC_MASK: u64 = 0x3FFFFFFFFFFF;
// did:web, and did:plc past 2^46 (≈ 70 trillion)
pub const DID_FLAG_NON_STANDARD: u64 = 1 << 47;
// we use some bits from the Did field to indicate whether a record has been deleted
pub const DID_FLAG_RECORD_DELETED: u64 = 1 << 46;

impl AppState {
    pub async fn resolve_did(&self, did: Did) -> Result<String> {
        if did & DID_FLAG_NON_STANDARD == 0 {
            return self.zplc_to_did(did).await;
        }

        let did = self
            .db_dids
            .get(did.to_be_bytes())?
            .context(format!("DID {} was not found in dids tree", did))?;
        Ok(String::from_utf8(did.to_vec())?)
    }

    pub async fn encode_did(&self, did: &str) -> Result<Did> {
        if did.starts_with("did:plc:") {
            if let Ok(Some(zplc)) = self.lookup_zplc(did).await {
                return Ok(zplc);
            }
        }

        self.encode_did_sync(did)
    }

    /// encodes a did but doesn't look up a zplc
    pub fn encode_did_sync(&self, did: &str) -> Result<Did> {
        if let Some(did) = self.db_dids_reverse.get(did)? {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&did[0..8]);
            let did = u64::from_be_bytes(bytes);
            return Ok(did);
        }

        let counter: Result<_, sled::transaction::TransactionError> =
            self.db_dids_reverse.transaction(|tx| {
                let counter = if let Some(counter) = tx.get([])? {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&counter[0..8]);
                    u64::from_be_bytes(bytes) + 1
                } else {
                    DID_FLAG_NON_STANDARD
                };
                tx.insert(&[], &counter.to_be_bytes())?;
                Ok(counter)
            });

        let counter = counter?;
        self.db_dids.insert(counter.to_be_bytes(), did)?;
        self.db_dids_reverse.insert(did, &counter.to_be_bytes())?;
        Ok(counter)
    }
}

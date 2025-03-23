use anyhow::Result;

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
        let did: String = {
            self.db()
                .query_row("SELECT did FROM outline_dids WHERE id = ?", [did], |row| {
                    row.get(0)
                })
        }?;
        Ok(did)
    }

    #[inline]
    pub fn try_encode_did_sync(&self, did: &str) -> Option<Did> {
        crate::zplc_client::ZPLC_CACHE.with_borrow(|cache| cache.get(did).cloned())
    }

    pub async fn encode_did(&self, did: &str) -> Result<Did> {
        if let Some(cached_value) = self.try_encode_did_sync(did) {
            return Ok(cached_value);
        }

        if did.starts_with("did:plc:") {
            if let Ok(Some(zplc)) = self.lookup_zplc(did).await {
                return Ok(zplc);
            }
        }

        self.encode_did_sync(did)
    }

    /// encodes a did but doesn't look up a zplc
    pub fn encode_did_sync(&self, did: &str) -> Result<Did> {
        let did: u64 = {
            let db = self.db();
            match db.query_row("SELECT id FROM outline_dids WHERE did = ?", [did], |row| {
                row.get::<_, u64>(0)
            }) {
                Ok(did) => Ok(did),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    db.execute("INSERT OR IGNORE INTO outline_dids (did) VALUES (?)", [did])?;
                    Ok(db.last_insert_rowid() as u64)
                }
                Err(e) => Err(e),
            }
        }?;
        Ok(did)
    }
}

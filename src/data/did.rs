use anyhow::Result;

use crate::AppContext;

pub const DID_MASK: u64 = 0x0000FFFFFFFFFFFF;
// did:web, and did:plc past 2^48 (≈ 280 trillion)
pub const DID_FLAG_NON_STANDARD: u64 = 1 << 63;

pub fn resolve_did(app: &AppContext, did: u64) -> Result<String> {
    if did & DID_FLAG_NON_STANDARD == 0 {
        // return app.async_block_on(app.zplc_resolver.zplc_to_did(did));
        return app.zplc_direct_resolver.zplc_to_did(did);
    }

    let did: String = app.db.query_row(
        "SELECT did FROM outline_dids WHERE id = ?",
        [did & DID_MASK],
        |row| row.get(0),
    )?;
    Ok(did)
}

pub fn encode_existing_did(app: &AppContext, did: &str) -> Result<Option<u64>> {
    if did.starts_with("did:plc:") {
        /* if let Ok(Some(zplc)) = app.async_block_on(app.zplc_resolver.lookup_zplc(did)) {
            return Ok(Some(zplc));
        } */
        if let Ok(Some(zplc)) = app.zplc_direct_resolver.lookup_zplc(did) {
            return Ok(Some(zplc));
        }
    }

    match app
        .db
        .query_row("SELECT id FROM outline_dids WHERE did = ?", [did], |row| {
            row.get::<_, u64>(0)
        }) {
        Ok(did) => Ok(Some(did | DID_FLAG_NON_STANDARD)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn encode_did(app: &mut AppContext, did: &str) -> Result<u64> {
    if let Some(cached) = app.caches.did.get(did) {
        return Ok(*cached);
    }

    let id = match encode_existing_did(&*app, did)? {
        Some(did) => did,
        None => {
            app.db
                .execute("INSERT OR IGNORE INTO outline_dids (did) VALUES (?)", [did])?;
            (app.db.last_insert_rowid() as u64) | DID_FLAG_NON_STANDARD
        }
    };
    app.caches.did.insert(did.into(), id);
    Ok(id)
}

use anyhow::Result;

pub struct ZplcDirectResolver {
    pub conn: rusqlite::Connection,
}

impl ZplcDirectResolver {
    pub fn zplc_to_did(&self, id: u64) -> Result<String> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT did FROM plc_idents WHERE id = ?")?;
        let did: String = statement.query_row([id], |row| row.get(0))?;
        Ok(did)
    }

    pub fn lookup_zplc(&self, did: &str) -> Result<Option<u64>> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT id FROM plc_idents WHERE did = ?")?;
        let id: Option<u64> = match statement.query_row([did], |row| row.get(0)) {
            Ok(v) => Some(v),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e.into()),
        };
        Ok(id)
    }
}

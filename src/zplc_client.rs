use anyhow::Result;

use crate::AppState;

// TODO: i need to write the zplc server before we can use this. lol

impl AppState {
    pub async fn zplc_to_did(&self, _id: u64) -> Result<String> {
        anyhow::bail!("nyi")
    }

    pub async fn lookup_zplc(&self, _did: &str) -> Result<Option<u64>> {
        // TODO: hit the zplc server
        Ok(None)
    }
}

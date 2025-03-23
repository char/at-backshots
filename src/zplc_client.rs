use std::{cell::RefCell, collections::HashMap};

use anyhow::Result;
use http_body_util::BodyExt;
use hyper::{Request, StatusCode};

use crate::{http::body_empty, http::client::fetch, AppState};

thread_local! {
    pub static ZPLC_CACHE: RefCell<HashMap<String, u64>> = Default::default();
}

impl AppState {
    pub async fn zplc_to_did(&self, id: u64) -> Result<String> {
        let zplc_server = &self.zplc_server;
        let req = Request::builder()
            .method("GET")
            .uri(format!("{zplc_server}/{id}"))
            .body(body_empty())?;
        let res = fetch(req).await?;
        if !res.status().is_success() {
            anyhow::bail!("got non-success response: {}", res.status());
        }
        let body = res.collect().await?.to_bytes();
        let did = String::from_utf8(body.to_vec())?;
        Ok(did)
    }

    pub async fn lookup_zplc(&self, did: &str) -> Result<Option<u64>> {
        let zplc_server = &self.zplc_server;
        let req = Request::builder()
            .method("GET")
            .uri(format!("{zplc_server}/{did}"))
            .body(body_empty())?;
        let res = fetch(req).await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        } else if !res.status().is_success() {
            anyhow::bail!("got non-success response: {}", res.status());
        }
        let body = res.collect().await?.to_bytes();
        let zplc_str = String::from_utf8(body.to_vec())?;
        let zplc_n: u64 = zplc_str.parse()?;
        ZPLC_CACHE.with_borrow_mut(|cache| cache.insert(did.into(), zplc_n));
        Ok(Some(zplc_n))
    }
}

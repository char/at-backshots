#![allow(dead_code)]

use ipld_core::cid::Cid;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct StreamEventHeader {
    pub op: i64,
    #[serde(default)]
    pub t: Option<String>,
}

// thank u rsky-lexicon <3

#[derive(Debug, Deserialize)]
pub struct RepoOperation {
    pub path: String,
    pub action: String,
    pub cid: Option<Cid>,
}

#[derive(Debug, Deserialize)]
pub struct SubscribeReposCommit {
    #[serde(with = "serde_bytes")]
    pub blocks: Vec<u8>,
    pub commit: Cid,
    #[serde(rename(deserialize = "ops"))]
    pub operations: Vec<RepoOperation>,
    pub prev: Option<Cid>,
    pub rebase: bool,
    pub repo: String,
    #[serde(rename(deserialize = "seq"))]
    pub sequence: i64,
    pub time: String,
    #[serde(rename(deserialize = "tooBig"))]
    pub too_big: bool,
}

#[derive(Debug, Deserialize)]
pub struct SubscribeReposInfo {
    pub message: Option<String>,
    pub name: String,
}

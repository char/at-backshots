use bytes::Bytes;
use ipld_core::cid::Cid;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct UnsignedCommitNode {
    pub did: String,
    pub version: u8,
    pub prev: Option<Cid>,
    pub rev: String,
    pub data: Cid,
}

#[derive(Debug, Deserialize)]
pub struct SignedCommitNode {
    #[serde(flatten)]
    pub data: UnsignedCommitNode,
    pub sig: Bytes,
}

#[derive(Debug, Deserialize)]
pub struct MSTEntry {
    pub p: u64,
    pub k: Bytes,
    pub v: Cid,
    pub t: Option<Cid>,
}

#[derive(Debug, Deserialize)]
pub struct MSTNode {
    pub l: Option<Cid>,
    pub e: Vec<MSTEntry>,
}

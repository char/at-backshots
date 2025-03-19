use std::sync::{atomic::AtomicU64, Arc};

use anyhow::Result;

pub mod car;
pub mod mst;

pub mod data;
pub mod http;
pub mod ingest;
pub mod lexicons;
pub mod storage;
pub mod web;
pub mod zplc_client;

pub struct AppState {
    pub zplc_server: String,

    pub db: sled::Db,
    pub db_rkeys: sled::Tree,
    pub db_collections: sled::Tree,
    pub db_collections_reverse: sled::Tree,
    pub db_dids: sled::Tree,
    pub db_dids_reverse: sled::Tree,

    pub targets_count: Arc<AtomicU64>,
}

impl AppState {
    pub fn new(zplc_server: String) -> Result<Self> {
        let db = sled::Config::default()
            .path("./data/db")
            .cache_capacity(1024 * 1024 * 1024)
            .mode(sled::Mode::LowSpace)
            .use_compression(true)
            .compression_factor(18)
            .open()?;

        let db_rkeys = db.open_tree(b"rkeys")?;
        let db_collections = db.open_tree(b"coll")?;
        let db_collections_reverse = db.open_tree(b"coll_r")?;

        let db_dids = db.open_tree(b"dids")?;
        let db_dids_reverse = db.open_tree(b"dids_r")?;

        let targets_count = Arc::new(AtomicU64::default());

        Ok(Self {
            zplc_server,

            db,

            db_rkeys,
            db_collections,
            db_collections_reverse,

            db_dids,
            db_dids_reverse,

            targets_count,
        })
    }

    pub fn fetch_backlink_count(&self) -> Result<u64> {
        Ok(self
            .db
            .get(b"backlinks_cnt")?
            .map(ivec_to_u64)
            .unwrap_or_default())
    }

    pub fn incr_backlink_count(&self, n: u64) -> Result<()> {
        let txn_result: Result<_, sled::transaction::TransactionError> =
            self.db.transaction(|tx| {
                let count = tx
                    .get(b"backlinks_cnt")?
                    .map(ivec_to_u64)
                    .unwrap_or_default();
                tx.insert(b"backlinks_cnt", &(count + n).to_be_bytes())?;
                Ok(())
            });
        txn_result?;
        Ok(())
    }
}

fn ivec_to_u64(v: sled::IVec) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&v);
    u64::from_be_bytes(bytes)
}

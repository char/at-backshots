use anyhow::Result;

pub mod car;
pub mod mst;

pub mod data;
pub mod http;
pub mod http_client;
pub mod ingest;
pub mod lexicons;
pub mod tls;
pub mod zplc_client;

fn concatenate_merge(
    _key: &[u8],
    old_value: Option<&[u8]>,
    merged_bytes: &[u8],
) -> Option<Vec<u8>> {
    let mut ret = old_value.map(|ov| ov.to_vec()).unwrap_or_default();
    ret.extend_from_slice(merged_bytes);
    Some(ret)
}

pub struct AppState {
    pub zplc_server: String,

    pub db: sled::Db,
    pub db_records: sled::Tree,

    pub db_rkeys: sled::Tree,
    pub db_collections: sled::Tree,
    pub db_collections_reverse: sled::Tree,

    pub did_db: sled::Db,
    pub db_dids: sled::Tree,
    pub db_dids_reverse: sled::Tree,
}
impl AppState {
    pub fn new(zplc_server: String) -> Result<Self> {
        let db = sled::Config::default()
            .path("./data")
            .cache_capacity(1024 * 1024 * 1024)
            .mode(sled::Mode::LowSpace)
            .use_compression(true)
            .compression_factor(8)
            .open()?;

        let db_records = db.open_tree(b"records")?;
        db_records.set_merge_operator(concatenate_merge);

        let did_db = sled::open("./data-did")?;
        let db_dids = did_db.open_tree(b"dids")?;
        let db_dids_reverse = did_db.open_tree(b"dids_r")?;

        let db_rkeys = db.open_tree(b"rkeys")?;
        let db_collections = db.open_tree(b"coll")?;
        let db_collections_reverse = db.open_tree(b"coll_r")?;

        Ok(Self {
            zplc_server,

            db,
            db_records,

            db_rkeys,
            db_collections,
            db_collections_reverse,

            did_db,
            db_dids,
            db_dids_reverse,
        })
    }
}

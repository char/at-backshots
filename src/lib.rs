use std::path::PathBuf;

use anyhow::Result;
use counter::MonotonicCounter;
use db::{setup_db, DbCaches, DbConnection};
use uuid::Uuid;
use zplc_client::ZplcDirectResolver;

pub mod car;
pub mod mst;

pub mod backfill;
pub mod counter;
pub mod data;
pub mod db;
pub mod firehose;
pub mod http;
pub mod ingest;
pub mod storage;
pub mod tid;
pub mod zplc_client;

pub struct AppConfig {
    pub zplc_path: String,
    pub data_dir: PathBuf,
}

pub fn get_app_config() -> Result<AppConfig> {
    // TODO: read from environment variables or whatever
    Ok(AppConfig {
        zplc_path: "../zplc-server/data/ids.db".into(),
        data_dir: "./data".into(),
    })
}

pub struct AppContext {
    pub node_id: Uuid,

    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub db: DbConnection,
    pub caches: DbCaches,
    pub backfill_db: Option<rusqlite::Connection>,

    pub zplc_direct_resolver: ZplcDirectResolver,
    pub backlinks_counter: MonotonicCounter,
}
impl AppContext {
    pub fn new(cfg: &AppConfig) -> Result<Self> {
        let node_id = Uuid::new_v4();

        let _ = std::fs::create_dir_all(&cfg.data_dir);
        let db_path = cfg.data_dir.join("db");
        let db = DbConnection::open(&db_path)?;
        setup_db(&db)?;
        Ok(Self {
            node_id,

            data_dir: cfg.data_dir.clone(),
            db_path,
            db,
            caches: DbCaches::default(),
            backfill_db: None,

            zplc_direct_resolver: ZplcDirectResolver {
                conn: rusqlite::Connection::open(cfg.zplc_path.clone())?,
            },
            backlinks_counter: MonotonicCounter::new("backlinks"),
        })
    }

    pub fn connect_to_db(&self) -> Result<DbConnection> {
        Ok(DbConnection::open(&self.db_path)?)
    }
}

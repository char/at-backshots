use std::path::PathBuf;

use anyhow::Result;
use counter::MonotonicCounter;
use db::{setup_db, DbCaches, DbConnection};
use zplc_client::ZplcResolver;

pub mod car;
pub mod mst;

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
    pub zplc_base: String,
    pub data_dir: PathBuf,
}

pub fn get_app_config() -> Result<AppConfig> {
    // TODO: read from environment variables or whatever
    Ok(AppConfig {
        zplc_base: "http://127.0.0.1:2485".into(),
        data_dir: "/dev/shm/backshots/data".into(),
    })
}

pub struct AppContext {
    pub db: DbConnection,
    pub caches: DbCaches,

    pub zplc_resolver: ZplcResolver,
    pub backlinks_counter: MonotonicCounter,

    pub tokio_rt: tokio::runtime::Runtime,
}
impl AppContext {
    pub fn new(cfg: &AppConfig) -> Result<Self> {
        let _ = std::fs::create_dir_all(&cfg.data_dir);
        let db = DbConnection::open(cfg.data_dir.join("db"))?;
        setup_db(&db)?;
        Ok(Self {
            db,
            caches: DbCaches::default(),

            zplc_resolver: ZplcResolver {
                base: cfg.zplc_base.clone(),
            },
            backlinks_counter: MonotonicCounter::new("backlinks"),
            tokio_rt: tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()?,
        })
    }
}

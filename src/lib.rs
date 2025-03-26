use std::path::PathBuf;

use anyhow::Result;
use counter::MonotonicCounter;
use db::{setup_db, DbCaches, DbConnection};
use tokio::runtime::{Handle, Runtime};
use uuid::Uuid;
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
    pub node_id: Uuid,

    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub db: DbConnection,
    pub caches: DbCaches,

    pub zplc_resolver: ZplcResolver,
    pub backlinks_counter: MonotonicCounter,

    owned_tokio_rt: Option<Box<tokio::runtime::Runtime>>,
    pub async_handle: Handle,
}
impl AppContext {
    pub fn new(cfg: &AppConfig) -> Result<Self> {
        Self::new_with_runtime(
            cfg,
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()?,
        )
    }

    pub fn new_with_runtime(cfg: &AppConfig, runtime: Runtime) -> Result<Self> {
        let handle = runtime.handle().clone();
        let mut ctx = Self::new_with_handle(cfg, handle)?;
        ctx.owned_tokio_rt = Some(Box::new(runtime));
        Ok(ctx)
    }

    pub fn new_with_handle(cfg: &AppConfig, async_handle: Handle) -> Result<Self> {
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

            zplc_resolver: ZplcResolver {
                base: cfg.zplc_base.clone(),
            },
            backlinks_counter: MonotonicCounter::new("backlinks"),
            owned_tokio_rt: None,
            async_handle,
        })
    }

    pub fn connect_to_db(&self) -> Result<DbConnection> {
        Ok(DbConnection::open(&self.db_path)?)
    }
}

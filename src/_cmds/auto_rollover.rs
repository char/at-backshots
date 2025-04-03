use std::{path::Path, time::Duration};

use anyhow::Result;
use backshots::{get_app_config, storage::live_guards::LiveWriteHandle, AppContext};

pub fn get_size(store_dir: &Path) -> Result<u64> {
    let index_size = store_dir
        .join("index.dat")
        .metadata()
        .map(|m| m.len())
        .unwrap_or_default();
    let links_size = store_dir
        .join("links.dat")
        .metadata()
        .map(|m| m.len())
        .unwrap_or_default();

    Ok(index_size + links_size)
}

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let app = AppContext::new(&cfg)?;

    loop {
        let store: String = app.db.query_row(
            "SELECT name FROM data_stores WHERE type = 'live' ORDER BY id DESC LIMIT 1",
            (),
            |row| row.get(0),
        )?;
        let store_dir = cfg.data_dir.join("live").join(&store);
        if get_size(&store_dir)? >= 2_147_483_648 {
            println!("rolling over from {store}");
            LiveWriteHandle::add_new(&app)?;
            std::thread::sleep(Duration::from_millis(10_000));
            continue;
        }

        std::thread::sleep(Duration::from_millis(1000));
    }
}

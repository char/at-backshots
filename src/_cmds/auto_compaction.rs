use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use backshots::{
    db::{setup_db, DbConnection},
    get_app_config,
    storage::{compacted::CompactedStorageWriter, live::LiveStorageReader},
    AppConfig,
};
use indicatif::ProgressBar;
use nix::{libc::pid_t, sys::signal::kill};

fn compact_oldest_live_store(cfg: &AppConfig, db: &DbConnection) -> Result<()> {
    let store: String = db.query_row(
        "SELECT name FROM data_stores WHERE type = 'live' ORDER BY id ASC LIMIT 1",
        (),
        |row| row.get(0),
    )?;

    let store_dir = cfg.data_dir.join("live").join(&store);

    let total_size = {
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
        index_size + links_size
    };
    if total_size < 2_147_483_648 {
        anyhow::bail!("not big enough yet ({total_size} bytes)");
    }

    for file in std::fs::read_dir(&store_dir)?.filter_map(Result::ok) {
        let Ok(file_name) = file.file_name().into_string() else {
            continue;
        };
        let Some(pid) = file_name.strip_suffix(".pid") else {
            continue;
        };
        let pid: pid_t = pid.parse()?;
        let pid = nix::unistd::Pid::from_raw(pid);
        let proc_running = !matches!(kill(pid, None), Err(nix::errno::Errno::ESRCH));
        if proc_running {
            anyhow::bail!("process with pid {pid} is still running!")
        }
    }

    let mut reader = LiveStorageReader::new(store_dir)?;
    println!("reading index from {store}…");
    let targets = reader.list_all_targets()?;
    println!("compacting {} targets…", targets.len());

    let compacted_store_dir = cfg.data_dir.join("compacted").join(&store);
    let mut writer = CompactedStorageWriter::new(compacted_store_dir)?;

    let pb = ProgressBar::new(targets.len() as u64);
    for (target, index_entry) in targets {
        let mut sources = BTreeSet::new();
        reader.read_backlinks_from_index_entry(&index_entry, &mut sources)?;
        writer.log_backlinks(&target, &sources)?;
        pb.inc(1);
    }
    pb.finish();

    let db = rusqlite::Connection::open(cfg.data_dir.join("db"))?;
    db.execute(
        "UPDATE data_stores SET type = 'compacted' WHERE name = ?",
        [store],
    )?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let db = rusqlite::Connection::open(cfg.data_dir.join("db"))?;
    setup_db(&db)?;

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.store(true, Ordering::Relaxed);
        });
    }

    while !shutdown.load(Ordering::Relaxed) {
        match tokio::task::block_in_place(|| compact_oldest_live_store(&cfg, &db)) {
            Ok(()) => {
                println!("done!");
                tokio::time::sleep(Duration::from_millis(10_000)).await;
            }
            Err(e) => {
                eprintln!("{e:?}");
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    }

    Ok(())
}

use std::collections::BTreeSet;

use anyhow::Result;
use backshots::{
    db::setup_db,
    get_app_config,
    storage::{compacted::CompactedStorageWriter, live::LiveStorageReader},
};
use indicatif::ProgressBar;

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let target = std::env::args()
        .nth(1)
        .expect("please provide name to compress"); // todo: probably should get some real option parsing

    let target = if &target == "oldest" {
        let db = rusqlite::Connection::open(cfg.data_dir.join("db"))?;
        setup_db(&db)?;
        db.query_row(
            "SELECT name FROM data_stores WHERE type = 'live' ORDER BY id ASC LIMIT 1",
            (),
            |row| row.get(0),
        )?
    } else {
        target
    };

    let mut reader = LiveStorageReader::new(cfg.data_dir.join("live").join(&target))?;
    println!("reading index from {target}…");
    let targets = reader.list_all_targets()?;
    println!("compacting {} targets…", targets.len());

    let mut writer = CompactedStorageWriter::new(cfg.data_dir.join("compacted").join(&target))?;

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
        [target],
    )?;

    Ok(())
}

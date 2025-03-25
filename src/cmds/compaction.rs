use std::collections::BTreeSet;

use anyhow::Result;
use backshots::{
    get_app_config,
    storage::{compacted::CompactedStorageWriter, live::LiveStorageReader},
};

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let target = std::env::args()
        .nth(1)
        .expect("please provide name to compress"); // todo: probably should get some real option parsing

    let mut reader = LiveStorageReader::new(cfg.data_dir.join("live").join(&target))?;
    println!("reading index…");
    let targets = reader.list_all_targets()?;
    println!("compacting {} targets…", targets.len());

    let mut writer = CompactedStorageWriter::new(cfg.data_dir.join("compacted").join(&target))?;
    for (target, index_entry) in targets {
        let mut sources = BTreeSet::new();
        reader.read_backlinks_from_index_entry(&index_entry, &mut sources)?;
        writer.log_backlinks(&target, &sources)?;
    }

    Ok(())
}

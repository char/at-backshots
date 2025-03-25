use crate::{
    car::read_car_v1,
    mst::{MSTNode, SignedCommitNode},
    storage::live::LiveStorageWriter,
    AppContext,
};
use anyhow::Result;
use ipld_core::cid::Cid;
use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Seek},
};

use super::carslice::handle_carslice;

pub fn ingest_repo_archive<R: Read + Seek>(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    repo: String,
    reader: &mut R,
) -> Result<()> {
    let car = read_car_v1(reader)?;

    let commit_cid = car.roots.first().copied().unwrap();
    let commit = car.read_block(reader, &commit_cid)?;
    let commit = serde_ipld_dagcbor::from_slice::<SignedCommitNode>(&commit)?;

    let mut entry_queue = VecDeque::<(Cid, bool)>::new();
    entry_queue.push_back((commit.node.data, false));

    let mut records = Vec::new();

    while let Some((ptr, left_visited)) = entry_queue.pop_front() {
        let buf = car.read_block(reader, &ptr)?;
        let mst_node = serde_ipld_dagcbor::from_slice::<MSTNode>(&buf)?;

        if !left_visited {
            if let Some(left) = mst_node.l {
                entry_queue.push_front((ptr, true));
                entry_queue.push_front((left, false));
                continue;
            }
        }

        let mut last_key = String::new();
        for entry in mst_node.e {
            last_key.truncate(entry.p as usize);
            last_key.push_str(std::str::from_utf8(&entry.k)?);

            let key = last_key.clone();
            records.push((key, entry.v));

            if let Some(t) = entry.t {
                entry_queue.push_front((t, false));
            }
        }
    }

    let records = records
        .into_iter()
        .map(|(path, cid)| (cid, path))
        .collect::<BTreeMap<_, _>>();

    handle_carslice(app, storage, repo, reader, &car, &records)?;

    Ok(())
}

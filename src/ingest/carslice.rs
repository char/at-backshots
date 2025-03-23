use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use ipld_core::{cid::Cid, ipld::Ipld};
use tokio::io::{AsyncRead, AsyncSeek};

use crate::{car::CarFile, storage::BacklinkStorage, AppState};

use super::common::handle_backlinks;

pub async fn handle_carslice<R: AsyncRead + AsyncSeek + Unpin>(
    app: &AppState,
    storage: &mut BacklinkStorage,
    repo: String,
    reader: &mut R,
    car_file: &CarFile,
    records: &BTreeMap<Cid, String>,
) -> Result<()> {
    for (cid, path) in records {
        let Some((collection, rkey)) = path.split_once('/') else {
            continue;
        };

        let cbor = match car_file.read_block(reader, cid).await {
            Ok(cbor) => cbor,
            Err(e) => {
                tracing::warn!(%repo, %path, "skipping record (could not read carslice block): {e:?}");
                continue;
            }
        };

        let ipld = serde_ipld_dagcbor::from_slice::<Ipld>(&cbor)?;

        let mut backlinks = HashSet::<(&str, &str)>::new();

        for child in ipld.iter() {
            // a StrongRef is an Ipld::Map with "cid" and "uri"
            let Ipld::Map(map) = child else {
                continue;
            };
            if let (Some(Ipld::String(cid)), Some(Ipld::String(uri))) =
                (map.get("cid"), map.get("uri"))
            {
                backlinks.insert((cid, uri));
            }
        }

        handle_backlinks(app, storage, &repo, collection, rkey, backlinks).await?;
    }

    app.flush_backlink_count(&app.db())?;

    Ok(())
}

use std::str::FromStr;

use anyhow::{anyhow, Result};
use tinyjson::JsonValue;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::{data::record::RecordId, storage::BacklinkStorage, AppState};

#[derive(Debug)]
pub enum Action {
    Create(CreateEntry),
    Delete(DeleteEntry),
}

#[derive(Debug)]
pub struct CreateEntry {
    pub did: String,
    pub rkey: String,
    pub uri: String,
}

#[derive(Debug)]
pub struct DeleteEntry {
    pub did: String,
    pub rkey: String,
}

impl FromStr for Action {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let parsed: JsonValue = s.parse()?;
        let entry = <Vec<_>>::try_from(parsed)?;
        if entry.len() != 4 {
            panic!("expected entries of length 4");
        }
        let action = String::try_from(entry[0].clone())?;
        let did = String::try_from(entry[1].clone())?;
        let rkey = String::try_from(entry[2].clone())?;
        match action.as_str() {
            "c" => {
                let uri = String::try_from(entry[3].clone())?;
                Ok(Action::Create(CreateEntry { did, rkey, uri }))
            }
            "d" => Ok(Action::Delete(DeleteEntry { did, rkey })),
            _ => Err(anyhow!("need 'c' or 'd' for entry action type")),
        }
    }
}

pub async fn ingest_json(app: &AppState, mut storage: BacklinkStorage) -> Result<()> {
    let f = tokio::fs::File::open("./target/likes5-simple.jsonl").await?;
    let reader = BufReader::new(f);
    let mut lines = reader.lines();

    let mut line_count = 0;
    while let Some(line) = lines.next_line().await? {
        let action: Action = line.parse()?;
        let action = match action {
            Action::Create(c) => c,
            Action::Delete(_) => continue,
        };

        let source = RecordId::new(
            app.encode_did(&action.did).await?,
            app.encode_collection("app.bsky.feed.like")?,
            app.encode_rkey(&action.rkey)?,
        );
        let source_display = format!("at://{}/app.bsky.feed.like/{}", &action.did, &action.rkey);
        let target = RecordId::from_at_uri(app, &action.uri).await?;
        storage.write_backlink(&target, &source)?;
        app.incr_backlink_count(1)?;

        if line_count % 4096 == 0 {
            app.flush_backlink_count(&app.db())?;
        }

        line_count += 1;
        tracing::debug!(from = source_display, to = action.uri, "backlink");
    }

    app.flush_backlink_count(&app.db())?;
    Ok(())
}

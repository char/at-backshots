use std::{
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

use anyhow::{anyhow, Result};
use tinyjson::JsonValue;

use crate::{
    data::{
        did::encode_did,
        record::{encode_collection, encode_rkey, RecordId},
    },
    storage::live_writer::LiveStorageWriter,
    AppContext,
};

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

pub fn ingest_json(app: &mut AppContext, mut storage: LiveStorageWriter) -> Result<()> {
    let f = File::open("./target/likes5-simple.jsonl")?;
    let reader = BufReader::new(f);

    let mut line_count = 0;
    for line in reader.lines() {
        let line = line?;

        let action: Action = line.parse()?;
        let action = match action {
            Action::Create(c) => c,
            Action::Delete(_) => continue,
        };

        let source = RecordId::new(
            encode_did(app, &action.did)?,
            encode_collection(app, "app.bsky.feed.like")?,
            encode_rkey(app, &action.rkey)?,
        );
        let source_display = format!("at://{}/app.bsky.feed.like/{}", &action.did, &action.rkey);
        let target = RecordId::from_at_uri(app, &action.uri)?;
        storage.log_backlink(&target, &source)?;
        app.backlinks_counter.add(1);

        if line_count % 4096 == 0 {
            app.backlinks_counter.flush(&app.db)?;
        }

        line_count += 1;
        tracing::debug!(from = source_display, to = action.uri, "backlink");
    }

    app.backlinks_counter.flush(&app.db)?;
    Ok(())
}

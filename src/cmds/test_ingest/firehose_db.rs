use anyhow::Result;
use backshots::{
    data::at_uri::parse_at_uri,
    ingest::{common::handle_backlinks, record::get_backlinks},
    storage::live::LiveStorageWriter,
    AppContext,
};
use ipld_core::ipld::Ipld;

pub fn ingest_firehose_db(app: &mut AppContext, storage: &mut LiveStorageWriter) -> Result<()> {
    let snapshot = rusqlite::Connection::open_with_flags(
        "./target/firehose_snapshot.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut stmt = snapshot.prepare("SELECT seq, aturi, value FROM records")?;
    let mut i = 0;
    for row in stmt.query(())?.mapped(|row| {
        let seq = row.get::<_, u64>(0)?;
        let aturi = row.get::<_, String>(1);
        let value = row.get_ref(2)?.as_blob()?;
        let value = serde_ipld_dagcbor::from_slice::<Ipld>(value);
        Ok((seq, aturi, value))
    }) {
        let (seq, at_uri, ipld) = row?;
        let at_uri = match at_uri {
            Ok(at_uri) => at_uri,
            Err(e) => {
                tracing::warn!(?seq, ?e, "got invalid text in at_uri column");
                continue;
            }
        };
        let ipld = match ipld {
            Ok(ipld) => ipld,
            Err(e) => {
                tracing::warn!(?at_uri, ?e, "got invalid CBOR in value column");
                continue;
            }
        };

        let Ok((repo, collection, rkey)) = parse_at_uri(&at_uri) else {
            continue;
        };

        let backlinks = get_backlinks(&ipld)?;
        let _ = handle_backlinks(app, storage, repo, collection, rkey, backlinks);

        i += 1;
        if i % 4096 == 0 {
            let _ = app.backlinks_counter.flush(&app.db);
            i = 0;
        }
    }

    Ok(())
}

# backshots

backlink aggregator for atproto focusing on compact representation of historical data

the schema for how backlink data is stored is currently subject to change.
don't run this just yet!

## requirements

- an up-to-date [zplc-server](https://github.com/char/zplc-server) as a sibling directory
  - backshots now directly accesses the database file; so you only need to ingest, not serve.
  - for backfill, you will want to run serve-plc.ts at 127.0.0.1:

## goals

- eventually operate a full backfill of the network
  - we will support ingesting carslices from `com.atproto.sync.getRepo` calls
  - will support firehose catch up
- small storage footprint
  - throw away almost all data given to us by the firehose
  - we use the zplc scheme for dids where possible (did:plc storage in 64 bits)
  - rkeys at or under 13 chars are inlined, otherwise stored in a table
  - we store data in a big tangle of linked lists in a flatfile, there is no extra indexing
    - data grows linearly with number of backlinks
    - a backlink source takes up 32 bytes and a backlink target takes up 40 bytes

## todo

- store record-to-identity links (i.e. `"subject": did`)

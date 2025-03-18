# backshots

backlink aggregator for atproto focusing on compact representation of historical data

## requirements

- zplc server listening at 127.0.0.1:2485

## goals

- eventually operate a full backfill of the network
  - we will support ingesting carslices from `com.atproto.sync.getRepo` calls
  - will support firehose catch up
- small storage footprint
  - throw away almost all data given to us by the firehose
  - we use the zplc scheme for dids where possible (did:plc storage in 64 bits)
  - rkeys under 15 chars are inlined, otherwise stored in a table
  - a backlink takes up 32 bytes

CREATE TABLE IF NOT EXISTS repos (
  -- since dids are sparse theyre a bad rowid
  id INTEGER PRIMARY KEY NOT NULL,
  did INTEGER UNIQUE NOT NULL, -- fk to zplc or db/outline_dids
  rev TEXT DEFAULT NULL,
  updated REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
  status TEXT NOT NULL DEFAULT 'outdated' -- 'outdated' | 'processing' | 'done' | 'errored'
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_repos_did ON repos (did);
CREATE INDEX IF NOT EXISTS idx_repos_updated ON REPOS (updated);

CREATE TABLE IF NOT EXISTS event_queue (
  id INTEGER PRIMARY KEY NOT NULL,
  did TEXT NOT NULL,
  event BLOB NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_event_queue_did ON event_queue (did);

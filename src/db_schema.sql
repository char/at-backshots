-- ran every time the db is loaded

CREATE TABLE IF NOT EXISTS counts (
  key TEXT NOT NULL PRIMARY KEY UNIQUE,
  count INTEGER NOT NULL
) STRICT;
INSERT OR IGNORE INTO counts (key, count) VALUES ('backlinks', 0);
CREATE TABLE IF NOT EXISTS outline_rkeys (
  id INTEGER PRIMARY KEY,
  rkey TEXT UNIQUE NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS outline_dids (
  id INTEGER PRIMARY KEY,
  did TEXT UNIQUE NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS collections (
  id INTEGER PRIMARY KEY,
  collection TEXT UNIQUE NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS data_stores (
  id INTEGER PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  compaction_in_progress INT DEFAULT 0,
  type TEXT NOT NULL -- 'live' | 'compacting' | 'compacted'
) STRICT;

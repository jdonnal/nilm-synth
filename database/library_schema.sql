PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS loads (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    stream TEXT NOT NULL,
    appliance_type TEXT NOT NULL,
    image TEXT
);
CREATE TABLE IF NOT EXISTS exemplars (
    on_start INTEGER NOT NULL,
    on_end INTEGER NOT NULL,
    ss_start INTEGER,
    ss_end INTEGER,
    off_start INTEGER NOT NULL,
    off_end INTEGER NOT NULL,
    delta_bytes BLOB NOT NULL,
    load_id INTEGER NOT NULL,
    FOREIGN KEY(load_id) REFERENCES loads(id) ON DELETE CASCADE
);
// TEST INFRASTRUCTURE — schema for the contended-writer probe table used by
// both the vitest side and the spawned child processes (kept identical by
// importing this module from both).

export const LEDGER_DDL = `
CREATE TABLE IF NOT EXISTS ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  writer TEXT NOT NULL,
  seq INTEGER NOT NULL,
  payload TEXT NOT NULL
);
`;

/** Minimal sessions/lease shape for cross-process lease scenarios. */
export const LEASE_PROBE_DDL = `
CREATE TABLE IF NOT EXISTS session_turn_leases (
	conversation_id TEXT PRIMARY KEY,
	holder TEXT NOT NULL,
	acquired_at REAL NOT NULL,
	expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY,
	parent_session_id TEXT,
	source TEXT,
	model_config TEXT,
	end_reason TEXT
);
`;

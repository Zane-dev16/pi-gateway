// pi_state/schema.ts — the declarative single source of truth for state.db.
//
// Spec: /root/pi-gateway/02-session-and-state.md
//   §2.1  "Proposed DDL for Pi Gateway state.db"  — copied VERBATIM (column
//         names/types/constraints/indexes/partial indexes exactly as stated;
//         `IF NOT EXISTS` added to every CREATE per the §3 step-1 mechanism:
//         "executescript(SCHEMA_SQL) # CREATE TABLE/INDEX IF NOT EXISTS").
//   §2.2  two-tier storage-version tracking.
//   §9    title-uniqueness partial index ensured OUTSIDE SCHEMA with dedup repair.
//
// Hermes anchors (READ-ONLY reference):
//   hermes_state_common.py:SCHEMA_SQL / SCHEMA_VERSION (=26) / FTS_STORAGE_VERSION (=1)
//   hermes_state_common.py:DEFERRED_INDEX_SQL (tier-2 indexes)
//   hermes_state_schema.py:_ensure_fts_schema (external-content FTS + gated triggers)
//
// Additive change = add a line to SCHEMA_SQL; reconcile does the rest forever (02 §3).
// Destructive change = explicit versioned migration, never reconcile.

/**
 * Single-row version counter; advances freely on writable open (02 §2.2).
 * Hermes parity value: hermes_state_common.py:SCHEMA_VERSION = 26.
 */
export const SCHEMA_VERSION = 26;

/**
 * FTS storage-layout version tracked independently in
 * state_meta['fts_storage_version'] (02 §2.2). 1 = external-content layout.
 * Hermes parity: hermes_state_common.py:FTS_STORAGE_VERSION = 1.
 */
export const FTS_STORAGE_VERSION = 1;

/** state_meta keys for the crash-safe FTS rebuild bookkeeping (02 §2.1). */
export const FTS_REBUILD_HIGH_WATER_KEY = "fts_rebuild_high_water";
export const FTS_REBUILD_PROGRESS_KEY = "fts_rebuild_progress";
export const FTS_STORAGE_VERSION_KEY = "fts_storage_version";

/**
 * Tier-1 schema: tables + indexes that are safe inside executescript at create
 * time (02 §2.1). Tier 2 lives in DEFERRED_INDEX_SQL and is created AFTER
 * reconcile because those indexes reference reconciled-in columns.
 *
 * NOTE ON EXECUTION GROUPING: on a legacy store whose `sessions`/`messages`
 * tables predate some declared columns, creating a tier-1 index BEFORE column
 * reconcile would fail with "no such column" and permanently block opens. So
 * SCHEMA_TABLES_SQL (tables only) executes first, reconcile adds any missing
 * columns, and SCHEMA_TIER1_INDEXES_SQL lands afterwards (all statements are
 * IF NOT EXISTS-idempotent). SCHEMA_SQL remains the full concatenation for
 * fresh stores. This realizes §3's own contract — "additive change = add a
 * line to SCHEMA; reconcile does the rest forever" — on legacy DBs too.
 *
 * The rebuild-gating predicate used by the FTS triggers lives here as a shared
 * fragment: a row belongs in an index iff
 *   id > COALESCE(high_water,-1) OR id <= COALESCE(progress,-1)
 * where absent keys ⇒ tautology (COALESCE defaults admit everything above -1).
 * Firing an external-content 'delete' for a rowid the index never held corrupts
 * it — the gating makes chunked backfills crash-safe (02 §2.1, §2.2).
 */
export const SCHEMA_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS schema_version      (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS state_meta          (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS system_prompts      (hash TEXT PRIMARY KEY, prompt TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,               -- 'cli' | 'telegram' | ... arrival surface
  session_key TEXT,                   -- routing key at creation (see 02 §4)
  user_id TEXT, chat_id TEXT, chat_type TEXT, thread_id TEXT,
  display_name TEXT, origin_json TEXT,-- adapter SessionSource snapshot
  model TEXT, model_config TEXT,      -- JSON: overrides, _reset_from/_branched_from markers
  system_prompt_hash TEXT REFERENCES system_prompts(hash),
  parent_session_id TEXT REFERENCES sessions(id),
  started_at REAL NOT NULL, ended_at REAL, end_reason TEXT,
  message_count INTEGER DEFAULT 0, tool_call_count INTEGER DEFAULT 0,
  api_call_count INTEGER DEFAULT 0,
  input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
  cache_read_tokens INTEGER DEFAULT 0, cache_write_tokens INTEGER DEFAULT 0,
  reasoning_tokens INTEGER DEFAULT 0,
  estimated_cost_usd REAL, actual_cost_usd REAL,
  cost_status TEXT, cost_source TEXT, pricing_version TEXT,
  billing_provider TEXT, billing_base_url TEXT, billing_mode TEXT,
  cwd TEXT, git_branch TEXT, git_repo_root TEXT,
  title TEXT, title_source TEXT,      -- provenance rank: derived < llm < user
  last_activity_at REAL, last_activity_description TEXT,
  handoff_state TEXT, handoff_platform TEXT, handoff_error TEXT,
  compression_failure_cooldown_until REAL,
  compression_fallback_streak INTEGER NOT NULL DEFAULT 0,
  profile_name TEXT, rewind_count INTEGER NOT NULL DEFAULT 0,
  archived INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0,
  hidden INTEGER NOT NULL DEFAULT 0   -- bot chats, cron sessions: addressable, unlisted
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  role TEXT NOT NULL,                 -- user|assistant|tool|system
  content TEXT,                       -- clean display/persisted form
  api_content TEXT,                   -- EXACT bytes sent to API when they differ
  tool_call_id TEXT, tool_calls TEXT, tool_name TEXT,
  effect_disposition TEXT, finish_reason TEXT, token_count INTEGER,
  reasoning TEXT, reasoning_content TEXT, reasoning_details TEXT,
  codex_reasoning_items TEXT, codex_message_items TEXT,
  platform_message_id TEXT, observed INTEGER DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1,  -- 0 = compacted away from replay head
  compacted INTEGER NOT NULL DEFAULT 0,
  timestamp REAL NOT NULL,
  display_kind TEXT, display_metadata TEXT
);

CREATE TABLE IF NOT EXISTS session_model_usage (
  session_id TEXT REFERENCES sessions(id) ON DELETE CASCADE,
  model TEXT NOT NULL, billing_provider TEXT NOT NULL DEFAULT '',
  billing_base_url TEXT NOT NULL DEFAULT '', billing_mode TEXT NOT NULL DEFAULT '',
  task TEXT NOT NULL DEFAULT '',
  api_call_count INTEGER NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens INTEGER NOT NULL DEFAULT 0, cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens INTEGER NOT NULL DEFAULT 0,
  estimated_cost_usd REAL NOT NULL DEFAULT 0, actual_cost_usd REAL NOT NULL DEFAULT 0,
  first_seen REAL, last_seen REAL,
  PRIMARY KEY (session_id, model, billing_provider, billing_base_url, billing_mode, task)
);

CREATE TABLE IF NOT EXISTS gateway_routing (
  scope TEXT NOT NULL DEFAULT '', session_key TEXT NOT NULL,
  entry_json TEXT NOT NULL, updated_at REAL NOT NULL,
  PRIMARY KEY (scope, session_key)
);
CREATE TABLE IF NOT EXISTS gateway_hygiene_state (session_key TEXT PRIMARY KEY, failure_streak INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS compression_locks (session_id TEXT PRIMARY KEY, holder TEXT NOT NULL,
                                acquired_at REAL NOT NULL, expires_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS session_turn_leases (conversation_id TEXT PRIMARY KEY, holder TEXT NOT NULL,
                                  acquired_at REAL NOT NULL, expires_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS async_delegations (
  delegation_id TEXT PRIMARY KEY,
  origin_session TEXT NOT NULL, parent_session_id TEXT,
  state TEXT NOT NULL, dispatched_at REAL NOT NULL,
  completed_at REAL, updated_at REAL NOT NULL,
  event_json TEXT, result_json TEXT, task_json TEXT,
  delivery_state TEXT NOT NULL DEFAULT 'pending',
  delivery_attempts INTEGER NOT NULL DEFAULT 0, delivered_at REAL,
  owner_pid INTEGER, owner_started_at INTEGER,
  delivery_claim TEXT, delivery_claimed_at REAL
);

CREATE TABLE IF NOT EXISTS delivery_obligations (
  obligation_id TEXT PRIMARY KEY,
  session_key TEXT NOT NULL, platform TEXT NOT NULL,
  chat_id TEXT NOT NULL, thread_id TEXT, content TEXT NOT NULL,
  state TEXT NOT NULL,                -- pending|attempting|delivered|failed|abandoned
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL, updated_at REAL NOT NULL,
  owner_pid INTEGER, owner_started_at INTEGER, last_error TEXT
);

-- End of tier-1 TABLE declarations.
`;

/**
 * Tier-1 indexes — created after tables AND after column reconcile so they
 * never reference a column a legacy store has not yet reconciled in.
 */
export const SCHEMA_TIER1_INDEXES_SQL = `
-- Tier 1 indexes (safe at create time — 02 §2.1).
CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
CREATE INDEX IF NOT EXISTS idx_sessions_source_id ON sessions(source, id);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id, id);
CREATE INDEX IF NOT EXISTS idx_messages_assistant_calls_by_session
  ON messages(session_id) WHERE role='assistant' AND tool_calls IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_compression_locks_expires ON compression_locks(expires_at);
CREATE INDEX IF NOT EXISTS idx_turn_leases_expires ON session_turn_leases(expires_at);
CREATE INDEX IF NOT EXISTS idx_session_model_usage_session ON session_model_usage(session_id);
CREATE INDEX IF NOT EXISTS idx_session_model_usage_model ON session_model_usage(model);
CREATE INDEX IF NOT EXISTS idx_async_delegations_delivery ON async_delegations(delivery_state, completed_at);
`;

/** Full tier-1 constant (fresh-store path): tables + their indexes. */
export const SCHEMA_SQL = SCHEMA_TABLES_SQL + SCHEMA_TIER1_INDEXES_SQL;

/**
 * Tier-2 indexes — created AFTER column reconcile; they reference reconciled-in
 * columns (02 §2.1 "Tier 2", §3 step 3). Parity: DEFERRED_INDEX_SQL.
 */
export const DEFERRED_INDEX_SQL = `
CREATE INDEX IF NOT EXISTS idx_messages_session_active ON messages(session_id, active, timestamp);
CREATE INDEX IF NOT EXISTS idx_sessions_session_key ON sessions(session_key, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_gateway_peer
  ON sessions(source, user_id, chat_id, chat_type, thread_id, started_at DESC);
`;

/**
 * Title uniqueness is a PARTIAL unique index enforced outside SCHEMA at every
 * open, with self-healing dedup repair (02 §9; DEC-007). Never aborts an open.
 */
export const TITLE_UNIQUE_INDEX_SQL =
	"CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_title_unique " +
	"ON sessions(title) WHERE title IS NOT NULL";

/**
 * Optional CJK source VIEW (02 §2.1): rows mirrored into messages_fts_cjk.
 * The cjk_unicode61 tokenizer is not part of stock SQLite builds, so both
 * objects are created best-effort behind an availability probe — failure logs
 * once and continues without CJK search (never aborts an open, §9 rule).
 */
export const FTS_CJK_VIEW_SQL =
	"CREATE VIEW IF NOT EXISTS messages_fts_cjk_src AS " +
	"SELECT id, content, tool_name, tool_calls FROM messages WHERE role <> 'tool'";
export const FTS_CJK_TABLE_SQL =
	"CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts_cjk USING fts5(" +
	"content, tool_name, tool_calls, content='messages_fts_cjk_src', content_rowid='id', " +
	"tokenize='cjk_unicode61')";

interface GatedFtsSpec {
	table: string;
}

const FTS_TABLES: readonly GatedFtsSpec[] = [
	{ table: "messages_fts" },
	{ table: "messages_fts_trigram" },
];

/**
 * Rebuild-predicate fragment (02 §2.1). Parameterized by NEW/OLD so insert and
 * delete trigger arms reference the right row. Absent keys ⇒ tautology.
 */
function ftsGate(rowRef: "new" | "old"): string {
	const r = rowRef;
	const hw = `(SELECT CAST(value AS INTEGER) FROM state_meta WHERE key = '${FTS_REBUILD_HIGH_WATER_KEY}')`;
	const prog = `(SELECT CAST(value AS INTEGER) FROM state_meta WHERE key = '${FTS_REBUILD_PROGRESS_KEY}')`;
	return `(${r}.id > COALESCE(${hw}, -1) OR ${r}.id <= COALESCE(${prog}, -1))`;
}

function ftsTriggersFor(table: string): string {
	const cols = "content, tool_name, tool_calls";
	const colRefsNew = `new.id, new.content, new.tool_name, new.tool_calls`;
	const colRefsOld = `old.id, old.content, old.tool_name, old.tool_calls`;
	return `
CREATE TRIGGER IF NOT EXISTS ${table}_ai AFTER INSERT ON messages BEGIN
  INSERT INTO ${table}(rowid, ${cols})
  SELECT ${colRefsNew}
  WHERE new.role <> 'tool' AND ${ftsGate("new")};
END;
CREATE TRIGGER IF NOT EXISTS ${table}_ad AFTER DELETE ON messages BEGIN
  INSERT INTO ${table}(${table}, rowid, ${cols})
  SELECT 'delete', ${colRefsOld}
  WHERE old.role <> 'tool' AND ${ftsGate("old")};
END;
CREATE TRIGGER IF NOT EXISTS ${table}_au AFTER UPDATE OF content, tool_name, tool_calls, role ON messages BEGIN
  INSERT INTO ${table}(${table}, rowid, ${cols})
  SELECT 'delete', ${colRefsOld}
  WHERE old.role <> 'tool' AND ${ftsGate("old")};
  INSERT INTO ${table}(rowid, ${cols})
  SELECT ${colRefsNew}
  WHERE new.role <> 'tool' AND ${ftsGate("new")};
END;
`;
}

/**
 * External-content FTS5 layout, Hermes storage-layout version 1 (02 §2.1):
 * messages_fts + messages_fts_trigram (+ optional CJK over a VIEW), each with
 * AFTER INSERT/DELETE/UPDATE triggers gated by the rebuild predicate. Tool
 * rows are excluded via `role <> 'tool'`.
 */
export function buildFtsDdl(): string {
	let ddl = `
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  content, tool_name, tool_calls, content='messages', content_rowid='id');
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts_trigram USING fts5(
  content, tool_name, tool_calls, content='messages', content_rowid='id',
  tokenize='trigram');
`;
	for (const spec of FTS_TABLES) ddl += ftsTriggersFor(spec.table);
	return ddl;
}

/** Trigger names owned by an FTS table (drop order during version-gated rebuilds). */
export function ftsTriggerNames(table: string): string[] {
	return [`${table}_ai`, `${table}_ad`, `${table}_au`];
}

// ---------------------------------------------------------------------------
// Declared-column derivation (reconcile diff + read-probe statements, 02 §3)
// ---------------------------------------------------------------------------

export interface TableColumns {
	readonly table: string;
	readonly columns: ReadonlyMap<string, string>;
}

const TABLE_CONSTRAINT_KEYWORDS = new Set([
	"PRIMARY",
	"UNIQUE",
	"CHECK",
	"FOREIGN",
	"CONSTRAINT",
]);

function stripSqlComments(sql: string): string {
	// Line comments then block comments; adequate for our own authored SCHEMA
	// constants (parity of intent with hermes_state_schema.py:_parse_schema_columns,
	// which likewise parses its own constant, never arbitrary SQL).
	return sql.replace(/--[^\n]*/g, "").replace(/\/\*[\s\S]*?\*\//g, "");
}

/**
 * Derive {table → {column → declared type}} from our SCHEMA SQL — auto-derived
 * so read-probes can't go stale (02 §3: a hand-maintained probe list went
 * stale within days in Hermes). Parses only the constants in this module.
 */
export function parseSchemaTables(sqlInput: string): TableColumns[] {
	const sql = stripSqlComments(sqlInput);
	const tables: TableColumns[] = [];
	const createRe =
		/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["`]?([A-Za-z_][A-Za-z0-9_]*)["`]?\s*\(/gi;
	let match: RegExpExecArray | null;
	while ((match = createRe.exec(sql)) !== null) {
		const tableName = match[1]!;
		const bodyStart = match.index + match[0].length;
		let depth = 1;
		let i = bodyStart;
		while (i < sql.length && depth > 0) {
			const ch = sql[i];
			if (ch === "(") depth++;
			else if (ch === ")") depth--;
			i++;
		}
		if (depth !== 0) break; // malformed constant — programmer error
		const body = sql.slice(bodyStart, i - 1);
		const columns = new Map<string, string>();
		let parenDepth = 0;
		let current = "";
		const parts: string[] = [];
		for (const ch of body) {
			if (ch === "(") parenDepth++;
			if (ch === ")") parenDepth--;
			if (ch === "," && parenDepth === 0) {
				parts.push(current.trim());
				current = "";
			} else {
				current += ch;
			}
		}
		if (current.trim() !== "") parts.push(current.trim());
		for (const part of parts) {
			const nameMatch = /^["`]?([A-Za-z_][A-Za-z0-9_]*)["`]?\s*([\s\S]*)$/.exec(
				part,
			);
			if (!nameMatch) continue;
			const firstName = nameMatch[1]!;
			const upper = firstName.toUpperCase();
			if (TABLE_CONSTRAINT_KEYWORDS.has(upper)) continue; // table constraint
			columns.set(firstName, nameMatch[2]!.trim());
		}
		tables.push({ table: tableName, columns });
	}
	return tables;
}

/** Every table declared across SCHEMA_SQL (tier-1 constant). */
export function declaredSchemaTables(): TableColumns[] {
	return parseSchemaTables(SCHEMA_SQL);
}

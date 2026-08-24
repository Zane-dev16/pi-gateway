// TEST INFRASTRUCTURE — shared kanban board harness for dispatcher contracts.
// Each test gets an isolated mkdtemp WAL database (production open path via
// pi_state openDatabase) with the dispatcher schema ensured, plus a
// ManualClock-driven SqliteKanbanBoard. NO wall-clock reads anywhere in the
// contracts: time moves only when a test advances the clock.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { openDatabase } from "../../../pi_state/wal.js";
import { ManualClock } from "./manual-clock.js";
import { SqliteKanbanBoard } from "../sqlite-board.js";

export interface KanbanHarness {
	dir: string;
	dbPath: string;
	/** Raw connection — invariant probes only; contracts go through board. */
	db: KanbanHarnessDb;
	board: SqliteKanbanBoard;
	clock: ManualClock;
	/**
	 * A SECOND independent connection to the SAME board file — used to prove
	 * cross-connection CAS behavior without spawning processes.
	 */
	openRivalBoard: () => Promise<SqliteKanbanBoard>;
	close: () => void;
}

import type Database from "better-sqlite3";
type KanbanHarnessDb = Database.Database;

export async function openKanbanHarness(
	label = "kanban-dispatch",
	board = "default",
): Promise<KanbanHarness> {
	const dir = mkdtempSync(join(tmpdir(), `pi-gw-${label}-`));
	const dbPath = join(dir, "board.db");
	const clock = new ManualClock(1_775_000_000);
	const opened = await openDatabase({ path: dbPath });
	SqliteKanbanBoard.ensureSchema(opened.db);
	const boardClient = new SqliteKanbanBoard(opened.db, { board });
	return {
		dir,
		dbPath,
		db: opened.db,
		board: boardClient,
		clock,
		openRivalBoard: async () => {
			const rival = await openDatabase({ path: dbPath });
			SqliteKanbanBoard.ensureSchema(rival.db);
			return new SqliteKanbanBoard(rival.db, { board });
		},
		close: () => {
			opened.db.close();
		},
	};
}

export { rmSync };

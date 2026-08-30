// REQUIRED CONTRACT: claim atomicity under N racers — exactly one owner.
//
// Real child PROCESSES (not threads, not fakes): each racer opens its own
// SQLite connection to the same WAL board file and races claimCard on a
// single ready card after a coordinated start. The CAS guard
// (`WHERE status='ready' AND claim_lock IS NULL` inside BEGIN IMMEDIATE)
// must yield exactly ONE winner across the fleet; every other racer must
// observe a definitive loss. This is the exactly-once claim that justifies
// real child processes under the suite's spawn budget.
//
// Completion is observed by POLLING coordination marker files
// (waitForMarker pattern from lifecycle/two-process.test.ts), NOT by
// awaiting ChildProcess 'close' events: marker files are authoritative
// state, immune to event-delivery scheduling quirks of nested fork pools.

import {
	existsSync,
	mkdtempSync,
	readFileSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { openDatabase } from "../../pi_state/wal.js";
import { SqliteKanbanBoard } from "./sqlite-board.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/claim-racer-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
let dbPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-kanban-claim-race-"));
	dbPath = join(dir, "board.db");
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

function spawnRacer(name: string, cardId: string): ChildProcess {
	return spawn(
		process.execPath,
		[
			"--import",
			RESOLVE_MJS,
			DRIVER_TS,
			"--db",
			dbPath,
			"--card-id",
			cardId,
			"--coord",
			dir,
			"--name",
			name,
		],
		{ stdio: ["ignore", "ignore", "ignore"] },
	);
}

describe("two-process claim atomicity (exactly one owner)", () => {
	it("3 concurrent OS processes race one ready card ⇒ exactly one winner, others definitively lose", {
		// child launches pay Node+TS-resolve startup under full-suite load
		// (4 CPUs); isolation runs ~0.5s — headroom against fork starvation
		// only (same disposition as obligations/two-process.test.ts).
		timeout: 120_000,
	}, async () => {
		const opened = await openDatabase({ path: dbPath });
		SqliteKanbanBoard.ensureSchema(opened.db);
		const board = new SqliteKanbanBoard(opened.db, { board: "default" });
		const card = board.createCard({ status: "ready", assignee: "worker" });

		const N = 3;
		const children: Array<{ child: ChildProcess; name: string }> = [];
		for (let i = 0; i < N; i++) {
			const name = `r${i}`;
			children.push({ child: spawnRacer(name, card.id), name });
		}
		// Starting gun: all racers are spawned and polling for this marker.
		writeFileSync(join(dir, "go"), "go");

		// Every racer publishes its terminal outcome as a marker file:
		// winner-<name> (content = its claim lock) or lost-<name> (content =
		// observed status). Poll until all N have published.
		const deadline = Date.now() + 30_000;
		for (;;) {
			const published = children.filter(
				({ name }) =>
					existsSync(join(dir, `winner-${name}`)) ||
					existsSync(join(dir, `lost-${name}`)),
			).length;
			if (published === N) break;
			if (Date.now() > deadline) {
				throw new Error(
					`only ${published}/${N} racers published outcomes within 30s`,
				);
			}
			await new Promise<void>((r) => setTimeout(r, 25));
		}

		// Reap stragglers best-effort so no child handle outlives the test.
		for (const { child } of children) {
			if (child.exitCode === null && child.signalCode === null) {
				child.kill("SIGKILL");
			}
		}
		await Promise.all(
			children.map(
				({ child }) =>
					new Promise<void>((resolvePromise) => {
						if (child.exitCode !== null || child.signalCode !== null) {
							resolvePromise();
							return;
						}
						child.once("close", () => resolvePromise());
						setTimeout(resolvePromise, 2_000);
					}),
			),
		);

		// Exactly ONE winner marker, and it names the actual lock owner.
		const markers = readdirSync(dir).filter(
			(f) => f.startsWith("winner-") || f.startsWith("lost-"),
		);
		const winners = markers.filter((f) => f.startsWith("winner-"));
		expect(winners).toHaveLength(1);
		const winnerName = winners[0]?.slice("winner-".length);

		// Every non-winner wrote an explicit loss marker (no silent hangs).
		const losers = markers.filter((f) => f.startsWith("lost-"));
		expect(losers).toHaveLength(N - 1);
		for (const l of losers) {
			expect(readFileSync(join(dir, l), "utf8")).toBe("running");
		}

		// The board itself carries the single owner.
		const final = await board.getCard(card.id);
		expect(final?.status).toBe("running");
		expect(final?.claimLock).toBe(`racer:${winnerName}`);

		// And the winner marker content agrees with the row's claim_lock.
		expect(readFileSync(join(dir, winners[0] as string), "utf8")).toBe(
			`racer:${winnerName}`,
		);
		opened.db.close();
	});
});

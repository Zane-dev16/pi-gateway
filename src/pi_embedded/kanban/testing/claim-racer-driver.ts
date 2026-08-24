// TEST INFRASTRUCTURE — child racer driver for the two-process claim
// atomicity contract. Each child opens its OWN SQLite connection to the same
// board file and hammers claimCard until it either WINS (writes
// winner-<name>) or provably loses (observes status != ready; writes
// lost-<name>). Protocol: wait for <coord>/go, then race.

import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { openDatabase } from "../../../pi_state/wal.js";
import { SqliteKanbanBoard } from "../sqlite-board.js";

interface Args {
	db: string;
	cardId: string;
	coord: string;
	name: string;
}

function parseArgs(argv: readonly string[]): Args {
	const out: Record<string, string> = {};
	for (let i = 0; i < argv.length; i += 2) {
		const key = argv[i];
		if (key?.startsWith("--")) {
			// kebab-case flag → camelCase property (--card-id → cardId)
			const prop = key
				.slice(2)
				.replace(/-([a-z])/g, (_, c: string) => c.toUpperCase());
			out[prop] = argv[i + 1] ?? "";
		}
	}
	if (!out.db || !out.cardId || !out.coord || !out.name) {
		throw new Error(
			"usage: --db <path> --card-id <id> --coord <dir> --name <racer>",
		);
	}
	return out as unknown as Args;
}

async function main(): Promise<void> {
	const args = parseArgs(process.argv.slice(2));
	// Wait for the starting gun so all racers are truly concurrent.
	const go = join(args.coord, "go");
	const deadline = Date.now() + 30_000;
	while (!existsSync(go)) {
		if (Date.now() > deadline) throw new Error("never saw go marker");
		await new Promise<void>((r) => setTimeout(r, 5));
	}

	const opened = await openDatabase({ path: args.db, busyTimeoutMs: 10_000 });
	SqliteKanbanBoard.ensureSchema(opened.db);
	const board = new SqliteKanbanBoard(opened.db, { board: "default" });

	for (let attempt = 0; attempt < 500; attempt++) {
		const won = await board.claimCard({
			cardId: args.cardId,
			lock: `racer:${args.name}`,
			expiresAt: Math.floor(Date.now() / 1000) + 600,
			nowSeconds: Math.floor(Date.now() / 1000),
		});
		if (won !== null) {
			writeFileSync(
				join(args.coord, `winner-${args.name}`),
				won.claimLock ?? "",
			);
			opened.db.close();
			return;
		}
		const current = await board.getCard(args.cardId);
		if (current === null) throw new Error(`card ${args.cardId} vanished`);
		if (current.status !== "ready") {
			writeFileSync(join(args.coord, `lost-${args.name}`), current.status);
			opened.db.close();
			return;
		}
		// Still ready but our CAS failed — transient contention; retry.
	}
	throw new Error(`racer ${args.name}: exhausted attempts without resolution`);
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});

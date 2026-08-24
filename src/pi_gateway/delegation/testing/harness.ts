// TEST INFRASTRUCTURE — shared store harness for delegation rail contracts.
// Each test gets an isolated mkdtemp StateStore (production open path: WAL
// ladder + full schema reconcile) plus a ManualClock-driven rail instance.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { StateStore } from "../../../pi_state/index.js";
import { DelegationRail } from "../rail.js";
import { ManualClock } from "./manual-clock.js";

export interface RailHarness {
	dir: string;
	dbPath: string;
	store: StateStore;
	rail: DelegationRail;
	clock: ManualClock;
	close: () => Promise<void>;
}

export async function openRailHarness(
	label = "delegation-rail",
): Promise<RailHarness> {
	const dir = mkdtempSync(join(tmpdir(), `pi-gw-${label}-`));
	const dbPath = join(dir, "state.db");
	const clock = new ManualClock(1_775_000_000); // fixed epoch seconds
	const store = await StateStore.open(dbPath);
	const rail = new DelegationRail(store.db, { clock });
	return {
		dir,
		dbPath,
		store,
		rail,
		clock,
		close: () => store.close(false),
	};
}

export { rmSync };

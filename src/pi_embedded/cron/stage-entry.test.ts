// Behavior contracts for the cron ticker's DEC-040 stage entry: native
// CronStartupResult mapped onto the shared EmbeddedServiceOutcome vocabulary
// (success ⇒ stoppable handle; failure ⇒ LOUD per-service degrade). Real
// store over an mkdtemp dir; the runner never fires (empty jobs file).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { CronJobStore, defaultCronStorePaths } from "./store.js";
import type { CronLogger } from "./logger.js";
import type { ScheduledJobRunner } from "./scheduler.js";
import {
	CRON_TICKER_SERVICE_NAME,
	cronTickerServiceEntry,
} from "./stage-entry.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-cron-stage-entry-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

const noopRunner: ScheduledJobRunner = {
	run: async () => ({ ok: true }),
	interrupt: async () => true,
};

const quietLogger: CronLogger = {
	info: () => {},
	warn: () => {},
	error: () => {},
};

describe("cronTickerServiceEntry (DEC-040 stage 7 wiring)", () => {
	it("maps a successful start onto ok:true with the cron handle", async () => {
		const entry = cronTickerServiceEntry({
			store: new CronJobStore({ paths: defaultCronStorePaths(home) }),
			runner: noopRunner,
			logger: quietLogger,
			intervalSeconds: 3600,
		});
		expect(entry.name).toBe(CRON_TICKER_SERVICE_NAME);

		const outcome = await entry.start();

		expect(outcome.ok).toBe(true);
		expect(outcome.degraded).toBeUndefined();
		expect(outcome.handle?.name).toBe("cron");
		expect(typeof outcome.handle?.stop).toBe("function");
		// Cooperative-drain input (#60432/#82161): the handle exposes the
		// scheduler's live in-flight run count for the lifecycle's own-budget
		// cron wait — idle ticker reports 0 through the mapping.
		expect(typeof outcome.handle?.inflightCount).toBe("function");
		expect(outcome.handle?.inflightCount?.()).toBe(0);
		// The handle joins deterministically — the drain contract depends on it.
		await outcome.handle?.stop?.();
	});

	it("maps construct/start failure onto ok:false + degraded:true (loud per-service degrade)", async () => {
		const explodingStore = new Proxy(
			{},
			{
				get() {
					throw new Error("store exploded");
				},
			},
		) as unknown as CronJobStore;

		const outcome = await cronTickerServiceEntry({
			store: explodingStore,
			runner: noopRunner,
			logger: quietLogger,
		}).start();

		expect(outcome.ok).toBe(false);
		expect(outcome.degraded).toBe(true);
		expect(outcome.reason).toContain("store exploded");
		expect(outcome.handle).toBeUndefined();
	});
});

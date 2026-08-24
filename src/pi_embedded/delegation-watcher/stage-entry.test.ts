// Behavior contracts for the delegation watcher's DEC-040 stage entry:
// create+start wrapped into an ok outcome with a stoppable handle; a broken
// dependency THROWS out of start() so the ENGINE classifies this service's
// loud degrade (the shim never swallows). Fake watcher — no sqlite, no clock.

import { describe, expect, it } from "vitest";
import type { DelegationWatcher } from "./watcher.js";
import {
	DELEGATION_WATCHER_SERVICE_NAME,
	delegationWatcherServiceEntry,
} from "./stage-entry.js";

interface FakeState {
	started: boolean;
	stopped: boolean;
}

function fakeWatcher(): { watcher: DelegationWatcher; state: FakeState } {
	const state: FakeState = { started: false, stopped: false };
	const watcher = {
		start: () => {
			state.started = true;
		},
		stop: async () => {
			state.stopped = true;
		},
	} as unknown as DelegationWatcher;
	return { watcher, state };
}

describe("delegationWatcherServiceEntry (DEC-040 stage 8 wiring)", () => {
	it("builds + starts the watcher and returns a stoppable handle", async () => {
		const built: string[] = [];
		const { watcher, state } = fakeWatcher();
		const entry = delegationWatcherServiceEntry({
			create: () => {
				built.push("built");
				return watcher;
			},
		});
		expect(entry.name).toBe(DELEGATION_WATCHER_SERVICE_NAME);

		const outcome = await entry.start();

		expect(built).toEqual(["built"]); // construction INSIDE start()
		expect(state.started).toBe(true);
		expect(outcome.ok).toBe(true);
		expect(outcome.handle?.name).toBe(DELEGATION_WATCHER_SERVICE_NAME);
		await outcome.handle?.stop?.();
		expect(state.stopped).toBe(true);
	});

	it("a broken dependency THROWS out of start() — engine classifies the degrade", async () => {
		const entry = delegationWatcherServiceEntry({
			create: () => {
				throw new Error("rail unwired");
			},
		});

		await expect(entry.start()).rejects.toThrow("rail unwired");
	});
});

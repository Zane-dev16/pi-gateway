// Behavior contracts for the handoff watcher's DEC-040 stage entry:
// create+start wrapped into an ok outcome with a stoppable handle; a broken
// dependency THROWS out of start() so the ENGINE classifies this service's
// loud degrade (the shim never swallows). Fake watcher — no sqlite, no clock.

import { describe, expect, it } from "vitest";
import type { HandoffWatcher } from "./watcher.js";
import {
	HANDOFF_WATCHER_SERVICE_NAME,
	handoffWatcherServiceEntry,
} from "./stage-entry.js";

interface FakeState {
	started: boolean;
	stopped: boolean;
}

function fakeWatcher(): { watcher: HandoffWatcher; state: FakeState } {
	const state: FakeState = { started: false, stopped: false };
	const watcher = {
		start: () => {
			state.started = true;
		},
		stop: async () => {
			state.stopped = true;
		},
	} as unknown as HandoffWatcher;
	return { watcher, state };
}

describe("handoffWatcherServiceEntry (DEC-040 stage 8 wiring)", () => {
	it("builds + starts the watcher and returns a stoppable handle", async () => {
		const built: string[] = [];
		const { watcher, state } = fakeWatcher();
		const entry = handoffWatcherServiceEntry({
			create: () => {
				built.push("built");
				return watcher;
			},
		});
		expect(entry.name).toBe(HANDOFF_WATCHER_SERVICE_NAME);

		const outcome = await entry.start();

		expect(built).toEqual(["built"]); // construction INSIDE start()
		expect(state.started).toBe(true);
		expect(outcome.ok).toBe(true);
		expect(outcome.handle?.name).toBe(HANDOFF_WATCHER_SERVICE_NAME);
		await outcome.handle?.stop?.();
		expect(state.stopped).toBe(true);
	});

	it("a broken dependency THROWS out of start() — engine classifies the degrade", async () => {
		const entry = handoffWatcherServiceEntry({
			create: () => {
				throw new Error("queue unwired");
			},
		});

		await expect(entry.start()).rejects.toThrow("queue unwired");
	});
});

// Behavior contracts for the embedded extensions' DEC-040 stage entry:
// startupEmbeddedExtensions never throws, so the mapped outcome is always
// ok:true — per-subsystem degradation stays INTERNAL and loud (its own
// contract). Real discovery over an mkdtemp PI_HOME override: a hook/plugin-
// free home loads nothing.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { resetPiHomeOverride, setPiHomeOverride } from "../../pi_home.js";
import { makeTempHome, type TempHome } from "./testing/fixtures.js";
import {
	EMBEDDED_EXTENSIONS_SERVICE_NAME,
	extensionsServiceEntry,
} from "./stage-entry.js";

let home: TempHome;

beforeEach(() => {
	home = makeTempHome("stage-entry-ext");
	setPiHomeOverride(home.home);
});

afterEach(() => {
	home.cleanup();
	resetPiHomeOverride();
});

describe("extensionsServiceEntry (DEC-040 stage 8 wiring)", () => {
	it("maps discovery onto a permanent ok:true outcome with NO handle", async () => {
		const lines: string[] = [];
		const entry = extensionsServiceEntry({
			log: {
				info: (m) => lines.push(m),
				warn: (m) => lines.push(m),
				error: (m) => lines.push(m),
			},
		});
		expect(entry.name).toBe(EMBEDDED_EXTENSIONS_SERVICE_NAME);

		const outcome = await entry.start();

		expect(outcome).toEqual({ ok: true });
		expect(outcome.handle).toBeUndefined();
		// The snapshot is retained for the composition root; an empty home
		// loads zero plugins and registers no hooks.
		expect(entry.lastSnapshot).not.toBeNull();
		expect(entry.lastSnapshot?.loadedPlugins).toEqual([]);
		expect(entry.lastSnapshot?.hooks.loadedHooks).toEqual([]);
	});
});

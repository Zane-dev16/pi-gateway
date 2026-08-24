// Behavior contracts for the file-drop hook registry (07-integrations.md
// §7(a); DEC-014). Every load path here is the REAL discovery scanner over an
// mkdtemp PI_HOME-style layout — no test-only backdoors, no direct fixture
// imports (frozen-plugin compat loads exactly what production loads).
//
// Contract groups:
//   1. Discovery: sorted determinism; silent skip (missing files) vs loud
//      skip (malformed members) with reasons; one broken hook never aborts
//      the scan.
//   2. Wildcard resolution: exact first, then <base>:*; base-only never fires.
//   3. Observer containment (DEC-014): a throwing/rejecting observer affects
//      neither emit's outcome nor other observers.
//   4. Collect filtering + sync/async handler support.
//   5. Frozen-plugin compat: unknown manifest fields ignored; additive payload
//      fields reach handlers unchanged.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
	resetPiHomeCacheForTests,
	resetPiHomeOverride,
	runWithPiHomeOverride,
	setPiHomeOverride,
} from "../../pi_home.js";
import { HookRegistry } from "./hook-registry.js";
import {
	captureLog,
	fixtureCalls,
	makeTempHome,
	resetFixtureCalls,
	writeHookFixture,
	type TempHome,
} from "./testing/fixtures.js";

let home: TempHome;

beforeEach(() => {
	home = makeTempHome("hooks");
	setPiHomeOverride(home.home);
	resetFixtureCalls();
});
afterEach(() => {
	home.cleanup();
	resetPiHomeOverride();
	resetPiHomeCacheForTests();
	resetFixtureCalls();
});

describe("discovery (gateway/hooks.py:discover_and_load port)", () => {
	it("sorted scan is deterministic: handlers fire in directory order", async () => {
		writeHookFixture(home.hooksDir, "zeta", { events: ["agent:start"] });
		writeHookFixture(home.hooksDir, "alpha", { events: ["agent:start"] });
		writeHookFixture(home.hooksDir, "mid", { events: ["agent:start"] });
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: captureLog().sink });
		await registry.emit("agent:start", {});
		expect(fixtureCalls()).toEqual([
			"alpha:agent:start",
			"mid:agent:start",
			"zeta:agent:start",
		]);
		expect(registry.loadedHooks.map((h) => h.name)).toEqual(
			["zeta", "alpha", "mid"].sort(),
		);
	});

	it("missing HOOK.yaml or handler → SILENT skip (parity of the both-files-exist gate)", async () => {
		const log = captureLog();
		mkdirSync(join(home.hooksDir, "no-manifest"), { recursive: true });
		writeFileSync(
			join(home.hooksDir, "no-manifest", "handler.mjs"),
			"export function handle() {}\n",
		);
		mkdirSync(join(home.hooksDir, "no-handler"), { recursive: true });
		writeFileSync(
			join(home.hooksDir, "no-handler", "HOOK.yaml"),
			"name: x\nevents:\n  - agent:start\n",
		);
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: log.sink });
		expect(registry.loadedHooks).toHaveLength(0);
		expect(log.lines).toEqual([]); // silent — no reasons printed
	});

	it("malformed members skip LOUDLY with printed reasons; siblings still load", async () => {
		const log = captureLog();
		writeHookFixture(home.hooksDir, "bad-yaml", {
			events: ["agent:start"],
			// Unterminated inline list ⇒ parse THROWS ⇒ "Error loading hook" (parity:
			// yaml.safe_load raising hits the outer per-member catch).
			manifestBody: "not: [valid\n",
			handlerSource: "export function handle() {}\n",
		});
		writeHookFixture(home.hooksDir, "empty-yaml", {
			events: ["agent:start"],
			// Comments-only document parses to null ⇒ "invalid HOOK.yaml" (parity of
			// yaml.safe_load returning None).
			manifestBody: "# nothing here\n",
			handlerSource: "export function handle() {}\n",
		});
		writeHookFixture(home.hooksDir, "no-events", {
			events: [],
			manifestBody: "name: no-events\n",
		});
		writeHookFixture(home.hooksDir, "no-handle-fn", {
			events: ["agent:start"],
			handlerSource: "export const notHandle = 1;\n",
		});
		writeHookFixture(home.hooksDir, "syntax-error", {
			events: ["agent:start"],
			handlerSource: "export function handle( {\n",
		});
		writeHookFixture(home.hooksDir, "good", { events: ["agent:start"] });

		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: log.sink });
		expect(registry.loadedHooks.map((h) => h.name)).toEqual(["good"]);
		expect(log.includes("[hooks] Skipping no-events: no events declared")).toBe(
			true,
		);
		expect(log.includes("[hooks] Skipping empty-yaml: invalid HOOK.yaml")).toBe(
			true,
		);
		expect(
			log.includes("[hooks] Skipping no-handle-fn: no 'handle' function found"),
		).toBe(true);
		expect(
			log.lines.some((l) =>
				l.startsWith("[hooks] Error loading hook syntax-error:"),
			),
		).toBe(true);
		expect(
			log.includes(`[hooks] Loaded hook 'good' for events: agent:start`),
		).toBe(true);
	});

	it("hook-free deployment: absent hooks dir loads nothing without error", async () => {
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: captureLog().sink });
		expect(registry.loadedHooks).toHaveLength(0);
		expect(registry.isEmpty).toBe(true);
	});

	it("manifest name overrides dir name in metadata and messages", async () => {
		const log = captureLog();
		writeHookFixture(home.hooksDir, "dir-name", {
			events: ["session:start"],
			extraManifestFields: {},
			manifestBody: "name: manifest-name\nevents:\n  - session:start\n",
		});
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: log.sink });
		expect(registry.loadedHooks[0]?.name).toBe("manifest-name");
		expect(log.includes("Loaded hook 'manifest-name'")).toBe(true);
	});
});

describe("wildcard resolution (_resolve_handlers parity)", () => {
	it("exact handlers fire FIRST, then base:* wildcard registrants", async () => {
		writeHookFixture(home.hooksDir, "wild", {
			events: ["command:*"],
			handleBody: "",
		});
		writeHookFixture(home.hooksDir, "exact", { events: ["command:reset"] });
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: captureLog().sink });
		await registry.emit("command:reset", {});
		// Directory sort loads wild before exact, but RESOLUTION puts exact first:
		expect(fixtureCalls()[0]).toBe("exact:command:reset");
		expect(fixtureCalls()).toContain("wild:command:reset");
	});

	it("base-only registration NEVER fires for base:event", async () => {
		const registry = new HookRegistry();
		let fired = 0;
		registry.register("agent", () => {
			fired++;
		});
		await registry.emit("agent:start", {});
		expect(fired).toBe(0);
		await registry.emit("agent", {});
		expect(fired).toBe(1);
	});

	it("wildcard only matches its own base (command:* ≠ other:x)", async () => {
		writeHookFixture(home.hooksDir, "wild", { events: ["command:*"] });
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: captureLog().sink });
		await registry.emit("other:event", {});
		expect(fixtureCalls()).toEqual([]);
	});
});

describe("observer containment (DEC-014: emit-and-log swallow)", () => {
	it("throwing sync observer does NOT propagate, does NOT stop later observers, does not affect emit result", async () => {
		writeHookFixture(home.hooksDir, "a-ok", {
			events: ["gateway:startup"],
			handleBody: "return undefined;",
		});
		writeHookFixture(home.hooksDir, "b-boom", {
			events: ["gateway:startup"],
			handleBody: "throw new Error('observer exploded');",
		});
		writeHookFixture(home.hooksDir, "c-ok", {
			events: ["gateway:startup"],
			handleBody: "return undefined;",
		});
		const log = captureLog();
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: log.sink });

		await expect(
			registry.emit(
				"gateway:startup",
				{ platforms: ["telegram"] },
				{ log: log.sink },
			),
		).resolves.toBeUndefined(); // emit never throws for observer failures

		expect(fixtureCalls()).toEqual([
			"a-ok:gateway:startup",
			"b-boom:gateway:startup",
			"c-ok:gateway:startup",
		]);
		expect(
			log.includes(
				"[hooks] Error in handler for 'gateway:startup': Error: observer exploded",
			),
		).toBe(true);
	});

	it("REJECTING async observer contained the same as throwing sync ones", async () => {
		const registry = new HookRegistry();
		let afterRan = false;
		registry.register("session:end", async () => {
			throw new Error("async boom");
		});
		registry.register("session:end", () => {
			afterRan = true;
		});
		const quiet = captureLog();
		await registry.emit("session:end", {}, { log: quiet.sink });
		expect(afterRan).toBe(true);
		expect(quiet.includes("async boom")).toBe(true);
	});

	it("emitCollect skips the thrower but still captures LATER results in order", async () => {
		const registry = new HookRegistry();
		registry.register("command:status", () => {
			throw new Error("collect-side crash");
		});
		registry.register("command:status", () => ({ decision: "deny" }));
		const quiet = captureLog();
		const results = await registry.emitCollect(
			"command:status",
			{},
			{ log: quiet.sink },
		);
		expect(results).toEqual([{ decision: "deny" }]);
		expect(quiet.includes("collect-side crash")).toBe(true);
	});
});

describe("collect value filtering + handler shapes", () => {
	it("non-null results kept IN ORDER incl. falsy-but-defined values", async () => {
		const registry = new HookRegistry();
		registry.register("e", () => undefined); // no-return ≙ Python None
		registry.register("e", () => null);
		registry.register("e", () => 0);
		registry.register("e", () => false);
		registry.register("e", () => "");
		registry.register("e", () => ({ decision: "allow" }));
		const results = await registry.emitCollect(
			"e",
			{},
			{ log: captureLog().sink },
		);
		expect(results).toEqual([0, false, "", { decision: "allow" }]);
	});

	it("sync AND async decision handlers both supported (iscoroutine parity)", async () => {
		const registry = new HookRegistry();
		registry.register("e", () => ({ n: 1 }));
		registry.register("e", async () => {
			return { n: 2 };
		});
		const results = await registry.emitCollect(
			"e",
			{},
			{ log: captureLog().sink },
		);
		expect(results).toEqual([{ n: 1 }, { n: 2 }]);
	});

	it("context defaults to {} and reaches handlers additively unchanged", async () => {
		writeHookFixture(home.hooksDir, "ctx-echo", {
			events: ["agent:end"],
			handleBody: `return { seen: Object.keys(context).sort(), eventType };`,
		});
		const registry = new HookRegistry();
		await registry.discoverAndLoad({ log: captureLog().sink });
		const results = (await registry.emitCollect(
			"agent:end",
			{ platform: "slack", response: "done", futureField: "added-later" },
			{ log: captureLog().sink },
		)) as Array<{ seen: string[]; eventType: string }>;
		const result = results[0];
		// Additive payload compat: NEW fields flow to frozen-era handlers untouched.
		expect(result?.seen).toEqual(["futureField", "platform", "response"]);
		expect(result?.eventType).toBe("agent:end");
	});
});

describe("frozen-plugin style compat through REAL discovery", () => {
	it("hook written against the frozen v1 surface loads via the real scanner with unknown manifest fields ignored", async () => {
		const log = captureLog();
		writeHookFixture(home.hooksDir, "frozen-era-hook", {
			events: ["command:vault"],
			handleBody: `return context.veto === true ? { decision: "deny" } : undefined;`,
			extraManifestFields: {
				description: "written before gateway v2 fields existed",
				priority: "9",
				future_unknown_field: "whatever-v2-adds",
				tags: "[vault, legacy]",
			},
		});
		const registry = new HookRegistry();
		await runWithPiHomeOverride(home.home, () =>
			registry.discoverAndLoad({ log: log.sink }),
		);

		expect(registry.loadedHooks).toHaveLength(1);
		expect(registry.loadedHooks[0]?.description).toBe(
			"written before gateway v2 fields existed",
		);
		const results = await registry.emitCollect(
			"command:vault",
			{ veto: true },
			{ log: log.sink },
		);
		expect(results).toEqual([{ decision: "deny" }]);
		expect(log.includes("Loaded hook 'frozen-era-hook'")).toBe(true);
	});
});

// Behavior contracts for decision-bearing command hooks (07-integrations.md
// §7(a) "Command-hook decision semantics"; DEC-014). The REQUIRED triples —
// deny blocks / handled replaces / rewrite mutates — are each proven
// END-TO-END: fixtures on disk loaded through the REAL HookRegistry discovery,
// results flowing through emitCollect, verdicts processed by
// processCommandHookResults exactly as the dispatcher will.
//
// Binding loop under test (run.py::process_command port):
//   non-dict / allow / missing decision  → continue (telemetry hook)
//   {"decision":"deny"[,"message"]}      → dispatch REPLACED by message
//   {"decision":"handled"[,"message"]}   → dispatch SKIPPED; null ⇒ silent success
//   {"decision":"rewrite", ...}          → text mutated, canonical RE-RESOLVED,
//                                          STOP after ONE hop (no re-interception)
// first decisive verdict wins; unknown decisions ignored; membership gate holds.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	resetPiHomeCacheForTests,
	resetPiHomeOverride,
	setPiHomeOverride,
} from "../../pi_home.js";
import type { CommandHookVerdict } from "./command-decisions.js";
import {
	collectCommandHookResults,
	processCommandHookResults,
	runCommandHooks,
} from "./command-decisions.js";
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
const log = captureLog();

beforeEach(() => {
	home = makeTempHome("cmdhooks");
	setPiHomeOverride(home.home);
	resetFixtureCalls();
	log.lines.length = 0;
});
afterEach(() => {
	home.cleanup();
	resetPiHomeOverride();
	resetPiHomeCacheForTests();
	resetFixtureCalls();
});

/** Real discovery + a registry the dispatcher would hold. */
async function discoveredRegistry(): Promise<HookRegistry> {
	const registry = new HookRegistry();
	await registry.discoverAndLoad({ log: log.sink });
	return registry;
}

function resolveOf(rows: Record<string, string>) {
	return (rawName: string | null | undefined) => {
		const name = (rawName ?? "").replace(/^\/+/, "").toLowerCase();
		if (name.length === 0) return null;
		const canonical = rows[name];
		return canonical !== undefined ? { name: canonical } : null;
	};
}

describe("REQUIRED triple: deny BLOCKS dispatch", () => {
	it("deny with custom message replaces dispatch end-to-end through real discovery", async () => {
		writeHookFixture(home.hooksDir, "policy-deny", {
			events: ["command:status"],
			handleBody: `return { decision: "deny", message: "⛔ /status is disabled by policy." };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "status",
			canonical: "status",
			knownCommand: true,
			resolve: resolveOf({ status: "status" }),
			context: { platform: "slack", user_id: "U1" },
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "blocked",
			message: "⛔ /status is disabled by policy.",
		});
		expect(fixtureCalls()).toEqual(["policy-deny:command:status"]);
	});

	it("bare deny (no message) falls back to the DEFAULT blocked message naming the TYPED token (alias, not canonical)", async () => {
		writeHookFixture(home.hooksDir, "policy-deny-bare", {
			events: ["command:new"],
			handleBody: `return { decision: "deny" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "n", // alias as typed
			canonical: "new", // canonical resolved upstream
			knownCommand: true,
			resolve: resolveOf({ n: "new" }),
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "blocked",
			message: "Command `/n` was blocked by a hook.",
		});
	});

	it("non-string / empty messages never leak into the default path", async () => {
		writeHookFixture(home.hooksDir, "deny-junk-message", {
			events: ["command:reset"],
			handleBody: `return { decision: "DENY", message: 42 };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "reset",
			canonical: "reset",
			knownCommand: true,
			resolve: resolveOf({ reset: "reset" }),
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "blocked",
			message: "Command `/reset` was blocked by a hook.",
		});
	});
});

describe("REQUIRED triple: handled REPLACES dispatch", () => {
	it("handled returns the plugin's message as THE reply end-to-end", async () => {
		writeHookFixture(home.hooksDir, "self-serve", {
			events: ["command:roll"],
			handleBody: `return { decision: "handled", message: "🎲 17" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "roll",
			canonical: "roll",
			knownCommand: true,
			resolve: resolveOf({ roll: "roll" }),
			log: log.sink,
		});
		expect(verdict).toEqual({ kind: "handled", message: "🎲 17" });
	});

	it("handled WITHOUT message ⇒ silent success (null reply)", async () => {
		writeHookFixture(home.hooksDir, "silent-success", {
			events: ["command:mute"],
			handleBody: `return { decision: "handled" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "mute",
			canonical: "mute",
			knownCommand: true,
			resolve: resolveOf({ mute: "mute" }),
			log: log.sink,
		});
		expect(verdict).toEqual({ kind: "handled", message: null });
	});

	it("non-string handled message degrades to silent success", async () => {
		writeHookFixture(home.hooksDir, "handled-object-msg", {
			events: ["command:mute"],
			handleBody: `return { decision: "handled", message: { text: "no" } };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "mute",
			canonical: "mute",
			knownCommand: true,
			resolve: resolveOf({ mute: "mute" }),
			log: log.sink,
		});
		expect(verdict).toEqual({ kind: "handled", message: null });
	});
});

describe("REQUIRED triple: rewrite MUTATES and re-resolves — ONE hop only", () => {
	it("rewrite mutates text, re-resolves canonical from the registry via get_command parity", async () => {
		writeHookFixture(home.hooksDir, "alias-shim", {
			events: ["command:start"],
			handleBody: `return { decision: "rewrite", command_name: "/resume", raw_args: "--latest yes" };`,
		});
		const registry = await discoveredRegistry();
		let resolvedWith: string | null = null;
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "start",
			canonical: "start",
			knownCommand: true,
			resolve: (rawName) => {
				resolvedWith = rawName ?? null;
				return resolveOf({ resume: "resume" })(rawName);
			},
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "rewritten",
			text: "/resume --latest yes",
			canonical: "resume",
		});
		expect(resolvedWith).toBe("resume"); // RE-RESOLVED from the registry
	});

	it("ONE hop: the rewritten command is NOT re-intercepted even though its own hook denies", async () => {
		writeHookFixture(home.hooksDir, "shim-to-status", {
			events: ["command:start"],
			handleBody: `return { decision: "rewrite", command_name: "status", raw_args: "" };`,
		});
		// The rewritten target HAS a denying hook — proof of no second hop is
		// that its handler NEVER fires.
		writeHookFixture(home.hooksDir, "status-denier", {
			events: ["command:status"],
			handleBody: `return { decision: "deny", message: "should never happen" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "start",
			canonical: "start",
			knownCommand: true,
			resolve: resolveOf({ status: "status" }),
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "rewritten",
			text: "/status",
			canonical: "status",
		});
		expect(fixtureCalls()).toEqual(["shim-to-status:command:start"]);
		expect(fixtureCalls()).not.toContain("status-denier:command:status");
	});

	it("rewrite to an UNRESOLVABLE name keeps the raw token as canonical (goes straight to core handling)", async () => {
		writeHookFixture(home.hooksDir, "shim-to-nowhere", {
			events: ["command:a"],
			handleBody: `return { decision: "rewrite", command_name: "ghost", raw_args: "" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "a",
			canonical: "a",
			knownCommand: true,
			resolve: resolveOf({}), // ghost resolves to nothing
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "rewritten",
			text: "/ghost",
			canonical: "ghost",
		});
	});

	it("empty/missing command_name is NOT decisive; scanning continues", async () => {
		writeHookFixture(home.hooksDir, "empty-rewrite", {
			events: ["command:x"],
			handleBody: `return { decision: "rewrite", command_name: "", raw_args: "" };`,
		});
		writeHookFixture(home.hooksDir, "then-deny", {
			events: ["command:*"],
			handleBody: `return { decision: "deny", message: "caught after empty rewrite" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "x",
			canonical: "x",
			knownCommand: true,
			resolve: resolveOf({ x: "x" }),
			log: log.sink,
		});
		expect(verdict).toEqual({
			kind: "blocked",
			message: "caught after empty rewrite",
		});
	});
});

describe("telemetry results and ordering", () => {
	it("non-dict / allow / missing-decision / unknown-decision results NEVER decide", async () => {
		const verdict = processCommandHookResults(
			[
				"a string result",
				42,
				["an", "array"],
				null,
				{ decision: "allow" },
				{},
				{ decision: "block" }, // unknown verb ignored
				{ decision: " DENY " }, // normalization: trim+lowercase honored
			],
			{
				typedCommand: "x",
				canonical: "x",
				resolve: resolveOf({ x: "x" }),
			},
		);
		expect(verdict).toEqual({
			kind: "blocked",
			message: "Command `/x` was blocked by a hook.",
		});
	});

	it("FIRST decisive verdict wins: earlier deny shadows later handled", async () => {
		writeHookFixture(home.hooksDir, "first-deny", {
			events: ["command:y"],
			handleBody: `return { decision: "deny", message: "first wins" };`,
		});
		writeHookFixture(home.hooksDir, "later-handled", {
			events: ["command:y"],
			handleBody: `return { decision: "handled", message: "never reached" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "y",
			canonical: "y",
			knownCommand: true,
			resolve: resolveOf({ y: "y" }),
			log: log.sink,
		});
		expect(verdict).toEqual({ kind: "blocked", message: "first wins" });
		expect(fixtureCalls()).toContain("later-handled:command:y"); // both fired…
		// …but only the FIRST decisive verdict was returned.
	});

	it("wildcard telemetry handler runs alongside exact handlers in resolution order", async () => {
		writeHookFixture(home.hooksDir, "meter", {
			events: ["command:*"],
			handleBody: `return undefined;`, // pure telemetry
		});
		writeHookFixture(home.hooksDir, "veto", {
			events: ["command:deploy"],
			handleBody: `return { decision: "deny", message: "deploys frozen" };`,
		});
		const registry = await discoveredRegistry();
		const results = await collectCommandHookResults({
			registry,
			canonical: "deploy",
			context: {},
		});
		expect(results).toEqual([{ decision: "deny", message: "deploys frozen" }]);
		expect(fixtureCalls()).toEqual([
			"veto:command:deploy", // exact list first
			"meter:command:deploy", // then wildcard list
		]);
	});
});

describe("membership gate + infrastructure containment", () => {
	it("unknown commands NEVER reach hooks: gate short-circuits without collecting", async () => {
		writeHookFixture(home.hooksDir, "curious", {
			events: ["command:*"],
			handleBody: "",
		});
		const registry = await discoveredRegistry();
		for (const knownCommand of [false]) {
			const verdict = await runCommandHooks({
				registry,
				typedCommand: "unknowncmd",
				canonical: "unknowncmd",
				knownCommand,
				resolve: resolveOf({}),
				log: log.sink,
			});
			expect(verdict).toEqual({ kind: "dispatch", canonical: "unknowncmd" });
		}
		expect(fixtureCalls()).toEqual([]); // no handler saw anything
	});

	it("empty typed token short-circuits too (plain-text safety)", async () => {
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "",
			canonical: "",
			knownCommand: true,
			resolve: resolveOf({}),
			log: log.sink,
		});
		expect(verdict.kind).toBe("dispatch");
	});

	it("emit_collect infra failure downgrades to [] (debug log), dispatch unblocked", async () => {
		const exploding = {
			emitCollect(): Promise<unknown[]> {
				throw new Error("hook transport exploded");
			},
		};
		const results = await collectCommandHookResults({
			registry: exploding,
			canonical: "status",
			log: log.sink,
		});
		expect(results).toEqual([]);
		expect(
			log.includes("[hooks] command:status hook dispatch failed (non-fatal)"),
		).toBe(true);

		const verdict: CommandHookVerdict = processCommandHookResults(results, {
			typedCommand: "status",
			canonical: "status",
			resolve: resolveOf({ status: "status" }),
		});
		expect(verdict).toEqual({ kind: "dispatch", canonical: "status" });
	});

	it("handler raising mid-collect is contained while later results still process (dispatcher view)", async () => {
		writeHookFixture(home.hooksDir, "crasher", {
			events: ["command:z"],
			handleBody: "throw new Error('mid-collect');",
		});
		writeHookFixture(home.hooksDir, "survivor", {
			events: ["command:*"],
			handleBody: `return { decision: "handled", message: "still here" };`,
		});
		const registry = await discoveredRegistry();
		const verdict = await runCommandHooks({
			registry,
			typedCommand: "z",
			canonical: "z",
			knownCommand: true,
			resolve: resolveOf({ z: "z" }),
			log: log.sink,
		});
		expect(verdict).toEqual({ kind: "handled", message: "still here" });
		expect(log.includes("mid-collect")).toBe(true);
	});
});

// Slash-command access control (gap-audit R14; 07 §2 L2-guard flow row):
// gateway/slash_access.py port + run.py:_check_slash_access gate sites. The
// load-bearing properties: (1) NO admin list ⇒ gating DISABLED for the scope
// (backward-compat — existing installs keep every command); (2) with gating
// on, non-admins reach ONLY user_allowed_commands plus the {help,whoami}
// floor; (3) the denial text is BYTE-STABLE; (4) the gate sits BETWEEN the
// status/context pre-gate and dispatch on BOTH the running-agent fast-path
// (RunnerBusyGuard.dispatchBusySlashCommand) and the cold path
// (checkColdPathSlashAccess) so an in-flight agent can't be used to bypass.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import type { CommandDef } from "./busy-policy.js";
import { RunnerBusyGuard, type RunnerBusyOptions } from "./runner-busy.js";
import {
	ALWAYS_ALLOWED_USER_COMMANDS,
	SLASH_ACCESS_DISABLED,
	canRunSlashCommand,
	checkSlashAccess,
	checkSourceSlashAccess,
	coerceCommandSet,
	coerceIdSet,
	isSlashAdmin,
	keysForScope,
	policyForSource,
	policyFromExtra,
	slashAccessDenialText,
	scopeForChatType,
} from "./slash-access.js";

const REGISTRY: CommandDef[] = [
	{ name: "stop", busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" },
	{
		name: "model",
		busyPolicy: "reject",
		busyHandler: "model",
		aliases: ["mdl"],
	},
	{ name: "status", busyPolicy: "dispatch" },
	{ name: "help", busyPolicy: "dispatch" },
	{ name: "whoami", busyPolicy: "dispatch" },
];

/** SessionSource-like arrival snapshot (what policyForSource reads). */
const dmSource = (
	userId?: string,
): { platform: string; chatType: string; userId?: string } => ({
	platform: "telegram",
	chatType: "dm",
	...(userId !== undefined ? { userId } : {}),
});

const dmEvent = (userId?: string): IncomingEvent => ({
	messageType: "text",
	text: "/x",
	source: dmSource(userId),
});

function gatedGuard(
	policyFactory: (event: IncomingEvent) => ReturnType<typeof makeDmPolicy>,
	extra: Partial<RunnerBusyOptions> = {},
): { guard: RunnerBusyGuard; warnings: string[] } {
	const warnings: string[] = [];
	const slots = new Map<string, IncomingEvent>();
	const guard = new RunnerBusyGuard({
		registry: REGISTRY,
		slots,
		onWarning: (m) => warnings.push(m),
		specialHandlers: { stop: () => "stopped" },
		plainHandlers: {
			help: () => "helped",
			whoami: () => "you are you",
			status: () => "status",
			context: () => "context",
		},
		slashAccessPolicyOf: policyFactory,
		...extra,
	});
	return { guard, warnings };
}

/** Enabled policy: alice admins; bob may run /queue only. */
const makeDmPolicy = () =>
	policyFromExtra(
		{ allow_admin_from: "alice", user_allowed_commands: ["queue"] },
		"dm",
	);

describe("policy coercion (slash_access.py _coerce_* helpers)", () => {
	it("id lists accept arrays/sets/comma-strings/scalars; blanks drop", () => {
		expect([...coerceIdSet([" 42 ", "7", ""])].sort()).toEqual(["42", "7"]);
		expect([...coerceIdSet("a, b,,c")].sort()).toEqual(["a", "b", "c"]);
		expect([...coerceIdSet(123)]).toEqual(["123"]);
		expect(coerceIdSet(null).size).toBe(0);
		expect(coerceIdSet(undefined).size).toBe(0);
	});

	it("command lists strip leading slashes and lowercase (either YAML style)", () => {
		expect(
			[...coerceCommandSet(["/Help", "QUEUE", "//Status"])].sort(),
		).toEqual(["help", "queue", "status"]);
		expect([...coerceCommandSet("/whoami, /Model")].sort()).toEqual([
			"model",
			"whoami",
		]);
		expect(coerceCommandSet(null).size).toBe(0);
	});
});

describe("scope resolution + per-scope keys", () => {
	it("dm/direct/private/'' are dm (case-insensitive); everything else is group", () => {
		for (const ct of ["dm", "DM", "direct", "Private", ""])
			expect(scopeForChatType(ct)).toBe("dm");
		for (const ct of ["group", "channel", "thread", undefined, null])
			expect(scopeForChatType(ct)).toBe("group");
	});

	it("group scope reads group_* keys; dm reads allow_admin_from/user_allowed_commands", () => {
		expect(keysForScope("group")).toEqual([
			"group_allow_admin_from",
			"group_user_allowed_commands",
		]);
		expect(keysForScope("dm")).toEqual([
			"allow_admin_from",
			"user_allowed_commands",
		]);
	});
});

describe("policyFromExtra — enabled flag, floor, and the dm→group fallback", () => {
	it("gating is ENABLED exactly when the scope has an admin list", () => {
		expect(policyFromExtra({ allow_admin_from: ["u1"] }, "dm").enabled).toBe(
			true,
		);
		expect(policyFromExtra({}, "dm").enabled).toBe(false);
		expect(
			policyFromExtra({ group_allow_admin_from: ["u1"] }, "group").enabled,
		).toBe(true);
	});

	it("admin lists NEVER cross scopes: a group admin is not a dm admin", () => {
		const p = policyFromExtra({ group_allow_admin_from: ["u1"] }, "dm");
		expect(p.enabled).toBe(false);
		expect(p.adminUserIds.size).toBe(0);
	});

	it("dm commands fall back to group_user_allowed_commands when unset", () => {
		const shared = { group_user_allowed_commands: ["status"] };
		expect([...policyFromExtra(shared, "dm").userAllowedCommands]).toEqual([
			"status",
		]);
		// …but never RESTRICTIVELY: an explicit dm list wins outright.
		const both = { ...shared, user_allowed_commands: ["queue"] };
		expect([...policyFromExtra(both, "dm").userAllowedCommands]).toEqual([
			"queue",
		]);
	});
});

describe("policyForSource over config/source shapes", () => {
	const config = (platforms: Record<string, unknown>) => ({ platforms });

	it("missing config or source ⇒ disabled policy (backward-compat allow-all)", () => {
		expect(policyForSource(null, dmSource("u"))).toEqual(SLASH_ACCESS_DISABLED);
		expect(policyForSource(config({}), null)).toEqual(SLASH_ACCESS_DISABLED);
	});

	it("resolves through BOTH Map-shaped and plain-object platform tables", () => {
		const extra = { allow_admin_from: ["alice"] };
		const viaMap = policyForSource(
			{ platforms: new Map([["telegram", { extra }]]) },
			dmSource(),
		);
		const viaRecord = policyForSource(
			config({ telegram: { extra } }),
			dmSource(),
		);
		expect(viaMap.enabled).toBe(true);
		expect(viaRecord.enabled).toBe(true);
		expect([...viaMap.adminUserIds]).toEqual(["alice"]);
	});

	it("unknown platform ⇒ no extra ⇒ disabled for that source", () => {
		expect(policyForSource(config({ telegram: {} }), dmSource()).enabled).toBe(
			false,
		);
	});

	it("chat_type drives the scope keys (dm vs group)", () => {
		const platforms = {
			telegram: {
				extra: {
					group_allow_admin_from: ["boss"],
					group_user_allowed_commands: ["/status"],
				},
			},
		};
		const group = policyForSource(config(platforms), {
			platform: "telegram",
			chatType: "group",
			userId: "boss",
		});
		expect(group.enabled).toBe(true);
		expect(canRunSlashCommand(group, "boss", "status")).toBe(true);
	});
});

describe("isAdmin / canRun — floor and membership semantics", () => {
	const policy = makeDmPolicy();

	it("disabled policy treats EVERY allowed user as admin (uniform downstream)", () => {
		expect(isSlashAdmin(SLASH_ACCESS_DISABLED, "nobody")).toBe(true);
		expect(canRunSlashCommand(SLASH_ACCESS_DISABLED, null, "restart")).toBe(
			true,
		);
	});

	it("admins pass everything; missing userId is never admin while gated", () => {
		expect(isSlashAdmin(policy, "alice")).toBe(true);
		expect(isSlashAdmin(policy, undefined)).toBe(false);
		expect(canRunSlashCommand(policy, "alice", "restart")).toBe(true);
	});

	it("the {help, whoami} floor stays reachable for non-admins", () => {
		expect(ALWAYS_ALLOWED_USER_COMMANDS.has("help")).toBe(true);
		expect(ALWAYS_ALLOWED_USER_COMMANDS.has("whoami")).toBe(true);
		expect(canRunSlashCommand(policy, "bob", "help")).toBe(true);
		expect(canRunSlashCommand(policy, "bob", "whoami")).toBe(true);
	});

	it("listed user_allowed_commands pass; everything else denies", () => {
		expect(canRunSlashCommand(policy, "bob", "queue")).toBe(true);
		expect(canRunSlashCommand(policy, "bob", "model")).toBe(false);
		expect(canRunSlashCommand(policy, "bob", "")).toBe(false);
	});
});

describe("byte-stable denial text (run.py:_check_slash_access)", () => {
	it("empty allowlist produces the exact admin-only sentence", () => {
		const p = policyFromExtra({ allow_admin_from: ["alice"] }, "dm");
		expect(slashAccessDenialText(p, "restart")).toBe(
			"⛔ /restart is admin-only here. No slash commands are enabled for " +
				"non-admins on this platform. Ask an admin to add you to " +
				"allow_admin_from or to set user_allowed_commands.",
		);
	});

	it("preview lists up to 12 SORTED entries, then ellipsis; floor not listed", () => {
		const many = [
			"zeta",
			"alpha",
			"mid",
			...Array.from({ length: 20 }, (_, i) => `c${String(i).padStart(2, "0")}`),
		];
		const p = policyFromExtra(
			{ allow_admin_from: ["alice"], user_allowed_commands: many },
			"dm",
		);
		const text = slashAccessDenialText(p, "restart");
		expect(
			text.startsWith("⛔ /restart is admin-only here. You can run: /"),
		).toBe(true);
		expect(text.endsWith(". Use /whoami for the full list.")).toBe(true);
		// Sorted preview capped at 12, ellipsis appended, floor entries absent.
		expect(text).toContain("/alpha");
		expect(text).toContain("…");
		expect(text).not.toContain("/c19"); // 23 entries → beyond the cap
		expect(text).not.toContain("/help");

		// Exactly ≤12 entries → NO ellipsis.
		const small = policyFromExtra(
			{ allow_admin_from: ["a"], user_allowed_commands: ["b", "a2"] },
			"dm",
		);
		const smallText = slashAccessDenialText(small, "restart");
		expect(smallText).not.toContain("…");
		expect(smallText).toContain("/a2, /b");
	});

	it("checkSlashAccess: disabled ⇒ null; allowed ⇒ null; denied ⇒ stable text", () => {
		const p = makeDmPolicy();
		expect(checkSlashAccess(p, "bob", null)).toBeNull();
		expect(checkSlashAccess(undefined, "bob", "model")).toBeNull();
		expect(checkSlashAccess(p, "alice", "model")).toBeNull();
		expect(checkSlashAccess(p, "bob", "model")).toBe(
			checkSlashAccess(makeDmPolicy(), "bob", "model"),
		);
		expect(checkSlashAccess(p, "bob", "model")).toMatch(
			/^⛔ \/model is admin-only here\./,
		);
	});

	it("checkSourceSlashAccess binds policy resolution + user attribution", () => {
		const cfg = {
			platforms: { telegram: { extra: { allow_admin_from: "carol" } } },
		};
		expect(
			checkSourceSlashAccess(cfg, dmSource("carol"), "restart"),
		).toBeNull();
		expect(checkSourceSlashAccess(cfg, dmSource("dave"), "restart")).toContain(
			"/restart is admin-only here.",
		);
	});
});

describe("RUNNING-AGENT FAST-PATH gate (run.py ~17282 between pregate and dispatch)", () => {
	it("denies a non-admin BEFORE the mid-run reject text (/model)", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("model", dmEvent("bob"), "k"),
		).resolves.toBe(checkSlashAccess(makeDmPolicy(), "bob", "model"));
	});

	it("/status pre-gates — answered even for gated non-admins", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("status", dmEvent("bob"), "k"),
		).resolves.toBe("status");
	});

	it("the {help, whoami} floor passes mid-run for non-admins", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("help", dmEvent("bob"), "k"),
		).resolves.toBe("helped");
		await expect(
			guard.dispatchBusySlashCommand("whoami", dmEvent("bob"), "k"),
		).resolves.toBe("you are you");
	});

	it("aliases resolve to the CANONICAL name before gating (/mdl ⇒ model)", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("mdl", dmEvent("bob"), "k"),
		).resolves.toContain("/model is admin-only here.");
	});

	it("unknown '/foo' is NOT gated (it queues as plain text → agent turn)", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("foo", dmEvent("bob"), "k"),
		).resolves.toBeNull();
	});

	it("admins keep full mid-run behavior (/model still busy-rejects)", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("model", dmEvent("alice"), "k"),
		).resolves.toBe(
			"Agent is running — wait or /stop first, then switch models.",
		);
	});

	it("control-plane /stop is gated too — an in-flight agent can't be leveraged", async () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		await expect(
			guard.dispatchBusySlashCommand("stop", dmEvent("bob"), "k"),
		).resolves.toContain("/stop is admin-only here.");
		const admin = gatedGuard(() => makeDmPolicy());
		await expect(
			admin.guard.dispatchBusySlashCommand("stop", dmEvent("alice"), "k"),
		).resolves.toBe("stopped");
	});

	it("no policy resolver configured ⇒ gating fully off (backward-compat)", async () => {
		const slots = new Map<string, IncomingEvent>();
		const guard = new RunnerBusyGuard({
			registry: REGISTRY,
			slots,
			plainHandlers: { help: () => "helped" },
		});
		await expect(
			guard.dispatchBusySlashCommand("model", dmEvent("bob"), "k"),
		).resolves.toBe(
			"Agent is running — wait or /stop first, then switch models.",
		);
	});

	it("denials log the Hermes line shape with platform:user attribution", async () => {
		const { guard, warnings } = gatedGuard(() => makeDmPolicy());
		await guard.dispatchBusySlashCommand("model", dmEvent("bob"), "k");
		expect(warnings).toContain(
			"Slash command /model denied for telegram:bob (not admin, not in user_allowed_commands)",
		);
	});
});

describe("COLD-PATH gate (run.py ~17507 before built-in dispatch)", () => {
	it("denies non-admins on registry-known commands, keyed CANONICALLY", () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		expect(guard.checkColdPathSlashAccess(dmEvent("bob"), "model")).toContain(
			"/model is admin-only here.",
		);
		expect(guard.checkColdPathSlashAccess(dmEvent("bob"), "mdl")).toContain(
			"/model is admin-only here.",
		);
	});

	it("cold path has NO pregate exemption: /status gates too (Hermes parity)", () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		expect(guard.checkColdPathSlashAccess(dmEvent("bob"), "status")).toContain(
			"/status is admin-only here.",
		);
		expect(
			guard.checkColdPathSlashAccess(dmEvent("alice"), "status"),
		).toBeNull();
	});

	it("unknown names return null (plain text is never access-gated)", () => {
		const { guard } = gatedGuard(() => makeDmPolicy());
		expect(guard.checkColdPathSlashAccess(dmEvent("bob"), "foo")).toBeNull();
	});

	it("the floor and listed commands pass on the cold path as well", () => {
		const queueOnly = policyFromExtra(
			{ allow_admin_from: ["alice"], user_allowed_commands: ["queue"] },
			"dm",
		);
		const { guard } = gatedGuard(() => queueOnly);
		expect(guard.checkColdPathSlashAccess(dmEvent("bob"), "whoami")).toBeNull();
	});
});

// Behavior contracts for the §2.1 decision chain (06 §2; roadmap Phase 4 §2).
// Table-driven matrix proving EACH precedence pair; the #34515 fail-closed
// regression; the A6 bot-bypass-before-no-user-id ordering; profile-scoped
// allowlist isolation through poisoned env (#86905/#72348 shapes); structural
// denial reason codes. No change detectors — every row asserts an OUTCOME
// (admit/deny + granting/failing gate + reason code).

import { mkdtempSync, rmSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	isUserAuthorized,
	unauthorizedDmBehavior,
	type AdapterAuthzView,
	type AuthzDecisionRecord,
	type AuthzDeps,
	type AuthzSource,
} from "./index.js";
import {
	runInSecretScope,
	secretMappingFromRecord,
	setMultiplexActive,
} from "../secretscope/index.js";
import { expandWhatsappAliases } from "../../resolution/whatsapp-identity.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-authz-decision-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

// ── env control helper (multiplex OFF ⇒ accessors read the real environment) ─

function withEnv<T>(vars: Record<string, string | undefined>, fn: () => T): T {
	const saved = new Map<string, string | undefined>();
	for (const k of Object.keys(vars)) saved.set(k, process.env[k]);
	try {
		for (const [k, v] of Object.entries(vars)) {
			if (v === undefined) delete process.env[k];
			else process.env[k] = v;
		}
		return fn();
	} finally {
		for (const [k, v] of saved) {
			if (v === undefined) delete process.env[k];
			else process.env[k] = v;
		}
	}
}

interface MatrixRow {
	name: string;
	source: AuthzSource;
	env?: Record<string, string>;
	deps?: AuthzDeps;
	expected: {
		allowed: boolean;
		gate: number;
		reasonCode: string;
		detail?: string;
	};
}

describe("§2.1 decision order — table-driven matrix", () => {
	const rows: MatrixRow[] = [
		// ── gate 0: system-generated / HMAC-authenticated sources ────────────
		{
			name: "webhook source allows with NOTHING configured (HMAC-authenticated upstream)",
			source: { platform: "webhook", userId: "" },
			expected: { allowed: true, gate: 0, reasonCode: "system_platform" },
		},
		// DEC-070: the homeassistant row (also gate-0 system_platform) was removed
		// with its adapter; the surviving webhook row proves the same gate-0 mechanism.

		// ── gate 1: upstream-auth delegation beats local allowlists ─────────
		{
			name: "relay-delivered event admits even when the author misses the allowlist",
			source: {
				platform: "discord",
				userId: "42",
				deliveredViaUpstreamRelay: true,
			},
			env: { DISCORD_ALLOWED_USERS: "someone_else" },
			expected: {
				allowed: true,
				gate: 1,
				reasonCode: "upstream_auth_delegation",
			},
		},
		{
			name: "adapter authorization_is_upstream flag delegates like relay delivery",
			source: { platform: "relay", userId: "42" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					authorizationIsUpstream: true,
				}),
			},
			env: {},
			expected: {
				allowed: true,
				gate: 1,
				reasonCode: "upstream_auth_delegation",
			},
		},
		{
			name: "relay marker must be EXACTLY true — a truthy stand-in never authorizes",
			source: {
				platform: "discord",
				userId: "42",
				deliveredViaUpstreamRelay: "yes" as unknown as boolean,
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},

		// ── gate 2 BEFORE gate 4: anonymous-admin/channel traffic ────────────
		{
			name: "group chat-ID allowlist admits a channel post with NO user id (2<4)",
			source: { platform: "telegram", chatType: "channel", chatId: "-100200" },
			env: { TELEGRAM_GROUP_ALLOWED_CHATS: "-100200,-100300" },
			expected: { allowed: true, gate: 2, reasonCode: "group_chat_allowlist" },
		},
		{
			name: "group chat '*' wildcard admits any listed-shape chat",
			source: { platform: "telegram", chatType: "group", chatId: "-999" },
			env: { TELEGRAM_GROUP_ALLOWED_CHATS: "*" },
			expected: { allowed: true, gate: 2, reasonCode: "group_chat_allowlist" },
		},
		{
			name: "group chat allowlist falls back to adapter config.extra",
			source: { platform: "discord", chatType: "forum", chatId: "77" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					extra: { group_allowed_chats: ["77"] },
				}),
			},
			expected: { allowed: true, gate: 2, reasonCode: "group_chat_allowlist" },
		},
		{
			name: "chat NOT in the group allowlist falls through to no-user-id deny",
			source: { platform: "telegram", chatType: "group", chatId: "-100400" },
			env: { TELEGRAM_GROUP_ALLOWED_CHATS: "-100200" },
			expected: { allowed: false, gate: 4, reasonCode: "no_user_id" },
		},

		// ── gate 3 BEFORE gate 4: {PLATFORM}_ALLOW_BOTS bypass (A6/#4466) ────
		{
			name: "bot sender with user=None admitted when ALLOW_BOTS=all (Slack Workflow shape)",
			source: { platform: "slack", isBot: true, userId: null },
			env: { SLACK_ALLOW_BOTS: "all" },
			expected: { allowed: true, gate: 3, reasonCode: "bot_bypass" },
		},
		{
			name: "bot bypass 'mentions' admits at the gateway (mention filtering is adapter intake)",
			source: { platform: "discord", isBot: true, userId: "" },
			env: { DISCORD_ALLOW_BOTS: "mentions" },
			expected: { allowed: true, gate: 3, reasonCode: "bot_bypass" },
		},
		{
			name: "per-platform map honored: telegram var does not unlock discord bots",
			source: { platform: "discord", isBot: true, userId: "" },
			env: { TELEGRAM_ALLOW_BOTS: "all" },
			expected: { allowed: false, gate: 4, reasonCode: "no_user_id" },
		},
		{
			name: "ALLOW_BOTS default none denies bot senders",
			source: { platform: "telegram", isBot: true, userId: "" },
			env: {},
			expected: { allowed: false, gate: 4, reasonCode: "no_user_id" },
		},
		{
			name: "unknown ALLOW_BOTS value normalizes to none (deny)",
			source: { platform: "feishu", isBot: true, userId: "" },
			env: { FEISHU_ALLOW_BOTS: "sometimes" },
			expected: { allowed: false, gate: 4, reasonCode: "no_user_id" },
		},

		// ── gate 4: no user id → DENY with reason code ────────────────────────
		{
			name: "human sender with no user id denies at gate 4",
			source: { platform: "telegram", chatType: "dm" },
			expected: { allowed: false, gate: 4, reasonCode: "no_user_id" },
		},

		// ── gate 5: per-platform ALLOW_ALL explicit opt-out ───────────────────
		{
			name: "ALLOW_ALL admits an UNLISTED user (beats allowlist-miss deny)",
			source: { platform: "discord", userId: "stranger" },
			env: { DISCORD_ALLOW_ALL_USERS: "true" },
			expected: { allowed: true, gate: 5, reasonCode: "allow_all_users" },
		},
		{
			name: "ALLOW_ALL beats role-auth (gate 5 grants before gate 6 runs)",
			source: { platform: "matrix", userId: "u1", roleAuthorized: true },
			env: { MATRIX_ALLOW_ALL_USERS: "yes" },
			expected: { allowed: true, gate: 5, reasonCode: "allow_all_users" },
		},
		{
			name: "truthy vocabulary is true/1/yes ONLY",
			source: { platform: "signal", userId: "u1" },
			env: { SIGNAL_ALLOW_ALL_USERS: "TRUE " },
			expected: { allowed: true, gate: 5, reasonCode: "allow_all_users" },
		},
		{
			name: "'on' is NOT a truthy allow-all value",
			source: { platform: "signal", userId: "u1" },
			env: { SIGNAL_ALLOW_ALL_USERS: "on" },
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},

		// ── gate 6: adapter-verified role auth (`is True` discipline) ────────
		{
			name: "role_authorized exactly true admits",
			source: { platform: "discord", userId: "42", roleAuthorized: true },
			expected: { allowed: true, gate: 6, reasonCode: "role_authorized" },
		},
		{
			name: "role_authorized truthy-but-not-true NEVER admits (pitfall #13)",
			source: {
				platform: "discord",
				userId: "42",
				roleAuthorized: "yes" as unknown as boolean,
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},

		// ── gate 7: pairing store admission (union with allowlists) ─────────
		{
			name: "paired user admits WITHOUT any allowlist configured",
			source: { platform: "telegram", userId: "777" },
			deps: {
				pairingStoreFor: () => ({ isApproved: () => true }),
			},
			expected: { allowed: true, gate: 7, reasonCode: "pairing_approved" },
		},
		{
			name: "a THROWING pairing store must never fail-open (deny, not crash)",
			source: { platform: "telegram", userId: "777" },
			deps: {
				pairingStoreFor: () => ({
					isApproved: () => {
						throw new Error("db gone");
					},
				}),
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},

		// ── step 8: adapter-policy trust ONLY under effective allowlist ─────
		{
			name: "#34515 REGRESSION: own-policy adapter with dm_policy=open DENIES (fail-closed)",
			source: { platform: "wecom", userId: "anyone" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					enforcesOwnAccessPolicy: true,
					dmPolicy: "open",
				}),
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},
		{
			name: "#34515 REGRESSION (group): group_policy=open misconfig DENIES, never admits",
			source: {
				platform: "wecom",
				userId: "anyone",
				chatType: "group",
				chatId: "g1",
			},
			deps: {
				adapterView: (): AdapterAuthzView => ({
					enforcesOwnAccessPolicy: true,
					groupPolicy: "open",
				}),
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},
		{
			name: "dm_policy=allowlist intake IS trustworthy (admits at gate 8)",
			source: { platform: "wecom", userId: "555" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					enforcesOwnAccessPolicy: true,
					dmPolicy: "allowlist",
				}),
			},
			expected: {
				allowed: true,
				gate: 8,
				reasonCode: "adapter_allowlist_policy",
			},
		},
		{
			name: "live DM recheck overrides stale construction-time snapshot (revocation staleness)",
			// DEC-070: rewritten from platform "whatsapp" (personal bridge, removed)
			// to wecom — asserts the SURVIVING gate-8 live-recheck mechanism, not the
			// removed adapter.
			source: { platform: "wecom", userId: "555" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					enforcesOwnAccessPolicy: true,
					dmPolicy: "allowlist",
					isDmAllowed: () => false,
				}),
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},
		{
			name: "dm_policy=pairing forwards ONLY for the handshake — not authorization",
			source: { platform: "wecom", userId: "555" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					enforcesOwnAccessPolicy: true,
					dmPolicy: "pairing",
				}),
			},
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},
		{
			name: "per-group sender allowlist admits a group message (WeCom groups.<id>.allow_from)",
			source: {
				platform: "wecom",
				userId: "555",
				chatType: "group",
				chatId: "g1",
			},
			deps: {
				adapterView: (): AdapterAuthzView => ({
					enforcesOwnAccessPolicy: true,
					groupPolicy: "open",
					groupHasSenderAllowlist: (chatId) => chatId === "g1",
				}),
			},
			expected: {
				allowed: true,
				gate: 8,
				reasonCode: "adapter_allowlist_policy",
				detail: "per-group sender allowlist",
			},
		},
		{
			name: "config.extra allow_from admits a DM without enforces_own_access_policy",
			source: { platform: "telegram", userId: "888" },
			deps: {
				adapterView: (): AdapterAuthzView => ({
					extra: { allow_from: "888,999" },
				}),
			},
			expected: { allowed: true, gate: 8, reasonCode: "config_allow_from" },
		},
		{
			name: "config.extra group_allow_from admits group senders",
			source: {
				platform: "telegram",
				userId: "888",
				chatType: "forum",
				chatId: "t",
			},
			deps: {
				adapterView: (): AdapterAuthzView => ({
					extra: { group_allow_from: ["888"] },
				}),
			},
			expected: { allowed: true, gate: 8, reasonCode: "config_allow_from" },
		},
		{
			name: "GATEWAY_ALLOW_ALL_USERS decides the no-allowlist branch (explicit opt-in)",
			source: { platform: "dingtalk", userId: "42" },
			env: { GATEWAY_ALLOW_ALL_USERS: "1" },
			expected: { allowed: true, gate: 8, reasonCode: "gateway_allow_all" },
		},

		// ── step 9: the allowlist union ────────────────────────────────────────
		{
			name: "platform allowlist admits its member",
			source: { platform: "telegram", userId: "111" },
			env: { TELEGRAM_ALLOWED_USERS: "111,222" },
			expected: { allowed: true, gate: 9, reasonCode: "allowlist_union" },
		},
		{
			name: "'*' in ANY allowlist admits everyone",
			source: { platform: "signal", userId: "whoever" },
			env: { SIGNAL_ALLOWED_USERS: "*,someone" },
			expected: { allowed: true, gate: 9, reasonCode: "allowlist_union" },
		},
		{
			name: "@domain stripping: bare allowlist name matches domain-ful sender",
			source: { platform: "email", userId: "alice@corp.example" },
			env: { EMAIL_ALLOWED_USERS: "alice" },
			expected: { allowed: true, gate: 9, reasonCode: "allowlist_union" },
		},
		{
			name: "global allowlist unions across platforms",
			source: { platform: "mattermost", userId: "guser" },
			env: { GATEWAY_ALLOWED_USERS: "guser" },
			expected: { allowed: true, gate: 9, reasonCode: "allowlist_union" },
		},
		{
			name: "scoped GROUP_ALLOWED_USERS admits group senders…",
			source: {
				platform: "telegram",
				userId: "g1",
				chatType: "group",
				chatId: "-1",
			},
			env: { TELEGRAM_GROUP_ALLOWED_USERS: "g1" },
			expected: { allowed: true, gate: 9, reasonCode: "allowlist_union" },
		},
		{
			name: "…but GROUP_ALLOWED_USERS does NOT imply DM access (scoped read)",
			source: { platform: "telegram", userId: "g1", chatType: "dm" },
			env: { TELEGRAM_GROUP_ALLOWED_USERS: "g1" },
			expected: { allowed: false, gate: 10, reasonCode: "default_deny" },
		},
		{
			name: "legacy '-' chat IDs in TELEGRAM_GROUP_ALLOWED_USERS honor group chats (+warn-once)",
			source: {
				platform: "telegram",
				userId: "x",
				chatType: "group",
				chatId: "-100200",
			},
			env: { TELEGRAM_GROUP_ALLOWED_USERS: "-100200" },
			expected: {
				allowed: true,
				gate: 9,
				reasonCode: "group_chat_allowlist",
				detail: "legacy telegram shim",
			},
		},
	];

	for (const row of rows) {
		it(row.name, () => {
			const warned: Array<[string, string]> = [];
			const deps: AuthzDeps = {
				warnOnce: (key, message) => {
					warned.push([key, message]);
				},
				...row.deps,
			};
			const record = withEnv(row.env ?? {}, () =>
				isUserAuthorized(row.source, deps),
			);
			expect(record.allowed).toBe(row.expected.allowed);
			expect(record.gate).toBe(row.expected.gate);
			expect(record.reasonCode).toBe(row.expected.reasonCode);
			if (row.expected.detail !== undefined) {
				expect(record.detail).toBe(row.expected.detail);
			}
			// Denials MUST carry the full identification triple (06 §2.3).
			if (!record.allowed) {
				expect(record.platform).toBe(String(row.source.platform ?? ""));
				expect(record.userId).toBe(String(row.source.userId ?? ""));
				expect(record.chatId).toBe(String(row.source.chatId ?? ""));
			}
			if (
				row.name.startsWith("legacy '-' chat IDs") ||
				row.name.startsWith("#34515")
			) {
				void warned; // warn-once asserted separately below
			}
		});
	}

	it("warn-once fires exactly once across repeated decisions", () => {
		const seen: string[] = [];
		const deps: AuthzDeps = {
			warnOnce: (key) => {
				if (!seen.includes(key)) seen.push(key);
				else seen.push(`dup:${key}`);
			},
		};
		const src: AuthzSource = {
			platform: "telegram",
			userId: "x",
			chatType: "group",
			chatId: "-100200",
		};
		withEnv({ TELEGRAM_GROUP_ALLOWED_USERS: "-100200" }, () => {
			isUserAuthorized(src, deps);
			isUserAuthorized(src, deps);
		});
		expect(
			seen.filter((k) => k === "telegram_group_users_legacy"),
		).toHaveLength(1);
	});
});

// ── WhatsApp alias expansion through the ONE identity module ────────────────

function writeMapping(
	sessionDir: string,
	id: string,
	mapped: string,
	reverse = false,
): void {
	mkdirSync(sessionDir, { recursive: true });
	writeFileSync(
		join(sessionDir, `lid-mapping-${id}${reverse ? "_reverse" : ""}.json`),
		JSON.stringify(mapped),
	);
}

describe("WhatsApp identity aliases in the allowlist union", () => {
	// DEC-070: these rows originally ran on platform "whatsapp" (the personal
	// bridge, removed); they exercise the SURVIVING alias-expansion mechanism and
	// now ride whatsapp_cloud — the remaining member of the isWhatsAppFamily.
	it("a paired LID matches the phone-JID allowlist entry (02 §4.3 fork/deny class)", () => {
		const sessionDir = join(dir, "wa-session");
		writeMapping(sessionDir, "15551234567", "999999999999999");
		writeMapping(sessionDir, "999999999999999", "15551234567", true);

		const record = withEnv(
			{ WHATSAPP_CLOUD_ALLOWED_USERS: "15551234567@s.whatsapp.net" },
			() =>
				isUserAuthorized(
					{ platform: "whatsapp_cloud", userId: "999999999999999@lid" },
					{ whatsappSessionDir: sessionDir },
				),
		);
		expect(record.allowed).toBe(true);
		expect(record.reasonCode).toBe("allowlist_union");
	});

	it("device-suffix senders normalize onto their canonical phone", () => {
		const sessionDir = join(dir, "wa-session-2");
		writeMapping(sessionDir, "15559990000", "15559990000"); // self-map seeds walk
		const record = withEnv(
			{ WHATSAPP_CLOUD_ALLOWED_USERS: "15559990000" },
			() =>
				isUserAuthorized({
					platform: "whatsapp_cloud",
					userId: "15559990000:47@s.whatsapp.net",
				}),
		);
		expect(record.allowed).toBe(true);
	});

	it("unrelated LIDs stay denied (expansion never widens beyond real mappings)", () => {
		const sessionDir = join(dir, "wa-session-3");
		const expanded = expandWhatsappAliases("999999999999998", {
			sessionDir,
		});
		expect(expanded.size).toBe(1); // fresh install degrades to input
		const record = withEnv(
			{ WHATSAPP_CLOUD_ALLOWED_USERS: "15551234567" },
			() =>
				isUserAuthorized(
					{ platform: "whatsapp_cloud", userId: "999999999999998@lid" },
					{ whatsappSessionDir: sessionDir },
				),
		);
		expect(record.allowed).toBe(false);
		expect(record.reasonCode).toBe("default_deny");
	});
});

// ── plugin-platform registry extension ───────────────────────────────────────

describe("plugin-platform registry entries extend the hardcoded maps", () => {
	it("SIMPLEX_ALLOWED_USERS accepts display names via the registry seam", () => {
		const record = withEnv({ SIMPLEX_ALLOWED_USERS: "Ada Lovelace" }, () =>
			isUserAuthorized(
				{ platform: "simplex", userId: "contact-9", userName: "Ada Lovelace" },
				{
					registryEntry: (p) =>
						p === "simplex"
							? { allowedUsersEnv: "SIMPLEX_ALLOWED_USERS" }
							: null,
				},
			),
		);
		expect(record.allowed).toBe(true);
	});

	it("registry allow_all_env participates at gate 5", () => {
		const record = withEnv({ CUSTOM_ALLOW_ALL: "yes" }, () =>
			isUserAuthorized(
				{ platform: "custom", userId: "42" },
				{ registryEntry: () => ({ allowAllEnv: "CUSTOM_ALLOW_ALL" }) },
			),
		);
		expect(record.allowed).toBe(true);
		expect(record.gate).toBe(5);
	});
});

// ── structured denial logging with reason codes (06 §10 row) ────────────────

describe("denial logging carries gate number + stable reason codes", () => {
	it("every denial reaches the injected sink with the full record", () => {
		const denials: AuthzDecisionRecord[] = [];
		const record = withEnv({}, () =>
			isUserAuthorized(
				{ platform: "telegram", userId: "", chatId: "-1", chatType: "group" },
				{ onDenial: (d) => denials.push(d) },
			),
		);
		expect(record.allowed).toBe(false);
		expect(denials).toHaveLength(1);
		expect(denials[0]).toMatchObject({
			allowed: false,
			gate: 4,
			reasonCode: "no_user_id",
			platform: "telegram",
			userId: "",
			chatId: "-1",
		});
	});

	it("allow decisions do NOT hit the denial sink", () => {
		const denials: AuthzDecisionRecord[] = [];
		withEnv({ TELEGRAM_ALLOWED_USERS: "1" }, () =>
			isUserAuthorized(
				{ platform: "telegram", userId: "1" },
				{ onDenial: (d) => denials.push(d) },
			),
		);
		expect(denials).toHaveLength(0);
	});

	it("each gate maps to its documented reason code", () => {
		const cases: Array<[AuthzSource, number, string]> = [
			[{ platform: "slack", isBot: true }, 4, "no_user_id"],
			[{ platform: "qqbot", userId: "nobody" }, 10, "default_deny"],
			[{ platform: "webhook" }, 0, "system_platform"],
		];
		for (const [source, gate, code] of cases) {
			const rec = withEnv({}, () => isUserAuthorized(source));
			expect(rec.gate).toBe(gate);
			expect(rec.reasonCode).toBe(code);
		}
	});
});

// ── profile-scoped allowlist isolation through the secret scope engine ──────
// (#86905/#72348 classes: a scoped miss NEVER borrows another profile's
// bridged process-env allowlist; exit criteria "profile B denied while
// profile A's allowlist would admit".)

describe("multiplex isolation of authz reads", () => {
	afterEach(() => {
		setMultiplexActive(false);
	});

	it("poisoned env cannot leak profile A's allowlist into scoped profile B", () => {
		setMultiplexActive(true);
		process.env.TELEGRAM_ALLOWED_USERS = "999"; // profile A's bridged value
		try {
			const scopeB = secretMappingFromRecord({ OTHER_SECRET: "x" }); // NO allowlist
			const verdict = runInSecretScope(scopeB, () =>
				isUserAuthorized({ platform: "telegram", userId: "999" }),
			);
			// Profile B's sender 999 must be DENIED even though the poisoned
			// env would admit them — the scoped miss returned the declared
			// default (empty), never the borrowed env value.
			expect(verdict.allowed).toBe(false);
			expect(verdict.reasonCode).toBe("default_deny");

			const own = runInSecretScope(
				secretMappingFromRecord({ TELEGRAM_ALLOWED_USERS: "b_user" }),
				() => isUserAuthorized({ platform: "telegram", userId: "b_user" }),
			);
			expect(own.allowed).toBe(true);
			expect(own.gate).toBe(9);
		} finally {
			delete process.env.TELEGRAM_ALLOWED_USERS;
		}
	});

	it("the UNSCOPED default-profile path reads its OWN env values (sanctioned wrapper)", () => {
		setMultiplexActive(true);
		process.env.DISCORD_ALLOWED_USERS = "default_profile_user";
		try {
			const verdict = isUserAuthorized({
				platform: "discord",
				userId: "default_profile_user",
			});
			expect(verdict.allowed).toBe(true);
			expect(verdict.gate).toBe(9);
		} finally {
			delete process.env.DISCORD_ALLOWED_USERS;
		}
	});

	it("ALLOW_BOTS reads are equally scope-authoritative under multiplex", () => {
		setMultiplexActive(true);
		process.env.SLACK_ALLOW_BOTS = "all"; // another profile's bridged flag
		try {
			const verdict = runInSecretScope(
				secretMappingFromRecord({}), // profile B did not opt into bots
				() => isUserAuthorized({ platform: "slack", isBot: true }),
			);
			expect(verdict.allowed).toBe(false);
			expect(verdict.gate).toBe(4);
		} finally {
			delete process.env.SLACK_ALLOW_BOTS;
		}
	});
});

// ── unauthorized DM behavior (pairing handshake entry) ──────────────────────

describe("unauthorizedDmBehavior resolution order", () => {
	const view =
		(dmPolicy: string): ((p: string) => AdapterAuthzView | null) =>
		() => ({ dmPolicy });

	it("explicit per-platform override always wins", () => {
		expect(
			unauthorizedDmBehavior("telegram", {
				config: { perPlatform: { telegram: "ignore" } },
			}),
		).toBe("ignore");
		expect(
			unauthorizedDmBehavior("email", {
				config: { perPlatform: { email: "pair" } },
			}),
		).toBe("pair");
	});

	it("email defaults to ignore (inbox-shaped; explicit opt-in for codes)", () => {
		expect(unauthorizedDmBehavior("email")).toBe("ignore");
	});

	it("non-default global override applies to chat platforms", () => {
		expect(
			unauthorizedDmBehavior("telegram", { config: { global: "ignore" } }),
		).toBe("ignore");
		// "pair" is the DEFAULT value, not an override.
		expect(
			unauthorizedDmBehavior("telegram", { config: { global: "pair" } }),
		).toBe("pair");
	});

	it("adapter dm_policy drives pairing opt-in / silent drop", () => {
		expect(
			unauthorizedDmBehavior("wecom", { adapterView: view("pairing") }),
		).toBe("pair");
		expect(
			unauthorizedDmBehavior("wecom", { adapterView: view("allowlist") }),
		).toBe("ignore");
		expect(
			unauthorizedDmBehavior("wecom", { adapterView: view("disabled") }),
		).toBe("ignore");
		expect(unauthorizedDmBehavior("wecom", { adapterView: view("open") })).toBe(
			"pair",
		);
	});

	it("a configured allowlist silences codes (#9337)", () => {
		withEnv({ TELEGRAM_ALLOWED_USERS: "op" }, () =>
			expect(unauthorizedDmBehavior("telegram")).toBe("ignore"),
		);
		withEnv({ TELEGRAM_GROUP_ALLOWED_CHATS: "-1" }, () =>
			expect(unauthorizedDmBehavior("telegram")).toBe("ignore"),
		);
		withEnv({ QQ_GROUP_ALLOWED_USERS: "u" }, () =>
			expect(unauthorizedDmBehavior("qqbot")).toBe("ignore"),
		);
		withEnv({ GATEWAY_ALLOWED_USERS: "op" }, () =>
			expect(unauthorizedDmBehavior("matrix")).toBe("ignore"),
		);
	});

	it("no config and no allowlist → pair (open-gateway default)", () => {
		expect(unauthorizedDmBehavior("telegram")).toBe("pair");
	});
});

// ── allowAdapterDelegation (secops-10 parity run.py:18981) ─────────────────
// The plugin message-injection re-auth caller passes False, disabling the
// three DELEGATION gates: upstream-relay admission (1), adapter-verified
// role auth (6), and adapter own-policy trust (8 own-policy branch only).
describe("allowAdapterDelegation:false — plugin re-auth re-authorizes locally", () => {
	it("relay-delivered event with delegation OFF falls through to local allowlists", () => {
		withEnv({ DISCORD_ALLOWED_USERS: "42" }, () => {
			const record = isUserAuthorized(
				{
					platform: "discord",
					userId: "42",
					deliveredViaUpstreamRelay: true,
				},
				{},
				{ allowAdapterDelegation: false },
			);
			expect(record).toMatchObject({
				allowed: true,
				gate: 9,
				reasonCode: "allowlist_union",
			});
		});
	});

	it("relay-delivered event with delegation OFF and no local grant DEFAULT-DENIES", () => {
		withEnv({}, () => {
			const record = isUserAuthorized(
				{
					platform: "discord",
					userId: "42",
					deliveredViaUpstreamRelay: true,
				},
				{},
				{ allowAdapterDelegation: false },
			);
			// The same source admits at gate 1 when delegation is on (matrix row
			// above); off, nothing local authorizes it — fail CLOSED.
			expect(record).toMatchObject({
				allowed: false,
				gate: 10,
				reasonCode: "default_deny",
			});
		});
	});

	it("adapter authorization_is_upstream with delegation OFF never admits", () => {
		withEnv({ TELEGRAM_ALLOWED_USERS: "" }, () => {
			const record = isUserAuthorized(
				{ platform: "relay", userId: "42" },
				{
					adapterView: (): AdapterAuthzView => ({
						authorizationIsUpstream: true,
					}),
				},
				{ allowAdapterDelegation: false },
			);
			expect(record).toMatchObject({ allowed: false, gate: 10 });
		});
	});

	it("role_authorized:true with delegation OFF is skipped (falls to default deny)", () => {
		withEnv({}, () => {
			const record = isUserAuthorized(
				{ platform: "discord", userId: "42", roleAuthorized: true },
				{},
				{ allowAdapterDelegation: false },
			);
			expect(record).toMatchObject({
				allowed: false,
				gate: 10,
				reasonCode: "default_deny",
			});
		});
	});

	it("own-policy adapter trust with delegation OFF is skipped; config.allow_from STILL admits", () => {
		withEnv({}, () => {
			// Own-policy intake trust disabled: dm_policy=allowlist alone must NOT
			// admit a plugin re-auth pass…
			const ownPolicy = isUserAuthorized(
				{ platform: "wecom", userId: "777" },
				{
					adapterView: (): AdapterAuthzView => ({
						enforcesOwnAccessPolicy: true,
						dmPolicy: "allowlist",
					}),
				},
				{ allowAdapterDelegation: false },
			);
			expect(ownPolicy).toMatchObject({
				allowed: false,
				gate: 10,
				reasonCode: "default_deny",
			});
			// …but an explicitly configured allow_from remains operator policy,
			// NOT delegation, so it still admits at gate 8.
			const fromConfig = isUserAuthorized(
				{ platform: "wecom", userId: "777" },
				{
					adapterView: (): AdapterAuthzView => ({
						enforcesOwnAccessPolicy: false,
						extra: { allow_from: ["777"] },
					}),
				},
				{ allowAdapterDelegation: false },
			);
			expect(fromConfig).toMatchObject({
				allowed: true,
				gate: 8,
				reasonCode: "config_allow_from",
			});
		});
	});

	it("delegation stays ON by default — omitted option never narrows policy", () => {
		const record = isUserAuthorized({
			platform: "discord",
			userId: "999",
			deliveredViaUpstreamRelay: true,
		});
		expect(record).toMatchObject({
			allowed: true,
			gate: 1,
			reasonCode: "upstream_auth_delegation",
		});
	});
});

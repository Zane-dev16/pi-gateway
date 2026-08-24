// Interactive-surface contracts (04 §9; DEC-016): builder→handler→resolver
// round-trip for EVERY prefix family, oversize rejected at BUILD time,
// namespace collisions impossible, unauthorized clicker ignored, consumed
// buttons stripped, double-tap resolves exactly once, stale/expired taps
// answered and never dispatched.

import { describe, expect, it } from "vitest";
import {
	CALLBACK_DATA_MAX_BYTES,
	CallbackDataOverflowError,
	buildClarifyCallback,
	buildChoicePickerCallback,
	buildExecApprovalCallback,
	buildModelCommitCallback,
	buildModelGroupNavCallback,
	buildModelMemberCallback,
	buildModelPageNavCallback,
	buildModelProviderCallback,
	buildModelProviderGroupCallback,
	buildSlashConfirmCallback,
	buildWhatsappApprovalCallback,
	parseCallbackData,
} from "./callback-grammar.js";
import {
	ClarifyPendingStore,
	CallbackQueryRouter,
	OneShotPendingStore,
	type CallbackTapContext,
} from "./callback-router.js";

const AUTHORIZED: CallbackTapContext = { userId: "42" };
const STRANGER: CallbackTapContext = { userId: "999" };

function allowUser42(ctx: CallbackTapContext): boolean {
	return ctx.userId === "42";
}

describe("builder→parser round-trip for EVERY prefix family", () => {
	const cases: Array<
		[string, string, () => ReturnType<typeof parseCallbackData>]
	> = [
		[
			"ea",
			buildExecApprovalCallback("once", 7),
			() => ({ family: "ea", choice: "once", approvalId: 7 }),
		],
		[
			"ea/session",
			buildExecApprovalCallback("session", 8),
			() => ({ family: "ea", choice: "session", approvalId: 8 }),
		],
		[
			"ea/always",
			buildExecApprovalCallback("always", 9),
			() => ({ family: "ea", choice: "always", approvalId: 9 }),
		],
		[
			"ea/deny",
			buildExecApprovalCallback("deny", 10),
			() => ({ family: "ea", choice: "deny", approvalId: 10 }),
		],
		[
			"sc/once",
			buildSlashConfirmCallback("once", 3),
			() => ({ family: "sc", choice: "once", confirmId: 3 }),
		],
		[
			"sc/always",
			buildSlashConfirmCallback("always", 4),
			() => ({ family: "sc", choice: "always", confirmId: 4 }),
		],
		[
			"sc/cancel",
			buildSlashConfirmCallback("cancel", 5),
			() => ({ family: "sc", choice: "cancel", confirmId: 5 }),
		],
		[
			"cl/idx",
			buildClarifyCallback(11, 2),
			() => ({ family: "cl", clarifyId: 11, idx: 2 }),
		],
		[
			"cl/other",
			buildClarifyCallback(11, "other"),
			() => ({ family: "cl", clarifyId: 11, idx: "other" }),
		],
		["cp", buildChoicePickerCallback(1), () => ({ family: "cp", index: 1 })],
		[
			"mp",
			buildModelProviderCallback("gpt-5"),
			() => ({ family: "mp", slug: "gpt-5" }),
		],
		[
			"mpg",
			buildModelProviderGroupCallback("openai"),
			() => ({ family: "mpg", groupId: "openai" }),
		],
		["mpv", buildModelPageNavCallback(3), () => ({ family: "mpv", page: 3 })],
		[
			"mm",
			buildModelMemberCallback(14),
			() => ({ family: "mm", absIndex: 14 }),
		],
		["mc", buildModelCommitCallback(6), () => ({ family: "mc", idx: 6 })],
		["mb", "mb", () => ({ family: "mb" })],
		["mx:noop", "mx:noop", () => ({ family: "mx" })],
		[
			"mg",
			buildModelGroupNavCallback("anthropic"),
			() => ({ family: "mg", groupId: "anthropic" }),
		],
		[
			"appr/approve",
			buildWhatsappApprovalCallback(21, "approve"),
			() => ({ family: "appr", id: 21, choice: "approve" }),
		],
		[
			"appr/deny",
			buildWhatsappApprovalCallback(22, "deny"),
			() => ({ family: "appr", id: 22, choice: "deny" }),
		],
	];

	for (const [name, data, expected] of cases) {
		it(`${name}: "${data}" round-trips exactly`, () => {
			expect(parseCallbackData(data)).toEqual(expected());
		});
	}

	it("every built callback fits the STRICTEST cap (64-byte Telegram callback_data)", () => {
		for (const [, data] of cases) {
			expect(Buffer.byteLength(data, "utf8")).toBeLessThanOrEqual(
				CALLBACK_DATA_MAX_BYTES,
			);
		}
	});

	it("garbage never matches a family", () => {
		for (const garbage of [
			"",
			":",
			"x",
			"zz:1",
			"ea:bogus:1",
			"sc:nope:1",
			"cl:x:y",
			"cp:notanum",
			"mpv:-1",
			"appr:1:sideways",
			"ea:once",
			"cl:onlyone",
		]) {
			expect(parseCallbackData(garbage).family).toBe("unknown");
		}
	});
});

describe("oversize data rejected at BUILD time", () => {
	it("a long provider slug overflows and the builder refuses", () => {
		const longSlug = "x".repeat(CALLBACK_DATA_MAX_BYTES); // "mp:" pushes past 64
		expect(() => buildModelProviderCallback(longSlug)).toThrow(
			CallbackDataOverflowError,
		);
	});

	it("non-integer ids are refused at build time (64-byte cap forces monotonic ints)", () => {
		expect(() => buildExecApprovalCallback("once", "abc")).toThrow();
		expect(() =>
			buildExecApprovalCallback("once", "550e8400-e29b-41d4-a716-446655440000"),
		).toThrow();
		expect(() => buildClarifyCallback("deadbeef", 0)).toThrow();
	});
});

describe("namespace collisions impossible", () => {
	it("family prefixes are pairwise disjoint", () => {
		// Every builder output must start with EXACTLY ONE family's prefix.
		const outputs = [
			buildExecApprovalCallback("once", 1),
			buildSlashConfirmCallback("cancel", 1),
			buildClarifyCallback(1, 0),
			buildChoicePickerCallback(0),
			buildModelProviderCallback("p"),
			buildModelProviderGroupCallback("g"),
			buildModelPageNavCallback(0),
			buildModelMemberCallback(0),
			buildModelCommitCallback(0),
			"mb",
			"mx:noop",
			buildModelGroupNavCallback("g"),
			buildWhatsappApprovalCallback(1, "approve"),
		];
		for (const out of outputs) {
			const matching = [
				"ea:",
				"sc:",
				"cl:",
				"cp:",
				"mp:",
				"mpg:",
				"mpv:",
				"mm:",
				"mc:",
				"mb",
				"mx:",
				"mg:",
				"appr:",
			].filter((prefix) =>
				prefix === "mb" ? out === "mb" : out.startsWith(prefix),
			);
			expect(matching).toHaveLength(1);
		}
		// The near-miss pairs stay distinct: mp:/mpg:/mpv:/mm:/mc:/mg: all
		// differ at the second character or beyond.
		expect(parseCallbackData("mpg:g").family).not.toBe("mp");
		expect(parseCallbackData("mg:g").family).not.toBe("mpg");
	});
});

function makeRouter(
	overrides: Partial<ConstructorParameters<typeof CallbackQueryRouter>[0]> = {},
) {
	const approvals = new OneShotPendingStore();
	const slashConfirms = new OneShotPendingStore();
	const appr = new OneShotPendingStore();
	const clarify = new ClarifyPendingStore();
	const resolvedChoices: string[] = [];
	// Overrides wrap (not replace) the recorders so assertions keep observing.
	const overriddenExec = overrides.onExecApproval;
	const overriddenSlash = overrides.onSlashConfirm;
	const overriddenClarify = overrides.onClarifyChoice;
	const overriddenAppr = overrides.onWhatsappApproval;
	const router = new CallbackQueryRouter({
		stores: { approvals, slashConfirms, appr, clarify },
		authorizer: overrides.authorizer ?? (() => true),
		nowMs: overrides.nowMs ?? (() => 1000),
		onExecApproval: async (s, choice) => {
			resolvedChoices.push(`ea:${choice}`);
			return overriddenExec ? overriddenExec(s, choice) : "ok";
		},
		onSlashConfirm: async (s, id, choice) => {
			resolvedChoices.push(`sc:${choice}`);
			return overriddenSlash ? overriddenSlash(s, id, choice) : "ok";
		},
		onClarifyChoice: async (s, id, idx) => {
			resolvedChoices.push(`cl:${id}:${idx}`);
			return overriddenClarify
				? overriddenClarify(s, id, idx)
				: `answer-${idx}`;
		},
		onWhatsappApproval: async (s, id, approve) => {
			resolvedChoices.push(`appr:${id}:${approve}`);
			return overriddenAppr ? overriddenAppr(s, id, approve) : "ok";
		},
		onPickerNav: overrides.onPickerNav,
	});
	return { router, approvals, slashConfirms, appr, clarify, resolvedChoices };
}

describe("unauthorized clicker ignored", () => {
	it("unauthorized taps are ANSWERED but never resolved", async () => {
		const { router, approvals, resolvedChoices } = makeRouter({
			authorizer: allowUser42,
		});
		approvals.register(1, "sess-a");
		const answer = await router.route(
			buildExecApprovalCallback("once", 1),
			STRANGER,
		);
		expect(answer.kind).toBe("unauthorized");
		if (answer.kind === "unauthorized") {
			expect(answer.answerText).toContain("not authorized");
			expect(answer.hostEdit).toBeNull();
		}
		expect(resolvedChoices).toEqual([]);
		expect(approvals.has(1)).toBe(true); // NOT consumed by a stranger
	});

	it("empty user ids fail closed via the authorizer contract", async () => {
		// Hermes' _is_callback_user_authorized returns False for an empty id
		// even when some allow-all flag is set (#24457) — the adapter-side
		// authorizer encodes that; the router only enforces the gate.
		const { router, approvals } = makeRouter({
			authorizer: (ctx) => ctx.userId !== "",
		});
		approvals.register(2, "sess-b");
		const answer = await router.route(buildExecApprovalCallback("once", 2), {
			userId: "",
		});
		expect(answer.kind).toBe("unauthorized");
		expect(approvals.has(2)).toBe(true);
	});
});

describe("double-tap resolves exactly once (atomic POP)", () => {
	it("second tap finds nothing and answers already-resolved", async () => {
		const { router, approvals, resolvedChoices } = makeRouter();
		approvals.register(9, "sess-c");

		const first = await router.route(
			buildExecApprovalCallback("always", 9),
			AUTHORIZED,
		);
		expect(first.kind).toBe("resolved");
		expect(resolvedChoices).toEqual(["ea:always"]);

		const second = await router.route(
			buildExecApprovalCallback("always", 9),
			AUTHORIZED,
		);
		expect(second.kind).toBe("stale");
		expect(resolvedChoices).toEqual(["ea:always"]); // resolver fired ONCE
	});

	it("concurrent double-tap: only one of two racing routes resolves", async () => {
		let resolveDelayMs = 25;
		const { router, approvals, resolvedChoices } = makeRouter({
			onExecApproval: async () => {
				await new Promise((r) => setTimeout(r, resolveDelayMs));
				return "ok";
			},
		});
		approvals.register(12, "sess-d");
		resolveDelayMs = 25;
		const [a, b] = await Promise.all([
			router.route(buildExecApprovalCallback("session", 12), AUTHORIZED),
			router.route(buildExecApprovalCallback("session", 12), AUTHORIZED),
		]);
		const kinds = [a.kind, b.kind].sort();
		expect(kinds).toEqual(["resolved", "stale"]);
		expect(resolvedChoices).toHaveLength(1);
	});
});

describe("consumed buttons stripped from rendered menu", () => {
	it("every RESOLVED outcome carries a host edit with keyboardRemoved", async () => {
		const { router, approvals, slashConfirms, appr, clarify } = makeRouter();
		approvals.register(1, "s1");
		slashConfirms.register(2, "s2");
		appr.register(3, "s3");
		clarify.register(4, "s4");

		const ea = await router.route(
			buildExecApprovalCallback("once", 1),
			AUTHORIZED,
		);
		const sc = await router.route(
			buildSlashConfirmCallback("once", 2),
			AUTHORIZED,
		);
		const ap = await router.route(
			buildWhatsappApprovalCallback(3, "deny"),
			AUTHORIZED,
		);
		const cl = await router.route(buildClarifyCallback(4, 0), AUTHORIZED);

		for (const answer of [ea, sc, ap, cl]) {
			expect(answer.kind).toBe("resolved");
			if (answer.kind === "resolved") {
				expect(answer.hostEdit.keyboardRemoved).toBe(true);
			}
		}
	});

	it("clarify `other` flips to text capture WITHOUT popping; numeric pops", async () => {
		const { router, clarify, resolvedChoices } = makeRouter();
		clarify.register(31, "sess-31");

		const other = await router.route(
			buildClarifyCallback(31, "other"),
			AUTHORIZED,
		);
		expect(other.kind).toBe("resolved");
		expect(clarify.has(31)).toBe(true);
		expect(clarify.isAwaitingText(31)).toBe(true);

		const numeric = await router.route(buildClarifyCallback(31, 1), AUTHORIZED);
		expect(numeric.kind).toBe("resolved");
		expect(resolvedChoices).toContain("cl:31:1");
		expect(clarify.has(31)).toBe(false);
	});
});

describe("stale/expired taps always answered, NEVER dispatched as turns", () => {
	it("unknown-family tap gets an explicit answer with no host edit", async () => {
		const { router, resolvedChoices } = makeRouter();
		const answer = await router.route("zz:garbage:stuff", AUTHORIZED);
		expect(answer.kind).toBe("unknown");
		expect(answer.answerText.length).toBeGreaterThan(0);
		if (answer.kind !== "nav") expect(answer.hostEdit).toBeNull();
		expect(resolvedChoices).toEqual([]);
	});

	it("expired one-shot entries pop but report expiry, not success", async () => {
		let now = 1000;
		const { router, approvals, resolvedChoices } = makeRouter({
			nowMs: () => now,
		});
		approvals.register(50, "sess-50", 2000); // expires at t=2000
		now = 2500;
		const answer = await router.route(
			buildExecApprovalCallback("once", 50),
			AUTHORIZED,
		);
		expect(answer.kind).toBe("stale");
		expect((answer as { answerText: string }).answerText).toContain("expired");
		expect(resolvedChoices).toEqual([]);
		expect(approvals.has(50)).toBe(false); // consumed even so
	});

	it("expired clarify reports expiry on both numeric and other taps", async () => {
		let now = 1000;
		const { router, clarify, resolvedChoices } = makeRouter({
			nowMs: () => now,
		});
		clarify.register(51, "sess-51", 1500);
		now = 3000;
		const other = await router.route(
			buildClarifyCallback(51, "other"),
			AUTHORIZED,
		);
		expect(other.kind).toBe("stale");
		const numeric = await router.route(buildClarifyCallback(51, 0), AUTHORIZED);
		expect(numeric.kind).toBe("stale");
		expect(resolvedChoices).toEqual([]);
	});

	it("EVERY route produces an answer text (spinner always clears)", async () => {
		const inputs = [
			"ea:once:1",
			"sc:once:1",
			"cl:1:0",
			"cl:1:other",
			"cp:0",
			"mp:x",
			"mpv:0",
			"mm:0",
			"mc:0",
			"mb",
			"mx:noop",
			"appr:1:approve",
			"garbage",
		];
		const { router } = makeRouter();
		for (const input of inputs) {
			const answer = await router.route(input, AUTHORIZED);
			expect(typeof answer.answerText).toBe("string");
			expect(answer.answerText.length).toBeGreaterThanOrEqual(1);
		}
	});
});

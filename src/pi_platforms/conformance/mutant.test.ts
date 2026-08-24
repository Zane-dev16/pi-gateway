// pi_platforms/conformance/mutant — NEGATIVE validation of the gate itself
// (roadmap Phase 3 exit requirement: prove the suite HAS TEETH). Each fixture
// below derives from the REFERENCE-CORRECT kit fake and deliberately breaks
// EXACTLY ONE encoded adapter property; the test asserts that >=1 named row
// FAILS with a detail naming the violated invariant, that NO OTHER rows fail
// (specificity), and that the unmutated baseline stays fully green (control).
//
// A mutant that slips through would mean the corresponding row is a
// change-detector-shaped no-op; a row that fails on the baseline would mean
// the gate is miscalibrated. Both directions are asserted here.

import { describe, expect, it } from "vitest";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { CallbackQueryRouter } from "../kit/index.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type { ActionHandlerRegistry } from "../kit/block-kit.js";
import type { ChokepointAuditEntry } from "../../pi_gateway/streaming/egress-door.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import { makeReferenceSubject } from "./reference/reference-adapter.js";
import type { ReferenceSubject } from "./reference/reference-adapter.js";
import { FakePlatformWire } from "./wire.js";
import { SCHEDULER_SYMBOL, type ConformanceSubject } from "./harness.js";
import { buildSharedRows } from "./rows.js";
import { runConformanceSuite, type SuiteReport } from "./runner.js";

// ── the mutation catalog ──────────────────────────────────────────────────
//
// Each entry: the broken property (what a real adapter bug looks like), the
// invariant it violates, and the rows that MUST reject it with a detail
// naming that invariant. `expectedFailures` is an EXACT set — collateral
// failures are part of the contract too (e.g. losing §6.3 chunking necessarily
// also kills the session-scoped formatting ladder, so two rows fire).

type MutationName =
	| "self-echo-forwarded"
	| "clarify-intercept-missing"
	| "interim-bypasses-door"
	| "chunking-lost"
	| "timeout-retried"
	| "plain-send-beside-sealed-stream"
	| "callback-authz-fail-open"
	| "wake-lane-misdeclared";

interface MutantSpec {
	name: MutationName;
	/** The adapter property the fixture breaks. */
	brokenProperty: string;
	/** The spec/DEC anchor whose invariant is violated. */
	violatedInvariant: string;
	/** EXACT set of row ids that must fail, with required detail substrings. */
	expectedFailures: Record<string, string>;
}

const MUTANTS: MutantSpec[] = [
	{
		name: "self-echo-forwarded",
		brokenProperty:
			"self/echo filter absent — bot-authored echoes are relabeled and forwarded into guard ingress",
		violatedInvariant: "§8 ingress: bot echo events filtered BEFORE the guard",
		expectedFailures: {
			"ingress.self-echo-filtered": "bot echo never becomes a turn",
		},
	},
	{
		name: "clarify-intercept-missing",
		brokenProperty:
			"Lane C clarify intercept never armed — free-form answers fall through as burst turns",
		violatedInvariant:
			"§5.3 clarify intercept resolves pending clarify INLINE, never a new turn",
		expectedFailures: {
			"ingress.clarify-intercept":
				"free-form reply routes to clarify resolver inline",
		},
	},
	{
		name: "interim-bypasses-door",
		brokenProperty:
			"interim beats sent through a legacy side channel OUTSIDE the audited chokepoint",
		violatedInvariant:
			"§5.1 single chokepoint: EVERY user-visible send admits through one audited door",
		expectedFailures: {
			"egress.single-chokepoint": "interim still admits through a door",
		},
	},
	{
		name: "chunking-lost",
		brokenProperty:
			"§6.3 split discipline lost — the whole body blasts out as ONE oversized send and the session-scoped formatting ladder is bypassed entirely",
		violatedInvariant:
			"§6.3/A15 length-policy pair governs ALL text delivery + §10.1 probe-once latch lives on the delivery pipeline",
		expectedFailures: {
			"egress.chunk-flood": "long content splits",
			"formatting.downgrade-latch": "capability probe fires ONCE per session",
		},
	},
	{
		name: "timeout-retried",
		brokenProperty:
			"timeout-classified failures are re-sent (duplicate-send risk)",
		violatedInvariant:
			"§6.1 timeouts are NEVER retried — the message may have landed",
		expectedFailures: {
			"egress.timeout-not-retried": "timeout failure surfaces",
		},
	},
	{
		name: "plain-send-beside-sealed-stream",
		brokenProperty:
			"door lost seal-interception — the turn-final PLAIN-SENDS beside the open native stream instead of sealing it",
		violatedInvariant:
			"§5 invariant 4 (DEC-006): beside a sealed stream, reconcile by seal — never a second plain send",
		expectedFailures: {
			"streaming.seal-discipline": "exactly one seal",
		},
	},
	{
		name: "callback-authz-fail-open",
		brokenProperty:
			"router wired with an ALWAYS-TRUE click authorizer — a stranger resolves pending approvals",
		violatedInvariant:
			"§9.1/DEC-017 trust boundary: unauthorized clicks answered ⛔ and NEVER resolved (fail closed)",
		expectedFailures: {
			"interactive.unauthorized-and-consumed":
				"unauthorized tap ignored for resolution",
		},
	},
	{
		name: "wake-lane-misdeclared",
		brokenProperty:
			"wake lane hardcoded inconsistent with the declared async-delivery capability",
		violatedInvariant: "DEC-022: wake lane DERIVES from supportsAsyncDelivery",
		expectedFailures: {
			"wake.lane-declaration-consistent":
				"wake lane derives from supportsAsyncDelivery",
		},
	},
];

// ── subject factories ─────────────────────────────────────────────────────

interface SubjectOpts {
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
	withSecret?: boolean | undefined;
	name?: string | undefined;
}

/** Baseline factory — byte-for-byte the reference wiring (the control). */
function makeBaselineSubject(opts: SubjectOpts = {}): ReferenceSubject {
	const scheduler = new ManualScheduler();
	return makeReferenceSubject({
		wire: new FakePlatformWire(),
		streamIsMessageChatIds: opts.streamIsMessageChatIds,
		withSecret: opts.withSecret,
		name: opts.name,
		spawner: scheduler.spawner,
		scheduler,
	});
}

/**
 * Mutant factory — a fresh reference-correct subject with EXACTLY the listed
 * properties broken. Every unbroken surface delegates transparently.
 */
function makeMutantSubject(
	mutations: ReadonlySet<MutationName>,
	opts: SubjectOpts = {},
): ConformanceSubject {
	return new MutantSubject(makeBaselineSubject(opts), mutations);
}

/**
 * Delegating wrapper over the reference subject. A mutation is ACTIVE only
 * where the broken property can be expressed at the ConformanceSubject
 * boundary; everything else is pass-through, so any row failure beyond the
 * documented set would itself be a gate bug.
 */
class MutantSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: ReferenceSubject["adapter"];
	readonly wire: FakePlatformWire;
	private brokenRouter: CallbackQueryRouter | null = null;

	constructor(
		private readonly base: ReferenceSubject,
		private readonly mutations: ReadonlySet<MutationName>,
	) {
		this.name = `mutant(${[...mutations].sort().join("+") || "none"})`;
		this.adapter = base.adapter;
		this.wire = base.wire;
		// Harness-stamp THIS subject so rows find their deterministic scheduler
		// on the surface they were actually handed.
		const sched = (base as unknown as Record<symbol, unknown>)[
			SCHEDULER_SYMBOL
		];
		if (sched !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] = sched;
		}
	}

	private has(m: MutationName): boolean {
		return this.mutations.has(m);
	}

	// ── observability ──
	doorAudit(): readonly ChokepointAuditEntry[] {
		return this.base.doorAudit();
	}
	turns(): readonly string[] {
		return this.base.turns();
	}
	replies(): readonly string[] {
		return this.base.replies();
	}
	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.base.lifecycleSnapshot();
	}

	// ── ingress lane ──
	deliverInbound(event: IncomingEvent, sessionKey: string): Promise<void> {
		if (!this.has("self-echo-forwarded")) {
			return this.base.deliverInbound(event, sessionKey);
		}
		// MUTANT BUG: no self/echo filter — bot-authored echoes are relabeled
		// like human traffic and handed straight to the guard.
		const forwarded: IncomingEvent =
			event.source === undefined
				? event
				: { ...event, source: { ...event.source, userId: "user-relabeled" } };
		return this.base.deliverInbound(forwarded, sessionKey);
	}
	holdTurnsForBurst(on: boolean): void {
		this.base.holdTurnsForBurst(on);
	}
	armClarifyIntercept(sessionKey: string): void {
		if (this.has("clarify-intercept-missing")) {
			// MUTANT BUG: Lane C intercept silently never arms.
			return;
		}
		this.base.armClarifyIntercept(sessionKey);
	}
	disarmClarifyIntercept(): void {
		this.base.disarmClarifyIntercept();
	}
	clarifyCaptures(): readonly string[] {
		return this.base.clarifyCaptures();
	}

	// ── egress lanes ──
	sendThroughDoor1(
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.base.sendThroughDoor1(chatId, content, metadata);
	}
	sendThroughDoor2(
		logicalPlatform: string,
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.base.sendThroughDoor2(
			logicalPlatform,
			chatId,
			content,
			metadata,
		);
	}
	sendInterim(chatId: string, content: string): Promise<SendResult> {
		if (!this.has("interim-bypasses-door")) {
			return this.base.sendInterim(chatId, content);
		}
		// MUTANT BUG: legacy side-channel send skips the audited chokepoint —
		// no door admission, no marker popping.
		return this.wire.transmitSend(chatId, content, {});
	}
	async deliverLongText(
		chatId: string,
		content: string,
	): Promise<SendResult[]> {
		let results: SendResult[];
		if (this.has("chunking-lost")) {
			// MUTANT BUG: §6.3 split discipline lost — blast the WHOLE body as
			// one raw send regardless of the chat length policy (ladder skipped).
			results = [await this.wire.transmitSend(chatId, content, {})];
		} else {
			results = await this.base.deliverLongText(chatId, content);
		}
		if (this.has("timeout-retried")) {
			const head = results[0];
			if (
				head !== undefined &&
				head.success === false &&
				(head.error ?? "").includes("timed out")
			) {
				// MUTANT BUG: timeout-classified failure treated as retryable —
				// duplicate-send risk the §6.1 ladder explicitly refuses.
				results = this.has("chunking-lost")
					? [await this.wire.transmitSend(chatId, content, {})]
					: await this.base.deliverLongText(chatId, content);
			}
		}
		return results;
	}
	deliverToUtf16Chat(chatId: string, content: string): Promise<SendResult[]> {
		return this.base.deliverToUtf16Chat(chatId, content);
	}
	deliverFormattingRejected(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.base.deliverFormattingRejected(chatId, content);
	}
	transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.base.transientRichFailureOutcome(chatId, content);
	}
	parseFailurePlainResend(chatId: string, content: string): Promise<string> {
		return this.base.parseFailurePlainResend(chatId, content);
	}
	chatPolicyFor(chatId: string): ChatLengthPolicy {
		return this.base.chatPolicyFor(chatId);
	}

	// ── streaming seam ──
	streamAdapter(): StreamEgressAdapter {
		if (!this.has("plain-send-beside-sealed-stream")) {
			return this.base.streamAdapter();
		}
		// MUTANT BUG: the transport LOST seal-interception — DOOR 1 plain-sends
		// every final even when a native draft stream is armed for the chat.
		const seam = this.base.streamAdapter() as StreamEgressAdapter;
		const wire = this.wire;
		const stream: StreamEgressAdapter = {
			requiresEditFinalize: seam.requiresEditFinalize,
			supportsDraftStreaming(chatType, metadata, chatId) {
				return (
					seam.supportsDraftStreaming?.(chatType, metadata, chatId) === true
				);
			},
			send(chatId, content, _replyTo, metadata) {
				void _replyTo;
				return wire.transmitSend(chatId, content, metadata ?? {});
			},
			editMessage(chatId, messageId, content, opts) {
				return seam.editMessage(chatId, messageId, content, opts);
			},
			sendDraft(args) {
				return seam.sendDraft(args);
			},
		};
		return stream;
	}
	armOpenNativeStream(chatId: string, draftId: number): Promise<void> {
		return this.base.armOpenNativeStream(chatId, draftId);
	}
	failNextSeals(n: number): void {
		this.base.failNextSeals(n);
	}

	// ── interactive surfaces ──
	callbackRouter(): CallbackQueryRouter | null {
		if (!this.has("callback-authz-fail-open")) {
			return this.base.callbackRouter();
		}
		if (this.brokenRouter === null) {
			const core = this.base.adapter;
			this.brokenRouter = new CallbackQueryRouter({
				stores: {
					approvals: core.approvals,
					slashConfirms: core.slashConfirms,
					appr: core.appr,
					clarify: core.clarify,
				},
				// MUTANT BUG: DEC-017 trust boundary fail-OPEN — every clicker
				// (strangers included) may resolve pending prompts.
				authorizer: () => true,
				onExecApproval: async () => {
					core.resolvedFamilies.push("ea");
					return "ok";
				},
				onSlashConfirm: async (_sessionKey, _id, choice) => {
					core.resolvedFamilies.push("sc");
					return `ok:${choice}`;
				},
				onClarifyChoice: async (_sessionKey, _id, idx) => {
					core.resolvedFamilies.push("cl");
					return `answer-${idx}`;
				},
				onWhatsappApproval: async () => {
					core.resolvedFamilies.push("appr");
					return "ok";
				},
				onPickerNav: async (parsed) => ({
					answerText: `nav:${parsed.family}`,
					hostEditText: JSON.stringify(parsed),
				}),
			});
		}
		return this.brokenRouter;
	}
	actionRegistry(): ActionHandlerRegistry {
		return this.base.actionRegistry();
	}
	registerApprovalPending(id: number, sessionKey: string): void {
		this.base.registerApprovalPending(id, sessionKey);
	}
	registerSlashConfirmPending(id: number, sessionKey: string): void {
		this.base.registerSlashConfirmPending(id, sessionKey);
	}
	registerClarifyPending(id: number, sessionKey: string): void {
		this.base.registerClarifyPending(id, sessionKey);
	}
	registerApprPending(id: number, sessionKey: string): void {
		this.base.registerApprPending(id, sessionKey);
	}
	setClickerAuthorization(allow: boolean): void {
		this.base.setClickerAuthorization(allow);
	}
	resolvedFamilies(): readonly string[] {
		return this.base.resolvedFamilies();
	}
	resolvedTurnDispatches(): readonly string[] {
		return this.base.resolvedTurnDispatches();
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		return this.base.secondInstanceTokenLockAttempt();
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		return this.base.missingSecretSubjectLifecycle();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		return this.base.resolveEnablementIgnoringProcessEnv(envKey);
	}

	// ── DEC-022 declaration ──
	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		if (!this.has("wake-lane-misdeclared")) {
			return this.base.wakeLaneDeclaration();
		}
		// MUTANT BUG: lane hardcodes the OPPOSITE of the declared capability.
		return this.base.adapter.supportsAsyncDelivery
			? "raw-key-direct"
			: "forged-event";
	}
}

// ── suite drivers ─────────────────────────────────────────────────────────

async function runSuiteWith(
	makeSubject: (opts?: SubjectOpts) => ConformanceSubject,
	subjectName: string,
): Promise<SuiteReport> {
	return runConformanceSuite({
		subjectName,
		shape: "polling",
		rows: buildSharedRows({ makeSubject }),
	});
}

/** Assert the EXACT set of failing rows, each naming the violated invariant. */
function expectExactlyTheseFailures(
	report: SuiteReport,
	expectedFailures: Record<string, string>,
): void {
	const failing = report.rows.filter((r) => !r.pass);
	expect(failing.map((r) => r.id).sort()).toEqual(
		Object.keys(expectedFailures).sort(),
	);
	for (const r of failing) {
		expect(
			r.detail ?? "",
			`row ${r.id} must NAME the violated invariant`,
		).toContain(expectedFailures[r.id]);
	}
}

// ── the tests ─────────────────────────────────────────────────────────────

describe("conformance gate MUTANT validation — the gate has teeth", () => {
	it("CONTROL: the UNMUTATED reference-correct subject passes every encoded shared row", async () => {
		const report = await runSuiteWith(makeBaselineSubject, "baseline-control");
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) {
			console.error(`CONTROL LEAK ${f.id}: ${f.detail}`);
		}
		expect(failures).toEqual([]);
		expect(report.passed).toBe(report.rows.length);
		expect(report.rows.length).toBeGreaterThanOrEqual(20);
	});

	for (const mutant of MUTANTS) {
		const rowIds = Object.keys(mutant.expectedFailures).join(" + ");
		it(`rejects MUTANT ${mutant.name} via ${rowIds}`, async () => {
			const report = await runSuiteWith(
				(opts) => makeMutantSubject(new Set([mutant.name]), opts),
				mutant.name,
			);
			expectExactlyTheseFailures(report, mutant.expectedFailures);
		});
	}

	it("GAUNTLET: ALL mutations at once — every targeted row rejects simultaneously, no undocumented collateral", async () => {
		const allNames = new Set(MUTANTS.map((m) => m.name));
		const report = await runSuiteWith(
			(opts) => makeMutantSubject(allNames, opts),
			"gauntlet-all-mutants",
		);
		const merged: Record<string, string> = {};
		for (const m of MUTANTS) {
			for (const [rowId, fragment] of Object.entries(m.expectedFailures)) {
				merged[rowId] = fragment;
			}
		}
		expectExactlyTheseFailures(report, merged);
		// The gate verdict for such an adapter must be rejection outright.
		expect(report.failed).toBe(Object.keys(merged).length);
		expect(report.failed).toBeGreaterThan(0);
	});

	it("harness sanity: mutants are FRESH subjects per row (a mutation cannot mask itself)", async () => {
		const a = makeMutantSubject(new Set<MutationName>(["chunking-lost"]));
		const b = makeMutantSubject(new Set<MutationName>(["chunking-lost"]));
		expect(a).not.toBe(b);
		expect(a.wire.ops.length).toBe(0);
		expect(b.wire.ops.length).toBe(0);
		void (a as unknown as ConformanceSubject);
		void (b as unknown as ConformanceSubject);
	});
});

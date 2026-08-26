// pi_gateway/guards/busy-policy.ts — registry-derived busy behavior for L2
// (03-message-routing.md §2.2, §5; DEC-005).
//
// VALID_BUSY_POLICIES = {"dispatch", "reject", "interrupt_then_dispatch"},
// default "reject". "queue" is a busy_INPUT_MODE, not a policy. No adapter or
// runner may hand-roll a command list: every busy decision resolves through
// THIS module against ONE command registry (07 §1).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/commands.py:VALID_BUSY_POLICIES / CommandDef.busy_policy
//   hermes_cli/commands.py:resolve_command / _build_command_lookup  → resolveCommand
//   hermes_cli/commands.py:should_bypass_active_session             → shouldBypassActiveSession
//   hermes_cli/commands.py:is_interrupt_then_dispatch               → isInterruptThenDispatch
//   gateway/run.py:_dispatch_busy_slash_command                     → resolveBusyDispatch
//   gateway/run.py:_BUSY_REJECT_TEXT + catch-all                    → byte-stable reject texts

export const BUSY_POLICIES = [
	"dispatch",
	"reject",
	"interrupt_then_dispatch",
] as const;

export type BusyPolicy = (typeof BUSY_POLICIES)[number];

/** DEC-005: the enum default is `reject` — never fall through to interrupt+discard. */
export const DEFAULT_BUSY_POLICY: BusyPolicy = "reject";

/**
 * One registry row (hermes_cli/commands.py:CommandDef, gateway-relevant
 * subset). The full CommandDef schema lands with Phase 2's registry module;
 * guards consume exactly these fields.
 */
export interface CommandDef {
	name: string;
	aliases?: readonly string[];
	busyPolicy?: BusyPolicy;
	/**
	 * Mid-run variant key (gateway/run.py:_dispatch_busy_slash_command step 1):
	 * stop/new/queue/steer/goal/loop/start/egress have special busy handlers;
	 * model/codex-runtime/moa carry command-specific reject texts.
	 */
	busyHandler?: string;
}

export type CommandRegistry = readonly CommandDef[];

/** Effective policy of a row — absent field means the DEC-005 default. */
export function effectiveBusyPolicy(cmd: CommandDef): BusyPolicy {
	return cmd.busyPolicy ?? DEFAULT_BUSY_POLICY;
}

function normalizeCommandName(raw: string): string {
	return raw.toLowerCase().replace(/^\//, "");
}

/**
 * commands.py:_build_command_lookup + resolve_command — map every name AND
 * alias to its row; accept leading slash and any case. First definition wins
 * on collision (registry order), mirroring dict-build semantics.
 */
export function buildCommandLookup(
	registry: CommandRegistry,
): ReadonlyMap<string, CommandDef> {
	const lookup = new Map<string, CommandDef>();
	for (const cmd of registry) {
		if (!lookup.has(cmd.name)) lookup.set(cmd.name, cmd);
		for (const alias of cmd.aliases ?? []) {
			if (!lookup.has(alias)) lookup.set(alias, cmd);
		}
	}
	return lookup;
}

/** Resolve a command name or alias ("reset" → the /new row); null when unknown. */
export function resolveCommand(
	lookup: ReadonlyMap<string, CommandDef>,
	rawName: string | null | undefined,
): CommandDef | null {
	if (!rawName) return null;
	return lookup.get(normalizeCommandName(rawName)) ?? null;
}

/**
 * commands.py:should_bypass_active_session — deliberately broader than the
 * interrupt set: ANY resolvable slash command dispatches inline, never queues,
 * because the runner's safety net discards command text that reaches the
 * pending queue (#5057/#6252/#10370). Unknown "/foo" is NOT resolvable and
 * queues as plain text.
 */
export function shouldBypassActiveSession(
	lookup: ReadonlyMap<string, CommandDef>,
	rawName: string | null | undefined,
): boolean {
	return resolveCommand(lookup, rawName) !== null;
}

/** commands.py:is_interrupt_then_dispatch — Lane A cancel-handoff set (/stop, /new, /reset). */
export function isInterruptThenDispatch(
	lookup: ReadonlyMap<string, CommandDef>,
	rawName: string | null | undefined,
): boolean {
	const cmd = resolveCommand(lookup, rawName);
	return cmd !== null && effectiveBusyPolicy(cmd) === "interrupt_then_dispatch";
}

/**
 * commands.py:ACTIVE_SESSION_BYPASS_COMMANDS analogue — names whose policy is
 * not reject. Registry-derived; never a hand-written list.
 */
export function bypassCommandNames(registry: CommandRegistry): Set<string> {
	const names = new Set<string>();
	for (const cmd of registry) {
		if (effectiveBusyPolicy(cmd) !== "reject") names.add(cmd.name);
	}
	return names;
}

/**
 * run.py:_dispatch_busy_slash_command special busy_handler keys (§5.4 table):
 * stop=new-style hard-kill family, queue=FIFO own-turns, steer=between-tool-
 * calls injection, goal/loop=control-verb whitelists, start=platform ping,
 * egress=status formatter.
 */
export const SPECIAL_BUSY_HANDLERS: ReadonlySet<string> = new Set([
	"start",
	"stop",
	"new",
	"queue",
	"steer",
	"egress",
	"goal",
	"loop",
]);

/** Pre-gate commands answer before access gating (users always see state). */
export const PREGATE_COMMANDS: ReadonlySet<string> = new Set([
	"status",
	"context",
]);

/**
 * run.py:_BUSY_REJECT_TEXT — command-specific mid-run reject texts
 * (busy_policy="reject" with a busy_handler naming an entry here).
 */
export const BUSY_REJECT_TEXT: Readonly<Record<string, string>> = {
	model: "Agent is running — wait or /stop first, then switch models.",
	"codex-runtime":
		"Agent is running — wait or /stop first, then change runtime.",
	moa: "Agent is running — wait or /stop first, then run /moa.",
};

/**
 * run.py:_dispatch_busy_slash_command catch-all tail — BYTE-STABLE. Rejecting
 * is required rather than interrupt+discard (#5057/#6252/#10370).
 */
export function catchAllBusyRejectText(name: string): string {
	return `⏳ Agent is running — \`/${name}\` can't run mid-turn. Wait for the current response or \`/stop\` first.`;
}

export type BusyDispatchKind =
	| "pregate" // /status, /context — answer before access gating
	| "special" // busy_handler mid-run variant
	| "plain" // busy_policy dispatch | interrupt_then_dispatch, no special
	| "reject"; // catch-all (or handler-keyed reject text)

export interface BusyDispatch {
	cmd: CommandDef;
	kind: BusyDispatchKind;
	/** Special/reject handler key when kind routes through one. */
	handlerKey?: string;
	/** Resolved reject text when kind === "reject" (command-specific or catch-all). */
	rejectText?: string;
	policy: BusyPolicy;
}

/**
 * run.py:_dispatch_busy_slash_command resolution ORDER (§5.4):
 *   0. staleness sweep + SLASH-ACCESS GATE (run.py ~17282 — enforced by the
 *      CALLER between pregate and everything else; see guards/slash-access.ts
 *      and RunnerBusyGuard.dispatchBusySlashCommand — this pure resolver has
 *      no event/userId to gate with),
 *   1. pre-gate (/status, /context),
 *   2. busy_handler special (mid-run variant differs from normal handler);
 *      a non-special handler key carrying known reject text rejects with it,
 *   3. busy_policy dispatch|interrupt_then_dispatch plain handler,
 *   4. catch-all reject (default).
 */
export function resolveBusyDispatch(
	lookup: ReadonlyMap<string, CommandDef>,
	rawName: string | null | undefined,
): BusyDispatch | null {
	const cmd = resolveCommand(lookup, rawName);
	if (cmd === null) return null; // unknown → caller treats as plain text
	const policy = effectiveBusyPolicy(cmd);

	if (PREGATE_COMMANDS.has(cmd.name)) {
		return { cmd, kind: "pregate", policy };
	}
	if (cmd.busyHandler) {
		if (SPECIAL_BUSY_HANDLERS.has(cmd.busyHandler)) {
			return { cmd, kind: "special", handlerKey: cmd.busyHandler, policy };
		}
		const rejectText = BUSY_REJECT_TEXT[cmd.busyHandler];
		if (rejectText !== undefined) {
			return {
				cmd,
				kind: "reject",
				handlerKey: cmd.busyHandler,
				rejectText,
				policy,
			};
		}
	}
	if (policy === "dispatch" || policy === "interrupt_then_dispatch") {
		return { cmd, kind: "plain", policy };
	}
	return {
		cmd,
		kind: "reject",
		rejectText: catchAllBusyRejectText(cmd.name),
		policy,
	};
}

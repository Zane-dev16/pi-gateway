// pi_gateway/commands/inject.ts — command execution as USER-MESSAGE
// injection with cache-aware invalidation (07 §1.6).
//
// Rule: commands mutating system-prompt state (skills install, toolset
// toggles) DEFAULT to DEFERRED invalidation — the prompt-cache-dirty mark
// lands at the NEXT session/turn boundary, never mid-turn — with an opt-in
// `--now` flag for immediate invalidation. Canonical pattern:
// `/skills install --now`.
//
// The runner-side integration lands later; THIS module wires the seam:
//   - parse + resolve a raw slash line,
//   - hand the InjectedUserMessage to the caller's inject() sink FIRST,
//   - THEN route invalidation: "--now" → hooks.invalidateNow immediately;
//     otherwise mutating rows queue a DeferredCacheInvalidation into the
//     pending buffer that ONLY a boundary drain delivers.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   07 §1.6 cache-awareness rule; hermes_cli/skills_hub.py do_install /
//   do_reset ("Change will take effect in your next session." / "Use /reset
//   to start a new session now, or --now to apply immediately (invalidates
//   prompt cache).")

import type { CommandDef } from "./command-def.js";
import { extractSlashToken } from "./slash-intake.js";

/** Default opt-in flag name for immediate invalidation (07 §1.6). */
export const NOW_FLAG = "--now";

export interface DeferredCacheInvalidation {
	kind: "deferred";
	/** Canonical command name that mutated prompt state. */
	command: string;
	/** The injected user-message text this invalidation is bound to. */
	injectedText: string;
	reason: "prompt-state-mutation";
}

export interface ImmediateCacheInvalidation {
	kind: "immediate";
	command: string;
	injectedText: string;
	requestedVia: typeof NOW_FLAG;
}

export interface CacheInvalidationHooks {
	/** Opt-out path: immediate prompt-cache invalidation (--now). */
	invalidateNow?(signal: ImmediateCacheInvalidation): void;
	/** Default path: mark dirty at the NEXT session/turn boundary. */
	deferInvalidation?(signal: DeferredCacheInvalidation): void;
}

/**
 * Pending deferred-invalidations awaiting a turn/session boundary. The runner
 * drains this at each drain boundary / session switch and applies the marks;
 * until drained NOTHING is invalidated (cache stays warm mid-turn).
 */
export class DeferredInvalidationBuffer {
	private readonly pending: DeferredCacheInvalidation[] = [];

	push(signal: DeferredCacheInvalidation): void {
		this.pending.push(signal);
	}

	get size(): number {
		return this.pending.length;
	}

	/**
	 * Boundary drain: return and clear every pending mark IN ORDER. The
	 * caller applies them AFTER the current turn's work (never mid-turn).
	 */
	drain(): DeferredCacheInvalidation[] {
		return [...this.pending.splice(0, this.pending.length)];
	}
}

export interface InjectedUserMessage {
	role: "user";
	origin: "slash-execution";
	/** Canonical resolved command. */
	command: string;
	/** Message text delivered to the turn pipeline. */
	text: string;
}

export type InvalidationDisposition = "none" | "deferred" | "immediate";

export type SlashInjectionResult =
	| {
			kind: "text-fallback";
			reason: "no-slash" | "unparseable" | "unknown-command";
			/** Original bytes — treat as plain text per 07 §2. */
			text: string;
			invalidation: Extract<InvalidationDisposition, "none">;
	  }
	| {
			kind: "injected";
			message: InjectedUserMessage;
			argsWithoutFlags: string;
			nowRequested: boolean;
			invalidation: InvalidationDisposition;
	  };

export interface SlashInjectionOptions {
	/**
	 * REQUIRED sink: delivers the injected message into the pipeline (the
	 * runner-side integration replaces this seam later). Called BEFORE any
	 * invalidation signal fires.
	 */
	inject(message: InjectedUserMessage): void;
	hooks?: CacheInvalidationHooks;
	/**
	 * Executor-declared mutation class: does this execution mutate
	 * system-prompt state? Non-mutating executions never invalidate anything.
	 */
	mutatesPromptState?: boolean;
	/** Where deferred marks queue when no hook intercepts them. */
	pendingDeferred?: DeferredInvalidationBuffer;
	/** Alternate flag spelling for tests; defaults to "--now". */
	nowFlag?: string;
}

function stripFlagTokens(
	args: string,
	flag: string,
): {
	rest: string;
	found: boolean;
} {
	if (args.length === 0) return { rest: "", found: false };
	const kept: string[] = [];
	let found = false;
	for (const token of args.split(/\s+/)) {
		if (token === flag) {
			found = true;
			continue;
		}
		kept.push(token);
	}
	return { rest: kept.join(" "), found };
}

/**
 * Execute one slash line as a user-message injection with 07 §1.6
 * cache-awareness. Ordering contract: inject() fires FIRST; only afterwards
 * is invalidation routed (--now → immediate hook; else → deferred buffer).
 */
export function runSlashInjection(
	resolve: (rawName: string | null | undefined) => CommandDef | null,
	line: string,
	options: SlashInjectionOptions,
): SlashInjectionResult {
	const { command, args } = extractSlashToken(line);
	if (command === null) {
		const startsWithSlash = line.replace(/^\s+/, "").startsWith("/");
		return {
			kind: "text-fallback",
			reason: startsWithSlash ? "unparseable" : "no-slash",
			text: line,
			invalidation: "none",
		};
	}
	const cmd = resolve(command);
	if (cmd === null) {
		return {
			kind: "text-fallback",
			reason: "unknown-command",
			text: line,
			invalidation: "none",
		};
	}

	const nowFlag = options.nowFlag ?? NOW_FLAG;
	const { rest, found: nowRequested } = stripFlagTokens(args, nowFlag);

	const message: InjectedUserMessage = {
		role: "user",
		origin: "slash-execution",
		command: cmd.name,
		text: rest.length > 0 ? `/${cmd.name} ${rest}` : `/${cmd.name}`,
	};

	// Injection FIRST — invalidation signals are sequenced after it.
	options.inject(message);

	if (options.mutatesPromptState !== true) {
		return {
			kind: "injected",
			message,
			argsWithoutFlags: rest,
			nowRequested,
			invalidation: "none",
		};
	}

	if (nowRequested && options.hooks?.invalidateNow) {
		options.hooks.invalidateNow({
			kind: "immediate",
			command: cmd.name,
			injectedText: message.text,
			requestedVia: nowFlag as typeof NOW_FLAG,
		});
		return {
			kind: "injected",
			message,
			argsWithoutFlags: rest,
			nowRequested,
			invalidation: "immediate",
		};
	}

	const signal: DeferredCacheInvalidation = {
		kind: "deferred",
		command: cmd.name,
		injectedText: message.text,
		reason: "prompt-state-mutation",
	};
	if (options.hooks?.deferInvalidation) {
		options.hooks.deferInvalidation(signal);
	} else if (options.pendingDeferred) {
		options.pendingDeferred.push(signal);
	}
	return {
		kind: "injected",
		message,
		argsWithoutFlags: rest,
		nowRequested,
		invalidation: "deferred",
	};
}

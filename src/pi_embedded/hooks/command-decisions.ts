// command-decisions.ts — decision-bearing `command:<canonical>` hook
// processing (07-integrations.md §7(a) "Command-hook decision semantics";
// DEC-014). This is the SINGLE interception point where plugin return values
// are HONORED before core handling; every other event class is emit-and-log.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/run.py::process_command (command-hook block)
//     → collectCommandHookResults + processCommandHookResults + runCommandHooks
//   gateway/run.py:is_gateway_known_command gate → knownCommand input
//     (unknown commands / plain text never reach the hook at all)
//   gateway/platforms/base.py:MessageEvent.get_command
//     → extractSlashToken (pi_gateway/commands/slash-intake.ts) for the ONE-HOP
//     rewrite re-parse; canonical RE-RESOLVED from the registry, never
//     re-intercepted (STOP after one hop).
//
// Binding loop (07 §7(a)):
//   non-dict r / {"decision":"allow"} / missing decision → continue (telemetry)
//   {"decision":"deny"[,"message"]}    → dispatch REPLACED by message
//                                        (default "Command `/x` was blocked by a hook.")
//   {"decision":"handled"[,"message"]} → dispatch SKIPPED; message is the reply
//                                        (null ⇒ silent success)
//   {"decision":"rewrite","command_name","raw_args"}
//                                      → text ← "/<name> <raw_args>"; canonical
//                                        RE-RESOLVED; STOP after ONE hop
// first decisive verdict wins; unknown decisions ignored.

import { extractSlashToken } from "../../pi_gateway/commands/slash-intake.js";

export type CommandHookVerdict =
	/** No decisive verdict — proceed to core handling under this canonical. */
	| { kind: "dispatch"; canonical: string }
	/** deny — dispatch replaced by the (defaulted) blocked message. */
	| { kind: "blocked"; message: string }
	/** handled — dispatch skipped; null message ⇒ silent success. */
	| { kind: "handled"; message: string | null }
	/** rewrite — exactly one hop; canonical already re-resolved from the registry. */
	| { kind: "rewritten"; text: string; canonical: string };

/** Minimal structural view of the command registry row (keeps this module decoupled). */
export interface CommandNameRow {
	name: string;
}

export interface ProcessCommandResultsOptions {
	/**
	 * The command token AS TYPED (post extraction: lowercased,
	 * mention-stripped) — drives the deny default message (`/{typed}`).
	 */
	typedCommand: string;
	/** Canonical name of the command being dispatched. */
	canonical: string;
	/**
	 * Live registry resolution used ONLY to re-resolve a rewritten command
	 * name (one hop). Parity of run.py::_resolve_cmd.
	 */
	resolve: (rawName: string | null | undefined) => CommandNameRow | null;
}

/**
 * Process collected hook results in handler order; FIRST decisive verdict
 * wins. Pure over its inputs — no I/O, no clock.
 */
export function processCommandHookResults(
	results: readonly unknown[],
	options: ProcessCommandResultsOptions,
): CommandHookVerdict {
	let typed = options.typedCommand;
	for (const result of results) {
		// isinstance(result, dict) parity — arrays are objects but not dicts.
		if (
			typeof result !== "object" ||
			result === null ||
			Array.isArray(result)
		) {
			continue;
		}
		const record = result as Record<string, unknown>;
		const decision = String(record.decision ?? "")
			.trim()
			.toLowerCase();
		if (decision.length === 0 || decision === "allow") continue;

		if (decision === "deny") {
			const message = record.message;
			return {
				kind: "blocked",
				message:
					typeof message === "string" && message.length > 0
						? message
						: `Command \`/${typed}\` was blocked by a hook.`,
			};
		}
		if (decision === "handled") {
			const message = record.message;
			return {
				kind: "handled",
				message:
					typeof message === "string" && message.length > 0 ? message : null,
			};
		}
		if (decision === "rewrite") {
			const newCommand = String(record.command_name ?? "")
				.trim()
				.replace(/^\/+/, "");
			if (newCommand.length === 0) continue; // empty target is NOT decisive
			const rawArgs = String(record.raw_args ?? "").trim();
			const text = `/${newCommand} ${rawArgs}`.trim();
			// One hop: re-extract the token exactly like event.get_command(),
			// re-resolve canonical from the registry, then STOP — the rewritten
			// command goes straight to core handling and is NOT re-intercepted.
			const token = extractSlashToken(text).command;
			const resolved = token !== null ? options.resolve(token) : null;
			typed = token ?? "";
			return {
				kind: "rewritten",
				text,
				canonical: resolved !== null ? resolved.name : (token ?? ""),
			};
		}
		// Unknown decisions ignored (forward-compat telemetry).
	}
	return { kind: "dispatch", canonical: options.canonical };
}

export interface CollectCommandHookOptions {
	registry: {
		emitCollect(
			eventType: string,
			context?: Record<string, unknown>,
			options?: { log?: (message: string) => void },
		): Promise<unknown[]>;
	};
	canonical: string;
	context?: Record<string, unknown>;
	/** Shared sink for BOTH infra downgrades and per-handler error logs. */
	log?: (message: string) => void;
}

/**
 * Collect `command:<canonical>` results with INFRASTRUCTURE failure
 * containment: if emit_collect itself throws, downgrade to `[]` with a debug
 * log so hook-system failure can never block dispatch (parity of run.py's
 * try/except around self.hooks.emit_collect).
 */
export async function collectCommandHookResults(
	options: CollectCommandHookOptions,
): Promise<unknown[]> {
	try {
		return await options.registry.emitCollect(
			`command:${options.canonical}`,
			options.context,
			options.log !== undefined ? { log: options.log } : {},
		);
	} catch (err) {
		options.log?.(
			`[hooks] command:${options.canonical} hook dispatch failed (non-fatal): ${String(err)}`,
		);
		return [];
	}
}

export interface RunCommandHooksOptions
	extends Omit<ProcessCommandResultsOptions, "resolve"> {
	registry: CollectCommandHookOptions["registry"];
	/** Registry-membership gate (is_gateway_known_command parity). */
	knownCommand: boolean;
	resolve: ProcessCommandResultsOptions["resolve"];
	context?: Record<string, unknown>;
	log?: (message: string) => void;
}

/**
 * The full interception point as the dispatcher invokes it: membership gate →
 * collect (infra-downgraded) → decision processing. Unknown commands / plain
 * text never reach the hook at all — they short-circuit to `dispatch`.
 */
export async function runCommandHooks(
	options: RunCommandHooksOptions,
): Promise<CommandHookVerdict> {
	if (!options.knownCommand || options.typedCommand.length === 0) {
		return { kind: "dispatch", canonical: options.canonical };
	}
	const collectOptions: CollectCommandHookOptions = {
		registry: options.registry,
		canonical: options.canonical,
	};
	if (options.context !== undefined) collectOptions.context = options.context;
	if (options.log !== undefined) collectOptions.log = options.log;
	const results = await collectCommandHookResults(collectOptions);
	return processCommandHookResults(results, {
		typedCommand: options.typedCommand,
		canonical: options.canonical,
		resolve: options.resolve,
	});
}

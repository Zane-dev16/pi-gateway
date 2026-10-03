// pi_gateway/commands/idle-executors.ts — idle-path executor table (DEC-078).
//
// Data shape first: a ReadonlyMap from CANONICAL command name to its idle
// executor. The table owns exactly the commands with a REAL local execution
// on the cold path; absence means passthrough — the original message bytes
// flow into the normal turn untouched (today's behavior, and the 07 §1.4
// unknown-as-text rule for names with no executor).
//
// Real today: /help renders gateway help lines derived from the live
// registry rows (pi names, no Hermes-only rows); /compact runs the REAL host
// AgentSession.compact() through the runner's cached host session (the seam
// blocker 1 asked for); /model resolves through the REAL host CLI resolution
// (resolveCliModel) and switches the cached host session via
// AgentSession.setModel (persist:false — per-session only); /export renders
// the REAL host branch serialization (serializeSessionBranch) INLINE as reply
// text, JSONL by default with an `html` disposition for a minimal HTML
// transcript. Path policy: chat-supplied /export paths never reach the filesystem
// (the host file-writing entry point is never called) — there is no gateway
// path policy for host file exports, so inline bytes are the only form.
// The /new + /resume + /switch-path + /new-path repoints live one layer up
// (guard-wiring switch executors over the binder); this table stays the
// passthrough default for everything else. A passthrough is never an error.

import type { TurnOutcome } from "../../pi_agent_core/runner-types.js";
import type { CommandDef } from "./command-def.js";
import { gatewayHelpLines } from "./derived.js";

/** Structural turn edge executors consume (guard-wiring ChatTurnRunner). */
export interface IdleTurnRunner {
	handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<TurnOutcome>;
	/**
	 * Manual compaction over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.compactSession). Optional — runners
	 * without a host-session cache omit it and /compact stays passthrough.
	 */
	compactSession?:
		| ((
				sessionId: string,
				customInstructions?: string,
			) => Promise<{ summary: string; tokensBefore: number }>)
		| undefined;
	/**
	 * /model over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.setSessionModel). Optional — runners
	 * without a host-session cache omit it and /model stays passthrough.
	 */
	setSessionModel?:
		| ((sessionId: string, modelRef: string) => Promise<{
					provider: string;
					id: string;
			  }>)
		| undefined;
	/** Current model of the cached host session (bare-/model listing). */
	getSessionModel?:
		| ((sessionId: string) => Promise<{ provider: string; id: string }>)
		| undefined;
	/** Catalog /model refs resolve against (catalog order). */
	listAvailableModels?:
		| (() => ReadonlyArray<{ provider: string; id: string }>)
		| undefined;
	/**
	 * /export over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.exportSessionJsonl — INLINE bytes,
	 * never a file write). Optional — without it /export stays passthrough.
	 */
	exportSessionJsonl?: ((sessionId: string) => Promise<string>) | undefined;
}

export interface IdleExecutorContext {
	sessionKey: string;
	/** Dash-repaired args after the command word. */
	args: string;
	/** Live registry rows the reply derives from (help/menus read THESE). */
	rows: readonly CommandDef[];
	runner: IdleTurnRunner;
	/** Original message bytes (passthrough echoes THESE, never a rebuild). */
	eventText: string;
	/**
	 * Binder-resolved host session id (guard-wiring driveSessionId).
	 * Absent ⇒ sessionKey (the pre-hop path where they are identical).
	 */
	hostSessionId?: string | undefined;
}

export type IdleExecutorResult =
	| { kind: "reply"; text: string | null }
	| { kind: "passthrough"; text: string };

export type IdleExecutor = (
	ctx: IdleExecutorContext,
) => Promise<IdleExecutorResult> | IdleExecutorResult;

/** /help: the registry rendered as reply text. Zero model calls. */
export function helpIdleExecutor(ctx: IdleExecutorContext): IdleExecutorResult {
	return {
		kind: "reply",
		text: gatewayHelpLines(ctx.rows).join("\n"),
	};
}

/**
 * /compact: the REAL host compaction over the runner's cached host session.
 * Custom instructions ride on the args tail ("/compact focus on the API");
 * a runner without the seam stays passthrough on original bytes. A host
 * refusal (session too small) renders as reply text — never a throw, never
 * silence — so the user sees why nothing compacted and no turn is consumed.
 */
export async function compactIdleExecutor(
	ctx: IdleExecutorContext,
): Promise<IdleExecutorResult> {
	const runner = ctx.runner;
	if (runner.compactSession === undefined) {
		return { kind: "passthrough", text: ctx.eventText };
	}
	const sessionId = ctx.hostSessionId ?? ctx.sessionKey;
	const instructions = ctx.args.trim();
	try {
		const result =
			instructions === ""
				? await runner.compactSession(sessionId)
				: await runner.compactSession(sessionId, instructions);
		return {
			kind: "reply",
			text: `Compacted ${result.tokensBefore} tokens of context.\n\n${result.summary}`,
		};
	} catch (err) {
		const reason = err instanceof Error ? err.message : String(err);
		return { kind: "reply", text: `Compaction failed: ${reason}` };
	}
}

/**
 * /model: the REAL host model switch over the runner's cached host session.
 * With a ref ("/model provider/id" or a bare id) the runner resolves and
 * switches; bare "/model" lists the current plus the available catalog.
 * A runner without the seam stays passthrough on original bytes. A host
 * refusal (unknown/ambiguous ref, missing auth) renders as reply text —
 * never a throw, never silence — and no turn is consumed.
 */
export async function modelIdleExecutor(
	ctx: IdleExecutorContext,
): Promise<IdleExecutorResult> {
	const runner = ctx.runner;
	if (runner.setSessionModel === undefined) {
		return { kind: "passthrough", text: ctx.eventText };
	}
	const sessionId = ctx.hostSessionId ?? ctx.sessionKey;
	const ref = ctx.args.trim();
	if (ref === "") {
		if (
			runner.getSessionModel === undefined ||
			runner.listAvailableModels === undefined
		) {
			return { kind: "passthrough", text: ctx.eventText };
		}
		try {
			const [current, available] = await Promise.all([
				runner.getSessionModel(sessionId),
				Promise.resolve(runner.listAvailableModels()),
			]);
			const names = available.map((m) => `${m.provider}/${m.id}`);
			return {
				kind: "reply",
				text: `Current model: ${current.provider}/${current.id}\nAvailable: ${names.join(", ")}`,
			};
		} catch (err) {
			const reason = err instanceof Error ? err.message : String(err);
			return { kind: "reply", text: `Model lookup failed: ${reason}` };
		}
	}
	try {
		const switched = await runner.setSessionModel(sessionId, ref);
		return {
			kind: "reply",
			text: `Model: ${switched.provider}/${switched.id}`,
		};
	} catch (err) {
		const reason = err instanceof Error ? err.message : String(err);
		return { kind: "reply", text: `Model switch failed: ${reason}` };
	}
}

/**
 * /export: the REAL host branch serialization over the runner's cached host
 * session, rendered INLINE as reply text. Format is a disposition on the
 * args tail (host TUI /export parity: `.jsonl` ⇒ JSONL, otherwise HTML) —
 * bare `/export` (or an explicit `.jsonl`) returns the JSONL branch bytes;
 * `/export html` (or `*.html`) returns a minimal self-contained HTML
 * transcript rendered from those same bytes. Chat-supplied path args never
 * reach the filesystem — the host file-writing entry points (and its rich
 * exportSessionToHtml, which needs a session FILE the gateway's in-memory
 * host sessions do not have) are never called. A runner without the seam
 * stays passthrough on original bytes. Delivery rides the existing reply-text
 * send path; a real `.html` file attachment needs a document disposition on
 * IdleExecutorResult plus platform senders — that seam does not exist yet.
 */
export async function exportIdleExecutor(
	ctx: IdleExecutorContext,
): Promise<IdleExecutorResult> {
	const runner = ctx.runner;
	if (runner.exportSessionJsonl === undefined) {
		return { kind: "passthrough", text: ctx.eventText };
	}
	const sessionId = ctx.hostSessionId ?? ctx.sessionKey;
	try {
		const jsonl = await runner.exportSessionJsonl(sessionId);
		const arg = ctx.args.trim().toLowerCase();
		if (arg === "html" || arg.endsWith(".html")) {
			return { kind: "reply", text: renderExportHtml(jsonl) };
		}
		return { kind: "reply", text: jsonl };
	} catch (err) {
		const reason = err instanceof Error ? err.message : String(err);
		return { kind: "reply", text: `Export failed: ${reason}` };
	}
}

/** Escape once for HTML text content (order matters: & first). */
function escapeHtml(s: string): string {
	return s
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;");
}

/**
 * Minimal self-contained HTML transcript from JSONL branch bytes. One card
 * per entry (role or type as the heading, string content or the raw entry
 * JSON as the body); unparsable lines ride as preformatted raw text so a
 * half-written branch still exports. Kept minimal on purpose — the host's
 * rich template needs a session FILE plus file delivery, neither of which
 * exists on this path.
 */
export function renderExportHtml(jsonl: string): string {
	const cards = jsonl
		.split("\n")
		.filter((line) => line.trim() !== "")
		.map((line) => {
			let entry: Record<string, unknown>;
			try {
				entry = JSON.parse(line) as Record<string, unknown>;
			} catch {
				return `<article class="entry"><pre>${escapeHtml(line)}</pre></article>`;
			}
			const role =
				typeof entry["role"] === "string"
					? (entry["role"] as string)
					: typeof entry["type"] === "string"
						? (entry["type"] as string)
						: "entry";
			const content = entry["content"];
			const body =
				typeof content === "string" ? content : JSON.stringify(entry);
			return `<article class="entry"><h2>${escapeHtml(role)}</h2><pre>${escapeHtml(body)}</pre></article>`;
		})
		.join("\n");
	return `<!DOCTYPE html>\n<html><head><meta charset="utf-8"><title>Session export</title></head><body>\n${cards}\n</body></html>\n`;
}

/**
 * The idle executor table over live rows. /help answers locally; /compact
 * compacts through the runner seam; /model switches through the runner seam;
 * /export renders through the runner seam; every other known command falls
 * through to the model turn on its original bytes until its real seam lands.
 */
export function buildIdleExecutors(
	_rows: readonly CommandDef[],
): ReadonlyMap<string, IdleExecutor> {
	return new Map<string, IdleExecutor>([
		["help", helpIdleExecutor],
		["compact", compactIdleExecutor],
		["model", modelIdleExecutor],
		["export", exportIdleExecutor],
	]);
}

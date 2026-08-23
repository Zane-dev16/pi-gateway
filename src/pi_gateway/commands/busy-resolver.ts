// pi_gateway/commands/busy-resolver.ts — the L2 busy-policy resolver derived
// FROM the one registry (DEC-005: "L2 per-command busy behavior comes only
// from the command registry"; 07 §1.2 consumer table rows "Gateway busy
// dispatch" / "Gateway interrupt-class routing" / "L2 busy policy").
//
// This module derives the L2 INPUT: it projects registry rows into the
// minimal shape the runner/guard busy machinery consumes and exposes the
// resolution predicates over the LIVE lookup. The dispatch EXECUTION itself
// (special handler tables, FIFO enqueueing, catch-all reject emission) stays
// in pi_gateway/guards — fed exclusively through THIS derivation; no consumer
// may hand-roll a command list.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/commands.py:_build_command_lookup      → buildBusyLookup
//   hermes_cli/commands.py:resolve_command            → BusyResolver.resolve
//   hermes_cli/commands.py:should_bypass_active_session
//                                                     → BusyResolver.shouldBypassActiveSession
//   hermes_cli/commands.py:is_interrupt_then_dispatch → BusyResolver.isInterruptThenDispatch
//   hermes_cli/commands.py:ACTIVE_SESSION_BYPASS_COMMANDS
//                                                     → BusyResolver.bypassCommandNames

import {
	type BusyPolicy,
	type CommandDef,
	effectiveBusyPolicy,
} from "./command-def.js";

/**
 * The guard-consumed projection of a registry row (guards' CommandDef shape).
 * Null/absent busy_handler is omitted — the projected type carries a string
 * or nothing, matching the guard feed under exactOptionalPropertyTypes.
 */
export interface GuardCommandRow {
	name: string;
	aliases?: readonly string[];
	busyPolicy?: BusyPolicy;
	busyHandler?: string;
}

/**
 * Minimal structural row the resolver reasons over — both full CommandDefs
 * and GuardCommandRows are assignable to it.
 */
interface ResolverRow {
	name: string;
	aliases?: readonly string[];
	busyPolicy?: BusyPolicy;
}

/**
 * Project registry rows into the exact fields the L2 guard machinery reads.
 * Extra schema fields never leak into the guard feed.
 */
export function toGuardRows(
	rows: readonly CommandDef[],
): readonly GuardCommandRow[] {
	return rows.map((cmd) => ({
		name: cmd.name,
		...(cmd.aliases !== undefined && cmd.aliases.length > 0
			? { aliases: [...cmd.aliases] }
			: {}),
		...(cmd.busyPolicy !== undefined ? { busyPolicy: cmd.busyPolicy } : {}),
		...(cmd.busyHandler != null ? { busyHandler: cmd.busyHandler } : {}),
	}));
}

/** commands.py:_build_command_lookup — name/alias → row; tolerant first-wins on raw arrays. */
export function buildBusyLookup<
	R extends { name: string; aliases?: readonly string[] },
>(rows: readonly R[]): ReadonlyMap<string, R> {
	const lookup = new Map<string, R>();
	for (const cmd of rows) {
		if (!lookup.has(cmd.name)) lookup.set(cmd.name, cmd);
		for (const alias of cmd.aliases ?? []) {
			if (!lookup.has(alias)) lookup.set(alias, cmd);
		}
	}
	return lookup;
}

function normalize(rawName: string): string {
	return rawName.toLowerCase().replace(/^\/+/, "");
}

/**
 * Registry-derived L2 resolver. Build against a LIVE lookup so runtime
 * registrations are visible without rebuilding consumers.
 */
export class BusyResolver {
	private constructor(
		private readonly lookup: ReadonlyMap<string, ResolverRow>,
	) {}

	/** Derive from registry/guard rows (builds the name+alias index). */
	static fromRows(
		rows: readonly (CommandDef | GuardCommandRow)[],
	): BusyResolver {
		return new BusyResolver(buildBusyLookup(rows));
	}

	/** Wrap an existing live lookup (e.g. CommandRegistry.lookup()). */
	static fromLookup(
		lookup: ReadonlyMap<
			string,
			{ name: string; aliases?: readonly string[]; busyPolicy?: BusyPolicy }
		>,
	): BusyResolver {
		return new BusyResolver(lookup);
	}

	/** Resolve any name/alias ("reset" → the /new row); null when unknown. */
	resolve(rawName: string | null | undefined): GuardCommandRow | null {
		if (!rawName) return null;
		return this.lookup.get(normalize(rawName)) ?? null;
	}

	policyOf(rawName: string | null | undefined): BusyPolicy | null {
		const cmd = this.resolve(rawName);
		return cmd === null ? null : effectiveBusyPolicy(cmd);
	}

	/** ANY resolvable command bypasses queueing (#5057); unknown "/foo" does NOT. */
	shouldBypassActiveSession(rawName: string | null | undefined): boolean {
		return this.resolve(rawName) !== null;
	}

	isInterruptThenDispatch(rawName: string | null | undefined): boolean {
		return this.policyOf(rawName) === "interrupt_then_dispatch";
	}

	/** commands.py:ACTIVE_SESSION_BYPASS_COMMANDS — canonical names with policy ≠ reject. */
	bypassCommandNames(): Set<string> {
		const names = new Set<string>();
		for (const cmd of new Set(this.lookup.values())) {
			if (effectiveBusyPolicy(cmd) !== "reject") {
				names.add(cmd.name);
			}
		}
		return names;
	}
}

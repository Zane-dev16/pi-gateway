// pi_gateway/commands/config-gates.ts — gateway_config_gate resolution (07 §1.3).
//
// A command may be cli_only AND carry a gateway_config_gate dotpath. It is
// then ALWAYS a member of the gateway known-command set (dispatchable — the
// handler re-checks the gate at runtime) but only SURFACES on gateway help /
// menus when the config value is truthy. Gate evaluation is PURE here: the
// raw-config reader is injected, and ANY reader failure degrades to the empty
// gate set (help/menus silently hide gated commands rather than crash).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/commands.py:_resolve_config_gates  → resolveConfigGates
//   hermes_cli/commands.py:_is_gateway_available  → isGatewayAvailable
//   utils.py:is_truthy_value                      → isTruthyValue (command-def.ts)

import { type CommandDef, isTruthyValue } from "./command-def.js";

/** Reads the RAW user config (no schema application). May throw. */
export type RawConfigReader = () => unknown;

/**
 * Walk a dot-separated path through plain-object nodes only
 * (commands.py `_resolve_config_gates` inner walk: `isinstance(val, dict)`
 * → `val.get(key)`, else None). Missing keys yield undefined.
 */
export function walkConfigDotPath(config: unknown, dotPath: string): unknown {
	let value = config;
	for (const key of dotPath.split(".")) {
		if (value === null || typeof value !== "object" || Array.isArray(value)) {
			return undefined;
		}
		value = (value as Record<string, unknown>)[key];
	}
	return value;
}

/**
 * commands.py:_resolve_config_gates — canonical names of rows whose
 * gateway_config_gate is currently truthy. Reads raw config ONCE per call;
 * any read error degrades to an EMPTY set (closed-safe). Rows without a gate
 * never enter the result.
 */
export function resolveConfigGates(
	rows: readonly CommandDef[],
	readRawConfig: RawConfigReader,
): Set<string> {
	const gated = rows.filter((cmd) => Boolean(cmd.gatewayConfigGate));
	if (gated.length === 0) return new Set();
	let config: unknown;
	try {
		config = readRawConfig();
	} catch {
		return new Set();
	}
	const open = new Set<string>();
	for (const cmd of gated) {
		const value = walkConfigDotPath(config, cmd.gatewayConfigGate ?? "");
		if (isTruthyValue(value)) open.add(cmd.name);
	}
	return open;
}

/**
 * commands.py:_is_gateway_available — THE single availability predicate used
 * by every gateway surface (help lines, menu model, completion filtering):
 * unconditionally visible unless cli_only; cli_only rows need an OPEN gate.
 * Pass `configOverrides` (from resolveConfigGates) to share one read across
 * all consumers; omitting it re-evaluates gates only for gated rows via the
 * injected reader.
 */
export function isGatewayAvailable(
	cmd: CommandDef,
	configOverrides?: Set<string> | null,
	readRawConfig?: RawConfigReader,
): boolean {
	if (!cmd.cliOnly) return true;
	const gate = cmd.gatewayConfigGate;
	if (!gate) return false;
	if (configOverrides !== undefined && configOverrides !== null) {
		return configOverrides.has(cmd.name);
	}
	if (readRawConfig === undefined) return false;
	try {
		return isTruthyValue(walkConfigDotPath(readRawConfig(), gate));
	} catch {
		return false;
	}
}

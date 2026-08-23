// pi_gateway/commands/command-def.ts — the ONE slash-command schema (07 §1.1).
//
// All slash commands live in one frozen central registry; every downstream
// consumer (help, known-set, menus, completions, busy policies) DERIVES from
// rows of exactly this shape — zero per-surface hardcoded lists (07 §1.2).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/commands.py:CommandDef            → CommandDef
//   hermes_cli/commands.py:VALID_BUSY_POLICIES   → BUSY_POLICIES / VALID_BUSY_POLICIES
//   utils.py:TRUTHY_STRINGS / is_truthy_value    → TRUTHY_STRINGS / isTruthyValue
//
// Field-for-field parity with the Python dataclass; camelCase follows the
// existing spine convention (guards/busy-policy.ts already ports
// busy_policy → busyPolicy). name/description/category are required
// positional fields in the dataclass and stay required here.

/** DEC-005 / 07 §1.1: the busy-policy enum is EXACTLY these three values. */
export const BUSY_POLICIES = [
	"dispatch",
	"reject",
	"interrupt_then_dispatch",
] as const;

export type BusyPolicy = (typeof BUSY_POLICIES)[number];

/** commands.py:VALID_BUSY_POLICIES frozenset analogue. */
export const VALID_BUSY_POLICIES: ReadonlySet<string> = new Set(BUSY_POLICIES);

/** DEC-005: absent busyPolicy means "reject" — never fall through to a queue. */
export const DEFAULT_BUSY_POLICY: BusyPolicy = "reject";

/**
 * One canonical registry row (07 §1.1 field-for-field).
 *
 * - `name` is the canonical name WITHOUT the leading slash ("background").
 * - `gatewayConfigGate` names a config dotpath ("skills.github.enabled");
 *   a truthy value opens the gate so a cli_only command surfaces on gateway
 *   help/menus while remaining routable EITHER WAY (07 §1.3).
 * - `busyHandler` keys the runner's L2 special-handler table or a
 *   command-specific reject text; `execute` keys the shared core-text
 *   executor (surface-independent formatter).
 */
export interface CommandDef {
	readonly name: string;
	readonly description: string;
	readonly category: string;
	readonly aliases?: readonly string[];
	readonly argsHint?: string;
	readonly subcommands?: readonly string[];
	readonly cliOnly?: boolean;
	readonly gatewayOnly?: boolean;
	readonly gatewayConfigGate?: string | null;
	readonly busyPolicy?: BusyPolicy;
	readonly busyHandler?: string | null;
	readonly execute?: string | null;
}

/** Effective policy of a row — absent field means the DEC-005 default.
 * Structural minimum: any registry-ish row shape works. */
export function effectiveBusyPolicy(cmd: {
	busyPolicy?: BusyPolicy;
}): BusyPolicy {
	return cmd.busyPolicy ?? DEFAULT_BUSY_POLICY;
}

export function aliasesOf(cmd: CommandDef): readonly string[] {
	return cmd.aliases ?? [];
}

export function argsHintOf(cmd: CommandDef): string {
	return cmd.argsHint ?? "";
}

export function subcommandsOf(cmd: CommandDef): readonly string[] {
	return cmd.subcommands ?? [];
}

// -- registration-time validation -------------------------------------------
//
// Hermes' registry is hand-authored, so the dataclass validates nothing; the
// Pi port exposes a runtime registration API (plugins, tests), and a bad row
// must be REJECTED loudly rather than silently poisoning every derived
// consumer. Validation only — no behavioral divergence.

export class CommandDefValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "CommandDefValidationError";
	}
}

/**
 * A canonical command token: slash-free (valid command names never contain
 * "/" — base.py:get_command), no whitespace, lowercase, never empty.
 */
export function isValidCommandToken(token: string): boolean {
	return (
		token.length > 0 &&
		!token.includes("/") &&
		!/\s/.test(token) &&
		token === token.toLowerCase()
	);
}

/**
 * Validate one row's shape before it may enter the registry. Throws
 * CommandDefValidationError on any violation; returns the row unchanged.
 */
export function validateCommandDef(cmd: CommandDef): CommandDef {
	if (typeof cmd.name !== "string" || !isValidCommandToken(cmd.name)) {
		throw new CommandDefValidationError(
			`command name must be a lowercase slash-free token without whitespace, got ${JSON.stringify(cmd.name)}`,
		);
	}
	if (typeof cmd.description !== "string" || cmd.description.length === 0) {
		throw new CommandDefValidationError(
			`/${cmd.name}: description is a required non-empty string`,
		);
	}
	if (typeof cmd.category !== "string" || cmd.category.length === 0) {
		throw new CommandDefValidationError(
			`/${cmd.name}: category is a required non-empty string`,
		);
	}
	if (
		cmd.busyPolicy !== undefined &&
		!VALID_BUSY_POLICIES.has(cmd.busyPolicy)
	) {
		throw new CommandDefValidationError(
			`/${cmd.name}: busy_policy ${JSON.stringify(cmd.busyPolicy)} not in {${[...VALID_BUSY_POLICIES].join(", ")}}`,
		);
	}
	const seenAliases = new Set<string>([cmd.name]);
	for (const alias of aliasesOf(cmd)) {
		if (!isValidCommandToken(alias)) {
			throw new CommandDefValidationError(
				`/${cmd.name}: alias ${JSON.stringify(alias)} must be a lowercase slash-free token without whitespace`,
			);
		}
		if (seenAliases.has(alias)) {
			throw new CommandDefValidationError(
				`/${cmd.name}: duplicate alias ${JSON.stringify(alias)} within one definition`,
			);
		}
		seenAliases.add(alias);
	}
	if (
		cmd.gatewayConfigGate !== undefined &&
		cmd.gatewayConfigGate !== null &&
		(typeof cmd.gatewayConfigGate !== "string" ||
			cmd.gatewayConfigGate.length === 0)
	) {
		throw new CommandDefValidationError(
			`/${cmd.name}: gateway_config_gate must be a non-empty dotpath or null`,
		);
	}
	return cmd;
}

// -- shared truthiness (config gate values) ---------------------------------

/** utils.py:TRUTHY_STRINGS — the project's shared truthy string set. */
export const TRUTHY_STRINGS: ReadonlySet<string> = new Set([
	"1",
	"true",
	"yes",
	"on",
]);

/** utils.py:is_truthy_value — bool passthrough; strings via TRUTHY_STRINGS. */
export function isTruthyValue(value: unknown, defaultValue = false): boolean {
	if (value === null || value === undefined) return defaultValue;
	if (typeof value === "boolean") return value;
	if (typeof value === "string") {
		return TRUTHY_STRINGS.has(value.trim().toLowerCase());
	}
	return Boolean(value);
}

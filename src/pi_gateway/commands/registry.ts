// pi_gateway/commands/registry.ts — the ONE frozen central registry (07 §1).
//
// Hermes parity: hermes_cli/commands.py:COMMAND_REGISTRY + _COMMAND_LOOKUP +
// resolve_command. The reference builds its lookup silently at import time
// (hand-authored rows); the Pi port adds a runtime registration API (plugins,
// tests) and therefore enforces at REGISTRATION TIME what dict-build would
// silently resolve: canonical-name uniqueness and bidirectional alias
// uniqueness — a collision REJECTS, it never overwrites (07 §9: no hand-built
// command lists; one registry, one name per token).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/commands.py:_build_command_lookup → CommandRegistry.lookup / register
//   hermes_cli/commands.py:resolve_command       → CommandRegistry.resolve

import {
	type BusyPolicy,
	CommandDefValidationError,
	type CommandDef,
	effectiveBusyPolicy,
	validateCommandDef,
} from "./command-def.js";

/** Thrown when a registration would shadow an existing name or alias. */
export class RegistryCollisionError extends Error {
	constructor(
		/** The conflicting token (name or alias). */
		readonly token: string,
		/** Canonical name of the row already owning the token. */
		readonly owner: string,
		/** Canonical name of the incoming row. */
		readonly incoming: string,
	) {
		super(
			`registry collision: "${token}" is already owned by /${owner}; refusing to overwrite it with /${incoming}`,
		);
		this.name = "RegistryCollisionError";
	}
}

/** Thrown when registering into a frozen registry. */
export class RegistryFrozenError extends Error {
	constructor() {
		super("registry is frozen; slash commands cannot be added anymore");
		this.name = "RegistryFrozenError";
	}
}

export interface RegisterOptions {
	/**
	 * Allow re-registering a byte-identical definition (same canonical row)
	 * idempotently. Default false — any collision rejects.
	 */
	idempotent?: boolean;
}

function sameRow(a: CommandDef, b: CommandDef): boolean {
	return (
		a === b ||
		(a.name === b.name &&
			a.description === b.description &&
			a.category === b.category &&
			JSON.stringify(a.aliases ?? []) === JSON.stringify(b.aliases ?? []))
	);
}

/**
 * The single source of truth for slash commands. Derived consumers take
 * `rows()` snapshots or the live `lookup()` map — they never hold per-surface
 * command lists.
 */
export class CommandRegistry {
	private readonly list: CommandDef[] = [];
	private readonly index = new Map<string, CommandDef>();
	private locked = false;

	constructor(rows?: readonly CommandDef[]) {
		if (rows !== undefined) this.registerMany(rows);
	}

	/** Built-and-frozen convenience for the shipped builtin set. */
	static frozen(rows: readonly CommandDef[]): CommandRegistry {
		const registry = new CommandRegistry(rows);
		registry.freeze();
		return registry;
	}

	get frozen(): boolean {
		return this.locked;
	}

	get size(): number {
		return this.list.length;
	}

	/**
	 * Register one row. Rejects (never overwrites):
	 * - a canonical name colliding with ANY existing name or alias;
	 * - an alias colliding with ANY existing name or alias.
	 * Validation errors (CommandDefValidationError) reject before insertion.
	 */
	register(cmd: CommandDef, options?: RegisterOptions): void {
		validateCommandDef(cmd);
		const aliases = cmd.aliases ?? [];
		const tokens = [cmd.name, ...aliases];
		for (const token of tokens) {
			const owner = this.index.get(token);
			if (owner === undefined) continue;
			if (
				options?.idempotent === true &&
				token === cmd.name &&
				sameRow(owner, cmd)
			) {
				return;
			}
			throw new RegistryCollisionError(token, owner.name, cmd.name);
		}
		if (this.locked) throw new RegistryFrozenError();
		this.list.push(cmd);
		for (const token of tokens) this.index.set(token, cmd);
	}

	registerMany(rows: readonly CommandDef[], options?: RegisterOptions): void {
		for (const cmd of rows) this.register(cmd, options);
	}

	/** Lock the registry against further registration. */
	freeze(): void {
		this.locked = true;
	}

	/** Snapshot of every registered row, in registration order. */
	rows(): readonly CommandDef[] {
		return [...this.list];
	}

	/**
	 * LIVE name/alias → row map (commands.py:_COMMAND_LOOKUP analogue).
	 * Every key maps to its owning row; consumers must derive from THIS map,
	 * never from a copied list captured earlier.
	 */
	lookup(): ReadonlyMap<string, CommandDef> {
		return this.index;
	}

	/**
	 * commands.py:resolve_command — resolve a name or alias to its row.
	 * Accepts names with or without the leading slash (ALL leading slashes are
	 * stripped, lstrip("/") semantics); case-insensitive. Null when unknown.
	 */
	resolve(rawName: string | null | undefined): CommandDef | null {
		if (!rawName) return null;
		const normalized = rawName.toLowerCase().replace(/^\/+/, "");
		if (normalized.length === 0) return null;
		return this.index.get(normalized) ?? null;
	}

	/** Effective busy policy of a resolved name (DEC-005 default "reject"). */
	busyPolicyOf(rawName: string | null | undefined): BusyPolicy | null {
		const cmd = this.resolve(rawName);
		return cmd === null ? null : effectiveBusyPolicy(cmd);
	}
}

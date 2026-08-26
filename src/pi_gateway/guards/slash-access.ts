// pi_gateway/guards/slash-access.ts — the SECOND authz axis for slash
// commands (06 §2 admission vs command permissions; gap-audit R14): of the
// users ALLOWED to talk to the gateway, which ones can run WHICH slash
// commands. This sits beside the per-platform allow_from lane — it never
// gates plain chat, only registry-resolvable command dispatch.
//
// Enforcement topology (07 §2 flow): the resolved gate runs BETWEEN the
// status/context pre-gate and command dispatch on BOTH the running-agent
// fast-path and the cold path, so admin/user gating can't be bypassed by an
// in-flight agent. Byte-stable denial text lives HERE and nowhere else.
//
// Backward compatibility is the load-bearing property: when a scope has NO
// allow_admin_from list, gating is DISABLED for that scope and every allowed
// user keeps every command — existing installs opt in by listing one admin.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/slash_access.py:_ALWAYS_ALLOWED_FOR_USERS    → ALWAYS_ALLOWED_USER_COMMANDS
//   gateway/slash_access.py:SlashAccessPolicy            → SlashAccessPolicy (+isAdmin/canRun)
//   gateway/slash_access.py:_coerce_id_list              → coerceIdSet
//   gateway/slash_access.py:_coerce_command_list         → coerceCommandSet
//   gateway/slash_access.py:_scope_for_chat_type         → scopeForChatType
//   gateway/slash_access.py:_keys_for_scope              → keysForScope
//   gateway/slash_access.py:policy_from_extra            → policyFromExtra
//   gateway/slash_access.py:policy_for_source            → policyForSource
//   gateway/run.py:_check_slash_access                   → checkSlashAccess /
//                                                          denialText (byte-stable)
//
// Call-site parity map (run.py): ~17282 running-agent fast-path (after the
// status/context pre-gate, before _dispatch_busy_slash_command),
// ~17507 cold path (registry-known commands, after alias expansion,
// before built-in dispatch), ~17966 quick-command sink (#44727 — raw typed
// name). pi wires the fast-path site through RunnerBusyGuard.dispatchBusySlashCommand
// and exposes the cold-path site as RunnerBusyGuard.checkColdPathSlashAccess.

/**
 * slash_access.py:_ALWAYS_ALLOWED_FOR_USERS — commands ANY allowed user can
 * always run while gated, so a non-admin can discover what they may do
 (/help, /whoami). Additive floor only: user_allowed_commands never
 * restricts these, operators narrow the floor by listing their own set.
 */
export const ALWAYS_ALLOWED_USER_COMMANDS: ReadonlySet<string> = new Set([
	"help",
	"whoami",
]);

/** Resolved access policy for one (platform, scope) pair. */
export interface SlashAccessPolicy {
	/** Gating active for this scope (an admin list exists)? */
	enabled: boolean;
	adminUserIds: ReadonlySet<string>;
	userAllowedCommands: ReadonlySet<string>;
}

const EMPTY_STRING_SET: ReadonlySet<string> = new Set();

/** slash_access.py disabled policy — gating off, everything allowed. */
export const SLASH_ACCESS_DISABLED: SlashAccessPolicy = Object.freeze({
	enabled: false,
	adminUserIds: EMPTY_STRING_SET,
	userAllowedCommands: EMPTY_STRING_SET,
});

/** slash_access.py:SlashAccessPolicy.is_admin. */
export function isSlashAdmin(
	policy: SlashAccessPolicy,
	userId: string | null | undefined,
): boolean {
	if (!policy.enabled) return true;
	if (!userId) return false;
	return policy.adminUserIds.has(String(userId));
}

/** slash_access.py:SlashAccessPolicy.can_run. */
export function canRunSlashCommand(
	policy: SlashAccessPolicy,
	userId: string | null | undefined,
	canonicalCmd: string | null | undefined,
): boolean {
	if (!policy.enabled) return true;
	if (isSlashAdmin(policy, userId)) return true;
	if (!canonicalCmd) return false;
	if (ALWAYS_ALLOWED_USER_COMMANDS.has(canonicalCmd)) return true;
	return policy.userAllowedCommands.has(canonicalCmd);
}

/**
 * slash_access.py:_coerce_id_list — normalize a YAML-loaded admin/user list
 * into a set of strings. Accepts null/undefined, arrays/sets, or a
 * comma-separated string; scalars wrap into a single entry. Entries are
 * stringified, trimmed; empties dropped.
 */
export function coerceIdSet(raw: unknown): ReadonlySet<string> {
	return coerceStringSet(raw, false);
}

/**
 * slash_access.py:_coerce_command_list — normalize a command allowlist.
 * Strips leading slashes (["/help"] ≡ ["help"]) and lowercases to match how
 * resolve stores names.
 */
export function coerceCommandSet(raw: unknown): ReadonlySet<string> {
	return coerceStringSet(raw, true);
}

function coerceStringSet(
	raw: unknown,
	commandStyle: boolean,
): ReadonlySet<string> {
	if (raw === null || raw === undefined) return new Set();
	let items: readonly unknown[];
	if (typeof raw === "string") {
		items = raw.split(",").filter((part) => part.trim() !== "");
	} else if (
		raw !== null &&
		raw !== undefined &&
		typeof raw === "object" &&
		(Symbol.iterator in raw || Array.isArray(raw))
	) {
		items = [...(raw as Iterable<unknown>)];
	} else {
		items = [raw]; // single scalar (int user id, etc.)
	}
	const out = new Set<string>();
	for (const item of items) {
		let entry = String(item).trim();
		if (commandStyle) {
			entry = entry.replace(/^\/+/, "").toLowerCase();
		}
		if (entry !== "") out.add(entry);
	}
	return out;
}

const DM_CHAT_TYPES: ReadonlySet<string> = new Set([
	"dm",
	"direct",
	"private",
	"",
]);

/** slash_access.py:_scope_for_chat_type — SessionSource.chat_type → scope. */
export function scopeForChatType(
	chatType: string | null | undefined,
): "dm" | "group" {
	if (
		chatType !== null &&
		chatType !== undefined &&
		DM_CHAT_TYPES.has(chatType.toLowerCase())
	) {
		return "dm";
	}
	return "group";
}

/** slash_access.py:_keys_for_scope — (admin key, user-commands key). */
export function keysForScope(scope: "dm" | "group"): readonly [string, string] {
	return scope === "group"
		? ["group_allow_admin_from", "group_user_allowed_commands"]
		: ["allow_admin_from", "user_allowed_commands"];
}

/**
 * slash_access.py:_platform_extra — the ``extra`` dict from a
 * PlatformConfig-like value. Plain-object shapes pass through directly
 * (Hermes: "some test harnesses pass dicts directly"); anything else
 * degrades to {}.
 */
export function platformExtraOf(
	platformConfig: unknown,
): Record<string, unknown> {
	if (platformConfig === null || typeof platformConfig !== "object") return {};
	if (platformConfig instanceof Map) return {};
	const record = platformConfig as Record<string, unknown>;
	const extra = record.extra;
	if (
		extra !== null &&
		typeof extra === "object" &&
		!Array.isArray(extra) &&
		!(extra instanceof Map)
	) {
		return extra as Record<string, unknown>;
	}
	if (!Array.isArray(platformConfig)) return record;
	return {};
}

/** Minimal structural slice of a gateway SessionSource the policy reads. */
export interface SlashAccessSourceLike {
	platform: string;
	chatType?: string | null;
	userId?: string | null;
}

/** Minimal structural slice of the gateway config the policy reads. */
export interface SlashAccessGatewayConfigLike {
	platforms?: ReadonlyMap<string, unknown> | Record<string, unknown> | null;
}

/**
 * slash_access.py:policy_from_extra — build the policy for one scope from a
 * platform ``extra`` dict. DM scope falls back to group_user_allowed_commands
 * ONLY for the command list (operator lists the shared set once); ADMIN
 * lists never cross scopes — a group admin is not implicitly a DM admin.
 */
export function policyFromExtra(
	extra: Record<string, unknown> | null | undefined,
	scope: "dm" | "group",
): SlashAccessPolicy {
	const [adminKey, cmdKey] = keysForScope(scope);
	const source = extra ?? {};
	const adminUserIds = coerceIdSet(source[adminKey]);
	let userAllowedCommands = coerceCommandSet(source[cmdKey]);
	if (scope === "dm" && userAllowedCommands.size === 0) {
		userAllowedCommands = coerceCommandSet(source.group_user_allowed_commands);
	}
	return {
		enabled: adminUserIds.size > 0,
		adminUserIds,
		userAllowedCommands,
	};
}

/**
 * slash_access.py:policy_for_source — resolve the policy for a SessionSource.
 * Returns SLASH_ACCESS_DISABLED (allow everything) when config/source are
 * missing or the scope has no admin list. Authoritative for slash COMMAND
 * gating only — plain chat is never gated here.
 */
export function policyForSource(
	gatewayConfig: SlashAccessGatewayConfigLike | null | undefined,
	source: SlashAccessSourceLike | null | undefined,
): SlashAccessPolicy {
	if (
		gatewayConfig === null ||
		gatewayConfig === undefined ||
		source === null ||
		source === undefined
	) {
		return SLASH_ACCESS_DISABLED;
	}
	const platforms = gatewayConfig.platforms ?? null;
	let platformConfig: unknown;
	if (platforms !== null) {
		if (typeof (platforms as ReadonlyMap<string, unknown>).get === "function") {
			platformConfig = (platforms as ReadonlyMap<string, unknown>).get(
				source.platform,
			);
		} else {
			platformConfig = (platforms as Record<string, unknown>)[source.platform];
		}
	}
	const scope = scopeForChatType(source.chatType);
	return policyFromExtra(platformExtraOf(platformConfig), scope);
}

/**
 * run.py:_check_slash_access denial body — BYTE-STABLE. The preview lists up
 * to 12 sorted entries from user_allowed_commands (the {help,whoami} floor is
 * deliberately NOT listed — it is implicit); longer sets get the ellipsis.
 */
export function slashAccessDenialText(
	policy: SlashAccessPolicy,
	canonicalCmd: string,
): string {
	const allowedPreview = [...policy.userAllowedCommands].sort();
	let suffix: string;
	if (allowedPreview.length > 0) {
		suffix =
			"You can run: " +
			allowedPreview
				.slice(0, 12)
				.map((cmd) => `/${cmd}`)
				.join(", ") +
			(allowedPreview.length > 12 ? "…" : "") +
			". Use /whoami for the full list.";
	} else {
		suffix =
			"No slash commands are enabled for non-admins on this platform. Ask an admin to add you to allow_admin_from or to set user_allowed_commands.";
	}
	return `⛔ /${canonicalCmd} is admin-only here. ${suffix}`;
}

/**
 * run.py:_check_slash_access — return the denial message when the caller
 * cannot run the canonical command, else null. Disabled policy ⇒ null
 * (backward-compat: no admin list ⇒ no gating). Unknown/empty commands are
 * NOT gated (they are plain text and become agent turns).
 */
export function checkSlashAccess(
	policy: SlashAccessPolicy | null | undefined,
	userId: string | null | undefined,
	canonicalCmd: string | null | undefined,
): string | null {
	if (!canonicalCmd) return null;
	const resolved = policy ?? SLASH_ACCESS_DISABLED;
	if (!resolved.enabled || canRunSlashCommand(resolved, userId, canonicalCmd)) {
		return null;
	}
	return slashAccessDenialText(resolved, canonicalCmd);
}

/**
 * run.py:_check_slash_access bound to a SessionSource — the full cold-path
 * call shape (policy resolution + user attribution + denial). The logger.info
 * side channel rides the wired call sites' warning sinks, not this pure core.
 */
export function checkSourceSlashAccess(
	gatewayConfig: SlashAccessGatewayConfigLike | null | undefined,
	source: SlashAccessSourceLike | null | undefined,
	canonicalCmd: string | null | undefined,
): string | null {
	return checkSlashAccess(
		policyForSource(gatewayConfig, source),
		source?.userId ?? null,
		canonicalCmd,
	);
}

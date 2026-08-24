// authz/decision — THE §2.1 authorization decision chain.
//
// Port of gateway/authz_mixin.py::GatewayAuthorizationMixin._is_user_authorized
// (READ-ONLY Hermes reference; semantics ported, no code vendored). Every
// consumer reads THIS chain — never re-derive the order (06 §2: the draft's
// "deny → allowlist → allow-all → group policy" ordering was WRONG; the
// verified order below is binding).
//
//   0. HOMEASSISTANT / WEBHOOK sources        → ALLOW (system-generated / HMAC)
//   1. Upstream-auth delegation               → ALLOW (relay-delivered event, or
//                                                adapter authorization_is_upstream)
//   2. Group chat-ID allowlists               → ALLOW if chat listed (runs BEFORE
//                                                the no-user-id guard so anonymous-
//                                                admin/channel traffic authorizes)
//   3. {PLATFORM}_ALLOW_BOTS ∈ {mentions,all} → ALLOW bot senders (#4466) — also
//                                                BEFORE the no-user-id deny (A6):
//                                                Slack Workflow Builder posts arrive
//                                                subtype=bot_message, user=None
//   4. No user_id                             → DENY (reason code no_user_id)
//   5. {P}_ALLOW_ALL_USERS truthy             → ALLOW (explicit operator opt-out)
//   6. Adapter-verified role                  → ALLOW (role_authorized is True)
//   7. Pairing store approval                 → ALLOW (unioned with allowlists)
//   8. NO allowlist configured anywhere?      → trust adapter-owned policy ONLY if
//                                                effective policy == "allowlist"
//                                                (#34515 fail-open fix); else
//                                                config.extra allow_from match; else
//                                                GATEWAY_ALLOW_ALL_USERS decides
//   9. Allowlists configured: union(platform ∪ group-user ∪ global) match
//      ("*" wildcard; @domain stripping; WhatsApp alias expansion; SimpleX
//       display-name matching)
//  10. Default → DENY, logged with reason code (never silent)
//
// Every decision returns a structured record; every denial carries the failing
// gate number and a stable reason code, forwarded to the injected sink.

import { authEnv, platformGateEnv } from "./env-accessors.js";
import {
	CHAT_TYPES_GROUP_FORUM,
	CHAT_TYPES_WITH_CHANNEL,
	coerceAllowSet,
	isTruthyFlag,
	normalizeAllowBotsValue,
	PLATFORM_ALLOW_ALL_ENV,
	PLATFORM_ALLOW_BOTS_ENV,
	PLATFORM_ALLOWED_USERS_ENV,
	PLATFORM_GROUP_CHAT_ENV,
	PLATFORM_GROUP_USER_ENV,
	SYSTEM_PLATFORMS,
} from "./platform-tables.js";
import {
	expandWhatsappAliases,
	normalizeWhatsappIdentifier,
	type WhatsappIdentityOptions,
} from "../../resolution/whatsapp-identity.js";

/** The inbound sender/surface snapshot the chain evaluates (SessionSource parity). */
export interface AuthzSource {
	platform: string | null | undefined;
	profile?: string | null | undefined;
	userId?: string | null | undefined;
	userName?: string | null | undefined;
	chatId?: string | null | undefined;
	/** "dm" | "group" | "forum" | "channel" (parity SessionSource.chat_type). */
	chatType?: string | null | undefined;
	isBot?: boolean | null | undefined;
	/**
	 * Adapter-verified role membership (e.g. Discord roles). Compared with
	 * `=== true` — an explicit identity check refuses to authorize a non-bool
	 * stand-in (Hermes pitfall #13 / #17 discipline).
	 */
	roleAuthorized?: unknown;
	/**
	 * Transport-stamped relay delivery marker. Compared with `=== true` — a
	 * real bool on a genuine source; MagicMock-style auto-truthy stand-ins
	 * must NOT authorize (defensive against accidental fail-open).
	 */
	deliveredViaUpstreamRelay?: unknown;
}

/**
 * What the chain needs to know about the LIVE adapter for a platform/profile.
 * All fields optional — absent means "no adapter" or "flag not exposed",
 * which defaults every flag to False (fail-closed parity of the getattr guards).
 */
export interface AdapterAuthzView {
	authorizationIsUpstream?: boolean | undefined;
	enforcesOwnAccessPolicy?: boolean | undefined;
	/** Resolved effective dm_policy (folds config.extra + <P>_DM_POLICY env). */
	dmPolicy?: string | null | undefined;
	/** Resolved effective group_policy. */
	groupPolicy?: string | null | undefined;
	/** Live adapter config.extra (group_allowed_chats / allow_from / group_allow_from). */
	extra?: Record<string, unknown> | null | undefined;
	/**
	 * Live DM allowlist recheck (adapter `_is_dm_allowed`). When exposed, an
	 * adapter-allowlist-policy admission REQUIRES this to admit — pairing
	 * revoke clears WHATSAPP_ALLOWED_USERS while a construction-time snapshot
	 * would otherwise keep authorizing until restart (#34515 follow-up).
	 */
	isDmAllowed?: ((userId: string) => boolean) | undefined;
	/** Per-group sender allowlist presence (WeCom groups.<id>.allow_from shape). */
	groupHasSenderAllowlist?: ((chatId: string) => boolean) | undefined;
}

/** Pairing-store selection parity of authz_mixin.py::_pairing_store_for. */
export interface PairingStoreLike {
	isApproved(platform: string, userId: string): boolean;
}

/** Plugin-platform registry entry (gateway.platform_registry parity). */
export interface PlatformRegistryEntry {
	allowedUsersEnv?: string | null | undefined;
	allowAllEnv?: string | null | undefined;
}

/** Structured decision/denial record — the reason-code contract (06 §2.3). */
export interface AuthzDecisionRecord {
	allowed: boolean;
	/**
	 * Granting gate number on ALLOW; the FAILING gate number on DENY
	 * (4 = no-user-id guard, 10 = default deny) per 06 §2.3: "every denial
	 * logs platform, user/chat id, and the failing gate number".
	 */
	gate: number | null;
	/** Stable snake_case reason code (see REASON_CODES). */
	reasonCode: string;
	platform: string;
	userId: string;
	chatId: string;
	/** Extra debuggability context (never a second gate encoding). */
	detail?: string | undefined;
}

export type DenialSink = (record: AuthzDecisionRecord) => void;

export interface AuthzDeps {
	/** Live adapter lookup by (platform, profile). Absent ⇒ no adapter view. */
	adapterView?:
		| ((
				platform: string,
				profile: string | null,
		  ) => AdapterAuthzView | null | undefined)
		| undefined;
	/**
	 * Per-profile pairing store selection (_pairing_store_for): return the
	 * profile-scoped store when registered, else the global default store.
	 */
	pairingStoreFor?:
		| ((source: AuthzSource) => PairingStoreLike | null | undefined)
		| undefined;
	/** Plugin-platform registry lookup for platforms outside the hardcoded maps. */
	registryEntry?:
		| ((platform: string) => PlatformRegistryEntry | null | undefined)
		| undefined;
	/** WhatsApp bridge session dir for alias expansion (tests inject temp dirs). */
	whatsappSessionDir?: string | undefined;
	/** Denial sink — wire to the structured logger at runner wiring time. */
	onDenial?: DenialSink | undefined;
	/** Warn-once seam (TELEGRAM_GROUP_ALLOWED_USERS legacy shim). */
	warnOnce?: ((key: string, message: string) => void) | undefined;
}

function text(v: unknown): string {
	return String(v ?? "").trim();
}

function lower(v: unknown): string {
	return text(v).toLowerCase();
}

function chatTypeOf(source: AuthzSource): string {
	return lower(source.chatType);
}

function resolveAdapterView(
	source: AuthzSource,
	deps: AuthzDeps,
	adapterProfile: string | null,
): AdapterAuthzView | null {
	if (!deps.adapterView) return null;
	const platform = text(source.platform);
	if (!platform) return null;
	try {
		return deps.adapterView(platform, adapterProfile) ?? null;
	} catch {
		// A throwing registry must never fail-open the chain.
		return null;
	}
}

/**
 * Adapter-profile resolution parity of _adapter_profile_for_source: consumers
 * that track transport-owning profiles pass source.profile; the chain never
 * invents one.
 */
function adapterProfileFor(source: AuthzSource): string | null {
	const p = text(source.profile);
	return p === "" ? null : p;
}

function extraRecord(view: AdapterAuthzView | null): Record<string, unknown> {
	const extra = view?.extra;
	return extra !== null && extra !== undefined && typeof extra === "object"
		? (extra as Record<string, unknown>)
		: {};
}

function isWhatsAppFamily(platform: string): boolean {
	return platform === "whatsapp" || platform === "whatsapp_cloud";
}

/**
 * Run the FULL §2.1 decision order for one inbound source. Never throws for
 * policy reasons — unavailability denies with a reason code.
 */
export function isUserAuthorized(
	source: AuthzSource,
	deps: AuthzDeps = {},
): AuthzDecisionRecord {
	const platform = text(source.platform);
	const userId = text(source.userId);
	const chatId = text(source.chatId);
	const chatType = chatTypeOf(source);

	const base = {
		platform,
		userId,
		chatId,
	};

	const deny = (
		gate: number,
		reasonCode: string,
		detail?: string,
	): AuthzDecisionRecord => {
		const record: AuthzDecisionRecord = {
			allowed: false,
			gate,
			reasonCode,
			...base,
		};
		if (detail !== undefined) record.detail = detail;
		if (deps.onDenial) deps.onDenial(record);
		return record;
	};

	const allow = (
		gate: number,
		reasonCode: string,
		detail?: string,
	): AuthzDecisionRecord => {
		const record: AuthzDecisionRecord = {
			allowed: true,
			gate,
			reasonCode,
			...base,
		};
		if (detail !== undefined) record.detail = detail;
		return record;
	};

	// ── 0. System-generated / HMAC-authenticated sources ────────────────────
	if (SYSTEM_PLATFORMS.has(platform)) {
		return allow(0, "system_platform");
	}

	const adapterProfile = adapterProfileFor(source);
	const view = resolveAdapterView(source, deps, adapterProfile);

	// ── 1. Upstream-auth delegation ──────────────────────────────────────────
	// `is True` discipline: the marker is a real bool on a genuine source; an
	// explicit identity check refuses non-bool stand-ins (fail-open defense).
	if (
		source.deliveredViaUpstreamRelay === true ||
		view?.authorizationIsUpstream === true
	) {
		return allow(1, "upstream_auth_delegation");
	}

	// ── 2. Group chat-ID allowlists (BEFORE the no-user-id guard) ───────────
	if (CHAT_TYPES_WITH_CHANNEL.has(chatType) && chatId !== "") {
		const chatAllowlistEnv = PLATFORM_GROUP_CHAT_ENV[platform] ?? "";
		if (chatAllowlistEnv !== "") {
			const rawChatAllowlist = platformGateEnv(chatAllowlistEnv);
			if (rawChatAllowlist !== "") {
				const allowedGroupIds = new Set(
					rawChatAllowlist
						.split(",")
						.map((cid) => cid.trim())
						.filter((cid) => cid !== ""),
				);
				if (allowedGroupIds.has("*") || allowedGroupIds.has(chatId)) {
					return allow(2, "group_chat_allowlist", "env");
				}
			}
		}
		// Fallback: adapter-level config.extra.group_allowed_chats (config.yaml),
		// consulted because e.g. Telegram observe-unmentioned mode strips
		// user_id from triggered group messages.
		const allowed = coerceAllowSet(extraRecord(view).group_allowed_chats);
		if (allowed.size > 0 && (allowed.has("*") || allowed.has(chatId))) {
			return allow(2, "group_chat_allowlist", "config.extra");
		}
	}

	// ── 3. Bot-sender bypass ({P}_ALLOW_BOTS), still BEFORE step 4 (A6) ─────
	if (source.isBot === true) {
		const allowBotsVar = PLATFORM_ALLOW_BOTS_ENV[platform] ?? "";
		if (
			allowBotsVar !== "" &&
			normalizeAllowBotsValue(platformGateEnv(allowBotsVar, "none")) !== "none"
		) {
			return allow(3, "bot_bypass");
		}
	}

	// ── 4. No user id → DENY ─────────────────────────────────────────────────
	if (userId === "") {
		return deny(4, "no_user_id");
	}

	// Hardcoded maps + plugin-registry extension for unknown platforms.
	let allowedUsersEnv = PLATFORM_ALLOWED_USERS_ENV[platform] ?? "";
	let allowAllEnv = PLATFORM_ALLOW_ALL_ENV[platform] ?? "";
	if (allowedUsersEnv === "") {
		const entry = (() => {
			try {
				return deps.registryEntry
					? (deps.registryEntry(platform) ?? null)
					: null;
			} catch {
				return null;
			}
		})();
		if (entry?.allowedUsersEnv) allowedUsersEnv = String(entry.allowedUsersEnv);
		if (entry?.allowAllEnv) allowAllEnv = String(entry.allowAllEnv);
	}

	// ── 5. Per-platform allow-all flag (explicit opt-out gate) ──────────────
	if (allowAllEnv !== "" && isTruthyFlag(authEnv(allowAllEnv))) {
		return allow(5, "allow_all_users");
	}

	// ── 6. Adapter-verified role auth (`is True` identity check) ────────────
	if (source.roleAuthorized === true) {
		return allow(6, "role_authorized");
	}

	// ── 7. Pairing store approval (per-profile store selection) ─────────────
	const pairingStore = deps.pairingStoreFor
		? deps.pairingStoreFor(source)
		: null;
	if (pairingStore !== null && pairingStore !== undefined) {
		let approved = false;
		try {
			approved = pairingStore.isApproved(platform, userId);
		} catch {
			approved = false; // a broken store must never fail-open
		}
		if (approved) return allow(7, "pairing_approved");
	}

	// ── 8/9. Allowlists ──────────────────────────────────────────────────────
	const platformAllowlist =
		allowedUsersEnv !== "" ? authEnv(allowedUsersEnv) : "";
	// Parity subtlety: the scoped group-user/group-chat env reads cover
	// {"group","forum"} ONLY — channel traffic does not consult them here.
	const scopedGroup = CHAT_TYPES_GROUP_FORUM.has(chatType);
	const groupUserAllowlist =
		scopedGroup && PLATFORM_GROUP_USER_ENV[platform]
			? authEnv(PLATFORM_GROUP_USER_ENV[platform] as string)
			: "";
	const groupChatAllowlist =
		scopedGroup && PLATFORM_GROUP_CHAT_ENV[platform]
			? authEnv(PLATFORM_GROUP_CHAT_ENV[platform] as string)
			: "";
	const globalAllowlist = authEnv("GATEWAY_ALLOWED_USERS");

	if (
		platformAllowlist === "" &&
		groupUserAllowlist === "" &&
		groupChatAllowlist === "" &&
		globalAllowlist === ""
	) {
		// No env allowlist configured anywhere. Trust an own-policy adapter's
		// intake decision ONLY when its effective policy for THIS chat type is
		// an actual "allowlist" restriction — "open" forwards EVERY sender and
		// reading reach as authorization was the #34515 fail-open bug; the
		// misconfigured-open case MUST land in the default deny below.
		if (view?.enforcesOwnAccessPolicy === true) {
			let effectivePolicy = "";
			if (CHAT_TYPES_WITH_CHANNEL.has(chatType)) {
				effectivePolicy = lower(view.groupPolicy);
				if (view.groupHasSenderAllowlist?.(chatId) === true) {
					return allow(
						8,
						"adapter_allowlist_policy",
						"per-group sender allowlist",
					);
				}
			} else {
				effectivePolicy = lower(view.dmPolicy);
			}
			if (effectivePolicy === "allowlist") {
				if (!CHAT_TYPES_WITH_CHANNEL.has(chatType)) {
					// Live re-check beats construction-time snapshots: a revoked
					// grant clears the env allowlist while a stale snapshot would
					// keep authorizing until restart (#34515 follow-up).
					if (view.isDmAllowed !== undefined) {
						if (view.isDmAllowed(userId)) {
							return allow(8, "adapter_allowlist_policy", "live dm recheck");
						}
					} else {
						return allow(8, "adapter_allowlist_policy");
					}
				} else {
					return allow(8, "adapter_allowlist_policy");
				}
			}
		}
		// Config-driven allow_from / group_allow_from without an
		// enforces_own_access_policy override (Telegram-style adapters).
		const extra = extraRecord(view);
		const adapterAllowRaw = CHAT_TYPES_WITH_CHANNEL.has(chatType)
			? extra.group_allow_from
			: extra.allow_from;
		const adapterAllowed = coerceAllowSet(adapterAllowRaw);
		if (
			adapterAllowed.size > 0 &&
			(adapterAllowed.has(userId) || adapterAllowed.has("*"))
		) {
			return allow(8, "config_allow_from");
		}
		// No allowlists configured — the global allow-all flag is the last
		// explicit opt-in; anything else is the DEFAULT DENY (gate 10).
		if (isTruthyFlag(authEnv("GATEWAY_ALLOW_ALL_USERS"))) {
			return allow(8, "gateway_allow_all");
		}
		return deny(
			10,
			"default_deny",
			"no allowlist configured; gateway allow-all off",
		);
	}

	if (
		groupChatAllowlist !== "" &&
		CHAT_TYPES_GROUP_FORUM.has(chatType) &&
		chatId !== ""
	) {
		const allowedGroupIds = new Set(
			groupChatAllowlist
				.split(",")
				.map((cid) => cid.trim())
				.filter((cid) => cid !== ""),
		);
		if (allowedGroupIds.has("*") || allowedGroupIds.has(chatId)) {
			return allow(9, "group_chat_allowlist", "union branch");
		}
	}

	// Backward-compat shim for #15027: pre-PR#17686 TELEGRAM_GROUP_ALLOWED_USERS
	// was (mis)used as a chat-ID list; "-"-prefixed values are chat IDs. Honor
	// them with a warn-once nudge toward TELEGRAM_GROUP_ALLOWED_CHATS.
	if (
		platform === "telegram" &&
		groupUserAllowlist !== "" &&
		CHAT_TYPES_GROUP_FORUM.has(chatType) &&
		chatId !== ""
	) {
		const legacyChatIds = new Set(
			groupUserAllowlist
				.split(",")
				.map((v) => v.trim())
				.filter((v) => v.startsWith("-")),
		);
		if (legacyChatIds.size > 0) {
			deps.warnOnce?.(
				"telegram_group_users_legacy",
				"TELEGRAM_GROUP_ALLOWED_USERS contains chat-ID-shaped values " +
					`(${[...legacyChatIds].sort().join(",")}). Treat them as moved to ` +
					"TELEGRAM_GROUP_ALLOWED_CHATS — the _USERS var is now for sender user IDs.",
			);
			if (legacyChatIds.has(chatId)) {
				return allow(9, "group_chat_allowlist", "legacy telegram shim");
			}
		}
	}

	// Union match: platform ∪ group-user ∪ global. "*" admits everyone
	// (SIGNAL_GROUP_ALLOWED_USERS precedent). In group/forum chats the scoped
	// GROUP_ALLOWED_USERS var must not imply DM access — it only unions here
	// because this branch already knows a chat-type-scoped read happened.
	const allowedIds = new Set<string>();
	for (const raw of [platformAllowlist, groupUserAllowlist, globalAllowlist]) {
		if (raw === "") continue;
		for (const uid of raw.split(",")) {
			const trimmed = uid.trim();
			if (trimmed !== "") allowedIds.add(trimmed);
		}
	}
	if (allowedIds.has("*")) return allow(9, "allowlist_union", "*");

	const checkIds = new Set<string>([userId]);
	if (userId.includes("@")) checkIds.add(userId.split("@")[0] as string);

	if (isWhatsAppFamily(platform)) {
		// exactOptionalPropertyTypes: only carry the override when set.
		const opts: WhatsappIdentityOptions =
			deps.whatsappSessionDir !== undefined
				? { sessionDir: deps.whatsappSessionDir }
				: {};
		// Parity: the ALLOWED side is REPLACED by its alias expansion, then the
		// sender's aliases + normalized form join the check set — a paired LID
		// matches the phone-JID allowlist entry and vice versa.
		const normalizedAllowedIds = new Set<string>();
		for (const allowedId of allowedIds) {
			for (const alias of expandWhatsappAliases(allowedId, opts)) {
				normalizedAllowedIds.add(alias);
			}
		}
		if (normalizedAllowedIds.size > 0) {
			allowedIds.clear();
			for (const alias of normalizedAllowedIds) allowedIds.add(alias);
			// check_ids.update(expand(user_id)) + normalized input parity:
			for (const alias of expandWhatsappAliases(userId, opts))
				checkIds.add(alias);
			const normalizedUserId = normalizeWhatsappIdentifier(userId);
			if (normalizedUserId !== "") checkIds.add(normalizedUserId);
		}
	}

	// SimpleX: SIMPLEX_ALLOWED_USERS accepts numeric contactId OR display name.
	if (platform === "simplex") {
		const userName = text(source.userName);
		if (userName !== "") checkIds.add(userName);
	}

	for (const id of checkIds) {
		if (allowedIds.has(id)) return allow(9, "allowlist_union");
	}
	return deny(10, "default_deny", "sender not in any configured allowlist");
}

// authz/dm-behavior — how an UNAUTHORIZED DM is handled (pairing handshake
// entry). Port of gateway/authz_mixin.py::_get_unauthorized_dm_behavior.
//
// Resolution order (verified, binding):
//   1. Explicit per-platform `unauthorized_dm_behavior` in config — always wins.
//   2. Email defaults to "ignore" unless explicitly opted into pairing: an
//      inbox may contain arbitrary unread human messages; replying with
//      pairing codes is neither safe nor polite there. Sits BEFORE the global
//      fallback (GatewayConfig.get_unauthorized_dm_behavior parity).
//   3. Explicit global config override — wins for chat-shaped platforms when
//      no per-platform override exists.
//   4. Adapter-level dm_policy opts into pairing or silent drop: "pairing" →
//      "pair"; "allowlist"/"disabled" → "ignore".
//   5. Any allowlist configured ({P}_ALLOWED_USERS / {P}_GROUP_ALLOWED_USERS /
//      {P}_GROUP_ALLOWED_CHATS / GATEWAY_ALLOWED_USERS) → "ignore" (#9337):
//      the allowlist signals deliberate restriction; spamming unknown contacts
//      with pairing codes is noisy and an info-leak. Reads go through the
//      multiplex-authoritative gate accessor (per-profile isolation).
//   6. No allowlist and no explicit config → "pair" (open-gateway default).

import { platformGateEnv } from "./env-accessors.js";
import {
	PLATFORM_ALLOWED_USERS_ENV,
	PLATFORM_GROUP_CHAT_ENV,
	PLATFORM_GROUP_USER_ENV,
} from "./platform-tables.js";
import type { AdapterAuthzView } from "./decision.js";

export type UnauthorizedDmBehavior = "pair" | "ignore";

export interface DmBehaviorConfig {
	/** Per-platform overrides keyed by lowercase platform name. */
	perPlatform?: Readonly<Record<string, string>> | undefined;
	/** Global config default (config.unauthorized_dm_behavior). */
	global?: string | null | undefined;
}

export interface DmBehaviorDeps {
	config?: DmBehaviorConfig | undefined;
	/** Live adapter view for dm_policy reads (profile-scoped under multiplex). */
	adapterView?:
		| ((platform: string) => AdapterAuthzView | null | undefined)
		| undefined;
}

function normalizePolicy(v: unknown): string {
	return String(v ?? "")
		.trim()
		.toLowerCase();
}

export function unauthorizedDmBehavior(
	platform: string,
	deps: DmBehaviorDeps = {},
): UnauthorizedDmBehavior {
	const p = platform.trim().toLowerCase();

	// 1. Explicit per-platform override always wins.
	const perPlatform = deps.config?.perPlatform?.[p];
	if (typeof perPlatform === "string" && perPlatform.trim() !== "") {
		const v = normalizePolicy(perPlatform);
		if (v === "pair" || v === "ignore") return v;
	}

	// 2. Email is inbox-shaped, not chat-shaped: require explicit opt-in.
	if (p === "email") return "ignore";

	// 3. Explicit global config override (non-default values only).
	const globalRaw = deps.config?.global;
	if (
		typeof globalRaw === "string" &&
		globalRaw.trim() !== "" &&
		normalizePolicy(globalRaw) !== "pair"
	) {
		const v = normalizePolicy(globalRaw);
		if (v === "ignore") return v;
	}

	// 4. Config-driven adapter dm_policy.
	let dmPolicy = "";
	try {
		dmPolicy = normalizePolicy(deps.adapterView?.(p)?.dmPolicy);
	} catch {
		dmPolicy = "";
	}
	if (dmPolicy === "pairing") return "pair";
	if (dmPolicy === "allowlist" || dmPolicy === "disabled") return "ignore";

	// 5. Allowlist-aware default: any configured allowlist ⇒ silent drop.
	const allowedUsersEnv = PLATFORM_ALLOWED_USERS_ENV[p];
	if (allowedUsersEnv && platformGateEnv(allowedUsersEnv).trim() !== "") {
		return "ignore";
	}
	for (const envKey of [
		PLATFORM_GROUP_USER_ENV[p],
		PLATFORM_GROUP_CHAT_ENV[p],
	]) {
		if (envKey && platformGateEnv(envKey).trim() !== "") return "ignore";
	}
	if (platformGateEnv("GATEWAY_ALLOWED_USERS").trim() !== "") return "ignore";

	// 6. Open-gateway default: run the pairing handshake.
	return "pair";
}

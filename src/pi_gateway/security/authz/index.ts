// authz — the §2 authorization engine: decision chain, pairing handshake,
// and the scoped env accessors every authz read routes through.
//
// SANCTIONED IMPORT SURFACE (mirrors secretscope/index.ts discipline):
// consumers import from here so the decision ORDER has exactly one home
// (06 §2: "every consumer reads THIS chain, never re-derives").

export {
	authEnv,
	platformGateEnv,
} from "./env-accessors.js";

export {
	CHAT_TYPES_GROUP_FORUM,
	CHAT_TYPES_WITH_CHANNEL,
	PAIRING_ALLOWLIST_ENV,
	PLATFORM_ALLOW_ALL_ENV,
	PLATFORM_ALLOW_BOTS_ENV,
	PLATFORM_ALLOWED_USERS_ENV,
	PLATFORM_GROUP_CHAT_ENV,
	PLATFORM_GROUP_USER_ENV,
	SYSTEM_PLATFORMS,
	coerceAllowSet,
	isTruthyFlag,
	normalizeAllowBotsValue,
} from "./platform-tables.js";

export {
	isUserAuthorized,
	type AdapterAuthzView,
	type AuthzDecisionRecord,
	type AuthzDeps,
	type AuthzOptions,
	type AuthzSource,
	type DenialSink,
	type PairingStoreLike,
	type PlatformRegistryEntry,
} from "./decision.js";

export {
	unauthorizedDmBehavior,
	type DmBehaviorConfig,
	type DmBehaviorDeps,
	type UnauthorizedDmBehavior,
} from "./dm-behavior.js";

export {
	MAX_FAILED_ATTEMPTS,
	MAX_PENDING_PER_PLATFORM,
	CODE_ALPHABET,
	CODE_LENGTH,
	CODE_TTL_SECONDS,
	LOCKOUT_SECONDS,
	RATE_LIMIT_SECONDS,
	PairingStore,
	compareDigest,
	hashCode,
	systemPairingClock,
	type ApprovalResult,
	type ApprovedUser,
	type PairingClock,
	type PairingStoreOptions,
	type PendingRequest,
} from "./pairing.js";

export { PairingStores } from "./pairing-stores.js";

export {
	defaultAllowlistMirrorForHome,
	envFileMode,
	fileAllowlistMirror,
	type AllowlistMirror,
} from "./env-mirror.js";

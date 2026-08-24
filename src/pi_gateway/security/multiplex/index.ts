// multiplex — profile isolation for one-process-many-profiles gateways
// (06 §4). Sanctioned import surface; consumers import from here so the
// fail-closed rules each primitive enforces have exactly one home.
//
//   profile-env      — profile-scoped env reads that NEVER borrow process env
//                      (#86905/#72348; DEC-003/DEC-009)
//   profile-turn     — per-turn home+scope+identity combinator with reverse-
//                      order reset under exceptions (06 §9 hygiene row)
//   profile-authz    — per-profile pairing/authz store instances + the
//                      adapter-view refusal router (_authorization_adapter)
//   check-fn-cache   — (fn, scope) availability-probe cache with BYPASS
//                      semantics (05 §3.2; registry.py port)

export {
	ProfileEnvMissingError,
	currentProfileEnv,
	profileEnvFor,
	type ProfileEnvReader,
	type ProfileMappingSource,
} from "./profile-env.js";

export {
	currentProfileTurn,
	withProfileIsolation,
	type ProfileIsolationOptions,
	type ProfileTurnContext,
} from "./profile-turn.js";

export {
	ProfileAdapterViews,
	ProfileAuthzIsolation,
	type OpenProfileDb,
	type ProfileAuthzIsolationOptions,
} from "./profile-authz.js";

export {
	CHECK_FN_CACHE_BYPASS,
	CHECK_FN_CACHE_MAX,
	CHECK_FN_FAILURE_GRACE_SECONDS,
	CHECK_FN_TTL_SECONDS,
	CheckFnCache,
	checkFnCache,
	currentRequestIdentity,
	runWithRequestIdentity,
	type CheckFnCacheOptions,
	type CheckFnScope,
	type CheckFnWarning,
	type CheckFnWarningSink,
	type RequestBoundIdentity,
} from "./check-fn-cache.js";

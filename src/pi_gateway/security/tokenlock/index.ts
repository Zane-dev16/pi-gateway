// pi_gateway/security/tokenlock — machine-local scoped token locks (06 §5).
//
// Real engine behind the Phase-3 kit seam (src/pi_platforms/kit/token-lock.ts):
// synchronous tuple-returning acquisition, named-holder refusal as a fatal
// adapter error class, PID + process-start-time staleness ladder (PID-reuse
// safe), kill-holder immediate reacquisition, and the §5.1 inventory.
// Layering: imports only node builtins (01 §5.3 downward-only).

export {
	TOKEN_LOCK_KIND,
	buildLockRecord,
	createLockFile,
	defaultLockDir,
	hashIdentity,
	isCorruptLockFile,
	refreshLockRecord,
	replaceLockFile,
	readLockRecord,
	removeLockFile,
	scopedLockPath,
	type LockReadResult,
	type ScopedLockRecord,
} from "./lock-record.js";
export {
	getProcessStartTime,
	isProcessAlive,
	isProcessStopped,
	isProcessZombie,
	probeSignaledDead,
	readProcessCmdline,
	readProcessState,
} from "./process-identity.js";
export {
	acquireScopedLock,
	classifyExistingRecord,
	releaseScopedLock,
	requireScopedLock,
	scopedLockHolder,
	scopedLockOwnerDescription,
	TokenLockConflictError,
	ScopedTokenLockManager,
	type AcquiredTokenLock,
	type LockAcquisition,
	type LockHolderInfo,
	type PlatformLockRequest,
	type ScopedTokenLockManagerOptions,
} from "./token-lock.js";
export {
	listScopedLocks,
	ownProcessFingerprint,
	type InventoryOptions,
	type ScopedLockInventoryRow,
} from "./inventory.js";

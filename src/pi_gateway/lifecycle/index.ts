// pi_gateway/lifecycle — public surface barrel (08 §1 process lifecycle).
//
// Downstream layers import from here; dependency layer per 01 §5.3:
// pi_gateway sits ABOVE pi_state/pi_home and imports them — never upward.

export * from "./stages.js";
export * from "./process-info.js";
export * from "./markers.js";
export * from "./instance-guard.js";
export * from "./takeover.js";
export * from "./boot-fingerprint.js";
export * from "./status-stamp.js";
export * from "./status-persist.js";
export * from "./shutdown.js";
export * from "./restart.js";
export * from "./forensics.js";
export * from "./watchdog.js";
export * from "./restore-gate.js";
export * from "./boot-recovery.js";
export * from "./lifecycle.js";
export type { FailedPlatformQueue } from "./reconnect-watcher.js";
export {
	createReconnectWatcherService,
	reconnectBackoffDelayMs,
} from "./reconnect-watcher.js";
export { createDrainRequestWatcherService } from "./drain-request-watcher.js";

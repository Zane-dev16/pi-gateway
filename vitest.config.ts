import { defineConfig } from "vitest/config";

// DEC-041: spec files that spawn REAL OS child processes run SERIALIZED under
// full-suite execution. On 4-CPU hosts, parallel fork load starves their
// children of CPU/time (observed twice: the delegation-rail SIGKILL test
// timing out at 120s then 300s while its isolation runtime is ~1.5s). The
// dedicated "heavy-process" project pins fileParallelism:false for exactly
// these specs; every other spec keeps the default parallel pool untouched.
//
// Membership = any src/**/*.test.ts that imports node:child_process to
// spawn/execFile real OS children:
//   *two-process*.test.ts          — the two-process contract suites (7 files)
//   + delegation/restore.test.ts   — spawnSync boot driver
//   + guards/lease-interplay       — spawn-based interplay harness
//   + lifecycle/guard.test.ts      — execFile + spawn takeover fixtures
//   + lifecycle/layering.test.ts   — execFile of check-layering.mjs
//   + secretscope/gate.test.ts     — execFile of check-secret-scope.mjs
//   + tokenlock/process-identity   — spawn identity probes
//   + tokenlock/token-lock         — spawn claim racers / doomed holders
//   + pi_state/lease.test.ts       — spawn dead-pid + contention probes
//   + pi_state/wal.test.ts         — spawn concurrent-writer children
//
// NOTE (vitest 4): root-level `test.*` options are NOT inherited into
// projects — every project below carries the shared options explicitly.
const SHARED = {
	pool: "forks",
	testTimeout: 30_000,
	hookTimeout: 30_000,
} as const;

const HEAVY_PROCESS_SPECS = [
	"src/**/*two-process*.test.ts",
	"src/pi_gateway/delegation/restore.test.ts",
	"src/pi_gateway/guards/lease-interplay.test.ts",
	"src/pi_gateway/lifecycle/guard.test.ts",
	"src/pi_gateway/lifecycle/layering.test.ts",
	"src/pi_gateway/security/secretscope/gate.test.ts",
	"src/pi_gateway/security/tokenlock/process-identity.test.ts",
	"src/pi_gateway/security/tokenlock/token-lock.test.ts",
	"src/pi_state/lease.test.ts",
	"src/pi_state/wal.test.ts",
];

export default defineConfig({
	test: {
		projects: [
			{
				test: {
					name: "heavy-process",
					include: HEAVY_PROCESS_SPECS,
					exclude: [],
					fileParallelism: false,
					...SHARED,
				},
			},
			{
				test: {
					name: "default",
					include: ["src/**/*.test.ts"],
					exclude: HEAVY_PROCESS_SPECS,
					...SHARED,
				},
			},
		],
	},
});

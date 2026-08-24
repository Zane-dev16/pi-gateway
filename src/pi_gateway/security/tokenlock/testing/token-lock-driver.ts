// TEST INFRASTRUCTURE — child-process driver for token-lock two-process
// contracts (06 §10 lock rows: refusal while held, kill-holder prompt
// reacquisition, racing starters on a stale record, release ownership).
// Run under: node --import <pi_state/testing/node-ts-resolve.mjs>
//            token-lock-driver.ts --scenario <name> --dir <locks> ...
// Protocol: prints `RESULT_JSON {...}` on stdout; coordinates via marker
// files (write = signal; poll = wait). Shape matches the lifecycle driver.

import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";

interface Args {
	[k: string]: string;
}

function parseArgs(argv: string[]): Args {
	const out: Args = {};
	for (let i = 0; i < argv.length; i++) {
		if (argv[i]?.startsWith("--") === true) {
			out[argv[i]!.slice(2)] = argv[i + 1] ?? "";
			i++;
		}
	}
	return out;
}

const args = parseArgs(process.argv.slice(2));
const coordDir = String(args["coord"] ?? ".");

function signal(name: string, payload: Record<string, unknown> = {}): void {
	writeFileSync(
		join(coordDir, name),
		JSON.stringify({ t: Date.now(), pid: process.pid, ...payload }),
	);
}

function waitForMarker(name: string, timeoutMs = 20_000): void {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(join(coordDir, name))) {
		if (Date.now() > deadline) {
			throw new Error(`child timeout waiting for marker ${name}`);
		}
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
	}
}

interface AcquiredHandle {
	release(): void;
}

interface Engine {
	ScopedTokenLockManager: new (opts: {
		dir: string;
	}) => {
		tryAcquire(
			scope: string,
			id: string,
			owner: string,
			opts?: { replace?: boolean | undefined },
		):
			| { acquired: true; lock: AcquiredHandle }
			| { acquired: false; holder: { owner: string } };
	};
	releaseScopedLock(
		scope: string,
		identity: string,
		owner: string,
		opts: { dir: string },
	): void;
	scopedLockPath(dir: string, scope: string, identity: string): string;
}

async function loadEngine(): Promise<Engine> {
	// Dynamic imports resolve AFTER the --import resolve hook is live.
	return (await import("../index.js")) as unknown as Engine;
}

function emit(result: Record<string, unknown>): void {
	console.log(`RESULT_JSON ${JSON.stringify(result)}`);
}

async function main(): Promise<void> {
	const engine = await loadEngine();
	const dir = String(args["dir"]);
	const scope = String(args["scope"] ?? "telegram-bot-token");
	const identity = String(args["identity"] ?? "shared-token");
	const owner = String(args["owner"] ?? "child-holder");
	switch (args["scenario"]) {
		case "hold": {
			// Acquire and HOLD until killed — the credential-consuming gateway.
			const mgr = new engine.ScopedTokenLockManager({ dir });
			const result = mgr.tryAcquire(scope, identity, owner);
			signal(String(args["ready-marker"]), { acquired: result.acquired });
			if (!result.acquired) return;
			setInterval(() => {}, 60_000); // hold forever (killed by the test)
			return;
		}
		case "try-once": {
			const mgr = new engine.ScopedTokenLockManager({ dir });
			const result = mgr.tryAcquire(scope, identity, owner);
			emit({
				acquired: result.acquired,
				holderOwner: result.acquired ? null : result.holder.owner,
			});
			return;
		}
		case "refuse-then-poll": {
			// First attempt happens WHILE A LIVES (refusal recorded), then poll
			// until the lock frees. elapsedMs = true kill-to-reclaim latency.
			const mgr = new engine.ScopedTokenLockManager({ dir });
			const first = mgr.tryAcquire(scope, identity, owner);
			const refusedFirst = !first.acquired;
			signal(String(args["attempted-marker"]), { refusedFirst });
			const deadline = Date.now() + Number(args["timeout-ms"] ?? 5000);
			let acquired = false;
			const start = Date.now();
			while (Date.now() <= deadline) {
				if (mgr.tryAcquire(scope, identity, owner).acquired) {
					acquired = true;
					break;
				}
				Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
			}
			emit({ acquired, refusedFirst, elapsedMs: Date.now() - start });
			return;
		}
		case "race": {
			// Barrier start: the PARENT gates both racers behind a go-marker so
			// they contend SIMULTANEOUSLY while both processes are alive (raw
			// spawn order lets the first racer boot, win, and EXIT before the
			// second even loads — the second would then rightly reclaim the
			// dead holder's lock per §5 liveness, not violate exclusion).
			signal(String(args["ready-marker"]));
			waitForMarker(String(args["go-marker"]));
			// One immediate attempt — two of these against a stale record must
			// produce EXACTLY one winner (§10 "racing starters").
			const mgr = new engine.ScopedTokenLockManager({ dir });
			const result = mgr.tryAcquire(scope, identity, owner);
			emit({
				acquired: result.acquired,
				holderOwner: result.acquired ? null : result.holder.owner,
			});
			signal(String(args["done-marker"]));
			if (result.acquired) {
				// Hold long enough that the loser observes a LIVE holder (a real
				// gateway holds its credential lock for its whole lifetime).
				Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 400);
			}
			return;
		}
		case "hold-then-release-on-marker": {
			// Acquire, report ready, wait for authority, release OWN lock.
			const mgr = new engine.ScopedTokenLockManager({ dir });
			const result = mgr.tryAcquire(scope, identity, owner);
			if (!result.acquired) {
				emit({ released: false, reason: "never-acquired" });
				return;
			}
			signal(String(args["ready-marker"]));
			waitForMarker(String(args["release-marker"]));
			result.lock.release();
			const path = engine.scopedLockPath(dir, scope, identity);
			emit({ released: !existsSync(path) });
			return;
		}
		default:
			throw new Error(`unknown scenario ${String(args["scenario"])}`);
	}
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});

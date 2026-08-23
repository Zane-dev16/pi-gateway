// Spike race-harness driver — throwaway, never ships. Lives next to the store
// module it exercises so child OS processes AND worker threads run the REAL
// DbTurnLeaseStore from spike/lease/db-turn-lease.ts (Node >=23 strips .ts
// types natively — no transpile step, no logic replica).
//
// Process mode:
//   node child-driver.mjs <dbPath> <storeModulePath> <base64url(cmdJson)>
// Worker-thread mode:
//   new Worker(this-file, { type: 'module', workerData: { dbPath, storePath, cmd } })
//
// Protocol: one JSON object per line on stdout (or postMessage to parentPort):
//   {event:"ready"} | {event:"held",holder,pid} | {event:"released"}
//   {event:"result",ok,...} | {event:"owner",...} | {event:"error",message}
// Release signal while holding: stdin line "release" (process) or
//   parentPort message {type:"release"} (worker). A maxHoldMs guard
//   auto-releases so a stray holder can never leak a lease into later tests.
// Exit codes: 0 normal (including ok:false results), 3 = hold/wait failed to
//   acquire, 1 = crash.

import { once } from "node:events";
import { isMainThread, parentPort, workerData } from "node:worker_threads";

const EXIT_ACQUIRE_FAILED = 3;

async function main() {
	const { dbPath, storePath, cmd } = workerData ?? readArgv();
	const mod = await import(storePath); // real TS module, type-stripped by Node
	const store = mod.DbTurnLeaseStore.open(dbPath);
	const emit = makeEmitter();

	// Structured holders: embed pid=<n> unless the caller already did, so the
	// dead-PID reclaim path has something provable to probe.
	const withPid = (holder) =>
		holder.includes("pid=") ? holder : `${holder}:pid=${process.pid}`;

	emit({ event: "ready" });

	try {
		switch (cmd.op) {
			case "probe": {
				emit({ event: "owner", owner: store.probeOwner(cmd.session) });
				finish(0);
				return;
			}
			case "lineage": {
				emit({ event: "root", root: store.lineageRoot(cmd.session) });
				finish(0);
				return;
			}
			case "once": {
				const holder = withPid(cmd.holder);
				const ok = store.tryAcquire(cmd.session, holder, cmd.ttlSeconds);
				if (ok) {
					store.releaseHolder(cmd.session, holder); // immediate release
					emit({ event: "result", ok: true, holder, released: true });
				} else {
					emit({ event: "result", ok: false, holder });
				}
				finish(0);
				return;
			}
			case "acquire_wait": {
				const holder = withPid(cmd.holder);
				const ok = await store.acquireWait(cmd.session, holder, {
					ttlSeconds: cmd.ttlSeconds,
					waitSeconds: cmd.waitSeconds,
					pollIntervalSeconds: cmd.pollIntervalSeconds,
				});
				emit({ event: "result", ok, holder });
				finish(ok ? 0 : EXIT_ACQUIRE_FAILED);
				return;
			}
			case "hold": {
				const holder = withPid(cmd.holder);
				const ok = await store.acquireWait(cmd.session, holder, {
					ttlSeconds: cmd.ttlSeconds,
					waitSeconds: cmd.waitSeconds,
					pollIntervalSeconds: cmd.pollIntervalSeconds,
				});
				if (!ok) {
					emit({ event: "result", ok: false, holder });
					finish(EXIT_ACQUIRE_FAILED);
					return;
				}
				emit({ event: "held", ok: true, holder, pid: process.pid });

				// Hold until a release signal arrives (stdin line / worker message)
				// or the safety guard fires.
				const released = waitForReleaseSignal(() =>
					store.releaseHolder(cmd.session, holder),
				);
				const guard = sleepMs(cmd.maxHoldMs ?? 20_000).then(() =>
					releaseOnce(() => store.releaseHolder(cmd.session, holder)),
				);
				await Promise.race([released, guard]);
				clearTimeout(guardTimer);
				emit({ event: "released", holder });
				finish(0);
				return;
			}
			default:
				throw new Error(`unknown op ${cmd.op}`);
		}
	} catch (err) {
		emit({
			event: "error",
			message: String(err && err.stack ? err.stack : err),
		});
		finish(1);
	} finally {
		store.close();
	}
}

// --- release plumbing -------------------------------------------------------

let guardTimer;

function releaseOnce(release) {
	if (releaseOnce.done) return;
	releaseOnce.done = true;
	release();
}
function resetReleaseOnce() {
	releaseOnce.done = false;
}

async function waitForReleaseSignal(doRelease) {
	resetReleaseOnce();
	if (!isMainThread) {
		await once(parentPort, "message");
		releaseOnce(doRelease);
		return;
	}
	// Read stdin lines until "release" (or EOF → graceful release).
	process.stdin.setEncoding("utf8");
	for await (const chunk of process.stdin) {
		for (const line of String(chunk).split("\n")) {
			if (line.trim() === "release") {
				releaseOnce(doRelease);
				return;
			}
		}
	}
	releaseOnce(doRelease); // stdin closed without release → graceful
}

function finish(code) {
	// Pipes are async in Node: never process.exit() with a non-empty write
	// queue or the JSON protocol lines can be truncated.
	const out = process.stdout;
	if (out.writableLength > 0) {
		out.once("drain", () => process.exit(code));
		setTimeout(() => process.exit(code), 500).unref();
	} else {
		process.exit(code);
	}
}

function makeEmitter() {
	if (!isMainThread) return (obj) => parentPort.postMessage(obj);
	return (obj) => process.stdout.write(JSON.stringify(obj) + "\n");
}

function sleepMs(ms) {
	return new Promise((resolve) => {
		guardTimer = setTimeout(resolve, ms);
	});
}

function readArgv() {
	const [, , dbPath, storePath, cmdB64] = process.argv;
	if (!dbPath || !storePath || !cmdB64) {
		console.error(
			"usage: child-driver.mjs <dbPath> <storeModulePath> <base64url(cmdJson)>",
		);
		process.exit(1);
	}
	try {
		const cmd = JSON.parse(Buffer.from(cmdB64, "base64url").toString("utf8"));
		return { dbPath, storePath, cmd };
	} catch (err) {
		console.error(`invalid cmd JSON: ${err?.message ?? err}`);
		process.exit(1);
	}
}

main().catch((err) => {
	console.error(err?.stack ?? err);
	process.exit(1);
});

// Silence unused-import lint for `once` when bundled oddly:
void once;

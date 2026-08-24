// TEST INFRASTRUCTURE — worker-thread contender for the in-process mutual
// exclusion contract (06 §10 "mutual exclusion across threads within process").
//
// Worker threads get SEPARATE module registries, so each contender loads the
// engine through the same node-ts-resolve.mjs hook the two-process drivers
// use — registered via dynamic import (hooks must exist BEFORE the engine
// graph resolves; static-import order would be too late).
//
// Protocol: workerData {dir, scope, identity, owner} → posts {type:"ready"}
// once loaded → waits for parent "go" → ONE synchronous tryAcquire → posts
// {type:"result"} → exits. The barrier keeps contenders genuinely concurrent.

import { pathToFileURL } from "node:url";
import { parentPort, workerData } from "node:worker_threads";

interface ContenderData {
	resolveHookUrl: string;
	engineUrl: string;
	dir: string;
	scope: string;
	identity: string;
	owner: string;
	replace?: boolean | undefined;
}

const data = workerData as ContenderData;

await import(pathToFileURL(data.resolveHookUrl).href);
const engine = (await import(pathToFileURL(data.engineUrl).href)) as {
	ScopedTokenLockManager: new (opts: {
		dir: string;
	}) => {
		tryAcquire(
			scope: string,
			id: string,
			owner: string,
			opts?: { replace?: boolean | undefined },
		):
			| { acquired: true; lock: unknown }
			| { acquired: false; holder: { owner: string } };
	};
};

parentPort?.postMessage({ type: "ready" });

parentPort?.on("message", (msg: { type: string }) => {
	if (msg.type !== "go") return;
	const manager = new engine.ScopedTokenLockManager({ dir: data.dir });
	const result = manager.tryAcquire(data.scope, data.identity, data.owner, {
		replace: data.replace,
	});
	parentPort?.postMessage({
		type: "result",
		acquired: result.acquired,
		holderOwner: result.acquired ? null : result.holder.owner,
	});
});

// TEST INFRASTRUCTURE — child-process driver for lifecycle two-process
// contracts (takeover handshake, runtime-lock death release). Run under:
//   node --import <pi_state/testing/node-ts-resolve.mjs> lifecycle-driver.ts
//       --scenario <name> --home <dir> --coord <dir> ...
//
// Protocol: prints `RESULT_JSON {...}` on stdout; coordinates via marker
// files (write = signal; poll = wait). Shape ported from the proven
// pi_state child-driver harness.

import { writeFileSync } from "node:fs";

interface Args {
	[k: string]: string;
}

function parseArgs(argv: string[]): Args {
	const out: Args = {};
	for (let i = 0; i < argv.length; i++) {
		if (argv[i] === "--scenario") {
			out["scenario"] = argv[i + 1] ?? "";
			i++;
			continue;
		}
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
		`${coordDir}/${name}`,
		JSON.stringify({ t: Date.now(), pid: process.pid, ...payload }),
	);
}

async function waitForMarker(name: string, timeoutMs = 20_000): Promise<void> {
	const { existsSync } = await import("node:fs");
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(`${coordDir}/${name}`)) {
		if (Date.now() > deadline) {
			throw new Error(`child timeout waiting for marker ${name}`);
		}
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

async function holdRunning(): Promise<Record<string, unknown>> {
	const { GatewayLifecycle } = await import("../lifecycle.js");
	const home = String(args["home"]);
	const lifecycle = new GatewayLifecycle({
		home,
		logger: {
			info() {},
			warn() {},
			error(_m, meta) {
				void meta;
			},
		},
	});
	lifecycle.installSignalHandlers();
	const result = await lifecycle.startup();
	signal(String(args["ready-marker"]), {
		ok: result.ok,
		failedStage: result.failedStage,
		pid: process.pid,
	});
	if (!result.ok) return { ok: false };
	const keepAlive = setInterval(() => {}, 60_000);
	try {
		// Park until the shutdown drain completes — safe before any signal.
		const outcome = await lifecycle.waitShutdown();
		return {
			ok: true,
			klass: outcome.klass,
			exitCode: outcome.exitCode,
			persistedStopped: outcome.persistedStopped,
			unexpected: lifecycle.unexpectedSignalInitiated,
		};
	} finally {
		clearInterval(keepAlive);
	}
}

async function holdRuntimeLock(): Promise<Record<string, unknown>> {
	const { RuntimeLock } = await import("../instance-guard.js");
	const lock = new RuntimeLock(String(args["home"]));
	const acquired = lock.acquire();
	if (!acquired) throw new Error("child could not acquire runtime lock");
	signal(String(args["ready-marker"]));
	await waitForMarker(String(args["release-marker"]));
	lock.release();
	return { released: true };
}

const SCENARIOS: Record<string, () => Promise<Record<string, unknown>>> = {
	"hold-running": holdRunning,
	"hold-runtime-lock": holdRuntimeLock,
};

async function main(): Promise<number> {
	const scenario = String(args["scenario"] ?? "");
	const run = SCENARIOS[scenario];
	if (run === undefined) throw new Error(`unknown scenario: ${scenario}`);
	const result = await run();
	console.log(`RESULT_JSON ${JSON.stringify(result)}`);
	return 0;
}

main()
	.then((code) => process.exit(code))
	.catch((err) => {
		console.error(`CHILD_ERROR ${String(err)}`);
		process.exit(1);
	});

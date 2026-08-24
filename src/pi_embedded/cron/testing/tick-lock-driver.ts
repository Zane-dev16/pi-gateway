// TEST INFRASTRUCTURE — child-process driver for the tick-lock two-process
// contracts (cross-process mutual exclusion is a liveness/exactly-once claim,
// so it demands a REAL second process). Run under:
//   node --import <node-ts-resolve.mjs> tick-lock-driver.ts \
//     --scenario hold-tick-lock --coord <dir> --cron-dir <dir> ...
//
// Protocol: prints `RESULT_JSON {...}` on stdout; coordinates via marker
// files in a shared dir (write = signal; poll = wait).

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
			const key = argv[i]!.slice(2);
			out[key] = argv[i + 1] ?? "";
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
		JSON.stringify({ t: Date.now(), ...payload }),
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

async function holdTickLock(): Promise<Record<string, unknown>> {
	const { TickLock } = await import("../tick-lock.js");
	const lock = new TickLock(String(args["cron-dir"]));
	const result = lock.acquire();
	if (!result.acquired) {
		signal(String(args["acquired-marker"]), { acquired: false });
		return { acquired: false };
	}
	signal(String(args["acquired-marker"]), { acquired: true });
	try {
		await waitForMarker(String(args["release-marker"]));
	} finally {
		result.lease.release();
	}
	return { acquired: true, held: true };
}

async function stealAttempt(): Promise<Record<string, unknown>> {
	// Contender in the SAME directory: the parent acquired BEFORE spawning us,
	// so our first attempt must observe contention (skip), and after release
	// we must win.
	const { TickLock } = await import("../tick-lock.js");
	const lock = new TickLock(String(args["cron-dir"]));
	const first = lock.acquire();
	const firstOutcome = first.acquired ? "won" : "contention";
	if (first.acquired) first.lease.release();
	await waitForMarker(String(args["release-marker"]));
	const second = lock.acquire();
	let secondOutcome = "contention";
	if (second.acquired) {
		secondOutcome = "won";
		second.lease.release();
	}
	return { firstOutcome, secondOutcome };
}

const SCENARIOS: Record<string, () => Promise<Record<string, unknown>>> = {
	"hold-tick-lock": holdTickLock,
	"steal-attempt": stealAttempt,
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

// TEST INFRASTRUCTURE — child-process driver for the composition-root
// two-process contracts (gateway-run.two-process.test.ts). Runs under:
//   node --import <pi_state/testing/node-ts-resolve.mjs> gateway-run-driver.ts
//       --home <dir> --coord <dir>
//
// Protocol: prints `RESULT_JSON {...}` on stdout; coordinates via marker
// files (write = signal; poll = wait). The child runs the REAL composition
// root — full engine stages + cron ticker + extensions discovery + a
// manifest-derived adapter entry + production drain overlays — against a
// temp profile home, installs the real OS signal handlers, and exits with
// the recorded drain class's exit status verbatim (supervisor contract).

import { readFileSync, writeFileSync } from "node:fs";

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
const home = String(args["home"] ?? ".");
const coordDir = String(args["coord"] ?? ".");

function marker(name: string, payload: Record<string, unknown> = {}): void {
	writeFileSync(
		`${coordDir}/${name}`,
		JSON.stringify({ t: Date.now(), pid: process.pid, ...payload }),
	);
}

async function main(): Promise<number> {
	// 01 §6 step 1–2: the override installs BEFORE any project import so every
	// path in the composed stack resolves under this child's profile home.
	const { setPiHomeOverride } = await import("../../pi_home.js");
	setPiHomeOverride(home);

	const { runGateway } = await import("../gateway-run.js");

	const adapterLog = `${coordDir}/adapter-log`;
	writeFileSync(adapterLog, JSON.stringify({ connects: 0, disconnects: 0 }));
	const bumpAdapterLog = (key: "connects" | "disconnects"): void => {
		const raw = JSON.parse(readFileSync(adapterLog, "utf8")) as Record<
			string,
			number
		>;
		raw[key] = (raw[key] ?? 0) + 1;
		writeFileSync(adapterLog, JSON.stringify(raw));
	};

	const hosting = {
		platform: "driver",
		manifest: {
			name: "driver",
			description: "two-process driver platform",
			transportShape: "polling" as const,
			requiresEnv: [{ name: "DRIVER_TOKEN" }],
			capabilities: {},
		},
		factory: () => ({
			async connect() {
				bumpAdapterLog("connects");
				return true;
			},
			async disconnect() {
				bumpAdapterLog("disconnects");
			},
		}),
	};

	const exit = await runGateway({
		home,
		cron: {
			runner: { run: async () => ({ ok: true }), interrupt: async () => true },
			intervalSeconds: 3600,
		},
		platforms: [hosting],
		secretReader: (name) =>
			name === "DRIVER_TOKEN" ? process.env.DRIVER_TOKEN : undefined,
		onStartupOk: () => {
			marker("ready", { ok: true });
		},
	});
	console.log(`RESULT_JSON ${JSON.stringify(exit)}`);
	return exit.exitCode;
}

main()
	.then((code) => process.exit(code))
	.catch((err) => {
		console.error(`CHILD_ERROR ${String(err)}`);
		process.exit(1);
	});

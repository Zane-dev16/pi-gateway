// TEST INFRASTRUCTURE — on-disk extension fixtures discovered through the
// REAL scanner (07 §7(a): no test-only backdoors; frozen-plugin compat loads
// via actual discovery over an mkdtemp PI_HOME-style layout).
//
// Handlers/plugins are written as self-contained .mjs modules OUTSIDE this
// package and are imported ONLY via the production dynamic-import paths
// (HookRegistry.discoverAndLoad / loadPlugins). Observation uses a
// namespaced globalThis array so fixture code stays import-free while tests
// can prove exactly WHICH handlers executed.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const CALLS_KEY = "__pi_gateway_hook_fixture_calls";

/** Recorded executions of fixture handlers/register fns (per test process). */
export function fixtureCalls(): string[] {
	return ((globalThis as Record<string, unknown>)[CALLS_KEY] as string[]) ?? [];
}

export function resetFixtureCalls(): void {
	delete (globalThis as Record<string, unknown>)[CALLS_KEY];
}

/** Self-contained recorder snippet embedded into every generated module. */
function recorderSnippet(labelExpression: string): string {
	return (
		`const __calls = ((globalThis["${CALLS_KEY}"] ??= []));\n` +
		`\t__calls.push(${labelExpression});`
	);
}

export interface TempHome {
	home: string;
	hooksDir: string;
	pluginsDir: string;
	cleanup: () => void;
}

/** mkdtemp PI_HOME-style layout; hooks/ + plugins/ created by fixture writers. */
export function makeTempHome(label = "ext"): TempHome {
	const home = mkdtempSync(join(tmpdir(), `pi-gw-${label}-`));
	return {
		home,
		hooksDir: join(home, "hooks"),
		pluginsDir: join(home, "plugins"),
		cleanup: () => {
			try {
				rmSync(home, { recursive: true, force: true });
			} catch {
				/* disposable temp */
			}
		},
	};
}

function writeText(path: string, content: string): void {
	mkdirSync(path.slice(0, path.lastIndexOf("/")), { recursive: true });
	writeFileSync(path, content);
}

export interface HookFixtureSpec {
	events?: string[];
	/** JS body INSIDE handle(); may `return` a decision object. */
	handleBody?: string;
	handleKind?: "sync" | "async";
	extraManifestFields?: Record<string, string>;
	/** Full manifest override (invalid-YAML tests etc.). */
	manifestBody?: string;
	/** Full handler-module override (syntax-error tests etc.). */
	handlerSource?: string;
	handlerBasename?: "handler.mjs" | "handler.js" | "handler.cjs";
}

/** Write `<hooksDir>/<dirName>/{HOOK.yaml,handler.mjs}` — the real layout. */
export function writeHookFixture(
	hooksDir: string,
	dirName: string,
	spec: HookFixtureSpec,
): string {
	const dir = join(hooksDir, dirName);
	let yaml: string;
	if (spec.manifestBody !== undefined) {
		yaml = spec.manifestBody;
	} else {
		yaml = `name: ${dirName}\n`;
		for (const [k, v] of Object.entries(spec.extraManifestFields ?? {})) {
			yaml += `${k}: ${v}\n`;
		}
		yaml += "events:\n";
		for (const event of spec.events ?? []) yaml += `  - ${event}\n`;
	}
	writeText(join(dir, "HOOK.yaml"), yaml);

	const basename = spec.handlerBasename ?? "handler.mjs";
	const handlerSource =
		spec.handlerSource ??
		`${spec.handleKind === "async" ? "async " : ""}export function handle(eventType, context) {\n\t${recorderSnippet(`${JSON.stringify(dirName)} + ":" + eventType`)};\n\t${spec.handleBody ?? ""}\n}\n`;
	writeText(join(dir, basename), handlerSource);
	return dir;
}

export interface PluginFixtureSpec {
	manifest?: string;
	entrySource?: string;
	entryBasename?: "index.mjs" | "index.js" | "index.cjs";
}

/** Write `<pluginsRoot>/<dirName>/{plugin.yaml,index.mjs}` — the real layout. */
export function writePluginFixture(
	pluginsRoot: string,
	dirName: string,
	spec: PluginFixtureSpec,
): string {
	const dir = join(pluginsRoot, dirName);
	const entrySource =
		spec.entrySource ??
		`export function register(ctx) {\n\t${recorderSnippet(`"register:" + ctx.manifest.key`)};\n}\n`;
	writeText(join(dir, spec.entryBasename ?? "index.mjs"), entrySource);
	writeText(
		join(dir, "plugin.yaml"),
		spec.manifest ?? `name: ${dirName}\nversion: 1\n`,
	);
	return dir;
}

export interface CapturedLog {
	lines: string[];
	sink: (message: string) => void;
	levelled: {
		info(message: string): void;
		warn(message: string): void;
		error(message: string): void;
		debug?(message: string): void;
	};
	includes(fragment: string): boolean;
}

/** Capturing log sink for loud-degradation + reason-message assertions. */
export function captureLog(): CapturedLog {
	const lines: string[] = [];
	const sink = (message: string): void => {
		lines.push(message);
	};
	return {
		lines,
		sink,
		levelled: {
			info: sink,
			warn: sink,
			error: sink,
			debug: sink,
		},
		includes: (fragment: string) => lines.some((l) => l.includes(fragment)),
	};
}

export { join };

// plugin-discovery.ts — plugin manifest discovery + KIND COERCION + load
// gating (07-integrations.md §7(b); DEC-014; 01 §4 "Plugin loader" row:
// startup-only, discovery idempotent, later-wins for model providers,
// bundled-first for memory providers).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/plugins.py:_VALID_PLUGIN_KINDS
//     → PLUGIN_KINDS {standalone, backend, exclusive, platform, model-provider}
//   hermes_cli/plugins.py:_scan_directory_level → scanPluginsDirectory
//     (sorted scan; plugin.yaml | plugin.yml; one-level category recursion
//     with `prefix/` keys; depth cap ignores deeper nesting)
//   hermes_cli/plugins.py:_parse_manifest → parsePluginManifest
//     (name default = dir name; kind non-string ⇒ standalone; unknown kind ⇒
//     warn once + standalone)
//   hermes_cli/plugins.py:_detect_kind_from_source → detectKindFromSource
//   hermes_cli/plugins.py:_parse_manifest auto-coercion block
//     → UNDECLARED kind + provider-shaped entry source ⇒ coerced kind
//   plugins.py load-all classification (kind == "exclusive" / "model-provider"
//     branches) → loadPlugins (record-but-do-not-import semantics)
//
// Provider marker vocabulary (proposed DEC text — see phase report): Hermes
// detects provider-shaped Python by substring markers on __init__.py source.
// The Pi surface transliterates to the camelCase registration API a TS plugin
// would call:
//   memory provider  → "registerMemoryProvider" OR "MemoryProvider"
//                      (either marker) ⇒ kind "exclusive"
//   model provider   → "registerModelProvider" AND "ProviderProfile"
//                      (both markers) ⇒ kind "model-provider"
// Memory is checked FIRST (parity — "register_provider" is a substring of
// "register_memory_provider", so order carries semantics). Detection reads
// ENTRY SOURCE TEXT ONLY (first 8192 chars) — provider modules are never
// imported by this loader (importing would double-instantiate profiles and
// break last-writer-wins override semantics).
//
// v0.1 scope gaps (each lands with its owning phase, none silent):
//   • no bundled-plugin root yet in the TS package layout (bundled-first
//     memory discovery activates with pi_embedded memory work);
//   • pip-entry-point discovery N/A (no pip); project plugins gated by
//     PI_ENABLE_PROJECT_PLUGINS (utils.env_var_enabled parity);
//   • `plugins.enabled` config plumbing arrives with config loading — the
//     opt-in set is option-injected here.

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { resolvePiHome } from "../../pi_home.js";
import { isTruthyValue } from "../../pi_gateway/commands/command-def.js";
import { parseFlatYaml } from "./manifest-yaml.js";

export const PLUGIN_KINDS = [
	"backend",
	"exclusive",
	"model-provider",
	"platform",
	"standalone",
] as const;

export type PluginKind = (typeof PLUGIN_KINDS)[number];

export const PLUGIN_MANIFEST_BASENAMES = ["plugin.yaml", "plugin.yml"] as const;

/** Entry-module candidates scanned for coercion + register() (see header). */
export const PLUGIN_ENTRY_BASENAMES = [
	"index.mjs",
	"index.js",
	"index.cjs",
] as const;

/** HERMES_ENABLE_PROJECT_PLUGINS analog. Truthy per utils.TRUTHY_STRINGS. */
export const PROJECT_PLUGINS_ENV = "PI_ENABLE_PROJECT_PLUGINS";

/**
 * The lifecycle-hook names §7(b) lists for plugin registration. Unknown
 * names are still RECORDED but warned (forward-compat precedent:
 * plugins.py:register_middleware stores unknown kinds with a warning).
 */
export const VALID_PLUGIN_LIFECYCLE_HOOKS: readonly string[] = [
	"pre_tool_call",
	"post_tool_call",
	"pre_llm_call",
	"post_llm_call",
	"on_session_start",
	"on_session_end",
];

export type PluginSource = "user" | "project";

export interface PluginManifest {
	name: string;
	version: string;
	description: string;
	source: PluginSource;
	path: string;
	key: string;
	kind: PluginKind;
	/** True when plugin.yaml declared `kind:` explicitly (coercion ineligible). */
	kindExplicit: boolean;
	/** The FULL parsed manifest verbatim — unknown fields ride along ignored. */
	data: Record<string, unknown>;
}

export interface PluginLogSink {
	info(message: string): void;
	warn(message: string): void;
	error(message: string): void;
	debug?(message: string): void;
}

function stderrPluginSink(): PluginLogSink {
	const write = (message: string): void => {
		try {
			process.stderr.write(`${message}\n`);
		} catch {
			/* stderr unavailable */
		}
	};
	return {
		info: write,
		warn: write,
		error: write,
		debug: write,
	};
}

export function detectKindFromSource(
	sourceText: string,
): PluginKind | undefined {
	if (
		sourceText.includes("registerMemoryProvider") ||
		sourceText.includes("MemoryProvider")
	) {
		return "exclusive";
	}
	if (
		sourceText.includes("registerModelProvider") &&
		sourceText.includes("ProviderProfile")
	) {
		return "model-provider";
	}
	return undefined;
}

/** First 8192 chars of the first existing entry module (errors="replace" ≙ utf8). */
function readEntrySource(pluginDir: string): string | undefined {
	for (const base of PLUGIN_ENTRY_BASENAMES) {
		const candidate = join(pluginDir, base);
		if (!existsSync(candidate)) continue;
		try {
			return readFileSync(candidate, "utf8").slice(0, 8192);
		} catch {
			return undefined;
		}
	}
	return undefined;
}

export function parsePluginManifest(
	manifestFile: string,
	pluginDir: string,
	source: PluginSource,
	prefix: string,
	log: PluginLogSink,
): PluginManifest | null {
	try {
		const data = parseFlatYaml(readFileSync(manifestFile, "utf8"));
		if (data === null || typeof data !== "object") return null;

		const dirName = pluginDir.split(/[\\/]/).pop() ?? pluginDir;
		const rawName = data.name;
		// data.get("name", dir_name) parity: any STRING value used verbatim.
		const name = typeof rawName === "string" ? rawName : dirName;
		const key = prefix.length > 0 ? `${prefix}/${dirName}` : name;

		const kindExplicit = Object.hasOwn(data, "kind");
		let rawKind: unknown = kindExplicit ? data.kind : "standalone";
		if (typeof rawKind !== "string") rawKind = "standalone";
		let kind = (rawKind as string).trim().toLowerCase() as PluginKind;
		if (!PLUGIN_KINDS.includes(kind)) {
			log.warn(
				`Plugin ${key}: unknown kind '${String(rawKind)}' (valid: ${PLUGIN_KINDS.join(", ")}); treating as 'standalone'`,
			);
			kind = "standalone";
		}

		// Auto-coerce UNDECLARED provider-shaped plugins so they route to their
		// own discovery systems instead of being loaded here. Explicit kinds are
		// NEVER coerced ("kind" not in data gate, parity).
		if (kind === "standalone" && !kindExplicit) {
			const detected = detectKindFromSource(readEntrySource(pluginDir) ?? "");
			if (detected !== undefined) {
				log.debug?.(
					`Plugin ${key}: detected ${detected}, treating as kind='${detected}'`,
				);
				kind = detected;
			}
		}

		const version = data.version;
		const description = data.description;
		return {
			name,
			version: version === undefined || version === null ? "" : String(version),
			description: typeof description === "string" ? description : "",
			source,
			path: pluginDir,
			key,
			kind,
			kindExplicit,
			data,
		};
	} catch (err) {
		log.warn(
			`Failed to parse ${manifestFile}: ${err instanceof Error ? err.message : String(err)}`,
		);
		return null;
	}
}

function scanLevel(
	root: string,
	source: PluginSource,
	prefix: string,
	depth: number,
	out: PluginManifest[],
	log: PluginLogSink,
): void {
	if (!existsSync(root)) return;
	let entries;
	try {
		entries = readdirSync(root, { withFileTypes: true });
	} catch (err) {
		log.warn(
			`Failed to scan ${root}: ${err instanceof Error ? err.message : String(err)}`,
		);
		return;
	}
	for (const entry of [...entries].sort((a, b) =>
		a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
	)) {
		if (!entry.isDirectory()) continue;
		const childDir = join(root, entry.name);
		let manifestFile: string | null = null;
		for (const base of PLUGIN_MANIFEST_BASENAMES) {
			const candidate = join(childDir, base);
			if (existsSync(candidate)) {
				manifestFile = candidate;
				break;
			}
		}
		if (manifestFile !== null) {
			const manifest = parsePluginManifest(
				manifestFile,
				childDir,
				source,
				prefix,
				log,
			);
			if (manifest !== null) out.push(manifest);
			continue;
		}
		// No manifest: within the depth cap this is a CATEGORY namespace —
		// recurse exactly one more level (`<root>/image_gen/openai/` shape).
		if (depth >= 1) {
			log.debug?.(`Skipping ${entry.name} (no plugin.yaml, depth cap reached)`);
			continue;
		}
		scanLevel(
			childDir,
			source,
			prefix.length > 0 ? `${prefix}/${entry.name}` : entry.name,
			depth + 1,
			out,
			log,
		);
	}
}

export interface DiscoverPluginsOptions {
	log?: PluginLogSink;
	/** Force the project-plugins gate (default: read PI_ENABLE_PROJECT_PLUGINS). */
	enableProjectPlugins?: boolean;
	/** Project root override (default process.cwd()) for `<root>/.pi/plugins`. */
	projectRoot?: string;
}

/**
 * Discovery over user dir (`<resolvePiHome()>/plugins`, always scanned) then
 * project dir (`<cwd>/.pi/plugins`, env-gated) — LATER sources override
 * earlier ones on key collision (later-writer-wins, module-docstring parity).
 * Pure over disk state: calling twice yields fresh identical results
 * (01 §4 "discovery idempotent").
 */
export function discoverPlugins(
	options: DiscoverPluginsOptions = {},
): PluginManifest[] {
	const log = options.log ?? stderrPluginSink();
	const found: PluginManifest[] = [];
	scanLevel(join(resolvePiHome(), "plugins"), "user", "", 0, found, log);

	const projectEnabled =
		options.enableProjectPlugins ??
		isTruthyValue(process.env[PROJECT_PLUGINS_ENV]);
	if (projectEnabled) {
		const root = options.projectRoot ?? process.cwd();
		scanLevel(join(root, ".pi", "plugins"), "project", "", 0, found, log);
	}

	// Later-writer-wins dedupe by registry key (Map.set keeps original position).
	const byKey = new Map<string, PluginManifest>();
	for (const manifest of found) byKey.set(manifest.key, manifest);
	return [...byKey.values()];
}

export type PluginLifecycleCallback = (
	payload: Record<string, unknown>,
) => unknown;

export interface PluginContext {
	readonly manifest: Readonly<{
		name: string;
		key: string;
		kind: PluginKind;
		version: string;
		description: string;
		path: string;
	}>;
	/** Home snapshot at load time (resolvePiHome()). */
	readonly home: string;
	readonly log: PluginLogSink;
	/**
	 * Subscribe a lifecycle callback. Unknown hook names are stored AND warned
	 * (forward-compat surface — plugins must never be broken by additive hook
	 * names; compat contract 07 §7(b)).
	 */
	on(name: string, callback: PluginLifecycleCallback): void;
}

export interface LoadedPlugin {
	manifest: PluginManifest;
	enabled: boolean;
	/** Recorded failure reason (load errors NEVER throw out of loadPlugins). */
	error?: string;
	subscriptions?: ReadonlyMap<string, PluginLifecycleCallback[]>;
}

export interface LoadPluginsOptions {
	manifests: readonly PluginManifest[];
	/**
	 * Opt-in enablement (config `plugins.enabled` parity). A plugin loads only
	 * when its KEY or bare NAME is in the set; "all" force-enables; absent ⇒
	 * nothing loads (opt-in system parity).
	 */
	enabled?: ReadonlySet<string> | "all";
	log?: PluginLogSink;
}

function isEnabledKey(
	enabled: ReadonlySet<string> | "all",
	key: string,
): boolean {
	if (enabled === "all") return true;
	return enabled.has(key);
}

/**
 * Classify + load manifests per the Hermes gating ladder. Provider kinds are
 * RECORDED but their modules are NEVER imported here: exclusive plugins
 * activate via `<category>.provider` config, model providers via the lazy
 * third discovery with last-writer-wins registration. Standalone/backend/
 * platform plugins from USER/PROJECT roots are opt-in via `enabled`.
 * A failing register() is recorded on that plugin and never blocks others.
 */
export async function loadPlugins(
	options: LoadPluginsOptions,
): Promise<LoadedPlugin[]> {
	const log = options.log ?? stderrPluginSink();
	const enabled = options.enabled;
	const out: LoadedPlugin[] = [];

	for (const manifest of options.manifests) {
		const lookupKey = manifest.key || manifest.name;

		// Exclusive plugins have their own activation path (memory).
		if (manifest.kind === "exclusive") {
			out.push({
				manifest,
				enabled: false,
				error: "exclusive plugin — activate via <category>.provider config",
			});
			continue;
		}
		// Model providers load via providers/ lazy discovery — recorded only.
		if (manifest.kind === "model-provider") {
			out.push({ manifest, enabled: true });
			log.debug?.(
				`Skipping '${lookupKey}' (model-provider, handled by providers/ discovery)`,
			);
			continue;
		}

		// Everything else is opt-in via the enablement set (key or legacy bare name).
		if (
			enabled === undefined ||
			(!isEnabledKey(enabled, lookupKey) &&
				!isEnabledKey(enabled, manifest.name))
		) {
			log.debug?.(`Skipping disabled plugin '${lookupKey}'`);
			continue;
		}

		await loadOnePlugin(manifest, log, out);
	}
	return out;
}

async function loadOnePlugin(
	manifest: PluginManifest,
	log: PluginLogSink,
	out: LoadedPlugin[],
): Promise<void> {
	const subscriptions = new Map<string, PluginLifecycleCallback[]>();
	try {
		let entryPath: string | null = null;
		for (const base of PLUGIN_ENTRY_BASENAMES) {
			const candidate = join(manifest.path, base);
			if (existsSync(candidate)) {
				entryPath = candidate;
				break;
			}
		}
		if (entryPath === null) {
			out.push({
				manifest,
				enabled: false,
				error: `no entry module (${PLUGIN_ENTRY_BASENAMES.join(" | ")}) found`,
			});
			return;
		}
		const module = (await import(pathToFileURL(entryPath).href)) as Record<
			string,
			unknown
		>;
		const registerFn =
			typeof module.register === "function"
				? module.register
				: typeof module.default === "object" &&
						module.default !== null &&
						typeof (module.default as Record<string, unknown>).register ===
							"function"
					? (module.default as Record<string, unknown>).register
					: undefined;
		if (typeof registerFn !== "function") {
			out.push({
				manifest,
				enabled: false,
				error: "no 'register(ctx)' export found",
			});
			return;
		}
		const context: PluginContext = {
			manifest: {
				name: manifest.name,
				key: manifest.key,
				kind: manifest.kind,
				version: manifest.version,
				description: manifest.description,
				path: manifest.path,
			},
			home: resolvePiHome(),
			log,
			on(name, callback) {
				if (!VALID_PLUGIN_LIFECYCLE_HOOKS.includes(name)) {
					log.warn(
						`Plugin '${manifest.name}' subscribed to UNKNOWN lifecycle hook '${name}' (valid: ${VALID_PLUGIN_LIFECYCLE_HOOKS.join(", ")}) — stored for forward compatibility`,
					);
				}
				const list = subscriptions.get(name);
				if (list === undefined) subscriptions.set(name, [callback]);
				else list.push(callback);
			},
		};
		await (registerFn as (ctx: PluginContext) => unknown)(context);
		out.push({ manifest, enabled: true, subscriptions });
	} catch (err) {
		out.push({
			manifest,
			enabled: false,
			error: `register failed: ${err instanceof Error ? err.message : String(err)}`,
		});
	}
}

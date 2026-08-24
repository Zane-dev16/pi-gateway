// startup.ts — the optional-stage-shaped entry for embedded extensions
// (01 §3.1 stage 8 "embedded_watchers" slot; degradation classification:
// per-service LOUD degrade without blocking other services).
//
// The future lifecycle stage body calls startupEmbeddedExtensions(): hook
// discovery or plugin discovery failing degrades THIS service loudly and
// leaves an empty-but-functional snapshot — never an aborted startup, never
// a silent empty set. No live reload (DEC-013): extensions are discovered
// once here; changes take effect next session.

import { type DiscoverOptions, HookRegistry } from "./hook-registry.js";
import {
	type DiscoverPluginsOptions,
	discoverPlugins,
	type LoadedPlugin,
	loadPlugins,
	type PluginLogSink,
} from "./plugin-discovery.js";

/** Log sink accepted everywhere: leveled plugin sink (debug optional). */
export type EmbeddedLogSink = PluginLogSink;

export interface EmbeddedExtensionsSnapshot {
	hooks: HookRegistry;
	pluginManifests: ReturnType<typeof discoverPlugins>;
	loadedPlugins: LoadedPlugin[];
}

export interface StartupEmbeddedExtensionsOptions {
	log: EmbeddedLogSink;
	/** Log sink forwarded to hook discovery. */
	hookDiscovery?: DiscoverOptions;
	enableProjectPlugins?: boolean;
	projectRoot?: string;
	/** Opt-in enablement forwarded to loadPlugins (undefined ⇒ nothing loads). */
	enabledPlugins?: ReadonlySet<string> | "all";
}

/**
 * Discover hooks + plugins with per-service loud degradation. NEVER throws:
 * a broken extension tree must not block platform adapters or cron from
 * starting (01 §3.1 optional-stage semantics).
 */
export async function startupEmbeddedExtensions(
	options: StartupEmbeddedExtensionsOptions,
): Promise<EmbeddedExtensionsSnapshot> {
	const hooks = new HookRegistry();
	try {
		await hooks.discoverAndLoad({
			log: options.log.debug ?? options.log.info,
		});
	} catch (err) {
		options.log.error(
			`[hooks] embedded EXTENSION discovery degraded — continuing WITHOUT user hooks: ${String(err)}`,
		);
	}

	let pluginManifests: EmbeddedExtensionsSnapshot["pluginManifests"] = [];
	const discoverOptions: DiscoverPluginsOptions = { log: options.log };
	if (options.enableProjectPlugins !== undefined) {
		discoverOptions.enableProjectPlugins = options.enableProjectPlugins;
	}
	if (options.projectRoot !== undefined) {
		discoverOptions.projectRoot = options.projectRoot;
	}
	try {
		pluginManifests = discoverPlugins(discoverOptions);
	} catch (err) {
		options.log.error(
			`[plugins] embedded PLUGIN discovery degraded — continuing WITHOUT user plugins: ${String(err)}`,
		);
	}

	let loadedPlugins: LoadedPlugin[] = [];
	try {
		const loadOptions: Parameters<typeof loadPlugins>[0] = {
			manifests: pluginManifests,
			log: options.log,
		};
		if (options.enabledPlugins !== undefined) {
			loadOptions.enabled = options.enabledPlugins;
		}
		loadedPlugins = await loadPlugins(loadOptions);
	} catch (err) {
		options.log.error(
			`[plugins] embedded PLUGIN loading degraded — continuing WITHOUT loaded plugins: ${String(err)}`,
		);
	}

	return { hooks, pluginManifests, loadedPlugins };
}

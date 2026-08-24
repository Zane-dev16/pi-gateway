// pi_embedded/hooks — file-drop event hooks + plugin discovery
// (07-integrations.md §7; DEC-014). Layer rank 4: imports pi_home /
// pi_gateway downward, never the reverse; started BY the runner through the
// optional-stage pattern (./startup.ts) but never importing it.
//
// THE INVARIANT SPLIT (DEC-014): observer events are emit-and-log with
// swallowed handler exceptions; `command:<canonical>` events are
// decision-bearing and their deny/handled/rewrite verdicts are HONORED at
// exactly one interception point before core handling. Flattening either
// class into the other is a correctness bug.

export {
	type CommandHookVerdict,
	collectCommandHookResults,
	type CollectCommandHookOptions,
	processCommandHookResults,
	type ProcessCommandResultsOptions,
	runCommandHooks,
	type RunCommandHooksOptions,
	type CommandNameRow,
} from "./command-decisions.js";

export {
	HookRegistry,
	type DiscoverOptions,
	type HookHandler,
	type HookLogSink,
	type LoadedHookInfo,
} from "./hook-registry.js";

export {
	ManifestSyntaxError,
	parseFlatYaml,
} from "./manifest-yaml.js";

export {
	PLUGIN_ENTRY_BASENAMES,
	PLUGIN_KINDS,
	PLUGIN_MANIFEST_BASENAMES,
	PROJECT_PLUGINS_ENV,
	VALID_PLUGIN_LIFECYCLE_HOOKS,
	type LoadedPlugin,
	loadPlugins,
	type LoadPluginsOptions,
	detectKindFromSource,
	discoverPlugins,
	type DiscoverPluginsOptions,
	type PluginContext,
	type PluginKind,
	type PluginLifecycleCallback,
	type PluginLogSink,
	type PluginManifest,
	type PluginSource,
	parsePluginManifest,
} from "./plugin-discovery.js";

export {
	startupEmbeddedExtensions,
	type EmbeddedExtensionsSnapshot,
	type StartupEmbeddedExtensionsOptions,
} from "./startup.js";

export {
	EMBEDDED_EXTENSIONS_SERVICE_NAME,
	extensionsServiceEntry,
	type ExtensionsServiceEntry,
} from "./stage-entry.js";

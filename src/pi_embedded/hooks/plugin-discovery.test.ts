// Behavior contracts for plugin discovery, KIND COERCION, and load gating
// (07-integrations.md §7(b); DEC-014; 01 §4 "Plugin loader" row).
//
// Contract groups:
//   1. Kind coercion (plugins.py:_parse_manifest auto-coercion port):
//      UNDECLARED kind + provider-shaped entry source coerces; explicit kinds
//      are never touched; invalid/non-string kinds fall back standalone+warn.
//   2. Load gating: exclusive/model-provider recorded-but-never-imported;
//      standalone opt-in via enablement set (key or bare name).
//   3. Frozen-plugin compat through REAL discovery: unknown manifest fields
//      ignored, module still loads via the real scanner over an mkdtemp
//      PI_HOME layout.
//   4. Discovery determinism: sorted scan, one-level category recursion with
//      depth cap, later-writer-wins across sources.
//   5. Startup facade: per-service LOUD degradation without blocking.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
	resetPiHomeCacheForTests,
	resetPiHomeOverride,
	setPiHomeOverride,
} from "../../pi_home.js";
import {
	type PLUGIN_KINDS,
	PROJECT_PLUGINS_ENV,
	detectKindFromSource,
	discoverPlugins,
	loadPlugins,
	parsePluginManifest,
	type PluginManifest,
} from "./plugin-discovery.js";
import { startupEmbeddedExtensions } from "./startup.js";
import {
	captureLog,
	fixtureCalls,
	makeTempHome,
	resetFixtureCalls,
	writePluginFixture,
	type TempHome,
} from "./testing/fixtures.js";

let home: TempHome;
const log = captureLog();

beforeEach(() => {
	home = makeTempHome("plugins");
	setPiHomeOverride(home.home);
	resetFixtureCalls();
	log.lines.length = 0;
});
afterEach(() => {
	home.cleanup();
	resetPiHomeOverride();
	resetPiHomeCacheForTests();
	delete process.env[PROJECT_PLUGINS_ENV];
	resetFixtureCalls();
});

function discover(): PluginManifest[] {
	return discoverPlugins({ log: log.levelled });
}

describe("kind coercion (model-provider shaped ⇒ correct kind)", () => {
	it("UNDECLARED kind + registerModelProvider/ProviderProfile source ⇒ kind 'model-provider', detected import-free", () => {
		writePluginFixture(home.pluginsDir, "acme-models", {
			entrySource: [
				"// registers custom models with the gateway",
				"import { registerModelProvider } from 'somewhere/providers';",
				"const profile = new ProviderProfile({ id: 'acme' });",
				"registerModelProvider(profile);",
				"export function register(ctx) { recordFixtureCall('MUST-NOT-LOAD'); }",
			].join("\n"),
			manifest: "name: acme-models\nversion: 1\n", // NO kind field
		});
		const [manifest] = discover();
		expect(manifest?.kind).toBe("model-provider");
		expect(manifest?.kindExplicit).toBe(false);
	});

	it("coerced model-provider is RECORDED but its module is NEVER imported by the general loader", async () => {
		writePluginFixture(home.pluginsDir, "lazy-models", {
			entrySource: [
				// Import of a nonexistent specifier would throw IF the module were
				// imported — surviving load proves the import-free detection.
				"import { registerModelProvider } from 'definitely-not-installed-pi-providers';",
				"export const profile = new ProviderProfile('x');",
				"registerModelProvider(profile);",
				"export function register(ctx) { ctx.log.info('MUST-NOT-LOAD'); }",
			].join("\n"),
			manifest: "name: lazy-models\n",
		});
		const manifests = discover();
		const loaded = await loadPlugins({
			manifests,
			enabled: "all",
			log: log.levelled,
		});
		expect(loaded).toHaveLength(1);
		expect(loaded[0]?.enabled).toBe(true);
		expect(loaded[0]?.error).toBeUndefined();
		expect(loaded[0]?.subscriptions).toBeUndefined(); // nothing registered
		expect(log.includes("MUST-NOT-LOAD")).toBe(false);
		expect(
			log.includes(
				"Skipping 'lazy-models' (model-provider, handled by providers/ discovery)",
			),
		).toBe(true);
	});

	it("memory-provider-shaped source coerces to 'exclusive' and loads NOTHING via activation-note error", async () => {
		writePluginFixture(home.pluginsDir, "my-memory", {
			entrySource:
				"export function registerMemoryProvider(p) { return p; }\nexport function register(ctx) { ctx.log.info('MEMORY-MUST-NOT-LOAD'); }\n",
			manifest: "name: my-memory\n",
		});
		const manifests = discover();
		const loaded = await loadPlugins({
			manifests,
			enabled: "all",
			log: log.levelled,
		});
		expect(manifests[0]?.kind).toBe("exclusive");
		expect(loaded[0]?.enabled).toBe(false);
		expect(loaded[0]?.error).toBe(
			"exclusive plugin — activate via <category>.provider config",
		);
		expect(log.includes("MEMORY-MUST-NOT-LOAD")).toBe(false);
	});

	it("EXPLICIT kind is never coerced even when source looks like a provider", () => {
		writePluginFixture(home.pluginsDir, "explicit-standalone", {
			entrySource:
				"export const p = new ProviderProfile('x'); registerModelProvider(p);\nexport function register(ctx) {}\n",
			manifest: "name: explicit-standalone\nkind: standalone\n",
		});
		const [manifest] = discover();
		expect(manifest?.kind).toBe("standalone");
		expect(manifest?.kindExplicit).toBe(true);
	});

	it("explicit model-provider honored without coercion", () => {
		writePluginFixture(home.pluginsDir, "declared-models", {
			entrySource: "export function register(ctx) {}\n",
			manifest: "name: declared-models\nkind: model-provider\n",
		});
		const [manifest] = discover();
		expect(manifest?.kind).toBe("model-provider");
		expect(manifest?.kindExplicit).toBe(true);
	});

	it("invalid kind string warns once and treats as standalone", () => {
		writePluginFixture(home.pluginsDir, "odd-kind", {
			manifest: "name: odd-kind\nkind: starship\n",
		});
		const [manifest] = discover();
		expect(manifest?.kind).toBe("standalone");
		expect(log.includes("Plugin odd-kind: unknown kind 'starship'")).toBe(true);
	});

	it("non-string kind value falls back to standalone without throwing", () => {
		writePluginFixture(home.pluginsDir, "numeric-kind", {
			manifest: "name: numeric-kind\nversion: 1\n",
			entrySource: "export function register(ctx) {}\n",
		});
		mkdirSync(join(home.pluginsDir, "list-kind"), { recursive: true });
		writeFileSync(
			join(home.pluginsDir, "list-kind", "plugin.yaml"),
			"name: list-kind\nkind: [a, b]\n",
		);
		const manifests = discover();
		const kinds = Object.fromEntries(
			manifests.map((m) => [m.name, m.kind]),
		) as Record<string, (typeof PLUGIN_KINDS)[number]>;
		// numeric-kind has no markers → stays standalone; list-kind's inline
		// LIST as kind is not a string ⇒ standalone.
		expect(kinds["numeric-kind"]).toBe("standalone");
		expect(kinds["list-kind"]).toBe("standalone");
	});

	it("marker detection ORDER matters: memory beats model-provider on overlapping text", () => {
		const both = detectKindFromSource(
			"registerMemoryProvider(new ProviderProfile()); registerModelProvider(x)",
		);
		expect(both).toBe("exclusive");
	});

	it("unrelated sources stay standalone (no markers)", () => {
		expect(
			detectKindFromSource("export function register(ctx) {}"),
		).toBeUndefined();
	});
});

describe("load gating (opt-in system parity)", () => {
	function standalone(name: string): void {
		writePluginFixture(home.pluginsDir, name, {
			manifest: `name: ${name}\n`,
		});
	}

	it("no enablement set ⇒ NOTHING executes (recorded manifests only)", async () => {
		standalone("toolkit-a");
		standalone("toolkit-b");
		const loaded = await loadPlugins({
			manifests: discover(),
			log: log.levelled,
		});
		expect(loaded).toHaveLength(0);
		expect(fixtureCalls()).toEqual([]);
		expect(
			log.lines.some((l) => l.includes("Skipping disabled plugin 'toolkit-a'")),
		).toBe(true);
	});

	it("enablement by KEY or legacy bare NAME both admit; others stay out", async () => {
		standalone("alpha-tool");
		standalone("beta-tool");
		const loaded = await loadPlugins({
			manifests: discover(),
			enabled: new Set(["alpha-tool"]), // key == name at top level
			log: log.levelled,
		});
		expect(loaded.map((p) => p.manifest.name)).toEqual(["alpha-tool"]);
		expect(fixtureCalls()).toEqual(["register:alpha-tool"]);
	});

	it("register() throwing records the error and NEVER blocks sibling plugins", async () => {
		standalone("a-crasher");
		standalone("b-worker");
		writePluginFixture(home.pluginsDir, "a-crasher", {
			manifest: "name: a-crasher\n",
			entrySource:
				"export function register() { throw new Error('ctx exploded'); }\n",
		});
		const loaded = await loadPlugins({
			manifests: discover(),
			enabled: "all",
			log: log.levelled,
		});
		const crasher = loaded.find((p) => p.manifest.name === "a-crasher");
		const worker = loaded.find((p) => p.manifest.name === "b-worker");
		expect(crasher?.enabled).toBe(false);
		expect(crasher?.error).toContain("ctx exploded");
		expect(worker?.enabled).toBe(true);
		expect(fixtureCalls()).toEqual(["register:b-worker"]);
	});

	it("missing entry module records a loud error for that plugin only", async () => {
		mkdirSync(join(home.pluginsDir, "hollow"), { recursive: true });
		writeFileSync(
			join(home.pluginsDir, "hollow", "plugin.yaml"),
			"name: hollow\n",
		);
		const loaded = await loadPlugins({
			manifests: discover(),
			enabled: "all",
			log: log.levelled,
		});
		expect(loaded[0]?.error).toContain("no entry module");
	});

	it("loaded plugins receive lifecycle subscriptions retrievable for runner wiring", async () => {
		writePluginFixture(home.pluginsDir, "subscribing", {
			manifest: "name: subscribing\n",
			entrySource: [
				"export function register(ctx) {",
				"\tctx.on('on_session_start', (payload) => payload);",
				"\tctx.on('pre_tool_call', (payload) => payload);",
				"}",
			].join("\n"),
		});
		const loaded = await loadPlugins({
			manifests: discover(),
			enabled: new Set(["subscribing"]),
			log: log.levelled,
		});
		const subs = loaded[0]?.subscriptions;
		expect(subs?.get("on_session_start")).toHaveLength(1);
		expect(subs?.get("pre_tool_call")).toHaveLength(1);
	});

	it("UNKNOWN lifecycle hook name stored but warned (forward-compat surface)", async () => {
		writePluginFixture(home.pluginsDir, "future-hooker", {
			manifest: "name: future-hooker\n",
			entrySource:
				"export function register(ctx) { ctx.on('on_quantum_sync', () => 1); }\n",
		});
		await loadPlugins({
			manifests: discover(),
			enabled: new Set(["future-hooker"]),
			log: log.levelled,
		});
		expect(log.includes("UNKNOWN lifecycle hook 'on_quantum_sync'")).toBe(true);
	});
});

describe("frozen-plugin compat through REAL discovery", () => {
	it("frozen-era plugin.yaml with UNKNOWN v2+ fields loads via the real scanner; unknown fields ignored", async () => {
		writePluginFixture(home.pluginsDir, "frozen-compat", {
			manifest: [
				"# written against the FROZEN v1 manifest surface",
				"name: frozen-compat",
				"version: 1",
				"description: predates every v2 field",
				"license: MIT",
				"homepage: https://example.invalid/frozen-compat",
				"tags: [legacy, compat]",
				"manifest_version: 2",
				"brand_new_unknown_field: whatever-v3-adds",
			].join("\n"),
		});
		const manifests = discover();
		expect(manifests).toHaveLength(1);
		// Unknown fields ride along verbatim in `data` but gate NOTHING:
		expect(manifests[0]?.data["brand_new_unknown_field"]).toBe(
			"whatever-v3-adds",
		);
		const loaded = await loadPlugins({
			manifests,
			enabled: new Set(["frozen-compat"]),
			log: log.levelled,
		});
		expect(loaded[0]?.enabled).toBe(true);
		expect(fixtureCalls()).toEqual(["register:frozen-compat"]); // executed via REAL discovery
	});

	it("malformed manifest skips LOUDLY while siblings still load", () => {
		writePluginFixture(home.pluginsDir, "broken-manifest", {
			manifest: "name: broken\ncategory:\n  nested: map\n",
		});
		writePluginFixture(home.pluginsDir, "healthy-sibling", {
			manifest: "name: healthy-sibling\n",
		});
		const manifests = discover();
		expect(manifests.map((m) => m.name)).toEqual(["healthy-sibling"]);
		expect(log.includes("Failed to parse")).toBe(true);
	});
});

describe("discovery order determinism + category recursion", () => {
	it("sorted scan; one-level category recursion yields prefix keys; depth cap ignores deeper nesting", () => {
		writePluginFixture(home.pluginsDir, "zeta", {});
		writePluginFixture(home.pluginsDir, "alpha", {});
		// Category namespace: <root>/image_gen/openai/
		writePluginFixture(join(home.pluginsDir, "image_gen"), "openai", {});
		// Beyond the depth cap: <root>/image_gen/openai/deeper/nested — IGNORED
		writePluginFixture(
			join(home.pluginsDir, "image_gen", "openai", "deeper"),
			"nested",
			{},
		);

		const manifests = discover();
		expect(manifests.map((m) => m.key)).toEqual([
			"alpha",
			"image_gen/openai",
			"zeta",
		]);
	});

	it("later-writer-wins: project plugin replaces same-key user plugin", () => {
		writePluginFixture(home.pluginsDir, "dupe", {
			manifest: "name: dupe\nversion: user-1\n",
		});
		const projectRoot = join(home.home, "project");
		writePluginFixture(join(projectRoot, ".pi", "plugins"), "dupe", {
			manifest: "name: dupe\nversion: project-1\n",
		});
		const manifests = discoverPlugins({
			log: log.levelled,
			enableProjectPlugins: true,
			projectRoot,
		});
		expect(manifests).toHaveLength(1);
		expect(manifests[0]?.version).toBe("project-1"); // project (LATER source) won
		expect(manifests[0]?.source).toBe("project");
	});

	it("project dir scanned ONLY when PI_ENABLE_PROJECT_PLUGINS truthy (env_var_enabled parity)", () => {
		const projectRoot = join(home.home, "proj");
		writePluginFixture(join(projectRoot, ".pi", "plugins"), "proj-only", {});

		expect(discover()).toHaveLength(0); // env unset → not scanned

		process.env[PROJECT_PLUGINS_ENV] = "1";
		let seen = discoverPlugins({ log: log.levelled, projectRoot });
		expect(seen.map((m) => m.key)).toEqual(["proj-only"]);

		process.env[PROJECT_PLUGINS_ENV] = "yes";
		seen = discoverPlugins({ log: log.levelled, projectRoot });
		expect(seen).toHaveLength(1);

		for (const falsy of ["0", "false", "off", ""]) {
			process.env[PROJECT_PLUGINS_ENV] = falsy;
			expect(discoverPlugins({ log: log.levelled, projectRoot })).toHaveLength(
				0,
			);
		}
	});
});

describe("startupEmbeddedExtensions — optional-stage loud degradation", () => {
	it("hook discovery failure degrades THIS service loudly; plugins still discovered+loaded", async () => {
		// <home>/hooks exists but is a FILE ⇒ readdirSync throws inside discovery.
		mkdirSync(home.home, { recursive: true });
		writeFileSync(join(home.home, "hooks"), "not a directory");
		writePluginFixture(home.pluginsDir, "survivor", {
			manifest: "name: survivor\n",
		});
		const snapshot = await startupEmbeddedExtensions({
			log: log.levelled,
			enabledPlugins: new Set(["survivor"]),
			enableProjectPlugins: false,
		});
		expect(snapshot.hooks.loadedHooks).toHaveLength(0);
		expect(snapshot.hooks.isEmpty).toBe(true);
		expect(snapshot.pluginManifests.map((m) => m.key)).toEqual(["survivor"]);
		expect(snapshot.loadedPlugins[0]?.enabled).toBe(true);
		expect(
			log.lines.some(
				(l) =>
					l.includes("embedded EXTENSION discovery degraded") ||
					l.startsWith("[hooks] Error scanning"),
			),
		).toBe(true);
	});

	it("happy path returns functional registry + loaded plugins in one snapshot", async () => {
		writeHookFixtureCompat();
		const snapshot = await startupEmbeddedExtensions({
			log: log.levelled,
			enabledPlugins: "all",
			enableProjectPlugins: false,
		});
		expect(snapshot.hooks.loadedHooks.map((h) => h.name)).toEqual([
			"boot-watcher",
		]);
		await snapshot.hooks.emit("gateway:startup", {});
		expect(fixtureCalls()).toContain("boot-watcher:gateway:startup");
	});

	function writeHookFixtureCompat(): void {
		mkdirSync(join(home.hooksDir, "boot-watcher"), { recursive: true });
		writeFileSync(
			join(home.hooksDir, "boot-watcher", "HOOK.yaml"),
			"name: boot-watcher\nevents:\n  - gateway:startup\n",
		);
		writeFileSync(
			join(home.hooksDir, "boot-watcher", "handler.mjs"),
			[
				`export function handle(eventType, context) {`,
				`\tconst __calls = ((globalThis["__pi_gateway_hook_fixture_calls"] ??= []));`,
				`\t__calls.push("boot-watcher:" + eventType);`,
				`}`,
			].join("\n"),
		);
	}
});

describe("parsePluginManifest unit seams", () => {
	it("dir name fills in when name missing; version/description default empty-string", () => {
		mkdirSync(join(home.pluginsDir, "nameless"), { recursive: true });
		writeFileSync(
			join(home.pluginsDir, "nameless", "plugin.yaml"),
			"description: hi\n",
		);
		const manifest = parsePluginManifest(
			join(home.pluginsDir, "nameless", "plugin.yaml"),
			join(home.pluginsDir, "nameless"),
			"user",
			"",
			log.levelled,
		);
		expect(manifest?.name).toBe("nameless");
		expect(manifest?.key).toBe("nameless");
		expect(manifest?.version).toBe("");
	});

	it("prefixed category scan builds <prefix>/<dirname> keys from DIR name, manifest name keeps identity", () => {
		const innerDir = join(home.pluginsDir, "cat", "innerdir");
		mkdirSync(innerDir, { recursive: true });
		writeFileSync(join(innerDir, "plugin.yaml"), "name: fancy-name\n");
		// A non-directory sibling is ignored by the sorted scan:
		writeFileSync(
			join(home.pluginsDir, "cat", "loose-file"),
			"not a plugin dir\n",
		);

		const manifests = discoverPlugins({ log: log.levelled });
		expect(manifests.map((m) => m.key)).toEqual(["cat/innerdir"]);
		expect(manifests[0]?.name).toBe("fancy-name");
	});
});

// hook-registry.ts — the file-drop event-hook registry (07-integrations.md
// §7(a); DEC-014). TWO event classes with different contracts:
//
//   • observer events  → `emit()`: emit-and-log, return values DISCARDED,
//     handler exceptions caught+logged and NEVER propagated (a lifecycle
//     event must not gain a denial-of-service path).
//   • command events   → `emitCollect()` (`command:<canonical>`): returns
//     non-null results in handler order for DECISION-BEARING processing —
//     the single plugin veto/rewrite interception point. Verdict semantics
//     live in ./command-decisions.ts.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/hooks.py:HookRegistry.__init__          → handlers map + loaded metadata
//   gateway/hooks.py:HookRegistry._register_builtin_hooks → registerBuiltin (ships ZERO)
//   gateway/hooks.py:HookRegistry.discover_and_load → discoverAndLoad
//   gateway/hooks.py:HookRegistry._resolve_handlers → resolveHandlers
//   gateway/hooks.py:HookRegistry.emit              → emit
//   gateway/hooks.py:HookRegistry.emit_collect      → emitCollect
//
// Discovery contract parity: `<home>/hooks/<dir>/{HOOK.yaml (name + events),
// handler module (handle(eventType, context), sync or async)}`, sorted scan;
// members missing manifest OR handler are skipped SILENTLY; malformed members
// skipped with a printed reason; a successful load logs
// `[hooks] Loaded hook '<name>' for events: …`. The sys.modules-BEFORE-exec
// registration of gateway/hooks.py exists so Python forward references
// resolve mid-exec; ESM `import()` resolves its module registry natively, so
// the TS realization needs no equivalent step (DEC-023 idiom adaptation).
//
// Handler module convention: `handler.mjs` | `handler.js` | `handler.cjs`
// exporting `handle(eventType, context)` (first existing wins). `.mjs` is the
// recommended fixture/authoring form — plain `.js` outside a `"type":"module"`
// package is CommonJS under Node resolution.

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { resolvePiHome } from "../../pi_home.js";
import { parseFlatYaml } from "./manifest-yaml.js";

/** A loaded handler: sync or async; return value meaningful ONLY for collect. */
export type HookHandler = (
	eventType: string,
	context: Record<string, unknown>,
) => unknown;

export interface LoadedHookInfo {
	name: string;
	description: string;
	events: readonly string[];
	path: string;
}

/** Injectable log sink; messages carry their own `[hooks]` prefix. */
export type HookLogSink = (message: string) => void;

function stderrSink(message: string): void {
	try {
		process.stderr.write(`${message}\n`);
	} catch {
		/* stderr unavailable — never let logging break the pipeline */
	}
}

const HANDLER_BASENAMES = ["handler.mjs", "handler.js", "handler.cjs"] as const;

function firstExisting(base: string, names: readonly string[]): string | null {
	for (const name of names) {
		const candidate = join(base, name);
		if (existsSync(candidate)) return candidate;
	}
	return null;
}

export interface DiscoverOptions {
	/** Log sink override (tests capture discovery reasons through this). */
	log?: HookLogSink;
}

/**
 * Discovers, loads, and fires event hooks. Call `discoverAndLoad()` ONCE at
 * startup (parity: gateway/run.py:start calls discover_and_load exactly once;
 * repeated calls would append duplicate handlers).
 *
 * A hook-free deployment loads nothing and pays zero per-event overhead —
 * callers gate optional callback wiring on `loadedHooks.length` (parity of
 * `if ctx._hooks_ref.loaded_hooks else None` in run.py's step-callback wire).
 */
export class HookRegistry {
	private readonly handlers = new Map<string, HookHandler[]>();
	private readonly loadedHooksList: LoadedHookInfo[] = [];

	/** Metadata about every successfully loaded hook (snapshot copy). */
	get loadedHooks(): readonly LoadedHookInfo[] {
		return this.loadedHooksList.map((h) => ({ ...h, events: [...h.events] }));
	}

	get isEmpty(): boolean {
		return this.handlers.size === 0 && this.loadedHooksList.length === 0;
	}

	/**
	 * Programmatic registration — the reserved extension point mirroring
	 * gateway/builtin_hooks/__init__.py: deliberately EMPTY in the shipped
	 * product ("none shipped"); user hooks from disk are the only other source.
	 */
	register(eventType: string, handler: HookHandler): void {
		const list = this.handlers.get(eventType);
		if (list === undefined) this.handlers.set(eventType, [handler]);
		else list.push(handler);
	}

	/**
	 * Scan `<resolvePiHome()>/hooks/` and load every well-formed member.
	 * Malformed members are skipped with a printed reason and NEVER abort the
	 * scan — one broken hook cannot take down discovery (loud per-member
	 * degradation, 01 §3.1 semantics).
	 */
	async discoverAndLoad(options: DiscoverOptions = {}): Promise<void> {
		const log = options.log ?? stderrSink;
		this.registerBuiltinHooks(log);

		const hooksDir = join(resolvePiHome(), "hooks");
		if (!existsSync(hooksDir)) return;

		let entries;
		try {
			entries = readdirSync(hooksDir, { withFileTypes: true });
		} catch (err) {
			log(
				`[hooks] Error scanning ${hooksDir}: ${err instanceof Error ? err.message : String(err)}`,
			);
			return;
		}
		const dirs = entries
			.filter((e) => e.isDirectory())
			.map((e) => e.name)
			.sort();
		for (const dirName of dirs) {
			await this.loadHookDir(join(hooksDir, dirName), dirName, log);
		}
	}

	/**
	 * Reserved always-on extension point — ships ZERO built-in hooks today,
	 * matching gateway/builtin_hooks/__init__.py being deliberately empty.
	 */
	protected registerBuiltinHooks(log: HookLogSink): void {
		void log; // no shipped builtin hooks ("none shipped" parity)
	}

	private async loadHookDir(
		hookDir: string,
		dirName: string,
		log: HookLogSink,
	): Promise<void> {
		const manifestPath = join(hookDir, "HOOK.yaml");
		const handlerPath = firstExisting(hookDir, HANDLER_BASENAMES);
		if (!existsSync(manifestPath) || handlerPath === null) return; // silent skip, parity

		try {
			const parsed = parseFlatYaml(readFileSync(manifestPath, "utf8"));
			if (parsed === null || typeof parsed !== "object") {
				log(`[hooks] Skipping ${dirName}: invalid HOOK.yaml`);
				return;
			}
			const rawName = parsed.name;
			// manifest.get("name", dir_name) parity: any STRING value is used
			// verbatim (even empty); only absent/non-string falls back to the dir.
			const hookName = typeof rawName === "string" ? rawName : dirName;
			const rawEvents = parsed.events;
			if (!Array.isArray(rawEvents) || rawEvents.length === 0) {
				log(`[hooks] Skipping ${hookName}: no events declared`);
				return;
			}
			const events = rawEvents.map((e) => String(e));

			// Dynamic import ≙ spec_from_file_location + sys.modules-before-exec +
			// exec_module. ESM resolves forward references natively (see header).
			const module = await import(pathToFileURL(handlerPath).href);
			const handle = (module as Record<string, unknown>).handle;
			if (typeof handle !== "function") {
				log(`[hooks] Skipping ${hookName}: no 'handle' function found`);
				return;
			}

			const handler = handle as unknown as HookHandler;
			for (const event of events) this.register(event, handler);

			this.loadedHooksList.push({
				name: hookName,
				description:
					typeof parsed.description === "string" ? parsed.description : "",
				events,
				path: hookDir,
			});
			log(`[hooks] Loaded hook '${hookName}' for events: ${events.join(", ")}`);
		} catch (err) {
			log(
				`[hooks] Error loading hook ${dirName}: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
	}

	/**
	 * Exact-match handlers fire FIRST, then `<base>:*` wildcard registrants
	 * (e.g. `command:*` sees `command:reset`). A base-only registration
	 * ("agent") NEVER fires for "agent:start" — only exact matches and
	 * explicit wildcards.
	 */
	resolveHandlers(eventType: string): HookHandler[] {
		const resolved: HookHandler[] = [...(this.handlers.get(eventType) ?? [])];
		const colonAt = eventType.indexOf(":");
		if (colonAt >= 0) {
			const base = eventType.slice(0, colonAt);
			resolved.push(...(this.handlers.get(`${base}:*`) ?? []));
		}
		return resolved;
	}

	/**
	 * Observer emission: fire-and-forget semantics with per-handler failure
	 * containment. Return values are discarded; a throwing/rejecting handler
	 * is logged (`[hooks] Error in handler for '<event>'`) and the pipeline
	 * proceeds — observer exceptions NEVER propagate to the caller.
	 */
	async emit(
		eventType: string,
		context?: Record<string, unknown>,
		options: { log?: HookLogSink } = {},
	): Promise<void> {
		const log = options.log ?? stderrSink;
		const ctx = context ?? {};
		for (const fn of this.resolveHandlers(eventType)) {
			try {
				const result = fn(eventType, ctx);
				if (
					result !== null &&
					result !== undefined &&
					typeof (result as { then?: unknown }).then === "function"
				) {
					await result;
				}
			} catch (err) {
				log(`[hooks] Error in handler for '${eventType}': ${String(err)}`);
			}
		}
	}

	/**
	 * Decision-bearing collection: like `emit` but captures each handler's
	 * non-null return value IN ORDER (exact handlers then wildcards). Used
	 * for `command:<canonical>` policies that veto/handle/rewrite before core
	 * dispatch. An individual handler raising is logged and skipped while
	 * remaining handlers still process.
	 */
	async emitCollect(
		eventType: string,
		context?: Record<string, unknown>,
		options: { log?: HookLogSink } = {},
	): Promise<unknown[]> {
		const log = options.log ?? stderrSink;
		const ctx = context ?? {};
		const results: unknown[] = [];
		for (const fn of this.resolveHandlers(eventType)) {
			try {
				let result: unknown = fn(eventType, ctx);
				if (
					result !== null &&
					result !== undefined &&
					typeof (result as { then?: unknown }).then === "function"
				) {
					result = await result;
				}
				if (result !== undefined && result !== null) results.push(result);
			} catch (err) {
				log(`[hooks] Error in handler for '${eventType}': ${String(err)}`);
			}
		}
		return results;
	}
}

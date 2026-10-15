/**
 * pi-gateway — pi extension entry point
 *
 * This is the thin pi-ecosystem shim over the composition root
 * `src/entrypoints/gateway-run.ts`. Pi discovers it via the `pi.extensions`
 * manifest in package.json (`pi install npm:pi-gateway` or
 * `pi install ./pi-gateway/pi-gateway`). The extension reuses the host pi
 * agent loop directly (DEC-023) — it does not reimplement it.
 *
 * Lifecycle: gateway is composed on session_start and torn down on
 * session_shutdown. `/gateway` exposes status/start/stop without forking a
 * second process; `PI_GATEWAY_AUTO_START=1` opts into auto-start.
 */

// pi types resolve at runtime via the host pi installation (jiti); tsc gets
// them through the peerDependency branch that npm ci installs (package-lock
// was aligned with package.json's peerDependencies in 71999d4).
import type {
	ExtensionAPI,
	ExtensionCommandContext,
	SessionShutdownEvent,
	SessionStartEvent,
} from "@earendil-works/pi-coding-agent";
import {
	composeGatewayLifecycle,
	type ComposedGateway,
} from "../src/entrypoints/gateway-run.js";
import type { TurnRunnerFactory } from "../src/entrypoints/guard-wiring.js";
import type { ChatProcRegistry } from "../src/pi_agent_core/chat-proc-registry.js";
import { resolvePiHome } from "../src/pi_home.js";
import {
	PI_GATEWAY_PLATFORMS_ENV,
	resolveConfiguredPlatforms,
} from "../src/entrypoints/platform-hosting.js";

export default function piGatewayExtension(pi: ExtensionAPI) {
	let gateway: ComposedGateway | null = null;
	let starting = false;
	// DEC-084: one shared per-chat process registry per extension process.
	// Mirrors the standalone runCommand composition (pi-gateway.ts): the
	// runner closes over the lifecycle-owned stage-6 store and children
	// share the gateway home. Lazy so platform-less boots load nothing.
	let chatRegistry: ChatProcRegistry | null = null;

	async function startGateway(home?: string): Promise<string> {
		if (gateway || starting)
			return gateway ? "gateway already running" : "gateway starting...";
		starting = true;
		try {
			// DEC-072: explicit boot platform list — the allowlist resolves to
			// hosted platforms with production factories, so stage 9 derives
			// real adapter entries for listed platforms. Unset/empty ⇒ [] ⇒
			// current no-platform behavior, unchanged. The shared stderr
			// logger rides BOTH the adapters (runtime fatals stay loud) and
			// the lifecycle (reconnect-queue lines land in one sink).
			const extLogger = {
				info: (m: string, meta?: Record<string, unknown>) => {
					try {
						process.stderr.write(`[pi-gateway] INFO ${m}\n`);
					} catch {
						/* stderr unavailable */
					}
					void meta;
				},
				warn: (m: string, meta?: Record<string, unknown>) => {
					try {
						process.stderr.write(`[pi-gateway] WARN ${m}\n`);
					} catch {
						/* stderr unavailable */
					}
					void meta;
				},
				error: (m: string, meta?: Record<string, unknown>) => {
					try {
						process.stderr.write(`[pi-gateway] ERROR ${m}\n`);
					} catch {
						/* stderr unavailable */
					}
					void meta;
				},
			};
			const platforms = resolveConfiguredPlatforms(
				process.env[PI_GATEWAY_PLATFORMS_ENV],
				undefined,
				extLogger,
			);
			// DEC-084: embedded turn factory dissolved; per-chat RPC children
			// own turns through the production factory below.
			const resolvedHome = home ?? resolvePiHome();
			let turnRunnerFactory: TurnRunnerFactory | undefined;
			let pickerServiceForLifecycle: unknown = null;
			if (platforms.length > 0) {
				const [
					{ ChatProcRegistry: Registry },
					{ RpcTurnRunner },
					{ PickerService },
					{
						buildDiscoveryIndex,
						listDiscoveryPaths,
						listRecentSessions,
						mostRecentSessionAtPath,
						resolveAgentDir,
					},
				] = await Promise.all([
					import("../src/pi_agent_core/chat-proc-registry.js"),
					import("../src/pi_agent_core/rpc-turn-runner.js"),
					import("../src/pi_gateway/pickers/service.js"),
					import("../src/pi_gateway/discovery.js"),
				]);
				if (chatRegistry === null) chatRegistry = new Registry();
				const registry = chatRegistry;
				const agentDir = resolveAgentDir();
				// Shared picker service (generic askSelect transport for bare
				// resume and model — no commit closures needed).
				const pickerService = new PickerService({});
				pickerServiceForLifecycle = pickerService;
				turnRunnerFactory = ({ store }) => {
					const runner = new RpcTurnRunner({
						registry,
						resolveHome: () => resolvedHome,
						listDiscoveryPaths: () =>
							listDiscoveryPaths(buildDiscoveryIndex(agentDir)),
						findSessionFileAtPath: (_homeDir, rawPath) =>
							mostRecentSessionAtPath(
								buildDiscoveryIndex(agentDir),
								rawPath,
							)?.file ?? null,
						listRecentSessions: () =>
							listRecentSessions(buildDiscoveryIndex(agentDir)),
						pickers: pickerService,
						...(store !== null ? { store } : {}),
					});
					return runner;
				};
			}
			gateway = composeGatewayLifecycle({
				home: resolvedHome,
				logger: extLogger,
				platforms,
				...(turnRunnerFactory !== undefined ? { turnRunnerFactory } : {}),
				...(pickerServiceForLifecycle !== null
					? {
							pickerService:
								pickerServiceForLifecycle as NonNullable<
									Parameters<typeof composeGatewayLifecycle>[0]["pickerService"]
								>,
						}
					: {}),
			});
			const res = await gateway.lifecycle.startup();
			if (!res.ok) {
				const g = gateway;
				gateway = null;
				g.lifecycle.dispose();
				return `gateway failed to start (exit ${res.exitCode ?? 1})`;
			}
			return `gateway running — home=${gateway.lifecycle.home} platforms=[${[...gateway.connectedPlatforms()].join(",") || "none"}]`;
		} finally {
			starting = false;
		}
	}

	async function stopGateway(): Promise<string> {
		if (!gateway) return "gateway not running";
		const g = gateway;
		gateway = null;
		try {
			await g.lifecycle.requestShutdown?.();
		} catch (err) {
			void err; // shutdown is best-effort; lifecycle owns the error log
		}
		try {
			g.lifecycle.dispose();
		} catch (err) {
			void err;
		}
		return "gateway stopped";
	}

	pi.on("session_start", async (_event: SessionStartEvent, ctx) => {
		if (process.env.PI_GATEWAY_AUTO_START === "1") {
			const msg = await startGateway();
			ctx.ui.notify(
				msg,
				msg.startsWith("gateway running") ? "info" : "warning",
			);
		}
	});

	pi.on("session_shutdown", async (_event: SessionShutdownEvent) => {
		if (gateway) await stopGateway();
	});

	pi.registerCommand("gateway", {
		description:
			"pi-gateway — status / start / stop (setup lives in the `pi-gateway setup` CLI)",
		handler: async (args: string, ctx: ExtensionCommandContext) => {
			const parts = args.trim().split(/\s+/).filter((p) => p !== "");
			const sub = parts[0] ?? "";
			if (sub === "setup") {
				// Chat setup is removed. The terminal CLI owns platform
				// setup now: no chat state, plain readline, one write at
				// completion. Point the operator there and do nothing else.
				ctx.ui.notify(
					"gateway setup moved to the terminal: run `pi-gateway setup` (try `pi-gateway setup --list` first)",
					"info",
				);
				return;
			}
			if (sub === "start") {
				const home = parts[1];
				ctx.ui.notify(await startGateway(home), "info");
				return;
			}
			if (sub === "stop") {
				ctx.ui.notify(await stopGateway(), "info");
				return;
			}
			// status (default)
			if (!gateway) {
				ctx.ui.notify(
					"gateway: not running — /gateway start [home] to start",
					"info",
				);
				return;
			}
			ctx.ui.notify(
				`gateway: running — home=${gateway.lifecycle.home} connected=[${[...gateway.connectedPlatforms()].join(",") || "none"}]`,
				"info",
			);
		},
	});
}

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
import {
	allSetupSpecs,
	runTuiSetup,
} from "../src/entrypoints/setup-wiring.js";

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
			// current no-platform behavior, unchanged.
			const platforms = resolveConfiguredPlatforms(
				process.env[PI_GATEWAY_PLATFORMS_ENV],
			);
			// DEC-084: embedded turn factory dissolved; per-chat RPC children
			// own turns through the production factory below.
			const resolvedHome = home ?? resolvePiHome();
			let turnRunnerFactory: TurnRunnerFactory | undefined;
			if (platforms.length > 0) {
				const [
					{ ChatProcRegistry: Registry },
					{ RpcTurnRunner },
					{
						buildDiscoveryIndex,
						listDiscoveryPaths,
						mostRecentSessionAtPath,
						resolveAgentDir,
					},
				] = await Promise.all([
					import("../src/pi_agent_core/chat-proc-registry.js"),
					import("../src/pi_agent_core/rpc-turn-runner.js"),
					import("../src/pi_gateway/discovery.js"),
				]);
				if (chatRegistry === null) chatRegistry = new Registry();
				const registry = chatRegistry;
				const agentDir = resolveAgentDir();
				turnRunnerFactory = ({ store }) =>
					new RpcTurnRunner({
						registry,
						resolveHome: () => resolvedHome,
						listDiscoveryPaths: () =>
							listDiscoveryPaths(buildDiscoveryIndex(agentDir)),
						findSessionFileAtPath: (_homeDir, rawPath) =>
							mostRecentSessionAtPath(
								buildDiscoveryIndex(agentDir),
								rawPath,
							)?.file ?? null,
						...(store !== null ? { store } : {}),
					});
			}
			gateway = composeGatewayLifecycle({
				home: resolvedHome,
				platforms,
				...(turnRunnerFactory !== undefined ? { turnRunnerFactory } : {}),
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
			"pi-gateway — status / start / stop / setup (Hermes parity, pi host loop)",
		handler: async (args: string, ctx: ExtensionCommandContext) => {
			const parts = args.trim().split(/\s+/).filter((p) => p !== "");
			const sub = parts[0] ?? "";
			if (sub === "setup") {
				if (!ctx.hasUI) {
					ctx.ui.notify("gateway setup needs TUI dialogs — run inside pi TUI", "warning");
					return;
				}
				const want = parts[1];
				const specs = allSetupSpecs().filter(
					(s) => want === undefined || s.name === want,
				);
				if (specs.length === 0) {
					ctx.ui.notify(`gateway setup: unknown platform ${JSON.stringify(want ?? "")}`, "warning");
					return;
				}
				// DEC-081 renderer one: the same state machine through TUI
				// dialogs. Values travel dialog-to-writer only, never into
				// notify lines or transcripts.
				const res = await runTuiSetup(resolvePiHome(), ctx.ui, { specs });
				if (res.ok) {
					ctx.ui.notify(`gateway setup: ${res.platform} saved ${res.vars.length} vars (${res.vars.join(", ")})`, "info");
				} else if ("cancelled" in res) {
					ctx.ui.notify("gateway setup: cancelled", "info");
				} else {
					ctx.ui.notify(`gateway setup failed: ${res.error}`, "error");
				}
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

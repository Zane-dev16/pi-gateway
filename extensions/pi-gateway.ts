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
import type { ExtensionAPI, ExtensionCommandContext, SessionShutdownEvent, SessionStartEvent } from "@earendil-works/pi-coding-agent";
import { composeGatewayLifecycle, type ComposedGateway } from "../src/entrypoints/gateway-run.js";

export default function piGatewayExtension(pi: ExtensionAPI) {
	let gateway: ComposedGateway | null = null;
	let starting = false;

	async function startGateway(home?: string): Promise<string> {
		if (gateway || starting) return gateway ? "gateway already running" : "gateway starting...";
		starting = true;
		try {
			gateway = composeGatewayLifecycle(home ? { home } : {});
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
			ctx.ui.notify(msg, msg.startsWith("gateway running") ? "info" : "warning");
		}
	});

	pi.on("session_shutdown", async (_event: SessionShutdownEvent) => {
		if (gateway) await stopGateway();
	});

	pi.registerCommand("gateway", {
		description: "pi-gateway — status / start / stop (Hermes parity, pi host loop)",
		handler: async (args: string, ctx: ExtensionCommandContext) => {
			const sub = args.trim().split(/\s+/)[0] ?? "";
			if (sub === "start") {
				const home = args.trim().split(/\s+/)[1];
				ctx.ui.notify(await startGateway(home), "info");
				return;
			}
			if (sub === "stop") {
				ctx.ui.notify(await stopGateway(), "info");
				return;
			}
			// status (default)
			if (!gateway) {
				ctx.ui.notify("gateway: not running — /gateway start [home] to start", "info");
				return;
			}
			ctx.ui.notify(
				`gateway: running — home=${gateway.lifecycle.home} connected=[${[...gateway.connectedPlatforms()].join(",") || "none"}]`,
				"info",
			);
		},
	});
}

// entrypoints/platform-hosting.ts — census hosting builders for the
// composition root (structure-7). Each builder pairs a platform's REAL
// manifest with its §4.2 register(ctx) helper (04-platform-adapters.md
// §4.2 — plugin.yaml + register(ctx) parity), so hosts compose:
//
//   platforms: [telegramHosting(myFactory), ntfyHosting(...)]
//
// SEPARATE MODULE ON PURPOSE: these helpers statically import the kit
// registration module and per-platform adapter chains, which use TypeScript
// parameter properties that bare-node strip-only runners cannot parse.
// gateway-run.ts itself stays strip-safe (lazy PluginContext load) so the
// two-process driver can run the full composed stack; production hosts that
// want census platforms import THIS module alongside it.

import { registerHomeAssistantPlatform } from "../pi_platforms/homeassistant/homeassistant-standalone.js";
import { HA_PLUGIN_MANIFEST } from "../pi_platforms/homeassistant/manifest.js";
import type { HomeAssistantStandaloneOptions } from "../pi_platforms/homeassistant/homeassistant-standalone.js";
import type { NtfyStandaloneOptions } from "../pi_platforms/ntfy/ntfy-standalone.js";
import { registerNtfyPlatform } from "../pi_platforms/ntfy/ntfy-standalone.js";
import { NTFY_PLUGIN_MANIFEST } from "../pi_platforms/ntfy/manifest.js";
import { TELEGRAM_MANIFEST } from "../pi_platforms/telegram/manifest.js";
import { registerTelegramPlatform } from "../pi_platforms/telegram/telegram-adapter.js";
import type {
	PlatformFactory,
	PluginContext,
} from "../pi_platforms/kit/index.js";
import type { PlatformHosting } from "./gateway-run.js";

/**
 * Boundary cast: gateway-run's structural mirror hands builders its
 * PluginRegistrationContext view; these helpers require the REAL
 * PluginContext (produced by the composition root's lazy kit import).
 */
function realCtx(ctx: unknown): PluginContext {
	return ctx as PluginContext;
}

/** telegram hosting (DEC-024 first production adapter). */
export function telegramHosting(factory: PlatformFactory): PlatformHosting {
	return {
		platform: TELEGRAM_MANIFEST.name,
		manifest: TELEGRAM_MANIFEST,
		factory,
		register: (ctx, f) => void registerTelegramPlatform(realCtx(ctx), f),
	};
}

/** ntfy hosting (registers its standalone out-of-process sender hook). */
export function ntfyHosting(
	factory: PlatformFactory,
	opts: { standalone?: NtfyStandaloneOptions } = {},
): PlatformHosting {
	return {
		platform: NTFY_PLUGIN_MANIFEST.name,
		manifest: NTFY_PLUGIN_MANIFEST,
		factory,
		register: (ctx, f) => void registerNtfyPlatform(realCtx(ctx), f, opts),
	};
}

/** home-assistant hosting (registers its standalone sender hook). */
export function homeAssistantHosting(
	factory: PlatformFactory,
	opts: { standalone?: HomeAssistantStandaloneOptions } = {},
): PlatformHosting {
	return {
		platform: HA_PLUGIN_MANIFEST.name,
		manifest: HA_PLUGIN_MANIFEST,
		factory,
		register: (ctx, f) =>
			void registerHomeAssistantPlatform(realCtx(ctx), f, opts),
	};
}

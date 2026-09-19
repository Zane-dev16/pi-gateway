// entrypoints/platform-hosting.ts — census hosting builders for the
// composition root (structure-7). Each builder pairs a platform's REAL
// manifest with its §4.2 register(ctx) helper (04-platform-adapters.md
// §4.2 — plugin.yaml + register(ctx) parity), so hosts compose:
//
//   platforms: [telegramHosting(myFactory)]
//
// SEPARATE MODULE ON PURPOSE: these helpers statically import the kit
// registration module and per-platform adapter chains, which use TypeScript
// parameter properties that bare-node strip-only runners cannot parse.
// gateway-run.ts itself stays strip-safe (lazy PluginContext load) so the
// two-process driver can run the full composed stack; production hosts that
// want census platforms import THIS module alongside it.

import { TELEGRAM_MANIFEST } from "../pi_platforms/telegram/manifest.js";
import { registerTelegramPlatform } from "../pi_platforms/telegram/telegram-adapter.js";
import { TelegramAdapter } from "../pi_platforms/telegram/telegram-adapter.js";
import { TelegramBotApiFake } from "../pi_platforms/telegram/telegram-fake-server.js";
import { MATRIX_MANIFEST } from "../pi_platforms/matrix/manifest.js";
import { MatrixAdapterCore } from "../pi_platforms/matrix/matrix-adapter.js";
import { FakeMatrixHomeserver } from "../pi_platforms/matrix/matrix-fake-server.js";
import { kitScopedSecretReader } from "../pi_gateway/security/secretscope/wrapper.js";
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

/**
 * matrix hosting (DEC-072 second production adapter): MATRIX_MANIFEST paired
 * with the Matrix adapter factory. No custom register hook — matrix has no
 * standalone-sender surface, so the default ctx.registerPlatform path runs.
 */
export function matrixHosting(factory: PlatformFactory): PlatformHosting {
	return {
		platform: MATRIX_MANIFEST.name,
		manifest: MATRIX_MANIFEST,
		factory,
	};
}

/** Env var carrying the explicit boot platform list (DEC-072). */
export const PI_GATEWAY_PLATFORMS_ENV = "PI_GATEWAY_PLATFORMS";

/**
 * Explicit boot platform list (DEC-072): parse the comma-separated allowlist
 * into hosted platforms with production factories, so the extension can pass
 * them as `platforms` and stage 9 derives real adapter entries. Empty/unset
 * ⇒ [] (current no-platform behavior, byte-identical). Names are trimmed,
 * lowercased, and deduplicated; unknown names stay absent. The per-entry
 * requiresEnv gate still runs at stage 9 — listed but uncredentialed ⇒ loud
 * adapter_disabled naming the missing secret, never silent.
 *
 * Production factories construct whatever the adapters use today (fake
 * transports); the real-transport swap is the NEXT task, not this one.
 * NOTE (secret-scope R3): this module references the scope engine, so it
 * takes the raw allowlist string as a parameter and never touches the
 * ambient environment itself — the extension (outside the gate's src/ scan)
 * reads the env var and passes the value in.
 */
export function resolveConfiguredPlatforms(
	allowlist: string | undefined,
	secrets: (name: string) => string | undefined = kitScopedSecretReader(),
): PlatformHosting[] {
	const seen = new Set<string>();
	const out: PlatformHosting[] = [];
	for (const raw of (allowlist ?? "").split(",")) {
		const name = raw.trim().toLowerCase();
		if (name === "" || seen.has(name)) continue;
		seen.add(name);
		if (name === TELEGRAM_MANIFEST.name) {
			out.push(
				telegramHosting(
					() =>
						new TelegramAdapter({
							wire: new TelegramBotApiFake(),
							secretReader: secrets,
						}),
				),
			);
		} else if (name === MATRIX_MANIFEST.name) {
			out.push(
				matrixHosting(
					() =>
						new MatrixAdapterCore({
							hs: new FakeMatrixHomeserver(),
							secretReader: secrets,
						}),
				),
			);
		}
		// Unknown names stay absent (DEC-072: loud unknown-name rejection
		// rejected — no logger plumbing inside this pure parser).
	}
	return out;
}

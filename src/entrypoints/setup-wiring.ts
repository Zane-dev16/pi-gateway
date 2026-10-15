// entrypoints/setup-wiring — DEC-081 wiring: manifest → setup spec plus
// live telegram validation for the terminal setup CLI.
//
// The CLI owns all prompting (plain readline, no TUI); this module binds the
// platform manifests and the real Bot API getMe check. Secrets discipline:
// error strings never carry values — the telegram validator redacts the
// token from transport messages before surfacing.

import { HttpTelegramBotApi } from "../pi_platforms/telegram/bot-api-client.js";
import { TELEGRAM_MANIFEST } from "../pi_platforms/telegram/manifest.js";
import type { PluginManifest } from "../pi_platforms/kit/index.js";
import { PLATFORM_ALLOWED_USERS_ENV } from "../pi_gateway/security/authz/platform-tables.js";
import type {
	SetupPlatformSpec,
	SetupValidation,
	SetupValidator,
	SetupVarSpec,
} from "../pi_gateway/setup/index.js";

export type {
	SetupPlatformSpec,
	SetupValidation,
	SetupValidator,
	SetupVarSpec,
};

/** Manifest requires/optional rows → setup table (relation both sides share). */
export function manifestToSetupSpec(
	manifest: Pick<PluginManifest, "name" | "requiresEnv" | "optionalEnv">,
): SetupPlatformSpec {
	return {
		name: manifest.name,
		required: manifest.requiresEnv.map((v) => ({
			name: v.name,
			...(v.description !== undefined ? { description: v.description } : {}),
			...(v.password === true ? { password: true as const } : {}),
		})),
		optional: (manifest.optionalEnv ?? []).map((v) => ({
			name: v.name,
			...(v.description !== undefined ? { description: v.description } : {}),
			...(v.password === true ? { password: true as const } : {}),
			optional: true as const,
		})),
	};
}

/** Telegram spec: manifest rows plus the DM allowlist as an optional var. */
export function buildTelegramSetupSpec(): SetupPlatformSpec {
	const base = manifestToSetupSpec(TELEGRAM_MANIFEST);
	const allowlist = PLATFORM_ALLOWED_USERS_ENV["telegram"];
	if (allowlist === undefined) return base;
	if (
		base.required.some((v) => v.name === allowlist) ||
		(base.optional ?? []).some((v) => v.name === allowlist)
	) {
		return base;
	}
	return {
		name: base.name,
		required: base.required,
		optional: [
			{
				name: allowlist,
				description:
					"Comma-separated operator user IDs (empty skips allowlist)",
				optional: true as const,
			},
			...(base.optional ?? []),
		],
	};
}

/** All platforms offering guided setup today (data-driven list to grow). */
export function allSetupSpecs(): SetupPlatformSpec[] {
	return [buildTelegramSetupSpec()];
}

/** Live telegram check: getMe must resolve a username. Never echoes token. */
export async function validateTelegramToken(
	token: string,
	opts: { baseUrl?: string | undefined } = {},
): Promise<SetupValidation> {
	const trimmed = token.trim();
	if (trimmed === "") return { ok: false, error: "bot token is required" };
	let client: HttpTelegramBotApi | undefined;
	try {
		client = new HttpTelegramBotApi({
			token: trimmed,
			...(opts.baseUrl !== undefined ? { baseUrl: opts.baseUrl } : {}),
		});
		await client.getMe();
		return { ok: true };
	} catch (err) {
		const raw = (err as Error)?.message ?? String(err);
		const redacted =
			trimmed !== "" ? raw.split(trimmed).join("[redacted]") : raw;
		return { ok: false, error: `telegram: validation failed (${redacted})` };
	} finally {
		try {
			client?.closeSessions();
		} catch {
			/* teardown is best-effort */
		}
	}
}

/** Live check per platform. Telegram runs getMe over HTTP. */
export type LiveValidator = (
	values: ReadonlyMap<string, string>,
	opts?: { baseUrl?: string | undefined },
) => Promise<SetupValidation>;

/** Platform to live check. Unlisted platforms fail closed. */
const LIVE_VALIDATORS: Record<string, LiveValidator> = {
	telegram: (values, opts) =>
		validateTelegramToken(values.get("TELEGRAM_BOT_TOKEN") ?? "", opts),
};

// Live dispatch runs the platform table. Unlisted platforms fail closed
// so setup never writes unvalidated secrets.
export async function validateSetupLive(
	platform: string,
	values: ReadonlyMap<string, string>,
	opts: { baseUrl?: string | undefined } = {},
): Promise<SetupValidation> {
	const validator = LIVE_VALIDATORS[platform];
	if (validator === undefined) {
		return { ok: false, error: `no live validator for ${platform}` };
	}
	return validator(values, opts);
}

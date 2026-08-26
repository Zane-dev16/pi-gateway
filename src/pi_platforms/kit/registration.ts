// pi_platforms/kit/registration — the plugin registration path
// (04-platform-adapters.md §4.2 — Hermes' ACTUAL flow, not an invented
// MANIFEST.yaml):
//
//   platforms/<name>/
//     plugin.yaml     → PluginManifest (name, description, requires_env[] /
//                       optional_env[] rich specs)
//     adapter module  → register(ctx): ctx.registerPlatform(...)
//
// Optional hooks reserved up front (§4.2 list):
//   envEnablementFn    — seed PlatformConfig.extra from env BEFORE construction
//   applyYamlConfigFn  — platform owns its YAML schema (env > YAML precedence)
//   cronDeliverEnvVar  — out-of-process cron delivery pair member (STILL
//                        reserved, not yet wired)
//   standaloneSenderFn — out-of-process cron delivery pair member (WIRED:
//                        registerPlatform({standaloneSenderFn}) stores the
//                        hook; getStandaloneSender() hands it to the cron /
//                        send-tool fallback lane)
//
// "Missing secrets disable the adapter LOUDLY (visible in /status), never
// silently" — enablement resolves through a SCOPED secret reader (fail-closed:
// scoped miss returns the declared default, NEVER borrows process env after a
// scoped miss — DEC-003/009).

import {
	AdapterLifecycleState,
	type AdapterDisabledError,
	type DisableReason,
} from "./lifecycle-state.js";
import type { CapabilityManifest } from "./capabilities.js";
import type { TrustBoundaryManifest } from "./trust.js";
import type { RateBudget } from "./capabilities.js";

export type TransportShape = "polling" | "ws" | "webhook";

/** Rich env spec (plugin.yaml requires_env/optional_env dicts). */
export interface EnvVarSpec {
	name: string;
	description?: string | undefined;
	prompt?: boolean | undefined;
	password?: boolean | undefined;
	url?: boolean | undefined;
}

export interface PluginManifest {
	name: string;
	description: string;
	transportShape: TransportShape;
	requiresEnv: readonly EnvVarSpec[];
	optionalEnv?: readonly EnvVarSpec[] | undefined;
	capabilities: Partial<CapabilityManifest>;
	/** Q17 rate budgets — per-platform numbers live HERE, not in core. */
	rateBudget?: RateBudget | undefined;
	/** DEC-017 trust boundaries for HTTP-ingress shapes. */
	trustBoundary?: TrustBoundaryManifest | undefined;
}

/**
 * Scoped secret reader seam. A SCOPED miss returns undefined (or the declared
 * default) and MUST NOT fall back to process env under multiplex (DEC-003/009).
 */
export type ScopedSecretReader = (name: string) => string | undefined;

export interface RegisteredPlatform {
	manifestName: string;
	state: AdapterLifecycleState;
	factory: PlatformFactory;
	/** Out-of-process cron/send-tool sender (StandaloneSenderFn), if any. */
	standaloneSenderFn?: StandaloneSenderFn | undefined;
}

/**
 * The plugin context handed to register(ctx). One instance per gateway.
 */
export class PluginContext {
	private readonly platforms = new Map<string, RegisteredPlatform>();

	constructor(private readonly secrets: ScopedSecretReader) {}

	registered(): RegisteredPlatform[] {
		return [...this.platforms.values()];
	}

	getPlatform(manifestName: string): RegisteredPlatform | undefined {
		return this.platforms.get(manifestName);
	}

	/** The registered out-of-process sender for a platform, if any. */
	getStandaloneSender(manifestName: string): StandaloneSenderFn | undefined {
		return this.platforms.get(manifestName)?.standaloneSenderFn;
	}

	/**
	 * §11 step 3/4 flow: resolve enablement from required env; missing secret
	 * ⇒ LOUD disable (state visible in /status); token lock acquired here so
	 * a second instance refuses at REGISTRATION time, not first send.
	 */
	registerPlatform(
		manifest: PluginManifest,
		factory: PlatformFactory,
		opts: {
			lockOwner?: string | undefined;
			/** Hermes register(ctx) parity: standalone_sender_fn=… */
			standaloneSenderFn?: StandaloneSenderFn | undefined;
		} = {},
	): AdapterDisabledError | null {
		const state = new AdapterLifecycleState();

		for (const spec of manifest.requiresEnv) {
			if (this.secrets(spec.name) === undefined) {
				const reason: DisableReason = {
					kind: "secret_missing",
					secretKey: spec.name,
					manifestName: manifest.name,
				};
				state.disable(reason);
				break;
			}
		}
		this.platforms.set(manifest.name, {
			manifestName: manifest.name,
			state,
			factory,
			...(opts.standaloneSenderFn !== undefined
				? { standaloneSenderFn: opts.standaloneSenderFn }
				: {}),
		});
		return null;
	}
}

export interface PlatformEnablement {
	enabled: boolean;
	reason?: DisableReason | undefined;
	/** Resolved secret VALUES live only with the adapter instance. */
}

/** Pure enablement check usable without constructing the context. */
export function resolveEnablement(
	manifest: PluginManifest,
	secrets: ScopedSecretReader,
): PlatformEnablement {
	for (const spec of manifest.requiresEnv) {
		if (secrets(spec.name) === undefined) {
			return {
				enabled: false,
				reason: {
					kind: "secret_missing",
					secretKey: spec.name,
					manifestName: manifest.name,
				},
			};
		}
	}
	return { enabled: true };
}

/** Factory producing the concrete adapter once enablement is proven. */
export type PlatformFactory = () => unknown;

// ── out-of-process cron delivery: the standalone sender hook ────────────────

/**
 * One out-of-process send request (tools/send_message_tool.py:
 * _send_via_adapter step-2 call shape — chat_id + message chunk plus the
 * signature-parity kwargs every platform accepts and MAY IGNORE).
 */
export interface StandaloneSendArgs {
	chatId: string;
	message: string;
	threadId?: string | undefined;
	mediaFiles?: readonly string[] | undefined;
	forceDocument?: boolean | undefined;
}

/**
 * Result-dict parity: {"success": true, "platform": …, "chat_id": …[,"message_id":…]]}
 * or {"error": str}. _send_via_adapter admits EXACTLY this disjunction
 * (`result.get("success") or result.get("error")`) — anything else is an
 * invalid-result error, so both arms are mandatory shapes here.
 */
export type StandaloneSendResult =
	| {
			success: true;
			platform: string;
			chatId: string;
			messageId?: string | undefined;
	  }
	| { error: string };

/**
 * Out-of-process publish hook, consulted when the gateway runner is NOT in
 * this process (cron running standalone; tools/send_message_tool.py:
 * _send_via_adapter attempt #2). Without a registered fn, deliver=<platform>
 * cron jobs fail with "No live adapter for platform …" — the failure mode
 * this hook exists to prevent.
 */
export type StandaloneSenderFn = (
	args: StandaloneSendArgs,
) => Promise<StandaloneSendResult>;

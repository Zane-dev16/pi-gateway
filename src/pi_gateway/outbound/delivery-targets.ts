// pi_gateway/outbound/delivery-targets.ts — where out-of-process outputs go
// (03-message-routing.md §9.5): target-string grammar plus the PURE
// precedence resolution used by cron/webhook/wake deliveries.
//
// Target strings (gateway/delivery.py::DeliveryTarget.parse):
//   origin              → the originating chat (falls back LOCAL when none recorded)
//   local               → files under $HERMES_HOME/cron/output/ only
//   telegram            → the platform HOME channel
//   telegram:123456     → explicit chat id
//   <plat>:<chat>:<thr> → explicit chat + thread (split at the FIRST TWO
//                         colons — DeliveryTarget.parse maxsplit=2 — so
//                         thread names containing colons stay whole)
//
// Per-delivery precedence: EXPLICIT TARGET > HOME CHANNEL > ORIGIN > LOCAL.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/delivery.py:DeliveryTarget.parse  → parseDeliveryTarget
//   gateway/delivery.py:DeliveryTarget.to_string → deliveryTargetToString
//   gateway/session.py SessionSource          → SessionSource (resolution module shape)

export interface HomeChannel {
	platform: string;
	chatId: string;
	threadId?: string;
}

/** Arrival snapshot needed to resolve `origin` (subset of the session-key module's shape, duplicated deliberately to avoid a sibling import). */
export interface OriginSource {
	platform: string;
	chatId?: string;
	threadId?: string;
}

export interface DeliveryTarget {
	platform: string;
	/** None ⇒ use the platform HOME channel. */
	chatId?: string;
	threadId?: string;
	isOrigin: boolean;
	/** True when a chat id was explicitly specified. */
	isExplicit: boolean;
}

export const LOCAL_PLATFORM = "local";

/** Syntactically valid dynamic/plugin platform token (config.py:Platform._missing_ parity). */
function isPlatformToken(value: string): boolean {
	return /^[a-z0-9][a-z0-9_-]*$/.test(value);
}

/** exactOptionalPropertyTypes-safe threadId assignment. */
function withThreadId(
	t: DeliveryTarget,
	threadId: string | undefined,
): DeliveryTarget {
	return threadId !== undefined ? { ...t, threadId } : t;
}

/**
 * Parse one target string. Unknown/garbage tokens fall back to LOCAL
 * (delivery.py treats unresolvable targets conservatively).
 */
export function parseDeliveryTarget(
	target: string,
	origin?: OriginSource | null,
): DeliveryTarget {
	const stripped = target.trim();
	const lower = stripped.toLowerCase();

	if (lower === "origin") {
		if (origin && origin.chatId) {
			return withThreadId(
				{
					platform: origin.platform,
					chatId: origin.chatId,
					isOrigin: true,
					isExplicit: false,
				},
				origin.threadId,
			);
		}
		// No origin recorded ⇒ fallback LOCAL.
		return { platform: LOCAL_PLATFORM, isOrigin: true, isExplicit: false };
	}

	if (lower === "local") {
		return { platform: LOCAL_PLATFORM, isOrigin: false, isExplicit: false };
	}

	if (stripped.includes(":")) {
		// Python maxsplit=2 parity: split at the FIRST TWO colons only, so a
		// thread NAME containing colons stays whole in the third component
		// ("telegram:123:Hermes API: Test" ⇒ thread "Hermes API: Test").
		const parts = stripped.split(":");
		const platformStr = (parts[0] ?? "").toLowerCase();
		const chatId = parts[1];
		const threadId = parts.length > 2 ? parts.slice(2).join(":") : undefined;
		if (!isPlatformToken(platformStr) || !chatId) {
			return { platform: LOCAL_PLATFORM, isOrigin: false, isExplicit: false };
		}
		return withThreadId(
			{ platform: platformStr, chatId, isOrigin: false, isExplicit: true },
			threadId || undefined,
		);
	}

	// Bare platform name ⇒ that platform's HOME channel (chat_id stays unset).
	if (isPlatformToken(lower)) {
		return { platform: lower, isOrigin: false, isExplicit: false };
	}
	return { platform: LOCAL_PLATFORM, isOrigin: false, isExplicit: false };
}

/** Round-trip serialization (DeliveryTarget.to_string). */
export function deliveryTargetToString(target: DeliveryTarget): string {
	if (target.isOrigin) return "origin";
	if (target.platform === LOCAL_PLATFORM && !target.chatId) return "local";
	if (target.chatId && target.threadId)
		return `${target.platform}:${target.chatId}:${target.threadId}`;
	if (target.chatId) return `${target.platform}:${target.chatId}`;
	return target.platform;
}

// ---------------------------------------------------------------------------
// Pure precedence resolution.
// ---------------------------------------------------------------------------

export type DestinationSource =
	| "explicit_target"
	| "home_channel"
	| "origin"
	| "local";

export interface ResolvedDestination {
	source: DestinationSource;
	target: DeliveryTarget;
}

export interface RoutingInputs {
	/** Configured explicit target strings for this delivery, in order. */
	explicitTargets?: readonly string[];
	/** The origin platform's persisted home channel, when one exists. */
	homeChannel?: HomeChannel | null;
	/** Where this output originated (job created from a conversation), if any. */
	origin?: OriginSource | null;
}

/**
 * Resolve WHERE a delivery goes, applying the §9.5 precedence table:
 *
 *   explicit target > home channel > origin > local
 *
 * Pure over its inputs: same inputs ⇒ same destinations, no clock, no I/O.
 * Multiple explicit targets resolve in their GIVEN order (fan-out keeps every
 * target); the precedence ladder only decides what happens when a level is
 * absent.
 */
export function resolveDeliveryRouting(
	inputs: RoutingInputs,
): ResolvedDestination[] {
	const { explicitTargets, homeChannel, origin } = inputs;

	// Level 1: explicit targets win — all of them, in order.
	if (explicitTargets && explicitTargets.length > 0) {
		return explicitTargets.map((t) => ({
			source: "explicit_target" as const,
			target: parseDeliveryTarget(t, origin),
		}));
	}

	// Level 2: the origin platform's home channel.
	if (homeChannel && homeChannel.chatId) {
		return [
			{
				source: "home_channel",
				target: withThreadId(
					{
						platform: homeChannel.platform,
						chatId: homeChannel.chatId,
						isOrigin: false,
						isExplicit: true,
					},
					homeChannel.threadId,
				),
			},
		];
	}

	// Level 3: back to the origin conversation.
	if (origin && origin.chatId) {
		return [
			{
				source: "origin",
				target: withThreadId(
					{
						platform: origin.platform,
						chatId: origin.chatId,
						isOrigin: true,
						isExplicit: false,
					},
					origin.threadId,
				),
			},
		];
	}

	// Level 4: always-available floor.
	return [
		{
			source: "local",
			target: { platform: LOCAL_PLATFORM, isOrigin: false, isExplicit: false },
		},
	];
}

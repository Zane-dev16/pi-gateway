// pi_gateway/outbound/delivery-router.ts — transport resolution + the deliver
// loop for out-of-process outputs (03-message-routing.md §9.5).
//
// Transport selection (delivery.py:resolve_delivery_transport): a live NATIVE
// adapter wins; RELAY is eligible only when it explicitly advertises fronting
// the logical platform — restart-time delivery must not depend on per-chat
// caches, and Relay must never hijack unrelated targets.
//
// Per-target behavior in DeliveryRouter.deliver:
//   - dead-target short-circuit with self-healing clear on success;
//   - oversize (>4000 chars): ALWAYS audit-save first (best-effort), then
//     truncate-with-footer for non-chunking adapters, full payload for
//     chunking-capable ones;
//   - substrate-level anti-loop: pure silence-narration tokens drop before
//     egress; LOCAL delivery is never filtered;
//   - relay home-channel sends re-attach user_id/scope_id metadata;
//   - Telegram private-chat threads: a non-numeric thread id is a topic NAME
//     created via adapter.ensureDmTopic before sending (fail closed when the
//     adapter lacks the capability), a legacy numeric one requires a reply
//     anchor, and a "thread not found" failure on a created topic refreshes
//     (force-create) and retries ONCE.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/delivery.py:MAX_PLATFORM_OUTPUT        → MAX_PLATFORM_OUTPUT
//   gateway/delivery.py:_is_silence_narration      → isSilenceNarration
//   gateway/delivery.py:resolve_delivery_transport → resolveDeliveryTransport
//   gateway/delivery.py:DeliveryRouter.deliver     → DeliveryRouter.deliver
//   gateway/dead_targets.py                        → dead-targets.ts

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { type DeadTargetRegistry, isDeadErrorKind } from "./dead-targets.js";
import {
	LOCAL_PLATFORM,
	type OriginSource,
	resolveDeliveryRouting,
} from "./delivery-targets.js";

/** Cap before truncation for non-chunking platforms (Telegram hard limit 4096 minus footer headroom). */
export const MAX_PLATFORM_OUTPUT = 4000;

const SILENCE_NARRATION_RE =
	/^[\s*_~`]*\(?\s*(silent|silence|no\s+response|no\s+reply)\s*\.?\)?[\s*_~`]*$|^[\s*_~`]*[\u{1F507}.\u2026]+[\s*_~`]*$/iu;

/**
 * True when content is ONLY a silence-narration token (*(silent)*, 🔇, a bare
 * ".", "…"). Length-guarded and anchored so prose mentioning "silent" never
 * matches. In bot-to-bot channels these tokens mirror until a model crashes —
 * they drop before egress.
 */
export function isSilenceNarration(
	content: string | null | undefined,
): boolean {
	if (!content) return false;
	const stripped = content.trim();
	if (!stripped || stripped.length > 64) return false;
	return SILENCE_NARRATION_RE.test(stripped);
}

export interface PlatformAdapterConfig {
	enabled?: boolean;
}

export interface HomeChannelInfo {
	chatId: string;
	userId?: string;
	scopeId?: string;
}

/** Minimal gateway-config surface the resolver reads. */
export interface RouterConfig {
	platforms: Record<string, PlatformAdapterConfig>;
	getHomeChannel(platform: string): HomeChannelInfo | null;
}

/** Adapter seam: native transports + relay share this shape. */
export interface RouterAdapter {
	name?: string;
	splitsLongMessages?: boolean;
	send(
		logicalPlatform: string,
		chatId: string,
		content: string,
		metadata?: Record<string, unknown>,
	): Promise<SendResult>;
	/** Relay-only capability advertisement. */
	frontsPlatform?(platform: string): boolean;
	/**
	 * Named-DM-topic capability (telegram adapter.py:ensure_dm_topic): resolve
	 * a private-chat DM-topic thread id by NAME, creating it when absent;
	 * `forceCreate` rebuilds a stale/deleted topic. OPTIONAL — when absent,
	 * named-topic deliveries FAIL CLOSED (delivery.py raises instead of
	 * sending a raw topic name to the wire as message_thread_id).
	 */
	ensureDmTopic?(
		chatId: string,
		topicName: string,
		forceCreate?: boolean,
	): Promise<string | null>;
}

export interface SendResult {
	success: boolean;
	error?: string;
	errorKind?: string;
}

export interface ResolvedTransport {
	adapter: RouterAdapter;
	config: PlatformAdapterConfig | null;
	isRelay: boolean;
}

/**
 * A concrete native adapter ALWAYS wins. Relay only fronts platforms it
 * explicitly advertises (and only when enabled); unrelated targets are never
 * hijacked.
 */
export function resolveDeliveryTransport(
	platform: string,
	config: RouterConfig,
	adapters: Record<string, RouterAdapter>,
): ResolvedTransport | null {
	const native = adapters[platform];
	const nativeConfig = config.platforms[platform] ?? null;
	if (native && (!nativeConfig || nativeConfig.enabled !== false)) {
		return { adapter: native, config: nativeConfig, isRelay: false };
	}
	const relay = adapters.relay ?? adapters["RELAY"];
	const relayConfig = config.platforms.relay ?? config.platforms.RELAY ?? null;
	const fronts = relay?.frontsPlatform;
	if (
		relay &&
		(!relayConfig || relayConfig.enabled !== false) &&
		typeof fronts === "function" &&
		fronts.call(relay, platform)
	) {
		return { adapter: relay, config: relayConfig, isRelay: true };
	}
	return null;
}

export interface DeliverTargetInput {
	targetString?: string;
	platform: string;
	chatId?: string;
	threadId?: string;
	isOrigin?: boolean;
	isExplicit?: boolean;
}

export interface DeliverOptions {
	content: string;
	targets: DeliverTargetInput[];
	jobId?: string;
	jobName?: string;
	metadata?: Record<string, unknown>;
	/** INJECTED clock (audit filenames + timestamps). */
	now?: Date;
	/** Silence-narration filter toggle (env > config > default true). */
	filterSilenceNarration?: boolean;
}

export interface TargetDeliverResult {
	success: boolean;
	skipped?: "dead_target" | "silence_narration" | "oversize_saved_only";
	result?: Record<string, unknown>;
	error?: string;
}

/**
 * The router. `outputDir` is `<home>/cron/output` parity; injected so tests
 * stay under mkdtemp.
 */
export class DeliveryRouter {
	constructor(
		private readonly config: RouterConfig,
		private readonly adapters: Record<string, RouterAdapter>,
		private readonly deadTargets: DeadTargetRegistry,
		private readonly outputDir: string,
	) {}

	async deliver(
		options: DeliverOptions,
	): Promise<Record<string, TargetDeliverResult>> {
		const results: Record<string, TargetDeliverResult> = {};
		for (const target of options.targets) {
			const key = target.targetString ?? targetKey(target);
			results[key] = await this.deliverOne(target, options);
		}
		return results;
	}

	private async deliverOne(
		target: DeliverTargetInput,
		options: DeliverOptions,
	): Promise<TargetDeliverResult> {
		const content = options.content;
		const now = options.now ?? new Date();

		// LOCAL delivery: files only, never dead-tracked, never filtered.
		if (target.platform === LOCAL_PLATFORM) {
			const localResult = this.deliverLocal(
				content,
				options.jobId,
				options.jobName,
				options.metadata,
				now,
			);
			return { success: true, result: localResult };
		}

		// Dead-target short-circuit (LOCAL and origin-less targets never track).
		if (
			target.chatId &&
			this.deadTargets.isDead(target.platform, target.chatId)
		) {
			return {
				success: false,
				skipped: "dead_target",
				error: "target previously confirmed unreachable",
			};
		}

		try {
			const result = await this.deliverToPlatform(
				target,
				content,
				options,
				now,
			);
			// Success clears any stale dead flag (self-healing).
			if (target.chatId) this.deadTargets.clear(target.platform, target.chatId);
			return { success: true, result };
		} catch (e) {
			const message = e instanceof Error ? e.message : String(e);
			// Whole-chat death ⇒ record so future deliveries short-circuit.
			if (target.chatId && classifiesWholeChatDeath(message)) {
				this.deadTargets.markDead(
					target.platform,
					target.chatId,
					message,
					now.getTime(),
				);
			}
			return { success: false, error: message };
		}
	}

	private async deliverToPlatform(
		target: DeliverTargetInput,
		content: string,
		options: DeliverOptions,
		now: Date,
	): Promise<Record<string, unknown>> {
		const transport = resolveDeliveryTransport(
			target.platform,
			this.config,
			this.adapters,
		);
		if (!transport)
			throw new Error(`No adapter configured for ${target.platform}`);
		let payload = content;

		// Oversize handling: AUDIT SAVE always; truncation only for non-chunkers.
		let savedPath: string | null = null;
		if (payload.length > MAX_PLATFORM_OUTPUT) {
			try {
				savedPath = this.saveFullOutput(
					payload,
					options.jobId ?? "unknown",
					now,
				);
			} catch {
				savedPath = null; // best-effort; delivery proceeds
			}
			const chunker = transport.adapter.splitsLongMessages === true;
			if (chunker) {
				// Full payload; adapter splits natively in send().
			} else {
				if (savedPath === null) {
					// Footer needs a valid path — retry the save; failure now is real.
					savedPath = this.saveFullOutput(
						payload,
						options.jobId ?? "unknown",
						now,
					);
				}
				const footer = `\n\n... [truncated, full output saved to ${savedPath}]`;
				const visible = Math.max(0, MAX_PLATFORM_OUTPUT - footer.length);
				payload = payload.slice(0, visible) + footer;
			}
		}

		// Substrate-level anti-loop guard (never applied to LOCAL above).
		const filterOn = options.filterSilenceNarration ?? true;
		if (filterOn && isSilenceNarration(payload)) {
			return { filtered: "silence_narration", delivered: false };
		}

		const metadata: Record<string, unknown> = { ...options.metadata };
		// Relay home-channel sends re-attach identity metadata behind the relay.
		if (transport.isRelay) {
			const home = this.config.getHomeChannel(target.platform);
			if (home && home.chatId === target.chatId) {
				if (home.userId !== undefined) metadata.user_id = home.userId;
				if (home.scopeId !== undefined) metadata.scope_id = home.scopeId;
			}
		}

		// Telegram private-chat thread ladder (_deliver_to_platform). A
		// NON-numeric thread id on a positive chat id is a topic NAME — it is
		// resolved/created via adapter.ensureDmTopic BEFORE the send (a raw name
		// must never reach the wire as message_thread_id); a numeric one is a
		// legacy forum-topic id that stays visible only with a reply anchor.
		// Metadata thread keys WIN over the target string — membership checks,
		// not value checks (`"thread_id" not in send_metadata` parity).
		let isNamedPrivateTopic = false;
		let namedTopicName: string | null = null;
		if (target.threadId) {
			const hasExplicitDirectTopic =
				"direct_messages_topic_id" in metadata ||
				"telegram_direct_messages_topic_id" in metadata;
			const hasThreadKey =
				"thread_id" in metadata || "message_thread_id" in metadata;
			let targetThreadId: string = target.threadId;
			isNamedPrivateTopic =
				target.platform === TELEGRAM_PLATFORM &&
				looksLikeTelegramPrivateChatId(target.chatId) &&
				!looksLikeInt(targetThreadId) &&
				!hasThreadKey &&
				!hasExplicitDirectTopic;
			if (isNamedPrivateTopic) {
				namedTopicName = targetThreadId;
				const ensureDmTopic = transport.adapter.ensureDmTopic;
				if (typeof ensureDmTopic !== "function") {
					throw new Error(
						"Telegram adapter cannot create named private DM topics",
					);
				}
				const createdThreadId = await ensureDmTopic.call(
					transport.adapter,
					target.chatId ?? "",
					targetThreadId,
				);
				if (!createdThreadId) {
					throw new Error(
						`Failed to create Telegram private DM topic '${targetThreadId}'`,
					);
				}
				targetThreadId = String(createdThreadId);
				metadata.thread_id = targetThreadId;
				metadata.telegram_dm_topic_created_for_send = true;
			} else if (
				target.platform === TELEGRAM_PLATFORM &&
				looksLikeTelegramPrivateChatId(target.chatId) &&
				!hasThreadKey &&
				!hasExplicitDirectTopic
			) {
				// Legacy private topic/thread ids that were not created by this
				// send path may still need a reply anchor to stay visible in the
				// requested lane. Named targets are created above via
				// createForumTopic and can use message_thread_id directly.
				const replyAnchor = metadata.telegram_reply_to_message_id;
				if (replyAnchor === undefined || replyAnchor === null) {
					throw new Error(
						"Telegram private DM topic delivery requires telegram_reply_to_message_id; " +
							"send to the bare chat or provide a reply anchor",
					);
				}
				metadata.thread_id = targetThreadId;
				metadata.telegram_dm_topic_reply_fallback = true;
			} else if (!hasThreadKey && !hasExplicitDirectTopic) {
				metadata.thread_id = targetThreadId;
			}
		}

		const wireSend = () =>
			transport.adapter.send(
				target.platform,
				target.chatId ?? "",
				payload,
				Object.keys(metadata).length > 0 ? metadata : undefined,
			);
		let sendResult = await wireSend();
		if (
			!sendResult.success &&
			isNamedPrivateTopic &&
			namedTopicName !== null &&
			isThreadNotFoundDeliveryError(sendResult.error)
		) {
			// Stale/deleted created topic: force-recreate ONCE and retry under
			// the fresh thread id (delivery.py refresh ladder).
			const ensureDmTopic = transport.adapter.ensureDmTopic;
			if (typeof ensureDmTopic !== "function") {
				throw new Error(
					"Telegram adapter cannot refresh named private DM topics",
				);
			}
			const refreshedThreadId = await ensureDmTopic.call(
				transport.adapter,
				target.chatId ?? "",
				namedTopicName,
				true,
			);
			if (!refreshedThreadId) {
				throw new Error(
					`Failed to refresh Telegram private DM topic '${namedTopicName}'`,
				);
			}
			metadata.thread_id = String(refreshedThreadId);
			metadata.telegram_dm_topic_created_for_send = true;
			sendResult = await wireSend();
		}
		if (!sendResult.success) {
			throw new Error(sendResult.error ?? `${target.platform} delivery failed`);
		}
		return { sent: true };
	}

	private saveFullOutput(content: string, jobId: string, now: Date): string {
		const stamp = formatStamp(now);
		mkdirSync(this.outputDir, { recursive: true });
		const path = join(this.outputDir, `${jobId}_${stamp}.txt`);
		writeFileSync(path, content, "utf8");
		return path;
	}

	/** Local/file delivery keeps header/footer framing (07 §cron parity). */
	private deliverLocal(
		content: string,
		jobId: string | undefined,
		jobName: string | undefined,
		metadata: Record<string, unknown> | undefined,
		now: Date,
	): Record<string, unknown> {
		const stamp = formatStamp(now);
		const dir = join(this.outputDir, jobId ?? "misc");
		mkdirSync(dir, { recursive: true });
		const path = join(dir, `${stamp}.md`);

		const lines: string[] = [
			`# ${jobName ?? "Delivery Output"}`,
			"",
			`**Timestamp:** ${formatHuman(now)}`,
		];
		if (jobId) lines.push(`**Job ID:** ${jobId}`);
		for (const [k, v] of Object.entries(metadata ?? {}))
			lines.push(`**${k}:** ${String(v)}`);
		lines.push("", "---", "", content);
		writeFileSync(path, lines.join("\n"), "utf8");
		return { path, timestamp: stamp };
	}
}

function formatStamp(now: Date): string {
	const p = (n: number, w = 2) => String(n).padStart(w, "0");
	return `${now.getUTCFullYear()}${p(now.getUTCMonth() + 1)}${p(now.getUTCDate())}_${p(now.getUTCHours())}${p(now.getUTCMinutes())}${p(now.getUTCSeconds())}`;
}

function formatHuman(now: Date): string {
	const p = (n: number) => String(n).padStart(2, "0");
	return `${now.getUTCFullYear()}-${p(now.getUTCMonth() + 1)}-${p(now.getUTCDate())} ${p(now.getUTCHours())}:${p(now.getUTCMinutes())}:${p(now.getUTCSeconds())}`;
}

function targetKey(t: DeliverTargetInput): string {
	if (t.isOrigin && t.chatId) return "origin";
	if (t.chatId && t.threadId) return `${t.platform}:${t.chatId}:${t.threadId}`;
	if (t.chatId) return `${t.platform}:${t.chatId}`;
	return t.platform;
}

const TELEGRAM_PLATFORM = "telegram";

/**
 * True when chat_id is a positive int — Telegram's private-chat shape
 * (delivery.py:looks_like_telegram_private_chat_id; groups/channels/
 * supergroups use negative ids).
 */
function looksLikeTelegramPrivateChatId(chatId: string | undefined): boolean {
	if (!chatId) return false;
	const text = chatId.trim();
	if (!/^[+-]?\d+$/.test(text)) return false; // Python int() strictness
	return Number.parseInt(text, 10) > 0;
}

/** Python int() parseability (delivery.py:_looks_like_int). */
function looksLikeInt(value: string): boolean {
	return /^[+-]?\d+$/.test(value.trim());
}

/** delivery.py:_is_thread_not_found_delivery_error. */
function isThreadNotFoundDeliveryError(
	error: string | null | undefined,
): boolean {
	return !!error && error.toLowerCase().includes("thread not found");
}

/**
 * Best-effort whole-chat-death classification from a raised error's text:
 * forbidden / chat-level not_found kinds. Thread-level not-found ("thread not
 * found", "message not found", topic deletions) must NOT mark chats dead.
 */
export function classifiesWholeChatDeath(
	errorText: string | null | undefined,
): boolean {
	if (!errorText) return false;
	const lower = errorText.toLowerCase();
	const kind = lower.includes("forbidden")
		? "forbidden"
		: lower.includes("not found") ||
				lower.includes("chat was deleted") ||
				lower.includes("deactivated")
			? "not_found"
			: null;
	if (!isDeadErrorKind(kind)) return false;
	if (kind === "not_found" && /\b(thread|message|topic|reply)\b/.test(lower)) {
		return false; // thread/topic/message-level failures self-heal upstream
	}
	return true;
}

/** Convenience: parse targets via the routing module and hand them to a router. */
export function routeViaPrecedence(
	inputs: Parameters<typeof resolveDeliveryRouting>[0],
	deliver: (
		targets: ReturnType<typeof resolveDeliveryRouting>,
	) => Promise<void>,
): Promise<void> {
	return deliver(resolveDeliveryRouting(inputs));
}

/** Re-exported for callers composing cron deliveries from session sources. */
export type { OriginSource };

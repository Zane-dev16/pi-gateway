// pi_platforms/photon/sidecar-wire — the sidecar control-plane TRANSPORT SEAM
// plus the in-process fake server (04 §8: rows run headless against fake
// platform servers; NO OS children, NO sockets, NO fetch).
//
// Production binds this seam to loopback HTTP POSTs against
// `http://127.0.0.1:<port><path>` with the X-Hermes-Sidecar-Token header
// (adapter.py:_sidecar_call @~2648); the conformance harness binds
// FakeSidecarServer, which models every edge the adapter uses with scriptable
// failure shapes and full call capture:
//
//   POST /healthz         → {ok, stream:{ok, state, degradedForMs, lastIssue}}
//                            (_monitor_sidecar_health degraded→fatal path)
//   POST /send            → {ok, messageId}          (text/poll clarify text)
//   POST /send-poll       → {ok, messageId}          (native iMessage poll)
//   POST /send-attachment → {ok, messageId}          (documented exclusion lane)
//   POST /send-effect     → {ok, messageId}          (bubble/screen effects)
//   POST /typing          → {ok}                     (state: start|stop)
//   POST /react           → {ok, messageId, reactionId}
//   POST /unreact         → {ok}
//   POST /send-richlink   → {ok, messageId}          (URL-only candidates)
//   POST /probe           → {ok}                     (presence watchdog)
//
// Body-less endpoints: /probe and the startup /healthz readiness ping ride
// HEADERS-ONLY POSTs upstream (adapter.py:_probe_once @~1869 and the
// _start_sidecar healthz wait @~1720 pass no JSON body) — the sidecar routes
// read before the body. The seam therefore treats `body` as OPTIONAL; every
// other endpoint posts a JSON object.
//
// Latency-free by construction: a scripted HUNG probe surfaces the SAME error
// shape the real httpx timeout produces (SidecarHungError) synchronously —
// injected latency, never wall-clock waits.

export type SidecarPath =
	| "/healthz"
	| "/send"
	| "/send-poll"
	| "/send-attachment"
	| "/send-effect"
	| "/typing"
	| "/react"
	| "/unreact"
	| "/send-richlink"
	| "/probe";

/**
 * THE transport seam. The adapter NEVER imports http/undici — production and
 * tests supply different implementations of this one call.
 */
export interface SidecarTransport {
	/**
	 * POST <loopback>/<path>; resolves the parsed response or THROWS:
	 * SidecarHttpError for non-200 / ok:false responses
	 * (_sidecar_error_from_response parity), plain Errors for transport
	 * failures, SidecarHungError when the call itself times out.
	 *
	 * `body` is OMITTED (headers-only POST) for the body-less endpoints
	 * /probe and the startup /healthz ping (_probe_once/_start_sidecar
	 * parity); every other endpoint posts a JSON object.
	 */
	call(
		path: string,
		body?: Record<string, unknown> | undefined,
	): Promise<Record<string, unknown>>;
}

/** Structured sidecar failure (adapter.py:PhotonSidecarError shape). */
export class SidecarHttpError extends Error {
	readonly path: string;
	readonly statusCode: number;
	readonly error: string;
	readonly errorClass: string;
	readonly retryable: boolean;
	/**
	 * Test-fabrication channel ONLY: a numeric retry hint embedded in an error
	 * body. The REAL photon wire carries no such field anywhere — the fake can
	 * inject one to prove the adapter ignores it (retry-after-capture row).
	 */
	readonly fabricatedRetryAfterSeconds?: number | undefined;

	constructor(opts: {
		path: string;
		statusCode: number;
		error: string;
		errorClass?: string | undefined;
		retryable?: boolean | undefined;
		fabricatedRetryAfterSeconds?: number | undefined;
	}) {
		super(
			`Photon sidecar ${opts.path} returned ${opts.statusCode} ` +
				`(${opts.errorClass ?? "sidecar_error"}, retryable=${opts.retryable === true}): ${opts.error}`,
		);
		this.name = "SidecarHttpError";
		this.path = opts.path;
		this.statusCode = opts.statusCode;
		this.error = opts.error;
		this.errorClass = opts.errorClass ?? "sidecar_error";
		this.retryable = opts.retryable === true;
		this.fabricatedRetryAfterSeconds = opts.fabricatedRetryAfterSeconds;
	}

	get retryableMarker(): "retryable=true" | "retryable=false" {
		return this.retryable ? "retryable=true" : "retryable=false";
	}
}

/** The probe/send HTTP call itself hung (httpx timeout parity → "hung"). */
export class SidecarHungError extends Error {
	constructor(message = "sidecar call timed out") {
		super(message);
		this.name = "SidecarHungError";
	}
}

export type SidecarBehavior =
	| { kind: "ok"; response?: Record<string, unknown> }
	| {
			kind: "error";
			status?: number;
			error: string;
			errorClass?: string;
			retryable?: boolean;
			/** Fabricated numeric hint (see SidecarHttpError). */
			retryAfterHint?: number;
	  }
	| { kind: "transport-error"; message: string }
	| { kind: "hung" };

export interface RecordedSidecarCall {
	path: string;
	/** Undefined for the body-less endpoints (/probe, startup /healthz). */
	body?: Record<string, unknown> | undefined;
	seq: number;
	outcome: "ok" | "error";
	/** The error BODY when outcome === "error" (mutant observability). */
	errorBody?: Record<string, unknown> | undefined;
}

/**
 * In-memory sidecar double. Behaviors are consumed FIFO PER PATH ("*" scripts
 // apply to any path without a specific script); an exhausted script defaults
 * to the vendor-shaped success body.
 */
export class FakeSidecarServer implements SidecarTransport {
	private scripts = new Map<string, SidecarBehavior[]>();
	private seqCounter = 0;

	/** Every call, in order, with its outcome (row observability). */
	readonly calls: RecordedSidecarCall[] = [];

	/** Configurable /healthz stream payload (degraded-stream fatal row). */
	healthzStream: Record<string, unknown> = { ok: true };

	script(path: SidecarPath | "*", ...behaviors: SidecarBehavior[]): void {
		const queue = this.scripts.get(path) ?? [];
		queue.push(...behaviors);
		this.scripts.set(path, queue);
	}

	clearScripts(): void {
		this.scripts.clear();
	}

	callsOf(path: string): RecordedSidecarCall[] {
		return this.calls.filter((c) => c.path === path);
	}

	private next(path: string): SidecarBehavior {
		const queue = this.scripts.get(path);
		if (queue !== undefined && queue.length > 0) {
			return queue.shift() as SidecarBehavior;
		}
		const wildcard = this.scripts.get("*");
		if (wildcard !== undefined && wildcard.length > 0) {
			return wildcard.shift() as SidecarBehavior;
		}
		return { kind: "ok" };
	}

	async call(
		path: string,
		body?: Record<string, unknown> | undefined,
	): Promise<Record<string, unknown>> {
		const behavior = this.next(path);
		if (behavior.kind === "hung") {
			this.seqCounter += 1;
			this.calls.push({
				path,
				body,
				seq: this.seqCounter,
				outcome: "error",
				errorBody: { error: "timed out" },
			});
			throw new SidecarHungError();
		}
		if (behavior.kind === "transport-error") {
			this.seqCounter += 1;
			this.calls.push({
				path,
				body,
				seq: this.seqCounter,
				outcome: "error",
				errorBody: { error: behavior.message },
			});
			throw new Error(behavior.message);
		}
		if (behavior.kind === "error") {
			this.seqCounter += 1;
			const errorBody: Record<string, unknown> = {
				error: behavior.error,
				error_class: behavior.errorClass ?? "sidecar_error",
				retryable: behavior.retryable === true,
			};
			if (behavior.retryAfterHint !== undefined) {
				// The ONLY place a numeric retry hint exists in this port —
				// inside a FABRICATED test body the real wire never carries.
				errorBody["retry_after"] = behavior.retryAfterHint;
			}
			this.calls.push({
				path,
				body,
				seq: this.seqCounter,
				outcome: "error",
				errorBody,
			});
			throw new SidecarHttpError({
				path,
				statusCode: behavior.status ?? 500,
				error: behavior.error,
				errorClass: behavior.errorClass,
				retryable: behavior.retryable,
				fabricatedRetryAfterSeconds: behavior.retryAfterHint,
			});
		}
		// Default vendor success shapes per endpoint.
		this.seqCounter += 1;
		let response = behavior.response;
		if (response === undefined) {
			response = path === "/healthz" ? { ok: true } : { ok: true };
			if (
				path === "/send" ||
				path === "/send-poll" ||
				path === "/send-attachment" ||
				path === "/send-effect" ||
				path === "/send-richlink"
			) {
				response = { ok: true, messageId: `spc-msg-${this.seqCounter}` };
			} else if (path === "/react") {
				response = { ok: true, reactionId: `react-${this.seqCounter}` };
			}
			if (path === "/healthz") {
				response = { ok: true, stream: this.healthzStream };
			}
		}
		this.calls.push({ path, body, seq: this.seqCounter, outcome: "ok" });
		return response;
	}
}

/**
 * PushIngress — the inbound NDJSON line driver. The real adapter consumes the
 * sidecar's GET /inbound stream (`async for line in resp.aiter_lines()`);
 * the harness feeds the SAME line handler directly: blank lines are
 * heartbeats, JSON lines are events, non-JSON is skipped (adapter.py:_on_inbound_line).
 */
export class PushIngress {
	constructor(private readonly onLine: (line: string) => Promise<void>) {}

	/** One sidecar event object, serialized exactly like the wire would. */
	async push(event: Record<string, unknown>): Promise<void> {
		await this.onLine(JSON.stringify(event));
	}

	/** A raw line (heartbeat "", garbage, …) straight onto the handler. */
	async pushRaw(line: string): Promise<void> {
		await this.onLine(line);
	}
}

// ── event builders (test-sidecar/index.mjs event shapes) ────────────────────

export interface PhotonSpaceShape {
	id: string;
	type: "dm" | "group";
	phone?: string | null;
}

export function photonDmEvent(
	text: string,
	messageId: string,
	opts: {
		spaceId?: string;
		senderId?: string;
		timestamp?: string;
	} = {},
): Record<string, unknown> {
	const phone = opts.spaceId ?? "+15551234567";
	return {
		messageId,
		platform: "iMessage",
		space: { id: phone, type: "dm", phone },
		sender: { id: opts.senderId ?? phone },
		content: { type: "text", text },
		timestamp: opts.timestamp ?? "2026-05-14T19:06:32.000Z",
	};
}

export function photonGroupEvent(
	text: string,
	messageId: string,
	opts: { senderId?: string } = {},
): Record<string, unknown> {
	return {
		messageId,
		platform: "iMessage",
		space: { id: "group-guid-xyz", type: "group", phone: null },
		sender: { id: opts.senderId ?? "+15551234567" },
		content: { type: "text", text },
		timestamp: "2026-05-14T19:06:32.000Z",
	};
}

export function photonReactionEvent(opts: {
	emoji?: string;
	targetId?: string | null;
	targetDirection?: "inbound" | "outbound" | null;
	/** Explicitly pass null to model an attachment-only hydrated target. */
	targetText?: string | null;
	spaceType?: "dm" | "group";
	messageId?: string;
	spaceId?: string;
	senderId?: string;
}): Record<string, unknown> {
	const hasTargetText = opts.targetText !== undefined;
	return {
		messageId: opts.messageId ?? "reaction-evt-1",
		platform: "iMessage",
		space: {
			id: opts.spaceId ?? "+15551234567",
			type: opts.spaceType ?? "dm",
			phone: opts.spaceId ?? "+15551234567",
		},
		sender: { id: opts.senderId ?? "+15551234567" },
		content: {
			type: "reaction",
			emoji: opts.emoji ?? "❤️",
			targetMessageId: opts.targetId ?? "bot-msg-1",
			targetDirection: opts.targetDirection ?? "outbound",
			// The sidecar always emits this key (hydrated target); null when the
			// reacted-to message carried no text.
			targetText: hasTargetText ? opts.targetText : "the bot's earlier reply",
		},
		timestamp: "2026-06-11T10:00:00.000Z",
	};
}

export function photonPollOptionEvent(opts: {
	title: string;
	selected?: boolean;
	pollTitle?: string;
	messageId?: string;
	spaceId?: string;
}): Record<string, unknown> {
	return {
		messageId: opts.messageId ?? "spc-msg-vote",
		platform: "iMessage",
		space: {
			id: opts.spaceId ?? "+155****4567",
			type: "dm",
			phone: opts.spaceId ?? "+155****4567",
		},
		sender: { id: opts.spaceId ?? "+155****4567" },
		content: {
			type: "poll_option",
			title: opts.title,
			selected: opts.selected ?? true,
			pollTitle: opts.pollTitle ?? "Pick one",
		},
		timestamp: "2026-05-14T19:06:32.000Z",
	};
}

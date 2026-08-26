// pi_platforms/buzz/relay-wire — THE WebSocket transport MEDIUM behind the
// live NIP-42 relay loop. The Hermes reference opens REAL sockets through the
// `websockets` library (adapter.py:_websocket_loop); this port NEVER opens
// network sockets — exactly like cli-wire.ts replaces the real binary, the
// relay rides an INJECTED async factory seam
//
//     factory(url) → { send(frame), recv() → frame|null, close() }
//
// so the handshake/subscription/routing/backoff LOOP itself is fully ported
// (buzz-adapter.ts) and behavior-tested against an in-memory NIP-42 relay
// (FakeBuzzRelay) that INDEPENDENTLY verifies the signed kind-22242 auth
// event: NIP-01 id recomputation over the compact serialization PLUS a spec
// BIP-340 verification built only on the exported nostr-auth primitives.
//
// Anchor map (adapter.py → this module):
//   websockets.connect(url, open_timeout=_WS_AUTH_TIMEOUT, ping_*, max_size)
//                                            → BuzzRelaySocketFactory /
//                                              BuzzRelaySocket (open timeout is
//                                              the factory's policy; the AUTH
//                                              recv deadlines live in the loop)
//   asyncio.wait_for(...)/asyncio.sleep(backoff)
//                                            → BuzzTiming.sleep handles
//                                             (injectable clock; losers cancel
//                                              their OS timer)

import { createHash } from "node:crypto";

import {
	CURVE_ORDER,
	FIELD_ORDER,
	pointAdd,
	pointMultiply,
	taggedHash,
} from "./nostr-auth.js";

/** One established relay connection (websockets.WebSocketClientConnection analog). */
export interface BuzzRelaySocket {
	readonly url: string;
	/** Send one TEXT frame (already JSON-serialized by the caller). */
	send(frame: string): void | Promise<void>;
	/**
	 * Next inbound TEXT frame; null = CLEAN close (async-for ends normally);
	 * a REJECTION models an abrupt transport error.
	 */
	recv(): Promise<string | null>;
	/** Close the socket; parked recv() calls settle with null. */
	close(): void;
}

/** adapter.py:websockets.connect(url, …) — the injected dialer. */
export type BuzzRelaySocketFactory = (url: string) => Promise<BuzzRelaySocket>;

// ── timing (backoff sleeps + auth/CLI deadlines) ─────────────────────────────

export interface BuzzSleepHandle {
	readonly promise: Promise<void>;
	/** Settle early + release any OS timer (loser cleanup of a race). */
	cancel(): void;
}

export interface BuzzTiming {
	/** Resolve after `seconds` (wall clock unless injected). */
	sleep(seconds: number): BuzzSleepHandle;
}

/** Production timing: real timers, cancellation-friendly (no event-loop hold). */
export function wallTiming(): BuzzTiming {
	return {
		sleep(seconds: number): BuzzSleepHandle {
			let settle!: () => void;
			const promise = new Promise<void>((resolve) => {
				settle = resolve;
			});
			const timer = setTimeout(settle, Math.max(0, seconds * 1000));
			return {
				promise,
				cancel() {
					clearTimeout(timer);
					settle();
				},
			};
		},
	};
}

export interface ManualSleepRecord {
	seconds: number;
	/** Fired or cancelled — no longer pending. */
	settled: boolean;
	cancelled: boolean;
}

/**
 * Deterministic clock for conformance rows: sleeps PARK until the test fires
 * them, so backoff ladders and deadline windows assert without wall time.
 */
export class ManualBuzzTiming implements BuzzTiming {
	private readonly records: ManualSleepRecord[] = [];
	private readonly resolvers = new Map<ManualSleepRecord, () => void>();

	sleep(seconds: number): BuzzSleepHandle {
		const record: ManualSleepRecord = {
			seconds,
			settled: false,
			cancelled: false,
		};
		this.records.push(record);
		let settle!: () => void;
		const promise = new Promise<void>((resolve) => {
			settle = resolve;
		});
		this.resolvers.set(record, settle);
		return {
			promise,
			cancel() {
				if (!record.settled) {
					record.settled = true;
					record.cancelled = true;
				}
				settle();
			},
		};
	}

	/** Requested-but-unsettled durations, in request order. */
	pendingSeconds(): number[] {
		return this.records
			.filter((record) => !record.settled)
			.map((record) => record.seconds);
	}

	/** Fire EVERY unsettled sleep of exactly `seconds`; returns how many. */
	fire(seconds: number): number {
		let fired = 0;
		for (const record of this.records) {
			if (record.settled || record.seconds !== seconds) continue;
			record.settled = true;
			fired += 1;
			this.resolvers.get(record)?.();
		}
		return fired;
	}

	/** Fire every unsettled sleep regardless of duration. */
	fireAll(): void {
		for (const record of this.records) {
			if (record.settled) continue;
			record.settled = true;
			this.resolvers.get(record)?.();
		}
	}
}

// ── independent auth-event verification (the fake relay is a REAL verifier) ──

function modPow(base: bigint, exponent: bigint, modulus: bigint): bigint {
	let result = 1n;
	let b = ((base % modulus) + modulus) % modulus;
	let e = exponent;
	while (e > 0n) {
		if (e & 1n) result = (result * b) % modulus;
		b = (b * b) % modulus;
		e >>= 1n;
	}
	return result;
}

function intTo32Bytes(value: bigint): Buffer {
	return Buffer.from(value.toString(16).padStart(64, "0"), "hex");
}

/**
 * Spec BIP-340 verification (x-only pubkey lift, tagged challenge,
 * R = sG − eP) built ONLY on the exported nostr-auth primitives — the same
 * role the independent verifier played when the CONTRACT VECTORS were
 * recorded (vectors.ts provenance note).
 */
export function verifyBip340Signature(
	message: Buffer,
	pubkeyHex: string,
	sigHex: string,
): boolean {
	if (pubkeyHex.length !== 64 || sigHex.length !== 128) return false;
	if (!/^[0-9a-f]+$/.test(pubkeyHex) || !/^[0-9a-f]+$/.test(sigHex)) {
		return false;
	}
	const pubX = BigInt(`0x${pubkeyHex}`);
	if (pubX >= FIELD_ORDER) return false;
	// lift_x: y = sqrt(x³+7); reject non-residues; take the EVEN lift.
	const ySquared = (modPow(pubX, 3n, FIELD_ORDER) + 7n) % FIELD_ORDER;
	let y = modPow(ySquared, (FIELD_ORDER + 1n) / 4n, FIELD_ORDER);
	if ((y * y) % FIELD_ORDER !== ySquared) return false;
	if (y % 2n !== 0n) y = FIELD_ORDER - y;
	const rX = BigInt(`0x${sigHex.slice(0, 64)}`);
	const scalar = BigInt(`0x${sigHex.slice(64)}`);
	if (rX >= FIELD_ORDER || scalar >= CURVE_ORDER) return false;
	const challenge =
		BigInt(
			`0x${taggedHash(
				"BIP0340/challenge",
				Buffer.concat([intTo32Bytes(rX), intTo32Bytes(pubX), message]),
			).toString("hex")}`,
		) % CURVE_ORDER;
	const rPoint = pointAdd(
		pointMultiply(scalar),
		pointMultiply((CURVE_ORDER - challenge) % CURVE_ORDER, [pubX, y] as const),
	);
	return rPoint !== null && rPoint[1] % 2n === 0n && rPoint[0] === rX;
}

export type AuthEventVerdict = { ok: true } | { ok: false; reason: string };

/**
 * Full NIP-42 acceptance check for a client auth event: structural shape,
 * relay/challenge tags bound to THIS connection, NIP-01 id recomputation over
 * the compact serialization, and BIP-340 signature verification.
 */
export function verifyAuthEvent(
	candidate: unknown,
	expectedChallenge: string,
	expectedRelayUrl: string,
): AuthEventVerdict {
	if (
		candidate === null ||
		typeof candidate !== "object" ||
		Array.isArray(candidate)
	) {
		return { ok: false, reason: "auth event is not an object" };
	}
	const event = candidate as Record<string, unknown>;
	if (event["kind"] !== 22242) {
		return { ok: false, reason: "auth event kind must be 22242" };
	}
	if (event["content"] !== "") {
		return { ok: false, reason: "auth event content must be empty" };
	}
	const pubkey = event["pubkey"];
	const eventId = event["id"];
	const sig = event["sig"];
	if (typeof pubkey !== "string" || !/^[0-9a-f]{64}$/.test(pubkey)) {
		return { ok: false, reason: "invalid pubkey encoding" };
	}
	if (typeof eventId !== "string" || !/^[0-9a-f]{64}$/.test(eventId)) {
		return { ok: false, reason: "invalid id encoding" };
	}
	if (typeof sig !== "string" || !/^[0-9a-f]{128}$/.test(sig)) {
		return { ok: false, reason: "invalid sig encoding" };
	}
	const createdAt = event["created_at"];
	if (
		typeof createdAt !== "number" ||
		!Number.isInteger(createdAt) ||
		createdAt < 0
	) {
		return { ok: false, reason: "invalid created_at" };
	}
	const tags = event["tags"];
	if (!Array.isArray(tags)) {
		return { ok: false, reason: "tags must be an array" };
	}
	let hasRelay = false;
	let hasChallenge = false;
	for (const tag of tags) {
		if (
			Array.isArray(tag) &&
			tag.length === 2 &&
			typeof tag[0] === "string" &&
			typeof tag[1] === "string"
		) {
			if (tag[0] === "relay" && tag[1] === expectedRelayUrl) hasRelay = true;
			if (tag[0] === "challenge" && tag[1] === expectedChallenge) {
				hasChallenge = true;
			}
		}
	}
	if (!hasRelay) return { ok: false, reason: "missing relay tag" };
	if (!hasChallenge) return { ok: false, reason: "missing challenge tag" };
	const serialized = JSON.stringify([0, pubkey, createdAt, 22242, tags, ""]);
	const recomputed = createHash("sha256")
		.update(serialized, "utf8")
		.digest("hex");
	if (recomputed !== eventId) {
		return {
			ok: false,
			reason: "id does not match the event serialization",
		};
	}
	if (!verifyBip340Signature(Buffer.from(eventId, "hex"), pubkey, sig)) {
		return { ok: false, reason: "signature verification failed" };
	}
	return { ok: true };
}

// ── the in-memory NIP-42 relay (conformance fixture; FakeBuzzCli precedent) ──

export type FakeRelayAuthMode =
	/** Proper NIP-42: challenge → verify client event → OK true/false. */
	| "nip42"
	/** Challenge → OK false "bad signature" (AUTH rejected ladder). */
	| "reject"
	/** NOTICE instead of a challenge (missing-challenge ladder). */
	| "notice"
	/** Silence — exercises the auth recv deadline / ready window. */
	| "silent";

export interface RecordedRelayAuth {
	/** Challenge this connection issued. */
	challenge: string;
	/** The client's event object (as received). */
	event: Record<string, unknown>;
	accepted: boolean;
	reason: string;
}

export interface RecordedRelayReq {
	subscriptionId: string;
	filter: Record<string, unknown>;
}

/**
 * Scriptable community relay: issues challenges, VERIFIES auth events,
 * records REQ filters, and lets rows push EVENT/CLOSED/NOTICE frames or
 * refuse connections to drive the reconnect ladder deterministically.
 */
export class FakeBuzzRelay {
	readonly url: string;
	authMode: FakeRelayAuthMode = "nip42";
	connectAttempts = 0;
	readonly connections: FakeRelayConnection[] = [];
	readonly authEvents: RecordedRelayAuth[] = [];
	readonly reqFilters: RecordedRelayReq[] = [];
	private refusedRemaining = 0;
	private challengeCounter = 0;

	constructor(opts: { url: string }) {
		this.url = opts.url;
	}

	/** Queue N refused dials (factory throws) for the backoff ladder. */
	refuseNextConnections(count: number): void {
		this.refusedRemaining += count;
	}

	factory(): BuzzRelaySocketFactory {
		return async (url: string): Promise<BuzzRelaySocket> => {
			this.connectAttempts += 1;
			if (this.refusedRemaining > 0) {
				this.refusedRemaining -= 1;
				throw new Error(`fake relay: connection refused (${url})`);
			}
			if (url !== this.url) {
				throw new Error(
					`fake relay: unexpected URL ${url} (expected ${this.url})`,
				);
			}
			const connection = new FakeRelayConnection(this);
			this.connections.push(connection);
			switch (this.authMode) {
				case "nip42":
				case "reject":
					connection.serverPush(["AUTH", this.nextChallenge()]);
					break;
				case "notice":
					connection.serverPush(["NOTICE", "auth required first"]);
					break;
				case "silent":
					break;
			}
			return connection;
		};
	}

	/** Latest connection still open, or null. */
	currentOpen(): FakeRelayConnection | null {
		const last = this.connections.at(-1);
		return last !== undefined && !last.closed ? last : null;
	}

	/** Push a frame to the latest open connection (false when none). */
	push(frame: unknown): boolean {
		const connection = this.currentOpen();
		if (connection === null) return false;
		connection.serverPush(frame);
		return true;
	}

	/** Clean-close the latest connection (async-for ends normally). */
	closeCurrent(): void {
		const connection = this.currentOpen();
		if (connection !== null) connection.close();
	}

	private nextChallenge(): string {
		this.challengeCounter += 1;
		return `challenge-${this.challengeCounter}`;
	}

	/** Server-side intake for one client frame (AUTH verify + REQ record). */
	handleClientFrame(connection: FakeRelayConnection, raw: string): void {
		let message: unknown;
		try {
			message = JSON.parse(raw);
		} catch {
			return; // drop unparseable client noise
		}
		if (!Array.isArray(message) || message.length === 0) return;
		if (message[0] === "REQ" && message.length >= 3) {
			const filter = message[2];
			if (
				filter !== null &&
				typeof filter === "object" &&
				!Array.isArray(filter)
			) {
				this.reqFilters.push({
					subscriptionId: String(message[1]),
					filter: filter as Record<string, unknown>,
				});
			}
			return;
		}
		if (message[0] === "AUTH" && message.length >= 2) {
			const challenge = connection.issuedChallenge;
			const candidate = message[1];
			const verdict = verifyAuthEvent(candidate, challenge, this.url);
			const eventObject =
				candidate !== null &&
				typeof candidate === "object" &&
				!Array.isArray(candidate)
					? (candidate as Record<string, unknown>)
					: {};
			this.authEvents.push({
				challenge,
				event: eventObject,
				accepted: verdict.ok,
				reason: verdict.ok ? "" : verdict.reason,
			});
			const eventId = eventObject["id"];
			connection.serverPush([
				"OK",
				typeof eventId === "string" ? eventId : "",
				verdict.ok,
				verdict.ok ? "" : verdict.reason,
			]);
		}
	}
}

/** One live connection: framed queues + parked recvs + full audit trail. */
export class FakeRelayConnection implements BuzzRelaySocket {
	readonly url: string;
	closed = false;
	/** The challenge issued on THIS connection ("" when mode never challenged). */
	issuedChallenge = "";
	/** Client→server frames, JSON-parsed, in order. */
	readonly clientFrames: unknown[] = [];
	/** Server→client frames, JSON-parsed, in order. */
	readonly serverFrames: unknown[] = [];

	private readonly relay: FakeBuzzRelay;
	private queue: string[] = [];
	private readonly parkedRecv: Array<(frame: string | null) => void> = [];

	constructor(relay: FakeBuzzRelay) {
		this.relay = relay;
		this.url = relay.url;
	}

	send = (frame: string): void => {
		if (this.closed) throw new Error("fake relay: send on closed socket");
		this.clientFrames.push(JSON.parse(frame));
		this.relay.handleClientFrame(this, frame);
	};

	recv = (): Promise<string | null> => {
		const next = this.queue.shift();
		if (next !== undefined) return Promise.resolve(next);
		return new Promise<string | null>((resolve) => {
			this.parkedRecv.push(resolve);
		});
	};

	close = (): void => {
		if (this.closed) return;
		this.closed = true;
		for (const resolve of this.parkedRecv.splice(0)) resolve(null);
	};

	/** Server→client push (frames queue when the client is not parked). */
	serverPush(frame: unknown): void {
		if (this.closed) return;
		this.serverFrames.push(frame);
		if (Array.isArray(frame) && frame[0] === "AUTH") {
			// Track the issued challenge for auth-event binding.
			this.issuedChallenge = String(frame[1]);
		}
		const waiter = this.parkedRecv.shift();
		if (waiter !== undefined) waiter(JSON.stringify(frame));
		else this.queue.push(JSON.stringify(frame));
	}
}

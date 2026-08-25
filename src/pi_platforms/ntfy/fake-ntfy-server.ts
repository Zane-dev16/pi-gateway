// pi_platforms/ntfy/fake-ntfy-server — the IN-PROCESS fake ntfy server
// (04 §8: headless, NO external network). Vendor wire shapes only:
//
//   - subscribe(topic): returns a stream handle; the adapter awaits
//     nextEvent() which yields {event:"message"| "keepalive", ...} records.
//     Keepalives arrive on the 55s vendor cadence (clock-driven).
//   - publish(topic, body, headers): captures POSTs; scripted statuses
//     (200 with JSON id · 401 · 404 · 500); request log exposes headers so
//     X-Tags/X-Markdown/Authorization contracts assert as DATA.
//   - scenario knobs: dropStream() (EOF), wedgeSilent() (no keepalives —
//     read-timeout path), authReject mode.

import { NTFY_ECHO_TAG } from "./manifest.js";

export type NtfyEvent =
	| {
			event: "message";
			id: string;
			topic: string;
			message: string;
			title?: string | undefined;
			tags?: string[] | undefined;
			time?: number | undefined;
	  }
	| { event: "keepalive" };

export interface PublishRequest {
	topic: string;
	body: string;
	headers: Record<string, string>;
}

export interface PublishResponse {
	status: number;
	jsonId?: string | undefined;
	body?: string | undefined;
}

let eventSeq = 0;

export class FakeNtfyStream {
	readonly topic: string;
	private readonly pending: NtfyEvent[] = [];
	private readonly waiters: Array<(e: NtfyEvent) => void> = [];
	closed = false;
	wedged = false;
	/** Keepalive cadence armed by the world's clock driver. */
	keepaliveEveryMs = 55_000;
	lastActivityAtMs = 0;

	constructor(topic: string, nowMs: () => number = () => Date.now()) {
		this.topic = topic;
		this.lastActivityAtMs = nowMs();
	}

	/** Server pushes one event to this subscriber. */
	push(event: NtfyEvent): void {
		if (this.closed || this.wedged) return;
		const waiter = this.waiters.shift();
		if (waiter !== undefined) waiter(event);
		else this.pending.push(event);
	}

	/** Scenario knob: silence the stream entirely (read-timeout path). */
	wedgeSilent(): void {
		this.wedged = true;
	}

	pushKeepalive(): void {
		this.push({ event: "keepalive" });
	}

	pushMessage(
		message: string,
		opts: { title?: string; tags?: string[]; id?: string } = {},
	): string {
		const id = opts.id ?? `msg-${++eventSeq}`;
		this.push({
			event: "message",
			id,
			topic: this.topic,
			message,
			...(opts.title !== undefined ? { title: opts.title } : {}),
			...(opts.tags !== undefined ? { tags: opts.tags } : {}),
		});
		return id;
	}

	/**
	 * Adapter-side receive. When wedged the server never delivers and never
	 * keepalives — the adapter's 90s read-timeout is the ONLY detector.
	 */
	nextEvent(): Promise<NtfyEvent> {
		if (this.wedged) {
			return new Promise(() => {}); // parked forever until timeout fires
		}
		const queued = this.pending.shift();
		if (queued !== undefined) return Promise.resolve(queued);
		return new Promise<NtfyEvent>((resolve) => {
			this.waiters.push(resolve);
		});
	}

	close(): void {
		this.closed = true;
		for (const w of this.waiters.splice(0)) {
			w({ event: "keepalive" }); // wake; adapter sees closed flag
		}
	}
}

export class FakeNtfyServer {
	readonly baseUrl = "https://ntfy.fake.example";

	readonly published: PublishRequest[] = [];
	/** Scripted publish responses consumed FIFO; exhausted ⇒ 200 + fresh id. */
	private readonly script: PublishResponse[] = [];

	authRejectMode = false;
	topicNotFound = false;
	streams: FakeNtfyStream[] = [];
	dropped = false;

	nowMs: () => number = () => Date.now();

	reset(): void {
		this.published.length = 0;
		this.script.length = 0;
		this.authRejectMode = false;
		this.topicNotFound = false;
		this.dropped = false;
		for (const s of this.streams) s.close();
		this.streams = [];
	}

	scriptPublish(...responses: PublishResponse[]): void {
		this.script.push(...responses);
	}

	subscribe(topic: string): FakeNtfyStream {
		if (this.authRejectMode) throw new Error("401 Unauthorized");
		if (this.topicNotFound) throw new Error("404 Not Found");
		const stream = new FakeNtfyStream(topic, this.nowMs);
		if (!this.dropped) this.streams.push(stream);
		else stream.close();
		return stream;
	}

	publish(
		topic: string,
		body: string,
		headers: Record<string, string>,
	): PublishResponse {
		this.published.push({ topic, body, headers });
		const scripted = this.script.shift();
		if (scripted !== undefined) return scripted;
		return { status: 200, jsonId: `pub-${Math.floor(Math.random() * 1e9)}` };
	}

	/** Drop EVERY active stream (EOF/RST death across the board). */
	dropStreams(): void {
		this.dropped = true;
		for (const s of [...this.streams]) s.close();
	}

	/** Echo-tag helper mirroring the vendor header contract. */
	static echoTag(): string {
		return NTFY_ECHO_TAG;
	}
}

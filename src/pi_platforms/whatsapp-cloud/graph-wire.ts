// pi_platforms/whatsapp-cloud/graph-wire — the Graph API transport SEAM plus
// the in-process fake server (04 §8: rows run headless against fake platform
// servers; NO external network, NO vendor SDK).
//
// Production binds this seam to HTTPS calls against
// `${GRAPH_API_BASE}/${apiVersion}/...` (whatsapp_cloud.py:_graph_url); tests
// bind FakeGraphServer, which models the four edges the adapter uses with
// scriptable Meta error shapes and full call capture:
//
//   POST /{phone_number_id}/messages   → {messages:[{id: wamid}]}
//     (text sends, interactive sends, media message blocks, status:"read"
//      receipts — whatsapp_cloud.py:send/_post_interactive/_send_media/
//      send_typing all POST this ONE endpoint)
//   POST /{phone_number_id}/media      → {id}          (_upload_media)
//   GET  /{version}/{media_id}         → {url, mime_type}  (download step 1)
//   GET  <signed temp url>             → bytes         (download step 2;
//        "auth required even though URL is signed" parity)

import type { Metadata } from "../../pi_gateway/streaming/adapter-seam.js";
import {
	DEFAULT_MEDIA_MIME,
	MEDIA_SIZE_LIMITS,
	type WaMediaKind,
} from "./manifest.js";

export interface GraphResponse {
	status: number;
	json: Record<string, unknown>;
}

/** Scriptable failure shaped exactly like a Meta Graph error body. */
export interface GraphFailure {
	status: number;
	error: { message: string; type?: string; code?: number; fbtrace_id?: string };
}

export interface WaMediaUploadInput {
	kind: WaMediaKind;
	filename?: string | undefined;
	mime?: string | undefined;
	bytes: Buffer;
	/**
	 * Meta-REQUIRED multipart field (whatsapp_cloud.py:_upload_media: every
	 * /media POST carries `messaging_product='whatsapp'` alongside `file`).
	 */
	messagingProduct: string;
	/**
	 * Meta-REQUIRED multipart field: the mime type of the file part, sent as
	 * the `type` form field alongside `file` and `messaging_product`.
	 */
	type: string;
}

/**
 * THE transport seam. The adapter NEVER imports http/undici — production and
 * tests supply different implementations of these five calls.
 */
export interface WaCloudTransport {
	/**
	 * POST …/{phone_number_id}/messages — text/interactive/media/read bodies.
	 * The optional `metadata` bag is DOOR metadata passed through verbatim;
	 * production transports ignore it, the conformance harness reads its
	 * script markers (e.g. forceFormattingError) exactly like the reference
	 * subjects do.
	 */
	postMessages(
		body: Record<string, unknown>,
		metadata?: Metadata,
	): Promise<GraphResponse>;
	/** POST …/{phone_number_id}/media — multipart upload; resolves {id}. */
	uploadMedia(upload: WaMediaUploadInput): Promise<GraphResponse>;
	/** GET …/{version}/{media_id} — metadata step of the two-step download. */
	getMediaMetadata(mediaId: string): Promise<GraphResponse>;
	/** GET the signed temp URL — bytes step; auth header still required. */
	fetchMediaBytes(url: string): Promise<{ status: number; bytes: Buffer }>;
	/**
	 * Harness seam ONLY: whether a "rich" behavior is scripted on the fake
	 * wire (models an optional rich endpoint for the §10.1 latch row — the
	 * real Cloud API has none, so the default false keeps the ladder latched
	 * off without burning a wire roundtrip).
	 */
	hasRichScript?(): boolean;
	/** Harness seam ONLY: record/consume one rich-probe attempt. */
	transmitRichProbe?(chatId: string, content: string): Promise<GraphResponse>;
}

type Endpoint = "messages" | "upload" | "metadata" | "bytes";

interface RecordedMessage {
	body: Record<string, unknown>;
	seq: number;
}

interface RecordedUpload extends WaMediaUploadInput {
	seq: number;
}

/**
 * In-memory Graph API double. Behaviors are consumed FIFO per endpoint; an
 * exhausted script defaults to the vendor-shaped success body.
 */
export class FakeGraphServer implements WaCloudTransport {
	private scripts = new Map<Endpoint, GraphFailure[]>();
	private seqCounter = 0;

	readonly sentMessages: RecordedMessage[] = [];
	readonly uploads: RecordedUpload[] = [];
	readonly metadataGets: Array<{ mediaId: string; seq: number }> = [];
	readonly bytesGets: Array<{ url: string; seq: number }> = [];
	/** Media ids served by the metadata/bytes steps, keyed by id. */
	private readonly mediaStore = new Map<
		string,
		{ mime: string; bytes: Buffer }
	>();
	private nextMediaNum = 0;

	/** Program the next N failures for an endpoint kind. */
	script(endpoint: Endpoint, ...failures: GraphFailure[]): void {
		const queue = this.scripts.get(endpoint) ?? [];
		queue.push(...failures);
		this.scripts.set(endpoint, queue);
	}

	hasScript(endpoint: Endpoint): boolean {
		return (this.scripts.get(endpoint)?.length ?? 0) > 0;
	}

	reset(): void {
		this.scripts.clear();
		this.sentMessages.length = 0;
		this.uploads.length = 0;
		this.metadataGets.length = 0;
		this.bytesGets.length = 0;
		this.mediaStore.clear();
	}

	/** Pre-seed downloadable inbound media (inbound webhook tests). */
	seedMedia(mime: string, bytes: Buffer): string {
		this.nextMediaNum += 1;
		const id = `wamedia-${this.nextMediaNum}`;
		this.mediaStore.set(id, { mime, bytes });
		return id;
	}

	// ── query helpers ──

	textSendsOf(
		recipient?: string,
	): Array<RecordedMessage & { to: string; textBody: string }> {
		return this.sentMessages
			.filter(
				(m) =>
					typeof m.body["type"] === "undefined" || m.body["type"] === "text",
			)
			.map((m) => ({
				...m,
				to: String(m.body["to"] ?? ""),
				textBody: String(
					(m.body["text"] as Record<string, unknown> | undefined)?.["body"] ??
						"",
				),
			}))
			.filter((m) => recipient === undefined || m.to === recipient);
	}

	readReceipts(): Array<RecordedMessage & { messageId: string }> {
		return this.sentMessages
			.filter((m) => m.body["status"] === "read")
			.map((m) => ({
				...m,
				messageId: String(m.body["message_id"] ?? ""),
			}));
	}

	uploadsOf(kind?: WaMediaKind): RecordedUpload[] {
		return this.uploads.filter((u) => kind === undefined || u.kind === kind);
	}

	private next(endpoint: Endpoint): GraphFailure | undefined {
		const queue = this.scripts.get(endpoint);
		if (queue === undefined || queue.length === 0) return undefined;
		return queue.shift();
	}

	private ok(json: Record<string, unknown>): GraphResponse {
		return { status: 200, json };
	}

	private fail(f: GraphFailure): GraphResponse {
		return { status: f.status, json: f as unknown as Record<string, unknown> };
	}

	// ── WaCloudTransport ──

	async postMessages(body: Record<string, unknown>): Promise<GraphResponse> {
		this.seqCounter += 1;
		this.sentMessages.push({ body, seq: this.seqCounter });
		const failure = this.next("messages");
		if (failure !== undefined) return this.fail(failure);
		const wamid = `wamid.out.${this.seqCounter}`;
		return this.ok({
			messaging_product: "whatsapp",
			contacts: [],
			messages: [{ id: wamid }],
		});
	}

	async uploadMedia(upload: WaMediaUploadInput): Promise<GraphResponse> {
		this.seqCounter += 1;
		this.uploads.push({ ...upload, seq: this.seqCounter });
		// Vendor-parity field gate: Meta REJECTS /media multipart uploads that
		// omit messaging_product='whatsapp' or the mime-typed `type` form field
		// (_upload_media always sends both alongside `file`).
		if (upload.messagingProduct !== "whatsapp" || !upload.type) {
			return this.fail({
				status: 400,
				error: {
					message:
						"(#100) Param messaging_product/type is required for media upload",
					type: "OAuthException",
					code: 100,
				},
			});
		}
		const failure = this.next("upload");
		if (failure !== undefined) return this.fail(failure);
		// Vendor-parity server-side cap: oversized uploads are rejected BY META.
		// (The ADAPTER must refuse first — that contract lives in the adapter.)
		const cap = MEDIA_SIZE_LIMITS[upload.kind];
		if (upload.bytes.length > cap) {
			return this.fail({
				status: 400,
				error: {
					message: `(#133010) File size exceeds limit for ${upload.kind}`,
					type: "OAuthException",
					code: 133010,
				},
			});
		}
		this.nextMediaNum += 1;
		const id = `uploaded-${this.nextMediaNum}`;
		this.mediaStore.set(id, {
			mime: upload.mime ?? DEFAULT_MEDIA_MIME[upload.kind],
			bytes: upload.bytes,
		});
		return this.ok({ id });
	}

	async getMediaMetadata(mediaId: string): Promise<GraphResponse> {
		this.seqCounter += 1;
		this.metadataGets.push({ mediaId, seq: this.seqCounter });
		const failure = this.next("metadata");
		if (failure !== undefined) return this.fail(failure);
		const entry = this.mediaStore.get(mediaId);
		if (entry === undefined) {
			return this.fail({
				status: 404,
				error: { message: "Unsupported get request", code: 100 },
			});
		}
		return this.ok({
			url: `fakegraph://media/${mediaId}/bytes`,
			mime_type: entry.mime,
			sha256: "fake",
			file_size: entry.bytes.length,
			id: mediaId,
		});
	}

	async fetchMediaBytes(
		url: string,
	): Promise<{ status: number; bytes: Buffer }> {
		this.seqCounter += 1;
		this.bytesGets.push({ url, seq: this.seqCounter });
		const failure = this.next("bytes");
		if (failure !== undefined)
			return { status: failure.status, bytes: Buffer.alloc(0) };
		const match = /^fakegraph:\/\/media\/([A-Za-z0-9._-]+)\/bytes$/.exec(url);
		const entry = match ? this.mediaStore.get(match[1] as string) : undefined;
		if (!match || entry === undefined)
			return { status: 404, bytes: Buffer.alloc(0) };
		return { status: 200, bytes: entry.bytes };
	}
}

// pi_platforms/whatsapp-cloud/wa-cloud-fixture — the REAL-engine fixture for
// the WhatsApp-Cloud shape-delta rows (makeWsRows/makeWebhookRows pattern):
// the actual WaCloudAdapter over FakeGraphServer with an injected clock,
// mkdtemp-isolated LID-mapping + media dirs, and Meta-envelope builders signed
// with the fixture app secret. NO stubbed return values — rows drive the real
// ingress pipeline, window classifier, media plane, and receipts.

import { createHmac } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { WaCloudAdapter } from "./wa-cloud-adapter.js";
import { FakeGraphServer } from "./graph-wire.js";
import type { WaMediaKind } from "./manifest.js";

export const FIXTURE_APP_SECRET = "wa-fixture-app-secret";
export const FIXTURE_VERIFY_TOKEN = "wa-fixture-verify-token";
const FIXTURE_DISPLAY_PHONE = "15550001111";

export interface WaCloudFixtureOptions {
	outsideWindowPolicy?: "record" | "refuse" | undefined;
	dedupCap?: number | undefined;
}

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it — the 24h boundary tests mutate THIS.
 */
export class FixtureClock {
	constructor(private nowValue: number = 1_700_000_000_000) {}
	get nowMs(): number {
		return this.nowValue;
	}
	advance(ms: number): void {
		this.nowValue += ms;
	}
}

export class WaCloudFixture {
	readonly graph = new FakeGraphServer();
	readonly adapter: WaCloudAdapter;
	readonly clock = new FixtureClock();
	readonly sessionDir: string;
	readonly mediaDir: string;

	private readonly tempRoot: string;

	constructor(opts: WaCloudFixtureOptions = {}) {
		this.tempRoot = mkdtempSync(join(tmpdir(), "wa-cloud-fix-"));
		this.sessionDir = join(this.tempRoot, "whatsapp-session");
		this.mediaDir = join(this.tempRoot, "media");
		mkdirSync(this.sessionDir, { recursive: true });
		mkdirSync(this.mediaDir, { recursive: true });
		this.adapter = new WaCloudAdapter({
			transport: this.graph,
			nowMs: () => this.clock.nowMs,
			whatsappSessionDir: this.sessionDir,
			mediaCacheDir: this.mediaDir,
			...(opts.dedupCap !== undefined ? { dedupCap: opts.dedupCap } : {}),
			...(opts.outsideWindowPolicy !== undefined
				? { outsideWindowPolicy: opts.outsideWindowPolicy }
				: {}),
			secretReader: (name) =>
				name === "WHATSAPP_CLOUD_PHONE_NUMBER_ID"
					? "wa-phone-id"
					: name === "WHATSAPP_CLOUD_ACCESS_TOKEN"
						? "wa-access-token"
						: name === "WHATSAPP_CLOUD_APP_SECRET"
							? FIXTURE_APP_SECRET
							: name === "WHATSAPP_CLOUD_VERIFY_TOKEN"
								? FIXTURE_VERIFY_TOKEN
								: undefined,
		});
		this.adapter.attachStandardGuard();
	}

	advance(ms: number): void {
		this.clock.advance(ms);
	}

	dispose(): void {
		rmSync(this.tempRoot, { recursive: true, force: true });
	}

	// ── signature + transport-level POST ──

	sign(body: string | Buffer): string {
		return `sha256=${createHmac("sha256", FIXTURE_APP_SECRET)
			.update(body)
			.digest("hex")}`;
	}

	async postRaw(
		headers: Record<string, string>,
		body: string | Buffer,
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const raw = Buffer.isBuffer(body) ? body : Buffer.from(body, "utf8");
		const normalized: Record<string, string> = {};
		for (const [k, v] of Object.entries(headers))
			normalized[k.toLowerCase()] = v;
		return this.adapter.handleWebhookPost(normalized, raw);
	}

	async postSigned(envelope: Record<string, unknown>): Promise<{
		status: number;
		json: Record<string, unknown>;
	}> {
		const body = JSON.stringify(envelope);
		return this.postRaw(
			{
				"x-hub-signature-256": this.sign(body),
				"content-type": "application/json",
			},
			body,
		);
	}

	verify(query: Record<string, string>) {
		return this.adapter.handleVerifyRequest(query);
	}

	// ── envelope builders (Meta webhook shapes) ──

	valueEnvelope(valueExtras: {
		messages?: Array<Record<string, unknown>>;
		statuses?: Array<Record<string, unknown>>;
		contacts?: Array<Record<string, unknown>>;
	}): Record<string, unknown> {
		return {
			object: "whatsapp_business_account",
			entry: [
				{
					id: "waba-1",
					changes: [
						{
							field: "messages",
							value: {
								messaging_product: "whatsapp",
								metadata: {
									display_phone_number: FIXTURE_DISPLAY_PHONE,
									phone_number_id: "wa-phone-id",
								},
								contacts:
									valueExtras.contacts ??
									valueExtras.messages?.map((m) => ({
										wa_id: m["from"],
										profile: { name: `User ${String(m["from"]).slice(-4)}` },
									})) ??
									[],
								...(valueExtras.messages !== undefined
									? { messages: valueExtras.messages }
									: {}),
								...(valueExtras.statuses !== undefined
									? { statuses: valueExtras.statuses }
									: {}),
							},
						},
					],
				},
			],
		};
	}

	textMessage(
		wamid: string,
		from: string,
		body: string,
	): Record<string, unknown> {
		return {
			id: wamid,
			from,
			timestamp: "1700000000",
			type: "text",
			text: { body },
		};
	}

	interactiveReply(
		wamid: string,
		from: string,
		buttonId: string,
		title: string,
		mode: "button_reply" | "list_reply" = "button_reply",
	): Record<string, unknown> {
		return {
			id: wamid,
			from,
			timestamp: "1700000000",
			type: "interactive",
			interactive: { [mode]: { id: buttonId, title } },
		};
	}

	mediaMessage(
		wamid: string,
		from: string,
		/** WIRE message type — voice notes arrive as "voice" (caps key: "audio"). */
		wireType: WaMediaKind | "voice",
		mediaId: string,
		mime: string,
		caption?: string,
	): Record<string, unknown> {
		return {
			id: wamid,
			from,
			timestamp: "1700000000",
			type: wireType,
			[wireType]: {
				id: mediaId,
				mime_type: mime,
				sha256: "fake",
				...(caption ? { caption } : {}),
			},
		};
	}

	statusUpdate(wamid: string, status: string): Record<string, unknown> {
		return {
			id: wamid,
			status,
			timestamp: "1700000000",
			recipient_id: "15551234567",
		};
	}

	// ── LID mapping files (02 §4.3 bridge artifacts) ──

	/**
	 * Bridge-mapping artifacts linking idA ↔ idB. The REAL bridge writes pairs
	 * as it learns them; BOTH directions are needed because the §4.3 walk reads
	 * `lid-mapping-{current}{,_reverse}.json` per dequeued identifier.
	 */
	writeLidMapping(idA: string, idB: string): void {
		for (const [from, to] of [
			[idA, idB],
			[idB, idA],
		] as const) {
			writeFileSync(
				join(this.sessionDir, `lid-mapping-${from}.json`),
				JSON.stringify(to),
			);
			writeFileSync(
				join(this.sessionDir, `lid-mapping-${from}_reverse.json`),
				JSON.stringify(to),
			);
		}
	}
}

export function makeWaCloudFixture(
	opts?: WaCloudFixtureOptions,
): WaCloudFixture {
	return new WaCloudFixture(opts);
}

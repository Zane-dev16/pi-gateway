// Post-stream rescan contracts (03 §9.3; §11 "Post-stream rescan" row).
// Core negative contract: the rescan finds NOTHING unless EXPLICITLY tagged —
// tricky near-miss text (bare paths, fence examples, stored JSON) must never
// upload. Plus: no cross-turn dedupe, [[as_document]] byte-preservation
// routing, image batching, and per-file sibling isolation.

import { describe, expect, it } from "vitest";
import {
	type PostStreamAdapter,
	rescanPostStream,
	shouldSendMediaAsAudio,
} from "./post-stream-rescan.js";

/** Recording adapter: every dispatch lands here; methods can be made to throw. */
function makeAdapter(failOn?: (kind: string, path: string) => boolean): {
	adapter: PostStreamAdapter;
	calls: Array<{ kind: string; arg: string | string[] }>;
} {
	const calls: Array<{ kind: string; arg: string | string[] }> = [];
	const record = async <T>(kind: string, arg: T): Promise<unknown> => {
		calls.push({ kind, arg: arg as string | string[] });
		return {};
	};
	return {
		calls,
		adapter: {
			name: "fake",
			sendMultipleImages: (_chatId, images) => {
				if (failOn?.("image_batch", images.join("|")))
					return Promise.reject(new Error("batch boom"));
				return record("image_batch", images);
			},
			sendVoice: (_chatId, p) => {
				if (failOn?.("voice_or_audio", p))
					return Promise.reject(new Error("voice boom"));
				return record("voice_or_audio", p);
			},
			sendVideo: (_chatId, p) => {
				if (failOn?.("video", p))
					return Promise.reject(new Error("video boom"));
				return record("video", p);
			},
			sendDocument: (_chatId, p) => {
				if (failOn?.("document", p))
					return Promise.reject(new Error("doc boom"));
				return record("document", p);
			},
		},
	};
}

/** Realistic path gate mirroring an fs where these files exist: known-ext paths validate, denylist/unknown do not. */
const realisticValidate = (p: string): string | null =>
	/^\/(etc|proc)(\/|$)|^~\/\.ssh\//.test(p)
		? null
		: /\.(png|jpg|jpeg|gif|webp|bmp|tiff|svg|mp4|mov|avi|mkv|webm|3gp|mp3|m2a|wav|ogg|opus|m4a|flac|pdf|docx|doc|odt|rtf|txt|md|epub|xlsx|xls|ods|csv|tsv|json|xml|yaml|yml|kmz|kml|geojson|gpx|pptx|ppt|odp|key|zip|tar|gz|tgz|bz2|xz|7z|rar|apk|ipa|html|htm)$/i.test(
					p,
				)
			? p
			: null;

const OPTS = (adapter: PostStreamAdapter, platform = "telegram") => ({
	adapter,
	chatId: "chat-1",
	chatPlatform: platform,
	validatePath: realisticValidate,
});

describe("EXPLICIT-ONLY: bare paths NEVER upload post-stream (#20834)", () => {
	it("a streamed reply full of deliverable-looking BARE paths uploads nothing", async () => {
		const { adapter } = makeAdapter();
		const r = await rescanPostStream(
			"Report written to /tmp/summary.pdf and chart at ~/out.png, see /var/data/dump.csv.",
			OPTS(adapter),
		);
		expect(r.attempts).toEqual([]);
	});

	it("near-miss text: MEDIA-like prose without the grammar does not upload", async () => {
		for (const text of [
			"check file:///tmp/photo.jpg in the browser",
			"saved screenshot to ./local/snap.gif",
			"see docs about MEDIA tags generally, no path follows: MEDIA:",
		]) {
			const { adapter } = makeAdapter();
			const r = await rescanPostStream(text, OPTS(adapter));
			expect(r.attempts, text).toEqual([]);
		}
	});

	it("an explicit tag with an UNSAFE path is filtered before dispatch (nothing uploaded)", async () => {
		const { adapter } = makeAdapter();
		const r = await rescanPostStream("MEDIA:/etc/shadow", OPTS(adapter));
		expect(r.attempts).toEqual([]);
	});
});

describe("explicit tags DO deliver post-stream — with deliberate NO cross-turn dedupe (#73771)", () => {
	it("an explicit tag in the final reply uploads even if the SAME path was sent in a previous turn", async () => {
		const { adapter } = makeAdapter();
		// Turn N: user-requested resend of an already-delivered file.
		const turnN = await rescanPostStream(
			"Here it is again: MEDIA:/tmp/resend.png",
			OPTS(adapter),
		);
		expect(turnN.attempts).toEqual([
			{ kind: "image_batch", paths: ["/tmp/resend.png"], status: "sent" },
		]);
		// A prior turn's delivery does NOT suppress this one — no history dedupe.
		const turnNPlus1 = await rescanPostStream(
			"Once more: MEDIA:/tmp/resend.png",
			OPTS(adapter),
		);
		expect(turnNPlus1.attempts).toHaveLength(1);
		expect(turnNPlus1.attempts[0]?.status).toBe("sent");
	});
});

describe("[[as_document]] forces byte-preserving document routing (§9.3)", () => {
	it("image-extension files skip the photo batch and go out as documents", async () => {
		const { adapter, calls } = makeAdapter();
		const r = await rescanPostStream(
			"[[as_document]] lossless: MEDIA:/tmp/infograph.jpg MEDIA:/tmp/second.png",
			OPTS(adapter),
		);
		expect(r.forceDocumentAttachments).toBe(true);
		expect(calls.map((c) => c.kind)).toEqual(["document", "document"]);
		expect(
			calls.every(
				(c) => c.arg === "/tmp/infograph.jpg" || c.arg === "/tmp/second.png",
			),
		).toBe(true);
	});

	it("without the directive the same files batch into ONE multi-image send", async () => {
		const { adapter, calls } = makeAdapter();
		await rescanPostStream("MEDIA:/tmp/a.jpg MEDIA:/tmp/b.png", OPTS(adapter));
		expect(calls).toEqual([
			{ kind: "image_batch", arg: ["/tmp/a.jpg", "/tmp/b.png"] },
		]);
	});

	it("is_voice routes audio out of the batch; batch fires FIRST (run.py order), then non-images", async () => {
		const { adapter, calls } = makeAdapter();
		await rescanPostStream(
			"[[audio_as_voice]]\nMEDIA:/tmp/v.ogg MEDIA:/tmp/pic.png",
			OPTS(adapter, "whatsapp"),
		);
		expect(calls.map((c) => c.kind)).toEqual(["image_batch", "voice_or_audio"]);
	});
});

describe("dispatch partition + platform audio rules", () => {
	it("telegram: ogg/opus need is_voice for the audio sender; mp3/m4a always; wav falls to document", async () => {
		expect(shouldSendMediaAsAudio("telegram", ".ogg", true)).toBe(true);
		expect(shouldSendMediaAsAudio("telegram", ".ogg", false)).toBe(false);
		expect(shouldSendMediaAsAudio("telegram", ".opus", true)).toBe(true);
		expect(shouldSendMediaAsAudio("telegram", ".mp3", false)).toBe(true);
		expect(shouldSendMediaAsAudio("telegram", ".m4a", false)).toBe(true);
		expect(shouldSendMediaAsAudio("telegram", ".wav", false)).toBe(false); // → document
	});

	it("other platforms: every recognized audio ext routes through the audio sender", () => {
		for (const ext of [".mp3", ".wav", ".ogg", ".flac"]) {
			expect(shouldSendMediaAsAudio("discord", ext, false)).toBe(true);
		}
	});

	it("non-audio non-video extensions route to document delivery", async () => {
		const { adapter, calls } = makeAdapter();
		await rescanPostStream(
			"MEDIA:/tmp/report.pdf MEDIA:/tmp/clip.mkv MEDIA:/tmp/notes.txt",
			OPTS(adapter),
		);
		expect(calls.map((c) => [c.kind, c.arg])).toEqual([
			["document", "/tmp/report.pdf"],
			["video", "/tmp/clip.mkv"],
			["document", "/tmp/notes.txt"],
		]);
	});
});

describe("sibling isolation — one failed upload never cancels the rest (§9.3)", () => {
	it("voice failure leaves video + document siblings delivered", async () => {
		const { adapter, calls } = makeAdapter((kind) => kind === "voice_or_audio");
		const r = await rescanPostStream(
			"MEDIA:/tmp/v.ogg MEDIA:/tmp/clip.mov MEDIA:/tmp/doc.pdf",
			OPTS(adapter, "whatsapp"),
		);
		expect(r.attempts.find((a) => a.paths[0] === "/tmp/v.ogg")?.status).toBe(
			"failed",
		);
		expect(calls.filter((c) => c.kind === "video").map((c) => c.arg)).toEqual([
			"/tmp/clip.mov",
		]);
		expect(
			calls.filter((c) => c.kind === "document").map((c) => c.arg),
		).toEqual(["/tmp/doc.pdf"]);
	});

	it("a failed IMAGE BATCH does not cancel independent non-image siblings either", async () => {
		const { adapter, calls } = makeAdapter((kind) => kind === "image_batch");
		const r = await rescanPostStream(
			"MEDIA:/tmp/i1.png MEDIA:/tmp/i2.jpg MEDIA:/tmp/doc.pdf",
			OPTS(adapter),
		);
		expect(r.attempts.find((a) => a.kind === "image_batch")?.status).toBe(
			"failed",
		);
		expect(calls.filter((c) => c.kind === "document")).toHaveLength(1);
	});

	it("cleaned text strips directives/tags but keeps protected example text verbatim", async () => {
		const { adapter } = makeAdapter();
		const r = await rescanPostStream(
			"text MEDIA:/tmp/x.pdf\n```\nMEDIA:/example/not-real.png\n```\n[[audio_as_voice]] tail",
			OPTS(adapter),
		);
		expect(r.cleaned).not.toContain("[[audio_as_voice]]");
		expect(r.cleaned).toContain("MEDIA:/example/not-real.png");
	});
});

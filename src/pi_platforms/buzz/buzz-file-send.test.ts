// pi_platforms/buzz/buzz-file-send.test.ts — the LOCAL-FILE image lane
// behavior contract (adjudication cn-8, Hermes anchor
// plugins/platforms/buzz/adapter.py:send_image):
//
//   - Local files go out as [messages, send, --channel, C, --file, PATH,
//     --content, -, (--reply-to R)] with the caption piped on STDIN.
//   - URLs (and MISSING local files, which fail the is_file probe) ride the
//     link-text fallback: caption ABOVE the URL on its own line, no --file.
//   - The sent event id lands in the channel seen-set (echo suppression).

import { describe, expect, it } from "vitest";
import { BuzzAdapter } from "./buzz-adapter.js";
import { FakeBuzzCli } from "./cli-wire.js";
import { FIXED_PUBKEY_HEX } from "./vectors.js";
import { FIXTURE_BUZZ_NSEC, FIXTURE_BUZZ_RELAY } from "./buzz-subject.js";

const CHANNEL = "ch-media-b07";

function makeRig(opts: {
	existingLocalFiles?: readonly string[] | undefined;
}): { cli: FakeBuzzCli; engine: BuzzAdapter } {
	const cli = new FakeBuzzCli({
		relayUrl: FIXTURE_BUZZ_RELAY,
		selfPubkey: FIXED_PUBKEY_HEX,
		selfDisplayName: "PiBot",
	});
	cli.addChannel(CHANNEL, "General", "media room");
	const existing = new Set(opts.existingLocalFiles ?? []);
	const engine = new BuzzAdapter({
		config: { cli_path: "/usr/local/bin/buzz", channels: [CHANNEL] },
		pathProbes: { fileExists: () => true },
		imageFileProbe: (path) => existing.has(path),
		secretReader: (key) =>
			key === "BUZZ_PRIVATE_KEY"
				? FIXTURE_BUZZ_NSEC
				: key === "BUZZ_RELAY_URL"
					? FIXTURE_BUZZ_RELAY
					: undefined,
		executor: cli.executor(),
		nowMs: () => cli.nowSeconds * 1000,
	});
	return { cli, engine };
}

async function connected(
	rig: ReturnType<typeof makeRig>,
): Promise<BuzzAdapter> {
	const ok = await rig.engine.connect({ isReconnect: false });
	if (!ok) throw new Error("fixture rig failed to connect");
	return rig.engine;
}

describe("buzz send_image --file lane (local media delivery)", () => {
	it("buildSendArgs emits --file before --content and keeps reply-to last", () => {
		const { engine } = makeRig({});
		expect(engine.buildSendArgs(CHANNEL)).toEqual([
			"messages",
			"send",
			"--channel",
			CHANNEL,
			"--content",
			"-",
		]);
		expect(engine.buildSendArgs(CHANNEL, "evt-9")).toEqual([
			"messages",
			"send",
			"--channel",
			CHANNEL,
			"--content",
			"-",
			"--reply-to",
			"evt-9",
		]);
		// Vendor argv order: --file sits between --channel and --content.
		expect(engine.buildSendArgs(CHANNEL, "evt-9", "/tmp/a.png")).toEqual([
			"messages",
			"send",
			"--channel",
			CHANNEL,
			"--file",
			"/tmp/a.png",
			"--content",
			"-",
			"--reply-to",
			"evt-9",
		]);
	});

	it("local files attach via --file with the caption riding stdin", async () => {
		const rig = makeRig({ existingLocalFiles: ["/tmp/pic.png"] });
		const engine = await connected(rig);

		const result = await engine.sendImage(
			CHANNEL,
			"/tmp/pic.png",
			"look at this",
			"evt-42",
		);

		expect(result.success).toBe(true);
		expect(result.messageId).toMatch(/^evt\d+$/);
		const sends = rig.cli.callsFor("send");
		expect(sends).toHaveLength(1);
		expect(sends[0]!.args).toEqual([
			"messages",
			"send",
			"--channel",
			CHANNEL,
			"--file",
			"/tmp/pic.png",
			"--content",
			"-",
			"--reply-to",
			"evt-42",
		]);
		// Caption piped on stdin — NEVER smuggled into argv.
		expect(sends[0]!.input).toBe("look at this");
		expect(rig.cli.argvContains("look at this")).toBe(false);
		// Own-send echo suppression marks the new event id seen.
		const state = engine.channelState.get(CHANNEL);
		expect(state?.seen.has(String(result.messageId))).toBe(true);
	});

	it("URLs ride the link-text fallback WITHOUT any --file flag", async () => {
		const rig = makeRig({});
		await connected(rig);

		const result = await rig.engine.sendImage(
			CHANNEL,
			"https://pics.example/x.jpg",
			"a photo",
		);

		expect(result.success).toBe(true);
		const sends = rig.cli.callsFor("send");
		expect(sends).toHaveLength(1);
		expect(sends[0]!.args.includes("--file")).toBe(false);
		// Fallback text: caption ABOVE the URL on its own line.
		expect(sends[0]!.input).toBe("a photo\nhttps://pics.example/x.jpg");
	});

	it("MISSING local files fail the existence probe and fall back to link text", async () => {
		const rig = makeRig({}); // /tmp/ghost.png NOT registered as existing
		await connected(rig);

		await rig.engine.sendImage(CHANNEL, "/tmp/ghost.png", "caption only");

		const sends = rig.cli.callsFor("send");
		expect(sends).toHaveLength(1);
		expect(sends[0]!.args.includes("--file")).toBe(false);
		expect(sends[0]!.input).toBe("caption only\n/tmp/ghost.png");
	});

	it("URL-only sends without a caption deliver the bare URL as text", async () => {
		const rig = makeRig({});
		await connected(rig);

		await rig.engine.sendImage(CHANNEL, "https://pics.example/y.gif");

		const sends = rig.cli.callsFor("send");
		expect(sends[0]!.input).toBe("https://pics.example/y.gif");
	});

	it("sendMultipleImages dispatches each file through the --file lane in order", async () => {
		const rig = makeRig({
			existingLocalFiles: ["/tmp/one.png", "/tmp/two.jpg"],
		});
		await connected(rig);

		const results = await rig.engine.sendMultipleImages(CHANNEL, [
			"/tmp/one.png",
			"/tmp/two.jpg",
			"https://pics.example/three.webp",
		]);

		expect(results).toHaveLength(3);
		expect(results.every((r) => r.success)).toBe(true);
		const sends = rig.cli.callsFor("send");
		expect(sends).toHaveLength(3);
		const fileArgs = sends.map((c) => {
			const idx = c.args.indexOf("--file");
			return idx >= 0 ? c.args[idx + 1] : undefined;
		});
		expect(fileArgs).toEqual(["/tmp/one.png", "/tmp/two.jpg", undefined]);
	});
});

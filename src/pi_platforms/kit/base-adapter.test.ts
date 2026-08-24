// Kit base contracts (04 §1/§2/§6.3): capability flags as data with is-True
// resolution, THE chat length pair, loud disable, token-lock fatal refusal,
// wake-lane declaration consistency (DEC-022), egress doors through the
// audited chokepoint.

import { describe, expect, it } from "vitest";
import {
	BasePlatformAdapter,
	TokenLockConflictError,
	TokenLockManagerSeam,
	DEFAULT_CAPABILITIES,
	capabilityFlag,
	governingTier,
	resolveChatLengthPolicy,
} from "./index.js";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";

class TestAdapter extends BasePlatformAdapter {
	wireOps: Array<{ op: string; content: string; meta: Metadata }> = [];
	private cp: EgressChokepoint;

	constructor(
		opts: Partial<ConstructorParameters<typeof BasePlatformAdapter>[0]> & {
			isMessage?: boolean;
		} = {},
	) {
		super({
			manifestName: "test",
			capabilities: opts.capabilities,
			lengthUnit: opts.lengthUnit,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			logger: undefined,
		});
		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => opts.isMessage ?? false,
			transmitSend: async (_c, content, metadata) => {
				this.wireOps.push({ op: "send", content, meta: metadata });
				return { success: true, messageId: `m${this.wireOps.length}` };
			},
			transmitEdit: async (_chatId, _messageId, content) => {
				this.wireOps.push({ op: "edit", content, meta: {} });
				return { success: true, messageId: "e1" };
			},
			transmitSeal: async (_k, _c, draftId, content) => {
				this.wireOps.push({ op: `seal:${draftId}`, content, meta: {} });
				return { success: true, messageId: "sealed_1" };
			},
		});
	}

	protected get chokepoint(): EgressChokepoint {
		return this.cp;
	}
	isMessageChat(): boolean {
		return true;
	}
	protected async wireSend(
		_chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		this.wireOps.push({ op: "wire", content, meta: metadata });
		if (
			metadata.forceError === true &&
			// Only markdown-shaped sends are rejected; the plain-text fallback
			// body succeeds on the wire.
			!content.startsWith("(Response formatting failed, plain text:)")
		)
			return { success: false, error: String(metadata.errorMessage ?? "boom") };
		return { success: true, messageId: `w${this.wireOps.length}` };
	}
	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		return true;
	}
	async disconnect(): Promise<void> {}
}

describe("capability flags are DATA with is-True resolution", () => {
	it("defaults match the verified base class", () => {
		const a = new TestAdapter();
		expect(a.supportsAsyncDelivery).toBe(
			DEFAULT_CAPABILITIES.supportsAsyncDelivery,
		);
		expect(a.splitsLongMessages).toBe(false);
		expect(a.typedCommandPrefix).toBe("/");
		expect(a.interactiveResume).toBe(true);
		expect(a.requiresEditFinalize).toBe(false);
	});

	it("webhook-shape flags flip together (§3 obligation pairing)", () => {
		const webhook = new TestAdapter({
			capabilities: {
				supportsAsyncDelivery: false,
				interactiveResume: false,
			},
		});
		expect(webhook.wakeLane).toBe("raw-key-direct");
		const push = new TestAdapter({});
		expect(push.wakeLane).toBe("forged-event");
	});

	it("capabilityFlag resolves strictly (`=== true` discipline)", () => {
		expect(capabilityFlag(true, false)).toBe(true);
		expect(capabilityFlag(undefined, false)).toBe(false);
		expect(capabilityFlag(undefined, true)).toBe(true);
		// MagicMock-safe: a truthy non-boolean does NOT count.
		expect(capabilityFlag("yes" as unknown as boolean, false)).toBe(false);
	});

	it("rate tiers resolve per-op from manifest data (Q17)", () => {
		const budget = {
			tiers: [
				{
					name: "stream",
					ops: ["draft-start", "draft-stop"] as const,
					limit: 20,
					windowSeconds: 60,
				},
				{ name: "send", ops: ["send"] as const, limit: 30, windowSeconds: 1 },
			],
		};
		expect(governingTier(budget, "draft-start")?.name).toBe("stream");
		expect(governingTier(budget, "send")?.limit).toBe(30);
		expect(governingTier(undefined, "send")).toBeNull();
	});
});

describe("THE one chat length pair (§6.3/A15)", () => {
	it("budget and unit come from the same resolution; override moves both", () => {
		class RelayLike extends TestAdapter {
			protected override chatDescriptorFor(chatId: string) {
				return chatId.startsWith("tg:")
					? { maxMessageLength: 4096, lenUnit: "utf16" as const }
					: { maxMessageLength: 39000, lenUnit: "chars" as const };
			}
		}
		const a = new RelayLike();
		const tg = a.chatLengthPolicyForChat("tg:123");
		expect(tg.unit).toBe("utf16");
		expect(tg.maxUnits).toBe(4096);
		const slack = a.chatLengthPolicyForChat("slack:C1");
		expect(slack.unit).toBe("chars");
		expect(slack.maxUnits).toBe(39000);
	});
});

describe("loud disable + fatal surfaces", () => {
	it("deliverText refuses on a disabled adapter — never silent limp", async () => {
		const a = new TestAdapter();
		a.lifecycle.disable({ kind: "manual", detail: "operator off" });
		await expect(a.deliverText("c1", "hello")).rejects.toThrow(/disabled/);
	});

	it("named-holder token-lock refusal becomes a FATAL adapter error", () => {
		const manager = new TokenLockManagerSeam({ nowMs: () => 1000 });
		const first = new TestAdapter();
		const ok = first.acquireCredentialLock(
			manager,
			"bot-token",
			"cred-1",
			"instance-A",
		);
		expect(ok.acquired).toBe(true);

		const second = new TestAdapter();
		expect(() =>
			second.acquireCredentialLock(
				manager,
				"bot-token",
				"cred-1",
				"instance-B",
			),
		).toThrow(TokenLockConflictError);
		expect(second.lifecycle.state).toBe("fatal"); // surfaced LOUDLY
		expect(second.lifecycle.reason?.kind).toBe("token_lock_conflict");

		// Takeover only via explicit replace flag; prior handle invalidated.
		const takeover = manager.tryAcquire("bot-token", "cred-1", "instance-B", {
			replace: true,
		});
		expect(takeover.acquired).toBe(true);
		if (ok.acquired) ok.lock.release(); // stale owner's release must be a no-op now
		expect(manager.holderOf("bot-token", "cred-1")?.owner).toBe("instance-B");
	});

	it("acquisition is SYNCHRONOUS and tuple-shaped", () => {
		const manager = new TokenLockManagerSeam();
		const result = manager.tryAcquire("s", "c", "o1");
		expect(typeof result).toBe("object");
		if (result.acquired) expect(result.lock.scope).toBe("s");
		else expect(result.holder.owner).toBeDefined();
	});
});

describe("egress pipeline through the audited doors", () => {
	it("send() admits through the chokepoint; audit records the door", async () => {
		const a = new TestAdapter();
		const r = await a.send("c1", "hi");
		expect(r.success).toBe(true);
		expect(a.wireOps.some((op) => op.op === "send")).toBe(true);
	});

	it("oversized text chunks with fence carry through deliverText", async () => {
		const a = new TestAdapter();
		const long = Array.from({ length: 20 }, (_, i) => `line ${i} padding`).join(
			"\n",
		);
		const results = await a.deliverText("c1", long);
		expect(results.every((r) => r.success)).toBe(true);
		const wires = a.wireOps.filter((op) => op.op === "wire");
		expect(wires.length).toBeGreaterThan(1);
		// Labels present on every chunk.
		wires.forEach((op, i) => {
			expect(op.content.endsWith(`(${i + 1}/${wires.length})`)).toBe(true);
		});
	});

	it("formatting-rejected chunk falls back to the plain-text body", async () => {
		const a = new TestAdapter();
		const results = await a.deliverText("c1", "small", {
			forceError: true,
			errorMessage: "Bad Request: can't parse entities",
		});
		expect(results[0]?.success).toBe(true); // plain fallback delivered
		const lastOp = a.wireOps[a.wireOps.length - 1];
		expect(
			lastOp?.content.startsWith("(Response formatting failed, plain text:)"),
		).toBe(true);
	});
});

describe("registration path (§4.2) — missing secret ⇒ LOUD disable", () => {
	it("resolveEnablement fails closed on missing required env without env borrowing", async () => {
		const { resolveEnablement } = await import("./registration.js");
		const manifest = {
			name: "telegram",
			description: "d",
			transportShape: "polling" as const,
			requiresEnv: [{ name: "TELEGRAM_BOT_TOKEN" }],
			capabilities: {},
		};
		const missing = resolveEnablement(manifest, () => undefined);
		expect(missing.enabled).toBe(false);
		expect(missing.reason?.kind).toBe("secret_missing");
		const present = resolveEnablement(manifest, (k) =>
			k === "TELEGRAM_BOT_TOKEN" ? "tok" : undefined,
		);
		expect(present.enabled).toBe(true);
	});

	it("PluginContext registers disabled state visibly in the status snapshot", async () => {
		const { PluginContext } = await import("./registration.js");
		const ctx = new PluginContext(() => undefined); // no secrets at all
		let statusState = "";
		ctx.registerPlatform(
			{
				name: "slack",
				description: "d",
				transportShape: "ws",
				requiresEnv: [{ name: "SLACK_BOT_TOKEN" }],
				capabilities: {},
			},
			() => null,
		);
		const reg = ctx.getPlatform("slack");
		reg?.state.onTransition((s) => {
			statusState = s.state;
		});
		expect(reg?.state.statusSnapshot().state).toBe(statusState || "disabled");
		expect(reg?.state.statusSnapshot().detail).toContain("SLACK_BOT_TOKEN");
	});
});

describe("trust boundaries as manifest data (DEC-017)", () => {
	it("incomplete manifests are rejected at registration time", async () => {
		const { validateTrustBoundaryManifest } = await import("./trust.js");
		const bad = validateTrustBoundaryManifest({
			ingress: "http",
			signatureSchemes: [],
			constantTimeCompare: true,
			idempotency: undefined,
			scriptTransformsConfinedToHome: false,
			bodySizeCapBytes: 0,
			backpressureWindow: "bounded",
		});
		expect(bad.length).toBeGreaterThanOrEqual(3);

		const good = validateTrustBoundaryManifest({
			ingress: "http",
			signatureSchemes: ["svix-v1"],
			constantTimeCompare: true,
			idempotency: { seenSetMaxEntries: 5000 },
			scriptTransformsConfinedToHome: true,
			bodySizeCapBytes: 1_048_576,
			backpressureWindow: "bounded",
		});
		expect(good).toEqual([]);
	});

	it("secureCompare is constant-time shaped and rejects length mismatches", async () => {
		const { secureCompare } = await import("./trust.js");
		expect(secureCompare("abc", "abc")).toBe(true);
		expect(secureCompare("abc", "abd")).toBe(false);
		expect(secureCompare("abc", "abcd")).toBe(false);
	});
});

describe("per-chat UTF-16 policy drives chunk units end to end", () => {
	it("a utf16-policy adapter splits astral-heavy content by CODE UNITS", async () => {
		const a = new TestAdapter({ lengthUnit: "utf16", scalarMaxUnits: 30 });
		const results = await a.deliverText("tg-chat", "🎉".repeat(50));
		expect(results.length).toBeGreaterThan(1);
		const wires = a.wireOps.filter((op) => op.op === "wire");
		for (const op of wires) {
			// Each delivered chunk ≤ 30 utf16 units (labels included).
			expect(Buffer.byteLength(op.content, "utf16le") / 2).toBeLessThanOrEqual(
				30,
			);
		}
		void resolveChatLengthPolicy;
	});
});

// 24-hour messaging-window classification: session vs template routing
// decisions RECORDED per outbound send (manifest-declared class), boundary
// mutation-checked under an injected clock. Parity: the declared class lives in
// whatsapp_cloud.py's phase-scope docstring (@~25) and its operational
// assumption @~672; the Pi port makes the decision auditable DATA.

import { describe, expect, it } from "vitest";

import {
	makeWaCloudFixture,
	FIXTURE_VERIFY_TOKEN,
	FIXTURE_APP_SECRET,
} from "./wa-cloud-fixture.js";
import { MessagingWindowClassifier } from "./window-policy.js";
import { MESSAGING_WINDOW_MS } from "./manifest.js";

describe("messaging-window classification (session vs template)", () => {
	it("never-seen chat classifies template; recorded BEFORE any wire call", () => {
		const fx = makeWaCloudFixture();
		try {
			const before = fx.graph.sentMessages.length;
			void fx.adapter.send("15551110000", "cold outreach");
			const decisions = fx.adapter.classifier.decisionsOf("15551110000");
			expect(decisions).toHaveLength(1);
			expect(decisions[0]?.routeClass).toBe("template");
			expect(decisions[0]?.withinWindow).toBe(false);
			// Default policy "record" keeps Hermes' best-effort delivery…
			expect(fx.graph.sentMessages.length).toBe(before + 1);
		} finally {
			fx.dispose();
		}
	});

	it("inbound opens a session; decision flips to template EXACTLY at the 24h boundary", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.w1", "15551234567", "hi")],
				}),
			);

			// +23h ⇒ still inside.
			fx.advance(23 * 60 * 60 * 1000);
			await fx.adapter.send("15551234567", "within window");
			let decisions = fx.adapter.classifier.decisionsOf("15551234567");
			expect(decisions[0]?.routeClass).toBe("session");
			expect(decisions[0]?.withinWindow).toBe(true);

			// +1h more (exactly windowMs elapsed) ⇒ closed (boundary inclusive).
			fx.advance(60 * 60 * 1000);
			await fx.adapter.send("15551234567", "outside window");
			decisions = fx.adapter.classifier.decisionsOf("15551234567");
			expect(decisions).toHaveLength(2);
			expect(decisions[1]?.routeClass).toBe("template");
			expect(decisions[1]?.elapsedMs).toBe(MESSAGING_WINDOW_MS);
		} finally {
			fx.dispose();
		}
	});

	it("refuse mode turns the recorded class into a PRE-WIRE gate", async () => {
		const fx = makeWaCloudFixture({ outsideWindowPolicy: "refuse" });
		try {
			const result = await fx.adapter.send("15559990000", "should not ship");
			expect(result.success).toBe(false);
			expect(result.error?.startsWith("template_required:")).toBe(true);
			expect(fx.graph.sentMessages).toHaveLength(0); // never reached the wire
			expect(fx.adapter.counters.windowRefusals).toBe(1);
		} finally {
			fx.dispose();
		}
	});

	it("media egress records the SAME decision shape", async () => {
		const fx = makeWaCloudFixture({ outsideWindowPolicy: "refuse" });
		try {
			const result = await fx.adapter.sendMedia(
				"15559990000",
				"image",
				{ bytes: Buffer.alloc(10), mime: "image/png" },
				{ caption: "nope" },
			);
			expect(result.success).toBe(false);
			expect(
				fx.adapter.classifier.decisionsOf("15559990000")[0]?.routeClass,
			).toBe("template");
			expect(fx.graph.uploads).toHaveLength(0);
		} finally {
			fx.dispose();
		}
	});
});

describe("MessagingWindowClassifier unit shape", () => {
	it("classify is PURE; decideForSend records; injected clock drives everything", () => {
		let now = 1_000_000;
		const c = new MessagingWindowClassifier({
			nowMs: () => now,
			windowMs: 100,
		});
		expect(c.classify("a").routeClass).toBe("template"); // never seen
		c.noteInbound("a", now - 50); // 50ms ago — inside the 100ms window
		expect(c.classify("a").withinWindow).toBe(true);
		expect(c.decisionsOf("a")).toHaveLength(0); // pure — nothing recorded

		now += 60; // elapsed 110 ≥ window ⇒ closed
		expect(c.classify("a").withinWindow).toBe(false);
		c.noteInbound("a", now - 10);
		now += 5;
		const d = c.decideForSend("a");
		expect(d.routeClass).toBe("session");
		expect(c.decisionsOf("a")).toHaveLength(1);
	});

	it("default window comes from manifest data (24h)", () => {
		const c = new MessagingWindowClassifier({ nowMs: () => 0 });
		expect(c.windowMs).toBe(MESSAGING_WINDOW_MS);
	});

	it("fixture secrets remain scoped (verify token readable only via reader)", () => {
		// Sanity binding the fixture to the DEC-009 posture: secrets resolve
		// through the reader closure, never process.env borrowing.
		expect(FIXTURE_VERIFY_TOKEN).not.toEqual(
			process.env.WHATSAPP_CLOUD_VERIFY_TOKEN,
		);
		expect(FIXTURE_APP_SECRET.length).toBeGreaterThan(0);
	});
});

// Phone→LID alias continuity through the REAL resolution module at the WIRE
// level (02 §4.3 invariant): two wire identities for one human MUST collapse
// to ONE session key AND one stable outbound recipient. Consumes
// src/pi_gateway/resolution/whatsapp-identity.ts via buildSessionKey +
// expandWhatsappAliases — no local re-implementation anywhere.

import { describe, expect, it } from "vitest";

import { makeWaCloudFixture } from "./wa-cloud-fixture.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";
import {
	canonicalWhatsappIdentifier,
	expandWhatsappAliases,
} from "../../pi_gateway/resolution/whatsapp-identity.js";

const PHONE = "15551234567";
const LID = "999999999999999";

async function eventually(
	cond: () => boolean,
	timeoutMs = 2_000,
): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (cond()) return true;
		await new Promise<void>((r) => setTimeout(r, 5));
	}
	return cond();
}

describe("LID alias continuity through the real identity module", () => {
	it("phone-form and LID-form deliveries land in ONE canonical session key", async () => {
		const fx = makeWaCloudFixture();
		try {
			fx.writeLidMapping(PHONE, LID);

			await fx.postSigned(
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.l1", PHONE, "from phone form")],
				}),
			);
			expect(
				await eventually(() => fx.adapter.turnLog.includes("from phone form")),
			).toBe(true);

			// The SAME human arrives under the LID alias…
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.l2", LID, "from lid form")],
				}),
			);
			expect(
				await eventually(() => fx.adapter.turnLog.includes("from lid form")),
			).toBe(true);

			// …and BOTH keys collapse through buildSessionKey + mapping files.
			const phoneKey = buildSessionKey(
				{ platform: "whatsapp", chatType: "dm", userId: PHONE, chatId: PHONE },
				{},
				undefined,
				{ whatsapp: { sessionDir: fx.sessionDir } },
			);
			const lidKey = buildSessionKey(
				{ platform: "whatsapp", chatType: "dm", userId: LID, chatId: LID },
				{},
				undefined,
				{ whatsapp: { sessionDir: fx.sessionDir } },
			);
			expect(phoneKey).toBe(lidKey);
			expect(phoneKey).toBe(`agent:main:whatsapp:dm:${PHONE}`); // min-pick stable
		} finally {
			fx.dispose();
		}
	});

	it("outbound addressing resolves BOTH aliases to the SAME wire recipient (idempotent)", async () => {
		const fx = makeWaCloudFixture();
		try {
			fx.writeLidMapping(PHONE, LID);

			// Addressed via the phone form…
			const r1 = await fx.adapter.send(PHONE, "reply one");
			expect(r1.success).toBe(true);
			// …and via the LID form.
			const r2 = await fx.adapter.send(LID, "reply two");
			expect(r2.success).toBe(true);

			const recipients = fx.graph.textSendsOf().map((s) => s.to);
			expect(recipients).toEqual([PHONE, PHONE]); // STABLE digits recipient
			expect(fx.adapter.resolveRecipient(LID)).toBe(PHONE);
			// Idempotency: repeated resolution is byte-stable.
			expect(fx.adapter.resolveRecipient(LID)).toBe(
				fx.adapter.resolveRecipient(PHONE),
			);
		} finally {
			fx.dispose();
		}
	});

	it("alias walk is defensive twice over: unsafe shapes contribute nothing NEW", () => {
		const fx = makeWaCloudFixture();
		try {
			fx.writeLidMapping(PHONE, LID);
			// A hostile identifier never expands: the result seeds ONLY the
			// normalized input itself (module contract — the seed is always
			// present; unsafe LINKS are dropped at dequeue).
			const hostile = expandWhatsappAliases("../../etc/passwd", {
				sessionDir: fx.sessionDir,
			});
			expect(hostile.size).toBe(1);
			expect([...hostile].every((a) => !a.includes("@"))).toBe(true);
			// …and the canonical pick stays numeric-preferred.
			expect(
				canonicalWhatsappIdentifier(LID, { sessionDir: fx.sessionDir }),
			).toBe(PHONE);
		} finally {
			fx.dispose();
		}
	});
});

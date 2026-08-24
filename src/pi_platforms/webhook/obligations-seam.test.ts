// BEHAVIOR CONTRACTS — the HELD-OPEN half of the C5 split lands through the
// pi_gateway delivery-obligations ledger (DEC-007): a reply that outlives its
// bounded HTTP window is durably recorded PENDING and redelivers byte-exactly
// through the claim/drive state machine. Real StateStore under mkdtemp;
// injected clock; scripted sender.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import {
	ManualClock,
	ScriptedSender,
} from "../../pi_gateway/obligations/testing/manual-clock.js";
import {
	DeliveryLedger,
	RECOVERED_MARKER,
	type OwnerStamp,
} from "../../pi_gateway/obligations/index.js";
import { LedgerObligationSink } from "./obligations-seam.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-webhook-obligations-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

const SELF_STAMP: OwnerStamp = { pid: 990_100, startedAt: 42 };

async function makeSink() {
	const store = await StateStore.open(
		join(dir, `db-${Math.random().toString(36).slice(2)}.db`),
	);
	const clock = new ManualClock();
	const ledger = new DeliveryLedger(store.db, {
		clock,
		selfStamp: SELF_STAMP,
		processAlive: (pid) => pid === SELF_STAMP.pid,
		processStartTime: () => 42,
	});
	const sink = new LedgerObligationSink(ledger, { platform: "webhook" });
	return { sink, ledger, clock };
}

describe("held-open obligation → ledger seam", () => {
	it("a reply that outlives its window is durably recorded PENDING", async () => {
		const { sink, ledger } = await makeSink();
		const { obligationId } = await sink.holdOpen({
			sessionKey: "agent:main:webhook:dm:webhook:ci:d-9",
			chatId: "webhook:ci:d-9",
			content: "the late final answer",
			messageRef: "d-9",
			route: "ci",
		});
		expect(ledger.stateOf(obligationId)).toBe("pending");
		const row = ledger.row(obligationId);
		expect(row?.content).toBe("the late final answer");
		expect(row?.platform).toBe("webhook");
	});

	it("redelivery lands the reply BYTE-EXACTLY and settles delivered", async () => {
		const { sink, ledger, clock } = await makeSink();
		await sink.holdOpen({
			sessionKey: "sk",
			chatId: "webhook:route:d-1",
			content: "byte-exact payload ✓",
			messageRef: "d-1",
			route: "route",
		});
		const sender = new ScriptedSender();
		// First backoff slot is RETRY_BASE_SECONDS away — advance past it.
		clock.advance(61);

		const driven = await sink.driveRedeliveries(sender.bind());
		expect(driven).toBe(1);
		expect(sender.callCount).toBe(1);
		const request = sender.calls[0];
		if (!request) throw new Error("no delivery request recorded");
		expect(request.content).toBe("byte-exact payload ✓"); // plain first send
		expect(request.needsMarker).toBe(false);
		expect(request.platform).toBe("webhook");
	});

	it("an AMBIGUOUS prior attempt redelivers WITH the visible recovered marker", async () => {
		const { sink, clock } = await makeSink();
		await sink.holdOpen({
			sessionKey: "sk",
			chatId: "chat",
			content: "second try",
			messageRef: "d-2",
			route: "r",
		});
		const failThenOk = new ScriptedSender().queue("fail", "ok");
		clock.advance(61);
		await sink.driveRedeliveries(failThenOk.bind()); // attempt 1 fails
		clock.advance(61 * 4); // next backoff slot (growth ×4)
		await sink.driveRedeliveries(failThenOk.bind()); // attempt 2 succeeds

		const okCall = failThenOk.calls[1];
		if (!okCall) throw new Error("second send missing");
		expect(okCall.content.startsWith(RECOVERED_MARKER)).toBe(true);
		expect(okCall.content.endsWith("second try")).toBe(true);
	});

	it("stateless rows never block on a dead platform set (sweep skips undeliverable)", async () => {
		const { sink } = await makeSink();
		await sink.holdOpen({
			sessionKey: "sk",
			chatId: "chat",
			content: "held",
			messageRef: "d-3",
			route: "r",
		});
		const sender = new ScriptedSender();
		// driveRedeliveries scopes claims to THIS platform; nothing due yet.
		const driven = await sink.driveRedeliveries(sender.bind());
		expect(driven).toBe(0);
		expect(sender.callCount).toBe(0);
	});
});

// Behavior contracts for pre-parse body caps (06 §8.3/§8.4; DEC-017):
// oversize bodies NEVER reach the parse seam — observable via a parse-call
// counter — at BOTH the declared-length gate and the lying-Content-Length
// (actual-bytes) gate.

import { describe, expect, it } from "vitest";
import {
	API_SERVER_BODY_CAP_BYTES,
	MSGRAPH_BODY_CAP_BYTES,
	readBodyWithinCap,
} from "./index.js";

describe("body cap enforced PRE-PARSE", () => {
	function harness(capBytes: number, declared: number | null, actual: number) {
		let readCalls = 0;
		let parseCalls = 0;
		const result = () =>
			readBodyWithinCap({
				capBytes,
				declaredContentLength: declared,
				readBody: () => {
					readCalls += 1;
					return Promise.resolve(Buffer.alloc(actual));
				},
				parse: (body) => {
					parseCalls += 1;
					return { parsed: body.length };
				},
			});
		return { result, counters: () => ({ readCalls, parseCalls }) };
	}

	it("declared length over the cap ⇒ 413 with ZERO reads and ZERO parses", async () => {
		const h = harness(1_048_576, 1_048_577, 1_048_577);
		const outcome = await h.result();
		expect(outcome.ok).toBe(false);
		expect(outcome.ok === false && outcome.phase).toBe("declared-length");
		expect(h.counters()).toEqual({ readCalls: 0, parseCalls: 0 });
	});

	it("LYING Content-Length: actual bytes over cap ⇒ 413 still before parse", async () => {
		const h = harness(1024, 10, 2048);
		const outcome = await h.result();
		expect(outcome.ok).toBe(false);
		expect(outcome.ok === false && outcome.phase).toBe("actual-bytes");
		expect(h.counters().parseCalls).toBe(0); // THE observable
	});

	it("within-cap body is read exactly once and parsed exactly once", async () => {
		const h = harness(1024, 512, 512);
		const outcome = await h.result();
		expect(outcome.ok).toBe(true);
		if (outcome.ok) expect(outcome.parsed).toEqual({ parsed: 512 });
		expect(h.counters()).toEqual({ readCalls: 1, parseCalls: 1 });
	});

	it("boundary: exactly AT the cap parses (cap is exclusive above)", async () => {
		const h = harness(1024, 1024, 1024);
		expect((await h.result()).ok).toBe(true);
		const over = harness(1024, null, 1025);
		const outcome = await over.result();
		expect(outcome.ok === false && outcome.phase).toBe("actual-bytes");
	});

	it("absent Content-Length (null) skips gate 1 but gate 2 still holds", async () => {
		const h = harness(128, null, 4096);
		const outcome = await h.result();
		expect(outcome.ok).toBe(false);
		expect(outcome.ok === false && outcome.phase).toBe("actual-bytes");
	});
});

describe("spec'd cap constants", () => {
	it("msgraph client_max_size = 1 MiB; api_server = 10_000_000", () => {
		expect(MSGRAPH_BODY_CAP_BYTES).toBe(1_048_576);
		expect(API_SERVER_BODY_CAP_BYTES).toBe(10_000_000);
	});
});

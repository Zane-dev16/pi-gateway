// Delivery-vs-persist response filter matrix (03 §9.1; §11 "Silence-filters"
// row). Behavior contracts: exact-marker rule, lane divergence, streaming
// hold-back, failure-forces-delivery. Persist is invariantly true — these
// filters gate egress only, never history.

import { describe, expect, it } from "vitest";
import {
	LIVE_GATEWAY_SILENT_MARKERS,
	isAutonomousSilenceResponse,
	isIntentionalSilenceAgentResult,
	isIntentionalSilenceResponse,
	isPartialSilenceMarker,
	resolveDeliveryDisposition,
} from "./response-filters.js";

describe("marker set (single source of truth)", () => {
	it("is EXACTLY {[SILENT], SILENT, NO_REPLY, NO REPLY}", () => {
		expect([...LIVE_GATEWAY_SILENT_MARKERS].sort()).toEqual(
			["[SILENT]", "NO REPLY", "NO_REPLY", "SILENT"].sort(),
		);
	});
});

describe("is_intentional_silence_response — interactive rule: ENTIRE response is exactly a marker", () => {
	it("accepts the four markers in any casing/whitespace padding", () => {
		for (const marker of ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"]) {
			expect(isIntentionalSilenceResponse(marker)).toBe(true);
			expect(isIntentionalSilenceResponse(`  ${marker.toLowerCase()}\n`)).toBe(
				true,
			);
		}
	});

	it("canonicalizes stray edge punctuation (.NO_REPLY, *NO_REPLY*) but keeps brackets structural", () => {
		expect(isIntentionalSilenceResponse(".NO_REPLY")).toBe(true);
		expect(isIntentionalSilenceResponse("*NO_REPLY*")).toBe(true);
		// Malformed bracket never canonicalizes to a bare marker.
		expect(isIntentionalSilenceResponse("[SILENT")).toBe(false);
		expect(isIntentionalSilenceResponse("SILENT]")).toBe(false);
	});

	it("prose merely mentioning a marker DELIVERS normally", () => {
		expect(
			isIntentionalSilenceResponse(
				"I decided to NO_REPLY here because nothing changed",
			),
		).toBe(false);
		expect(isIntentionalSilenceResponse("The [SILENT] flag means quiet")).toBe(
			false,
		);
	});

	it("blank is NOT silence (empty-response failure path) and >64 chars never matches", () => {
		expect(isIntentionalSilenceResponse("")).toBe(false);
		expect(isIntentionalSilenceResponse("   \n\t ")).toBe(false);
		const long = `NO_REPLY ${"x".repeat(64)}`;
		expect(long.length).toBeGreaterThan(64);
		expect(isIntentionalSilenceResponse(long)).toBe(false);
	});

	it("non-string input is never silence", () => {
		expect(isIntentionalSilenceResponse(null)).toBe(false);
		expect(isIntentionalSilenceResponse(undefined)).toBe(false);
		expect(isIntentionalSilenceResponse(42)).toBe(false);
	});
});

describe("is_autonomous_silence_response — loose autonomous-lane rule, SAME marker set", () => {
	it("whole-response markers suppress", () => {
		expect(isAutonomousSilenceResponse("[silent]")).toBe(true);
		expect(isAutonomousSilenceResponse("no_reply")).toBe(true);
	});

	it("marker alone on first OR last line suppresses (note on the other line)", () => {
		expect(isAutonomousSilenceResponse("[SILENT] No changes detected")).toBe(
			true,
		);
		expect(isAutonomousSilenceResponse("2 deals filtered\n\n[SILENT]")).toBe(
			true,
		);
		// Marker ALONE on the first line with prose after still suppresses —
		// parity: lines[0]/lines[-1] token test, loose by design for cron.
		expect(isAutonomousSilenceResponse("NO_REPLY\nall clear")).toBe(true);
		// But a marker buried mid-body (not first/last line) delivers.
		expect(
			isAutonomousSilenceResponse("report:\nNO_REPLY was considered\ndone"),
		).toBe(false);
	});

	it("bracketed sentinel same-line PREFIX suppresses; bare word does not", () => {
		expect(isAutonomousSilenceResponse("[SILENT] no diffs found")).toBe(true);
		expect(isAutonomousSilenceResponse("Silent retry succeeded")).toBe(false);
	});

	it("a token buried mid-sentence in a genuine report still delivers", () => {
		expect(
			isAutonomousSilenceResponse(
				"Ran 3 checks. [SILENT] appeared in logs but all passed.",
			),
		).toBe(false);
	});
});

describe("delivery-vs-persist filter MATRIX — ordered disposition per lane", () => {
	it("interactive: exact-marker silence suppresses DELIVERY, persists ALWAYS", () => {
		const d = resolveDeliveryDisposition({
			lane: "interactive",
			response: "NO_REPLY",
			agentResult: { failed: false },
		});
		expect(d.deliver).toBe(false);
		expect(d.persist).toBe(true);
		expect(d.reason).toBe("intentional_silence");
		expect(d.matcher).toBe("is_intentional_silence_response");
	});

	it("interactive: loose forms do NOT suppress (lane asymmetry vs cron)", () => {
		const d = resolveDeliveryDisposition({
			lane: "interactive",
			response: "[SILENT] No changes detected",
			agentResult: { failed: false },
		});
		expect(d.deliver).toBe(true);
		expect(d.reason).toBeNull();
	});

	it("cron/webhook: loose line/prefix forms DO suppress", () => {
		for (const text of [
			"[SILENT]",
			"[SILENT] No changes detected",
			"tick ran\n\nNO_REPLY",
		]) {
			for (const lane of ["cron", "webhook"] as const) {
				const d = resolveDeliveryDisposition({ lane, response: text });
				expect(d.deliver, `${lane}:${text}`).toBe(false);
				expect(d.persist).toBe(true);
				expect(d.reason).toBe("autonomous_silence");
			}
		}
	});

	it("FAILED turns deliver their errors on every lane even with an exact marker", () => {
		for (const lane of ["interactive", "cron", "webhook"] as const) {
			const d = resolveDeliveryDisposition({
				lane,
				response: "Error: tool exploded",
				agentResult: { failed: true },
			});
			expect(d.deliver).toBe(true);
			expect(d.reason).toBeNull();
			expect(d.matcher).toBe("none");
		}
	});

	it("failed=true overrides a marker response too (silence is for successful turns only)", () => {
		const d = resolveDeliveryDisposition({
			lane: "interactive",
			response: "NO_REPLY",
			agentResult: { failed: true },
		});
		expect(d.deliver).toBe(true);
	});

	it("missing agent result ⇒ interactive delivers (no success assertion, parity None-agent_result)", () => {
		const d = resolveDeliveryDisposition({
			lane: "interactive",
			response: "NO_REPLY",
		});
		expect(d.deliver).toBe(true);
	});

	it("ordinary prose delivers on every lane and always persists", () => {
		for (const lane of ["interactive", "cron", "webhook"] as const) {
			const d = resolveDeliveryDisposition({
				lane,
				response: "Deployment finished, see dashboard.",
			});
			expect(d.deliver).toBe(true);
			expect(d.persist).toBe(true);
		}
	});
});

describe("is_partial_silence_marker — streaming hold-back", () => {
	it("holds back non-empty prefixes of any marker ('NO' on the way to 'NO_REPLY')", () => {
		expect(isPartialSilenceMarker("N")).toBe(true);
		expect(isPartialSilenceMarker("NO")).toBe(true);
		expect(isPartialSilenceMarker("NO_")).toBe(true);
		expect(isPartialSilenceMarker("NO_REPL")).toBe(true);
		expect(isPartialSilenceMarker("[SILEN")).toBe(true);
	});

	it("an exact unterminated marker stays held back until stream-end decides", () => {
		expect(isPartialSilenceMarker("NO_REPLY")).toBe(true);
		expect(isPartialSilenceMarker("SILENT")).toBe(true);
	});

	it("diverged prose resumes normal streaming immediately", () => {
		expect(isPartialSilenceMarker("No way")).toBe(false);
		expect(isPartialSilenceMarker("NOTHING to report")).toBe(false);
		expect(isPartialSilenceMarker("The deployment worked")).toBe(false);
	});

	it("over-length buffers are never held back", () => {
		expect(isPartialSilenceMarker("N".repeat(65))).toBe(false);
	});

	it("shares canonicalization with the exact matcher so the pair cannot drift", () => {
		// ".NO_R" strips edge punctuation → prefix of NO_REPLY ⇒ held back.
		expect(isPartialSilenceMarker(".NO_R")).toBe(true);
	});
});

describe("is_intentional_silence_agent_result — failure gate", () => {
	it("suppresses only when result present, not failed, AND response exactly a marker", () => {
		expect(isIntentionalSilenceAgentResult({ failed: false }, "SILENT")).toBe(
			true,
		);
		expect(isIntentionalSilenceAgentResult({ failed: true }, "SILENT")).toBe(
			false,
		);
		expect(
			isIntentionalSilenceAgentResult({ failed: false }, "real reply"),
		).toBe(false);
		expect(isIntentionalSilenceAgentResult(null, "SILENT")).toBe(false);
	});
});

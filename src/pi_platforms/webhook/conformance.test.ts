// CONFORMANCE WIRING — the WEBHOOK reference adapter vs the executable 04 §8
// matrix:
//   1. ALL shared rows pass for shape="webhook" against the kit-built subject.
//   2. Transport-specific rows (flags/trust boundary, bounded-window answer)
//      pass via makeWebhookRows over the REAL adapter.
//   3. E2E pipeline row: signed ingress → guards → bounded-window answer,
//      idempotent replay, body-cap rejection BEFORE parse, and the api_server-
//      class SSE lanes (approval / steer / stop) over a REAL loopback socket —
//      plus the DEC-022 stateless wake rail self-posting its RAW-key turn.

import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "../conformance/wire.js";
import { makeWebhookSubject, type WebhookSubject } from "./webhook-subject.js";
import type { WebhookAdapter } from "./webhook-adapter.js";
import type { ConformanceSubject } from "../conformance/harness.js";
import { buildSharedRows } from "../conformance/rows.js";
import { runConformanceSuite, formatReport } from "../conformance/runner.js";
import { makeWebhookRows } from "../conformance/shapes.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makeWebhookSubject({
		wire: new FakePlatformWire(),
		streamIsMessageChatIds: opts.streamIsMessageChatIds,
		withSecret: opts.withSecret,
		name: opts.name,
		spawner: scheduler.spawner,
		scheduler,
	});
	return subject;
}

describe("shared 04 §8 rows — webhook shape", () => {
	it("passes EVERY encoded shared row against the webhook reference adapter", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(20);
		const report = await runConformanceSuite({
			subjectName: "webhook-reference",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});
});

describe("transport-specific rows (makeWebhookRows over the REAL adapter)", () => {
	it("flag pairing + DEC-017 trust boundary completeness", async () => {
		const subject = makeWebhookSubject({
			wire: new FakePlatformWire(),
			withSecret: true,
		}) as unknown as WebhookSubject;
		const probe = subject.flagsAndTrustProbe();
		const rows = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 0, windowCapMs: 5_000 };
			},
		});
		const report = await runConformanceSuite({
			subjectName: "webhook-flags",
			shape: "webhook",
			rows,
		});
		expect(report.failed).toBe(0);
	});

	it("a violating mutant FAILS the flag-pairing row (the gate detects)", async () => {
		const rows = makeWebhookRows({
			async flagsAndTrust() {
				return {
					interactiveResumeFalse: true,
					supportsAsyncDeliveryFalse: false, // MUTANT: push-capable stateless?
					trustBoundaryComplete: true,
				};
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 1, windowCapMs: 5_000 };
			},
		});
		const report = await runConformanceSuite({
			subjectName: "mutant-webhook",
			shape: "webhook",
			rows,
		});
		expect(report.failed).toBe(1);
	});
});

// ── E2E: real loopback sockets ──────────────────────────────────────────────

const ROUTE_SECRET = "e2e-route-secret";
const API_KEY = "e2e-api-server-key-0123456789";

interface E2EHarness {
	baseUrl: string;
	server: import("./server.js").WebhookHttpServer;
	adapter: WebhookAdapter;
	runs: import("./runs.js").RunRegistry;
	parsedBodies: { count: number };
	close(): Promise<void>;
}

async function makeE2E(): Promise<E2EHarness> {
	const [
		{ WebhookAdapter },
		{ WebhookHttpServer },
		{ CompletionsEndpoint },
		{ RunRegistry },
		{ WebhookIngressPipeline, createTimeoutSeam },
		{ SlidingWindowRateLimiter },
		{ DeliveryIdempotencyStore },
		{ webhookTrustBoundary },
	] = await Promise.all([
		import("./webhook-adapter.js"),
		import("./server.js"),
		import("./completions.js"),
		import("./runs.js"),
		import("./http-ingress.js"),
		import("./rate-limit.js"),
		import("./idempotency.js"),
		import("./manifest.js"),
	]);
	const wire = new FakePlatformWire();
	const adapter = new WebhookAdapter({
		wire,
		globalSecretReader: () => ROUTE_SECRET,
		apiKeyProvider: () => API_KEY,
	});
	adapter.attachStandardGuard();

	const parsedBodies = { count: 0 };
	const timers = createTimeoutSeam();
	const nowMsValue = Date.now();
	const pipeline = new WebhookIngressPipeline({
		trust: webhookTrustBoundary(),
		routes: new Map([
			[
				"ci",
				{
					name: "ci",
					secret: ROUTE_SECRET,
					events: ["push"],
					windowCapMs: 5_000,
				},
			],
		]),
		rateLimiter: new SlidingWindowRateLimiter({
			// Slots consumed before the trip assertion: first delivery + replay
			// (replays count — rate check PRECEDES idempotency) + 3 bursts.
			limit: 5,
			nowMs: () => nowMsValue,
		}),
		idempotency: new DeliveryIdempotencyStore({
			maxEntries: 128,
			nowMs: () => nowMsValue,
		}),
		nowSeconds: () => Math.floor(nowMsValue / 1000),
		timers,
		globalSecret: ROUTE_SECRET,
		parseJson: (text) => {
			parsedBodies.count += 1;
			let parsed: unknown;
			try {
				parsed = JSON.parse(text);
			} catch {
				throw new Error("Cannot parse body");
			}
			if (parsed === null || typeof parsed !== "object") {
				throw new Error("Cannot parse body");
			}
			return parsed as Record<string, unknown>;
		},
		runAgentTurn: (dispatch) => adapter.runAgentTurn(dispatch),
	});

	const runs = new RunRegistry();
	const completions = new CompletionsEndpoint({
		apiKeyProvider: () => API_KEY,
		idempotency: new DeliveryIdempotencyStore({
			maxEntries: 128,
			nowMs: () => nowMsValue,
		}),
		nowMs: () => nowMsValue,
		runDirectTurn: async ({ rawSessionId, prompt }) =>
			adapter.runDirectTurnForTest(rawSessionId, prompt),
	});
	const server = new WebhookHttpServer({
		pipeline,
		completions,
		runs,
		bodyCapBytes: 64 * 1024,
	});
	// Runs started over HTTP get the lifecycle-complete default executor.
	server.startRunWithDefaultExecutor = (input) =>
		runs.start(input, async (controls, text) => {
			controls.emitDelta(`working on ${text}`);
			if (text.includes("need-approval")) {
				await controls.requestApproval("rm -rf /tmp/staging");
				return "output after approval"; // approval runs complete after the choice
			}
			while (!controls.shouldStop()) {
				const steered = runs.consumeSteer(controls.runId);
				if (steered !== null) {
					controls.emitDelta(`steered:${steered}`);
					return "output after steer";
				}
				await new Promise<void>((r) => setTimeout(r, 2));
			}
			throw new Error("stopped");
		});

	const baseUrl = await server.listen();
	void nowMsValue;
	return {
		baseUrl,
		server,
		adapter,
		runs,
		parsedBodies,
		close: () => server.close(),
	};
}

function sign(body: string, secret = ROUTE_SECRET): string {
	return createHmac("sha256", secret).update(body).digest("hex");
}

/** Collect SSE frames until a predicate matches or timeout. */
async function collectFrames(
	url: string,
	until: (type: string) => boolean,
	timeoutMs = 5_000,
): Promise<Array<{ type: string; payload: Record<string, unknown> }>> {
	const controller = new AbortController();
	const collected: Array<{ type: string; payload: Record<string, unknown> }> =
		[];
	try {
		const res = await fetch(url, { signal: controller.signal });
		const reader = res.body?.getReader();
		if (!reader) throw new Error("no stream");
		const decoder = new TextDecoder();
		let buffer = "";
		const timer = setTimeout(() => controller.abort(), timeoutMs);
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			let idx: number;
			while ((idx = buffer.indexOf("\n\n")) >= 0) {
				const frame = buffer.slice(0, idx);
				buffer = buffer.slice(idx + 2);
				const eventLine = /^event: (.+)$/m.exec(frame);
				const dataLine = /^data: (.+)$/m.exec(frame);
				if (!eventLine || !dataLine) continue;
				const type = eventLine[1] ?? "";
				collected.push({
					type,
					payload: JSON.parse(dataLine[1] ?? "{}") as Record<string, unknown>,
				});
				if (until(type)) {
					clearTimeout(timer);
					controller.abort();
					return collected;
				}
			}
		}
		clearTimeout(timer);
	} catch {
		// aborted — return what we have
	}
	return collected;
}

describe("E2E pipeline row — real loopback sockets", () => {
	it("signed ingress → guards → bounded-window answer; replay cached; oversized rejected before parse", async () => {
		const h = await makeE2E();
		try {
			// Health lane up.
			const health = await fetch(`${h.baseUrl}/health`);
			expect(health.status).toBe(200);

			// 1. Signed delivery completes INSIDE the bounded window and the
			// reply rides the response.
			const body = JSON.stringify({ job: "build" });
			const startedAt = Date.now();
			const res = await fetch(`${h.baseUrl}/webhooks/ci`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-hub-signature-256": `sha256=${sign(body)}`,
					"x-github-delivery": "e2e-d-1",
					"x-github-event": "push",
				},
				body,
			});
			const elapsed = Date.now() - startedAt;
			expect(res.status).toBe(200);
			const json = (await res.json()) as Record<string, unknown>;
			expect(json["status"]).toBe("completed");
			expect(json["reply"]).toBe('reply:{"job":"build"}');
			// Bounded window respected (windowCapMs=5000; generous wall bound).
			expect(elapsed).toBeLessThan(5_000);
			const turnsAfterFirst = h.adapter.turnLog.length;

			// 2. Replay SAME delivery-id → CACHED outcome, ZERO reprocessing.
			const replay = await fetch(`${h.baseUrl}/webhooks/ci`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-hub-signature-256": `sha256=${sign(body)}`,
					"x-github-delivery": "e2e-d-1",
					"x-github-event": "push",
				},
				body,
			});
			expect(replay.status).toBe(res.status);
			expect(await replay.json()).toEqual(json);
			expect(h.adapter.turnLog.length).toBe(turnsAfterFirst);

			// 3. Oversized body → 413 BEFORE parse (parse counter untouched).
			const parsesBefore = h.parsedBodies.count;
			const huge = "x".repeat(65 * 1024);
			const tooBig = await fetch(`${h.baseUrl}/webhooks/ci`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-hub-signature-256": `sha256=${sign(huge)}`,
					"x-github-delivery": "e2e-d-2",
					"x-github-event": "push",
				},
				body: huge,
			});
			expect(tooBig.status).toBe(413);
			expect(h.parsedBodies.count).toBe(parsesBefore);

			// 4. Unsigned garbage → 401.
			const unsigned = await fetch(`${h.baseUrl}/webhooks/ci`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-github-delivery": "e2e-d-3",
					"x-github-event": "push",
				},
				body,
			});
			expect(unsigned.status).toBe(401);

			// 5. Rate limit trips at the configured threshold (limit 4).
			for (let i = 0; i < 3; i++) {
				const r = await fetch(`${h.baseUrl}/webhooks/ci`, {
					method: "POST",
					headers: {
						"content-type": "application/json",
						"x-hub-signature-256": `sha256=${sign(body)}`,
						"x-github-delivery": `burst-${i}`,
						"x-github-event": "push",
					},
					body,
				});
				expect(r.status).toBe(200);
			}
			const tripped = await fetch(`${h.baseUrl}/webhooks/ci`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-hub-signature-256": `sha256=${sign(body)}`,
					"x-github-delivery": "burst-over",
					"x-github-event": "push",
				},
				body,
			});
			expect(tripped.status).toBe(429);
		} finally {
			await h.close();
		}
	}, 20_000);

	it("SSE lanes: approval / steer / stop each REACHABLE end-to-end", async () => {
		const h = await makeE2E();
		try {
			// ── APPROVAL: run holds open; endpoint resolves; run completes. ──
			const startA = await fetch(`${h.baseUrl}/v1/runs`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ input: "deploy need-approval please" }),
			});
			expect(startA.status).toBe(202);
			const { run_id: runA } = (await startA.json()) as { run_id: string };

			const framesA = collectFrames(
				`${h.baseUrl}/v1/runs/${runA}/events`,
				(t) => t === "run.completed" || t === "run.cancelled",
			);
			// Give the run a moment to open the approval gate…
			await new Promise<void>((r) => setTimeout(r, 15));
			const approveRes = await fetch(`${h.baseUrl}/v1/runs/${runA}/approvals`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ choice: "once" }),
			});
			expect(approveRes.status).toBe(200);
			const seenA = await framesA;
			const typesA = seenA.map((f) => f.type);
			expect(typesA).toContain("message.delta");
			expect(typesA).toContain("approval.request");
			expect(typesA).toContain("approval.responded");
			expect(typesA).toContain("run.completed");

			// Double-resolve answers 409 (pop-or-409).
			const again = await fetch(`${h.baseUrl}/v1/runs/${runA}/approvals`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ choice: "once" }),
			});
			expect([409, 400]).toContain(again.status); // invalid_choice(400): slot popped ⇒ not active

			// ── STEER: only while running; text reaches the executor. ──
			const startB = await fetch(`${h.baseUrl}/v1/runs`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ input: "refactor steerable-target" }),
			});
			const { run_id: runB } = (await startB.json()) as { run_id: string };
			const steerRes = await fetch(`${h.baseUrl}/v1/runs/${runB}/steer`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ input: "prefer the small diff" }),
			});
			expect(steerRes.status).toBe(200);

			// ── STOP: cooperative cancel lands run.cancelled. ──
			const startC = await fetch(`${h.baseUrl}/v1/runs`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ input: "endless loop work" }),
			});
			const { run_id: runC } = (await startC.json()) as { run_id: string };
			const framesC = collectFrames(
				`${h.baseUrl}/v1/runs/${runC}/events`,
				(t) => t === "run.cancelled",
			);
			await new Promise<void>((r) => setTimeout(r, 10));
			const stopRes = await fetch(`${h.baseUrl}/v1/runs/${runC}/stop`, {
				method: "POST",
			});
			expect(stopRes.status).toBe(200);
			const seenC = await framesC;
			expect(seenC.some((f) => f.type === "run.cancelled")).toBe(true);

			// Unknown run ids answer 409/404-shaped errors, never crash.
			const ghost = await fetch(`${h.baseUrl}/v1/runs/run_ghost/stop`, {
				method: "POST",
			});
			expect(ghost.status).toBe(409);
		} finally {
			await h.close();
		}
	});

	it("DEC-022 close-out: wake rail self-posts the RAW-key direct turn through the REAL server", async () => {
		const h = await makeE2E();
		try {
			const rail = h.adapter.buildWakeRail(h.baseUrl);
			const rawSessionId = "agent:main:api_server:dm:e2e-wake";
			const outcome = await rail.wake(
				rawSessionId,
				"[internal wake] build done",
			);

			// The completion landed IN the real session under the RAW key.
			expect(outcome.ok).toBe(true);
			if (outcome.ok) {
				expect(outcome.reply).toContain("[internal wake]");
			}
			// Guard-traversed turn bound to the RAW id (no derived reshaping).
			expect(h.adapter.turnLog.some((t) => t.includes("[internal wake]"))).toBe(
				true,
			);
			expect(h.adapter.guardSessionsForTest()).toContain(rawSessionId);
		} finally {
			await h.close();
		}
	});
});

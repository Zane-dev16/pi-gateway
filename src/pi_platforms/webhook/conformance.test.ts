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
	completions: import("./completions.js").CompletionsEndpoint;
	parsedBodies: { count: number };
	/** Memory scopes adopted by RUNS-lane starts (api-5 observability). */
	boundRunMemoryScopes: string[];
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
		runDirectTurn: async ({ rawSessionId, prompt, sessionKey }) =>
			adapter.runDirectTurnForTest(rawSessionId, prompt, sessionKey),
	});
	const server = new WebhookHttpServer({
		pipeline,
		completions,
		runs,
		bodyCapBytes: 64 * 1024,
		// webhook-44: EVERY /v1/runs lane is Bearer-gated like /v1/chat/completions.
		apiKeyProvider: () => API_KEY,
	});
	// Runs started over HTTP get the lifecycle-complete default executor.
	// api-3/api-5 seams: queued model + adopted memory-scope key ride start opts
	// (Hermes _create_agent gateway_session_key parity — bound observably here).
	const boundRunMemoryScopes: string[] = [];
	server.startRunWithDefaultExecutor = (
		input,
		sessionId,
		model,
		memoryScopeKey,
	) => {
		if (memoryScopeKey !== undefined) boundRunMemoryScopes.push(memoryScopeKey);
		return runs.start(
			input,
			async (controls, text) => {
				controls.emitDelta(`working on ${text}`);
				const USAGE = {
					promptTokens: 12,
					completionTokens: 8,
					totalTokens: 20,
				};
				if (text.includes("need-approval")) {
					await controls.requestApproval("rm -rf /tmp/staging");
					return { output: "output after approval", usage: USAGE };
				}
				if (text.includes("need-double-approval")) {
					// TWO concurrent gates under ONE run — the resolve_all target.
					const [a, b] = await Promise.all([
						controls.requestApproval("cmd-one"),
						controls.requestApproval("cmd-two"),
					]);
					return { output: `double:${a},${b}`, usage: USAGE };
				}
				while (!controls.shouldStop()) {
					const steered = runs.consumeSteer(controls.runId);
					if (steered !== null) {
						controls.emitDelta(`steered:${steered}`);
						return { output: "output after steer", usage: USAGE };
					}
					await new Promise<void>((r) => setTimeout(r, 2));
				}
				throw new Error("stopped");
			},
			{ sessionId, model },
		);
	};

	const baseUrl = await server.listen();
	void nowMsValue;
	return {
		baseUrl,
		server,
		adapter,
		runs,
		completions,
		parsedBodies,
		boundRunMemoryScopes,
		close: () => server.close(),
	};
}

function sign(body: string, secret = ROUTE_SECRET): string {
	return createHmac("sha256", secret).update(body).digest("hex");
}

/** POST a JSON body to a /v1/runs lane WITH the required Bearer key. */
function postRun(
	baseUrl: string,
	path: string,
	body: unknown,
	headers: Record<string, string> = {},
): Promise<Response> {
	return fetch(`${baseUrl}${path}`, {
		method: "POST",
		headers: {
			"content-type": "application/json",
			authorization: `Bearer ${API_KEY}`,
			...headers,
		},
		body: typeof body === "string" ? body : JSON.stringify(body),
	});
}

/** Collect SSE frames until a predicate matches or timeout. */
async function collectFrames(
	baseUrl: string,
	runId: string,
	until: (type: string) => boolean,
	timeoutMs = 5_000,
): Promise<Array<{ type: string; payload: Record<string, unknown> }>> {
	const url = `${baseUrl}/v1/runs/${runId}/events`;
	const controller = new AbortController();
	const collected: Array<{ type: string; payload: Record<string, unknown> }> =
		[];
	try {
		const res = await fetch(url, {
			signal: controller.signal,
			headers: { authorization: `Bearer ${API_KEY}` },
		});
		const reader = res.body?.getReader();
		if (!reader) throw new Error("no stream");
		const decoder = new TextDecoder();
		let buffer = "";
		const timer = setTimeout(() => controller.abort(), timeoutMs);
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			let idx = buffer.indexOf("\n\n");
			while (idx >= 0) {
				const frame = buffer.slice(0, idx);
				buffer = buffer.slice(idx + 2);
				idx = buffer.indexOf("\n\n");
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
			const startA = await postRun(h.baseUrl, "/v1/runs", {
				input: "deploy need-approval please",
			});
			expect(startA.status).toBe(202);
			const { run_id: runA } = (await startA.json()) as { run_id: string };

			const framesA = collectFrames(
				h.baseUrl,
				runA,
				(t) => t === "done", // ride through the terminal sentinel
			);
			// Give the run a moment to open the approval gate…
			await new Promise<void>((r) => setTimeout(r, 15));
			// SINGULAR /approval is the documented route (webhook-45). The 200
			// body is the hermes.run.approval_response envelope with the
			// resolved COUNT — no invented status field (api-1, @8140-8146).
			const approveRes = await postRun(h.baseUrl, `/v1/runs/${runA}/approval`, {
				choice: "once",
			});
			expect(approveRes.status).toBe(200);
			expect(await approveRes.json()).toEqual({
				object: "hermes.run.approval_response",
				run_id: runA,
				choice: "once",
				resolved: 1,
			});
			const seenA = await framesA;
			const typesA = seenA.map((f) => f.type);
			expect(typesA).toContain("assistant.delta");
			expect(typesA).toContain("approval.request");
			expect(typesA).toContain("approval.responded");
			expect(typesA).toContain("run.completed");
			// The approval.responded frame carries the resolved count (@8147).
			const respondedFrame = seenA.find((f) => f.type === "approval.responded");
			expect(respondedFrame?.payload["resolved"]).toBe(1);
			expect(respondedFrame?.payload["choice"]).toBe("once");
			// Terminal done sentinel closes every stream (webhook-46).
			expect(typesA[typesA.length - 1]).toBe("done");
			// snake_case payload parity on the wire.
			for (const frame of seenA) {
				if (frame.type === "done") continue;
				expect(frame.payload["run_id"]).toBe(runA);
				expect(frame.payload["session_id"]).toBe(runA);
				expect(typeof frame.payload["seq"]).toBe("number");
				expect(typeof frame.payload["ts"]).toBe("number");
			}
			const approvalFrame = seenA.find((f) => f.type === "approval.request");
			expect(approvalFrame?.payload["command"]).toBe("rm -rf /tmp/staging");
			expect(Array.isArray(approvalFrame?.payload["choices"])).toBe(true);

			// The plural alias still resolves (route-table compatibility).
			const startAlias = await postRun(h.baseUrl, "/v1/runs", {
				input: "deploy need-approval please",
			});
			const { run_id: runAlias } = (await startAlias.json()) as {
				run_id: string;
			};
			await new Promise<void>((r) => setTimeout(r, 15));
			const aliasRes = await postRun(
				h.baseUrl,
				`/v1/runs/${runAlias}/approvals`,
				{ choice: "always" },
			);
			expect(aliasRes.status).toBe(200);

			// Double-resolve answers 409 (pop-or-409).
			const again = await postRun(h.baseUrl, `/v1/runs/${runA}/approval`, {
				choice: "once",
			});
			expect([409, 400]).toContain(again.status); // invalid_choice(400): slot popped ⇒ not active

			// ── RESOLVE_ALL: body booleans drain every gate under ONE run ──
			const startD = await postRun(h.baseUrl, "/v1/runs", {
				input: "deploy need-double-approval please",
			});
			const { run_id: runD } = (await startD.json()) as { run_id: string };
			await new Promise<void>((r) => setTimeout(r, 15));
			// coerceRequestBool parity: STRING "yes" is truthy for all/resolve_all.
			const resolveAllRes = await postRun(
				h.baseUrl,
				`/v1/runs/${runD}/approval`,
				{ choice: "deny", all: "yes" },
			);
			expect(resolveAllRes.status).toBe(200);
			expect(await resolveAllRes.json()).toEqual({
				object: "hermes.run.approval_response",
				run_id: runD,
				choice: "deny",
				resolved: 2,
			});
			await new Promise<void>((r) => setTimeout(r, 10));
			expect(h.runs.status(runD)?.status).toBe("completed");

			// Explicit false strings do NOT trigger the drain (single resolve).
			const startE = await postRun(h.baseUrl, "/v1/runs", {
				input: "deploy need-double-approval again",
			});
			const { run_id: runE } = (await startE.json()) as { run_id: string };
			await new Promise<void>((r) => setTimeout(r, 15));
			const singleRes = await postRun(h.baseUrl, `/v1/runs/${runE}/approval`, {
				choice: "once",
				resolve_all: "false",
			});
			expect(singleRes.status).toBe(200);
			expect(await singleRes.json()).toMatchObject({ resolved: 1 });

			// ── STEER: only while running; text reaches the executor. ──
			const startB = await postRun(h.baseUrl, "/v1/runs", {
				input: "refactor steerable-target",
			});
			const { run_id: runB } = (await startB.json()) as { run_id: string };
			const steerRes = await postRun(h.baseUrl, `/v1/runs/${runB}/steer`, {
				input: "prefer the small diff",
			});
			expect(steerRes.status).toBe(200);
			// hermes.run.steer envelope (api_server.py:_handle_steer_run parity).
			expect(await steerRes.json()).toEqual({
				object: "hermes.run.steer",
				run_id: runB,
				accepted: true,
			});

			// ── STOP: cooperative cancel lands run.cancelled. ──
			const startC = await postRun(h.baseUrl, "/v1/runs", {
				input: "endless loop work",
			});
			const { run_id: runC } = (await startC.json()) as { run_id: string };
			const framesC = collectFrames(h.baseUrl, runC, (t) => t === "done");
			await new Promise<void>((r) => setTimeout(r, 10));
			const stopRes = await postRun(h.baseUrl, `/v1/runs/${runC}/stop`, {});
			expect(stopRes.status).toBe(200);
			const seenC = await framesC;
			expect(seenC.some((f) => f.type === "run.cancelled")).toBe(true);
			expect(seenC[seenC.length - 1]?.type).toBe("done");

			// Terminal runs have NO live refs ⇒ late stop answers 404
			// run_not_found, NEVER 409 run_already_finished (api-4, @8199).
			await new Promise<void>((r) => setTimeout(r, 5));
			const lateStop = await postRun(h.baseUrl, `/v1/runs/${runC}/stop`, {});
			expect(lateStop.status).toBe(404);
			expect(
				((await lateStop.json()) as { error: { code: string } }).error.code,
			).toBe("run_not_found");

			// UNKNOWN runs answer 404 run_not_found envelopes (webhook-50),
			// never 409 and never a crash.
			for (const lane of ["stop", "steer", "approval"] as const) {
				const ghost = await postRun(
					h.baseUrl,
					`/v1/runs/run_ghost/${lane}`,
					lane === "steer" ? { input: "x" } : {},
				);
				expect(ghost.status).toBe(404);
				const body = (await ghost.json()) as {
					error: {
						message: string;
						type: string;
						param: unknown;
						code: string;
					};
				};
				expect(body.error.code).toBe("run_not_found");
				expect(body.error.type).toBe("invalid_request_error");
			}
		} finally {
			await h.close();
		}
	});

	it("Bearer gate covers EVERY /v1/runs lane (webhook-44)", async () => {
		const h = await makeE2E();
		try {
			const started = await fetch(`${h.baseUrl}/v1/runs`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ input: "auth probe" }),
			});
			const { run_id: runId } = (await started.json()) as { run_id: string };
			const lanes: Array<[string, RequestInit]> = [
				[
					"/v1/runs",
					{
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ input: "x" }),
					},
				],
				[`/v1/runs/${runId}`, { method: "GET" }],
				[`/v1/runs/${runId}/events`, { method: "GET" }],
				[
					`/v1/runs/${runId}/approval`,
					{ method: "POST", body: JSON.stringify({ choice: "once" }) },
				],
				[
					`/v1/runs/${runId}/steer`,
					{ method: "POST", body: JSON.stringify({ input: "x" }) },
				],
				[`/v1/runs/${runId}/stop`, { method: "POST" }],
			];
			for (const [path, init] of lanes) {
				const res = await fetch(`${h.baseUrl}${path}`); // NO Authorization
				expect(res.status).toBe(401);
				const body = (await res.json()) as {
					error: { message: string; type: string; code: string };
				};
				expect(body.error.message).toBe(
					"Invalid gateway API key (API_SERVER_KEY)",
				);
				expect(body.error.type).toBe("gateway_auth_error");
				expect(body.error.code).toBe("gateway_auth_failed");
				const wrong = await fetch(`${h.baseUrl}${path}`, {
					...init,
					headers: {
						...(init.headers ?? {}),
						authorization: "Bearer wrong-key",
					},
				});
				expect(wrong.status).toBe(401);
			}
		} finally {
			await h.close();
		}
	});

	it("POST /v1/runs validation ladder answers error-shaped 400s (webhook-49)", async () => {
		const h = await makeE2E();
		try {
			// Invalid JSON never becomes a raw-text prompt.
			const badJson = await postRun(h.baseUrl, "/v1/runs", "{not json");
			expect(badJson.status).toBe(400);
			expect(
				((await badJson.json()) as { error: { message: string } }).error
					.message,
			).toBe("Invalid JSON");

			// Missing 'input'.
			const noInput = await postRun(h.baseUrl, "/v1/runs", {
				model: "m",
			});
			expect(noInput.status).toBe(400);
			expect(
				((await noInput.json()) as { error: { message: string } }).error
					.message,
			).toBe("Missing 'input' field");

			// Empty user messages: empty LIST, non-message shapes (NOTE: an empty
			// STRING is falsy like Python and answers "Missing 'input' field").
			for (const input of [[], [{ role: "user" }], 42]) {
				const empty = await postRun(h.baseUrl, "/v1/runs", { input });
				expect(empty.status).toBe(400);
				expect(
					((await empty.json()) as { error: { message: string } }).error
						.message,
				).toBe("No user message found in input");
			}
			const emptyString = await postRun(h.baseUrl, "/v1/runs", { input: "" });
			expect(emptyString.status).toBe(400);
			expect(
				((await emptyString.json()) as { error: { message: string } }).error
					.message,
			).toBe("Missing 'input' field");
			expect(h.runs.runIds().length).toBe(0); // nothing was ever started

			// A multi-message list takes the LAST message's content.
			const multi = await postRun(h.baseUrl, "/v1/runs", {
				input: [
					{ role: "user", content: "earlier" },
					{ role: "user", content: "the real task" },
				],
			});
			expect(multi.status).toBe(202);
			const { run_id } = (await multi.json()) as { run_id: string };
			const view = h.runs.status(run_id);
			expect(view?.sessionId).toBe(run_id); // session defaults to run id
		} finally {
			await h.close();
		}
	}, 20_000);

	it("GET /v1/runs/:id answers the hermes.run status envelope (webhook-50)", async () => {
		const h = await makeE2E();
		const bearer = { authorization: `Bearer ${API_KEY}` };
		try {
			// need-approval ⇒ the scripted executor completes once approved.
			const started = await postRun(h.baseUrl, "/v1/runs", {
				input: "deploy need-approval please",
				session_id: "agent:main:api_server:dm:probe",
			});
			expect(started.status).toBe(202);
			const { run_id } = (await started.json()) as { run_id: string };
			await new Promise<void>((r) => setTimeout(r, 15));
			const midFlight = await fetch(`${h.baseUrl}/v1/runs/${run_id}`, {
				headers: bearer,
			});
			expect(midFlight.status).toBe(200);
			const midView = (await midFlight.json()) as Record<string, unknown>;
			expect(midView).toMatchObject({
				object: "hermes.run",
				run_id,
				created_at: expect.any(Number),
				updated_at: expect.any(Number),
				session_id: "agent:main:api_server:dm:probe",
				// Queued-status model field (@7690): body value or default.
				model: "pi-gateway",
			});
			await postRun(h.baseUrl, `/v1/runs/${run_id}/approval`, {
				choice: "once",
			});
			await new Promise<void>((r) => setTimeout(r, 10));
			const res = await fetch(`${h.baseUrl}/v1/runs/${run_id}`, {
				headers: bearer,
			});
			const view = (await res.json()) as Record<string, unknown>;
			expect(view).toMatchObject({
				object: "hermes.run",
				run_id,
				status: "completed",
				session_id: "agent:main:api_server:dm:probe",
				model: "pi-gateway",
				usage: { prompt_tokens: 12, completion_tokens: 8, total_tokens: 20 },
				output: "output after approval",
				last_event: "run.completed",
			});
			// pending_steer stays ABSENT when no steer text is undelivered.
			expect(view["pending_steer"]).toBeUndefined();
		} finally {
			await h.close();
		}
	});

	it("api-3: explicit body model lands on the queued view; pending_steer surfaces", async () => {
		const h = await makeE2E();
		try {
			const started = await postRun(h.baseUrl, "/v1/runs", {
				input: "endless loop work",
				model: "custom-model-9",
			});
			expect(started.status).toBe(202);
			const { run_id } = (await started.json()) as { run_id: string };
			await new Promise<void>((r) => setTimeout(r, 10));
			const mid = await fetch(`${h.baseUrl}/v1/runs/${run_id}`, {
				headers: { authorization: `Bearer ${API_KEY}` },
			});
			expect(((await mid.json()) as { model?: string }).model).toBe(
				"custom-model-9",
			);
			// Steer text that the executor never consumes rides the completed
			// status as pending_steer (@7926-7936).
			await postRun(h.baseUrl, `/v1/runs/${run_id}/steer`, {
				input: "undelivered guidance",
			});
			await postRun(h.baseUrl, `/v1/runs/${run_id}/stop`, {});
			await new Promise<void>((r) => setTimeout(r, 15));
			// Cancelled runs do NOT carry pending_steer (Hermes sets it on the
			// COMPLETED branch only) — drive a completing run instead.
			const start2 = await postRun(h.baseUrl, "/v1/runs", {
				input: "refactor steerable-target",
				model: "custom-model-9",
			});
			const { run_id: run2 } = (await start2.json()) as { run_id: string };
			await postRun(h.baseUrl, `/v1/runs/${run2}/steer`, {
				input: "late-but-consumed",
			});
			await new Promise<void>((r) => setTimeout(r, 20));
			const doneView = h.runs.status(run2);
			expect(doneView?.status).toBe("completed");
			expect(doneView?.pendingSteer).toBeUndefined(); // executor CONSUMED it
		} finally {
			await h.close();
		}
	});

	it("api-5: X-Hermes-Session-Key ladder gates POST /v1/runs and echoes the scope", async () => {
		const h = await makeE2E();
		try {
			// Over-length rejects at _MAX_SESSION_HEADER_LEN=256 BEFORE any run
			// starts (control-character rejection is covered at the trust-engine
			// unit layer — HTTP clients cannot transmit raw \r\n\x00 header bytes).
			expect(h.runs.runIds().length).toBe(0);
			const tooLong = await postRun(
				h.baseUrl,
				"/v1/runs",
				{
					input: "x",
				},
				{ "x-hermes-session-key": "k".repeat(257) },
			);
			expect(tooLong.status).toBe(400);
			expect(await tooLong.json()).toEqual({
				error: {
					message: "Session key too long",
					type: "invalid_request_error",
				},
			});

			// A valid key starts the run, echoes back on the 202…
			const ok = await postRun(
				h.baseUrl,
				"/v1/runs",
				{
					input: "scoped work",
				},
				{ "x-hermes-session-key": `  memory-scope-A  ` },
			);
			expect(ok.status).toBe(202);
			expect(ok.headers.get("x-hermes-session-key")).toBe("memory-scope-A");
			// …and BINDS the memory scope for the run's turn (DEC-017).
			await new Promise<void>((r) => setTimeout(r, 5));
			expect(h.boundRunMemoryScopes).toContain("memory-scope-A");
		} finally {
			await h.close();
		}
	});

	it("api-5: completions lane honors the session-key ladder, echo, and binding", async () => {
		const h = await makeE2E();
		try {
			const BEARER = { authorization: `Bearer ${API_KEY}` };
			// Echo + memory-scope binding on the JSON lane.
			const res = await fetch(`${h.baseUrl}/v1/chat/completions`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					...BEARER,
					"x-hermes-session-key": "scope-json-1",
				},
				body: JSON.stringify({
					model: "pi-gateway",
					messages: [{ role: "user", content: "scoped probe" }],
				}),
			});
			expect(res.status).toBe(200);
			expect(res.headers.get("x-hermes-session-key")).toBe("scope-json-1");
			expect(res.headers.get("x-hermes-session-id")).toBeTruthy();
			await res.text();
			expect(h.adapter.memoryScopeBindings()).toContain("scope-json-1");

			// The SSE chunk-stream lane echoes the key too (@5427 parity).
			const stream = await fetch(`${h.baseUrl}/v1/chat/completions`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					...BEARER,
					"x-hermes-session-key": "scope-sse-1",
				},
				body: JSON.stringify({
					model: "pi-gateway",
					messages: [{ role: "user", content: "scoped stream" }],
					stream: true,
				}),
			});
			expect(stream.headers.get("x-hermes-session-key")).toBe("scope-sse-1");
			await stream.text();
			expect(h.adapter.memoryScopeBindings()).toContain("scope-sse-1");

			// Injection + over-length 400s ride the shared trust-engine verdicts,
			// rendered verbatim by the lane ({message,type} dicts like Hermes).
			for (const [hostile, message] of [
				["bad\nk", "Invalid session key"],
				["k".repeat(257), "Session key too long"],
			] as const) {
				const bad = await h.completions.handle({
					headers: {
						authorization: BEARER.authorization,
						"x-hermes-session-key": hostile,
					},
					bodyText: JSON.stringify({
						messages: [{ role: "user", content: "x" }],
					}),
				});
				expect(bad.status).toBe(400);
				expect(bad.json).toEqual({
					error: { message, type: "invalid_request_error" },
				});
			}
		} finally {
			await h.close();
		}
	});

	it("api-5: WITHOUT a configured API key the session key is 403-rejected, never anonymous", async () => {
		// Key-less server (vendor no-key wiring): the /v1/runs lanes are
		// ungated BUT adopting a memory scope still requires auth — the
		// _parse_session_key_header ladder answers 403 (@2318-2328).
		const [
			{ WebhookHttpServer },
			{ WebhookIngressPipeline, createTimeoutSeam },
			{ RunRegistry },
			{ webhookTrustBoundary },
			{ SlidingWindowRateLimiter },
			{ DeliveryIdempotencyStore },
		] = await Promise.all([
			import("./server.js"),
			import("./http-ingress.js"),
			import("./runs.js"),
			import("./manifest.js"),
			import("./rate-limit.js"),
			import("./idempotency.js"),
		]);
		const nowMsValue = Date.now();
		const server = new WebhookHttpServer({
			pipeline: new WebhookIngressPipeline({
				trust: webhookTrustBoundary(),
				routes: new Map(),
				rateLimiter: new SlidingWindowRateLimiter({
					limit: 10,
					nowMs: () => nowMsValue,
				}),
				idempotency: new DeliveryIdempotencyStore({
					maxEntries: 16,
					nowMs: () => nowMsValue,
				}),
				nowSeconds: () => Math.floor(nowMsValue / 1000),
				timers: createTimeoutSeam(),
				parseJson: (text) => JSON.parse(text) as Record<string, unknown>,
				runAgentTurn: async () => null,
			}),
			completions: {} as never,
			runs: new RunRegistry(),
			bodyCapBytes: 64 * 1024,
			// NO apiKeyProvider ⇒ ungated runs lanes.
		});
		const baseUrl = await server.listen();
		try {
			const res = await fetch(`${baseUrl}/v1/runs`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"x-hermes-session-key": "sneaky-scope",
				},
				body: JSON.stringify({ input: "anonymous scope grab" }),
			});
			expect(res.status).toBe(403);
			const body = (await res.json()) as {
				error: { type: string; message: string };
			};
			expect(body.error.type).toBe("gateway_auth_error");
			expect(body.error.message).toMatch(/requires API key authentication/);
		} finally {
			await server.close();
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

// ── /v1/chat/completions response contract (webhook-47/48/51) ────────────

describe("completions lane contract", () => {
	const BEARER = { authorization: `Bearer ${API_KEY}` };

	function postCompletions(
		h: E2EHarness,
		body: Record<string, unknown>,
		headers: Record<string, string> = {},
	): Promise<Response> {
		return fetch(`${h.baseUrl}/v1/chat/completions`, {
			method: "POST",
			headers: { "content-type": "application/json", ...BEARER, ...headers },
			body: JSON.stringify(body),
		});
	}

	it("non-streaming responses carry created/model/usage/finish_reason (webhook-47)", async () => {
		const h = await makeE2E();
		try {
			const res = await postCompletions(h, {
				model: "pi-gateway",
				messages: [{ role: "user", content: "completions contract" }],
				stream: false,
			});
			expect(res.status).toBe(200);
			expect(res.headers.get("x-hermes-session-id")).toBeTruthy();
			const body = (await res.json()) as {
				id: string;
				object: string;
				created: number;
				model: string;
				choices: Array<{
					index: number;
					message: { role: string; content: string };
					finish_reason: string;
				}>;
				usage: {
					prompt_tokens: number;
					completion_tokens: number;
					total_tokens: number;
				};
			};
			expect(body.object).toBe("chat.completion");
			expect(body.id.startsWith("chatcmpl-")).toBe(true);
			expect(Number.isFinite(body.created)).toBe(true);
			expect(body.model).toBe("pi-gateway"); // caller's model echoed
			expect(body.choices[0]?.finish_reason).toBe("stop");
			expect(body.choices[0]?.message).toEqual({
				role: "assistant",
				content: "reply:completions contract",
			});
			expect(body.usage).toEqual({
				prompt_tokens: 0,
				completion_tokens: 0,
				total_tokens: 0,
			});
		} finally {
			await h.close();
		}
	});

	it("stream=true rides SSE chat.completion.chunk frames ending data: [DONE] (webhook-48)", async () => {
		const h = await makeE2E();
		try {
			const res = await postCompletions(h, {
				model: "pi-gateway",
				messages: [{ role: "user", content: "stream me" }],
				stream: true,
			});
			expect(res.status).toBe(200);
			expect(res.headers.get("content-type")).toContain("text/event-stream");
			const text = await res.text();
			const dataLines = text
				.split("\n\n")
				.filter((frame) => frame.startsWith("data: "))
				.map((frame) => frame.slice("data: ".length));
			expect(dataLines[dataLines.length - 1]).toBe("[DONE]");
			const chunks = dataLines
				.slice(0, -1)
				.map((line) => JSON.parse(line) as Record<string, unknown>);
			for (const chunk of chunks) {
				expect(chunk["object"]).toBe("chat.completion.chunk");
				expect(String(chunk["id"]).startsWith("chatcmpl-")).toBe(true);
			}
			// Role chunk first…
			expect(chunks[0]).toMatchObject({
				choices: [{ delta: { role: "assistant" }, finish_reason: null }],
			});
			// …content chunk(s) in the middle…
			const contentChunk = chunks.find(
				(c) =>
					(c as { choices?: Array<{ delta?: { content?: string } }> })
						.choices?.[0]?.delta?.content !== undefined,
			);
			expect(
				(
					contentChunk as {
						choices: Array<{ delta: { content: string } }>;
					}
				).choices[0]?.delta.content,
			).toBe("reply:stream me");
			// …finish chunk LAST with the usage block.
			const finish = chunks[chunks.length - 1] as {
				choices: Array<{
					delta: Record<string, unknown>;
					finish_reason: string;
				}>;
				usage?: Record<string, unknown>;
			};
			expect(finish.choices[0]?.finish_reason).toBe("stop");
			expect(finish.usage).toEqual({
				prompt_tokens: 0,
				completion_tokens: 0,
				total_tokens: 0,
			});
		} finally {
			await h.close();
		}
	});

	it("Idempotency-Key replays ONLY on request-fingerprint match (webhook-51)", async () => {
		const h = await makeE2E();
		try {
			const bodyA = {
				model: "pi-gateway",
				messages: [{ role: "user", content: "idem probe A" }],
			};
			const first = await postCompletions(h, bodyA, {
				"idempotency-key": "key-1",
			});
			expect(first.status).toBe(200);
			const firstJson = (await first.json()) as {
				choices: Array<{ message: { content: string } }>;
			};

			// SAME key + SAME body → cached replay.
			const replay = await postCompletions(h, bodyA, {
				"idempotency-key": "key-1",
			});
			expect(await replay.json()).toEqual(firstJson);

			// SAME key + DIFFERENT body → fingerprint mismatch ⇒ FRESH compute,
			// never a stale replay of the first answer.
			const different = await postCompletions(
				h,
				{
					model: "pi-gateway",
					messages: [{ role: "user", content: "idem probe B" }],
				},
				{ "idempotency-key": "key-1" },
			);
			expect(different.status).toBe(200);
			const differentJson = (await different.json()) as {
				choices: Array<{ message: { content: string } }>;
			};
			expect(differentJson.choices[0]?.message.content).toBe(
				"reply:idem probe B",
			);
		} finally {
			await h.close();
		}
	});
});

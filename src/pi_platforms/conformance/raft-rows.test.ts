// CONFORMANCE WIRING — the raft wake-channel census port vs the executable
// 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built RaftSubject. Applicability is COMPUTED from capability data
//      (04 §8 conditional headers): the streaming family applies only when
//      supportsDraftStreaming() holds — a wake-only channel has no native
//      lanes, so those three rows are excluded BY THE PROBE, never by a
//      hardcoded skip.
//   2. The INHERITED webhook transport rows (reference-fixture inheritance,
//      roadmap §Phase 6 heuristic 2) run over the REAL adapter probes:
//      stateless flag pairing (manifest DIVERGENCE note in manifest.ts) +
//      DEC-017 trust-boundary completeness + bounded-window answer measured
//      while the wake endpoint answers synchronously inside the bridge's
//      HTTP window.
//   3. Fresh raft shape-delta rows execute through the REAL engine fixture:
//      token auth matrix (refuses-closed pre-connect, constant-time compare),
//      wake verdict ladder (413 caps, invalid_json, invalid_payload,
//      content_not_allowed), the CONTENT-FREE contract (recursive scan),
//      not-ready 503 boundary, activity validation matrix + bounded queue +
//      drain/drop accounting, unauthenticated health topology, deterministic
//      token auto-generation, bridge spawn plan as pure data, and the
//      zero-outbound-call no-op send posture.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a content-free-gate-defeating mutant fixture fails
//      ITS OWN named row.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { makeRaftSubject, type RaftSubject } from "../raft/raft-subject.js";
import { FIXTURE_RAFT_PROFILE } from "../raft/raft-subject.js";
import { RaftAdapter } from "../raft/raft-adapter.js";
import { RAFT_BODY_CAP_BYTES } from "../raft/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeRaftSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

interface RaftWorld {
	subject: RaftSubject;
	adapter: RaftAdapter;
	scheduler: ManualScheduler;
}

/** A connected engine + attached guard: the ready-to-serve posture. */
function makeWorld(opts: { attachGuard?: boolean } = {}): RaftWorld {
	const scheduler = new ManualScheduler();
	const subject = makeRaftSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
	});
	if (opts.attachGuard !== false) {
		void subject; // the subject constructor already attaches the standard guard
	}
	return { subject, adapter: subject.adapter, scheduler };
}

// ── raft shape-delta rows (executed over the REAL engine) ───────────────────

function raftDeltaRows(newEngine: () => RaftAdapter): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: RaftAdapter) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["webhook"]),
		run: async () => {
			const fx = newEngine();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["webhook"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"transport.raft.wake-auth-matrix",
			"raft: every wake/activity/drain endpoint refuses closed BEFORE connect (no configured token ⇒ 401 even for a presented credential); after connect ONLY the generated token admits; wrong/missing header stays 401",
			async (fx) => {
				const presented = { "x-raft-bridge-token": "attacker-guess" };
				// Pre-connect: nothing is configured — refuse CLOSED.
				const preBody = Buffer.from("{}");
				expect(
					(await fx.handleWakePost({ headers: presented, rawBody: preBody }))
						.status,
				).toBe(401);
				expect(
					fx.handleActivityPost({ headers: presented, rawBody: preBody })
						.status,
				).toBe(401);
				expect(
					fx.handleActivityDrainGet({
						headers: presented,
						rawBody: Buffer.alloc(0),
					}).status,
				).toBe(401);
				expect(fx.counters.unauthorized).toBe(3);

				await fx.connect({ isReconnect: false });
				const live = fx.clientToken as string;

				// Wrong credential still refuses AFTER connect.
				expect(
					(await fx.handleWakePost({ headers: presented, rawBody: preBody }))
						.status,
				).toBe(401);
				// Missing header refuses.
				expect(
					(await fx.handleWakePost({ headers: {}, rawBody: preBody })).status,
				).toBe(401);
				// The exact generated token admits past the gate.
				const ok = await fx.handleWakePost({
					headers: { "x-raft-bridge-token": live },
					rawBody: preBody,
				});
				expect(ok.status).toBe(202);
			},
		),
		mk(
			"transport.raft.wake-verdict-ladder",
			"raft: lying Content-Length > cap ⇒ 413 without reading; actual oversized body ⇒ 413; malformed JSON ⇒ 400 invalid_json; non-object payload ⇒ 400 invalid_payload; accepted hint ⇒ 202 {ok:true,runtimeSession}",
			async (fx) => {
				await fx.connect({ isReconnect: false });
				const auth = { "x-raft-bridge-token": fx.clientToken as string };

				// Declared-length cap fires on the HEADER before any body read —
				// an empty body with a lying length still gets 413.
				const lying = await fx.handleWakePost({
					headers: {
						...auth,
						"content-length": String(RAFT_BODY_CAP_BYTES + 1),
					},
					rawBody: Buffer.from("{}"),
				});
				expect(lying.status).toBe(413);

				// Actual-bytes cap: small declared length, oversized bytes.
				const oversized = Buffer.alloc(RAFT_BODY_CAP_BYTES + 1, 0x61);
				const fat = await fx.handleWakePost({
					headers: { ...auth, "content-length": String(16) },
					rawBody: oversized,
				});
				expect(fat.status).toBe(413);
				expect(fat.body).toEqual({ ok: false, error: "payload_too_large" });

				// Malformed JSON.
				const badJson = await fx.handleWakePost({
					headers: auth,
					rawBody: Buffer.from("{not json"),
				});
				expect(badJson.status).toBe(400);
				expect(badJson.body).toEqual({ ok: false, error: "invalid_json" });

				// Non-object payloads.
				for (const shape of ["[1,2]", "42", '"str"', "null"]) {
					const resp = await fx.handleWakePost({
						headers: auth,
						rawBody: Buffer.from(shape),
					});
					expect(resp.status).toBe(400);
					expect(resp.body).toEqual({ ok: false, error: "invalid_payload" });
				}

				// Empty-body hint accepted → 202 with runtimeSession echo.
				const ok = await fx.handleWakePost({
					headers: auth,
					rawBody: Buffer.alloc(0),
				});
				expect(ok.status).toBe(202);
				expect(ok.body).toEqual({
					ok: true,
					runtimeSession: fx.runtimeSession,
				});
			},
		),
		mk(
			"transport.raft.content-free-contract",
			"raft: ANY content-shaped field (top-level, nested object, or inside arrays) rejects the wake 400 content_not_allowed BEFORE dispatch; field names match case-insensitively; schema fields never gate",
			async (fx) => {
				await fx.connect({ isReconnect: false });
				const auth = { "x-raft-bridge-token": fx.clientToken as string };

				for (const body of [
					JSON.stringify({ text: "hello agent" }),
					JSON.stringify({ eventId: "e1", meta: { message: "smuggled" } }),
					JSON.stringify({ eventId: "e2", trail: [{}, { preview: "p" }] }),
					JSON.stringify({ eventId: "e3", Content: "case-insensitive" }),
					JSON.stringify({ snippet: " s " }),
				]) {
					const resp = await fx.handleWakePost({
						headers: auth,
						rawBody: Buffer.from(body),
					});
					expect(resp.status).toBe(400);
					expect(resp.body).toEqual({
						ok: false,
						error: "content_not_allowed",
					});
				}
				expect(fx.counters.wakesRejectedContent).toBe(5);
				// NOTHING reached the pipeline.
				expect(fx.dispatchedEvents).toHaveLength(0);

				// Deep-but-clean payloads still pass.
				const clean = await fx.handleWakePost({
					headers: auth,
					rawBody: Buffer.from(
						JSON.stringify({
							eventId: "evt-9",
							nested: { deeper: [{ fine: "value", other: 7 }] },
						}),
					),
				});
				expect(clean.status).toBe(202);
				expect(fx.dispatchedEvents).toHaveLength(1);
				expect(fx.dispatchedEvents[0]?.messageId).toBe("evt-9");
				// The dispatched turn is the FIXED wake prompt — never body text.
				expect(fx.dispatchedEvents[0]?.text).toContain(
					"Raft wake hint received",
				);
			},
		),
		mk(
			"transport.raft.not-ready-503-boundary",
			"raft: a wake arriving before the gateway handler attaches answers 503 not_ready (runtimeSession echoed) and dispatches NOTHING; attaching flips the verdict to 202",
			async () => {
				const scheduler = new ManualScheduler();
				const bare = new RaftAdapter({
					secretReader: () => FIXTURE_RAFT_PROFILE,
				});
				await bare.connect({ isReconnect: false });
				const auth = { "x-raft-bridge-token": bare.clientToken as string };
				const early = await bare.handleWakePost({
					headers: auth,
					rawBody: Buffer.from(JSON.stringify({ eventId: "early-1" })),
				});
				expect(early.status).toBe(503);
				expect(early.body).toEqual({
					ok: false,
					error: "not_ready",
					runtimeSession: bare.runtimeSession,
				});
				expect(bare.counters.notReady).toBe(1);

				bare.attachStandardGuard(scheduler.spawner);
				const late = await bare.handleWakePost({
					headers: auth,
					rawBody: Buffer.from(JSON.stringify({ eventId: "late-1" })),
				});
				expect(late.status).toBe(202);
				expect(bare.dispatchedEvents.map((e) => e.messageId)).toEqual([
					"late-1",
				]);
			},
		),
		mk(
			"transport.raft.activity-validation-matrix",
			"raft: activity events validate against the closed vocabulary — wrong schema / unknown field / unsafe scalar / bad status / negative-or-boolean durationMs each reject 400 naming the fault; valid events enqueue 202; over-cap toolInput truncates WITH flags",
			async (fx) => {
				await fx.connect({ isReconnect: false });
				const auth = { "x-raft-bridge-token": fx.clientToken as string };
				const post = (body: string) =>
					fx.handleActivityPost({
						headers: auth,
						rawBody: Buffer.from(body),
					});

				const base = {
					schema: "raft-activity.v1",
					eventId: "act-1",
					sessionId: "sess-1",
					hookEventName: "PreToolUse",
					status: "ok",
					occurredAt: "2026-08-25T00:00:00.000Z",
				};

				// Valid event enqueues.
				let resp = await post(JSON.stringify(base));
				expect(resp.status).toBe(202);
				expect(fx.activityQueue.size).toBe(1);

				// Wrong schema.
				resp = await post(JSON.stringify({ ...base, schema: "other.v9" }));
				expect(resp.status).toBe(400);
				expect(resp.body).toEqual({
					ok: false,
					error: "unsupported activity event schema",
				});

				// Unknown field names the offender.
				resp = await post(JSON.stringify({ ...base, sneakyField: "x" }));
				expect(resp.status).toBe(400);
				expect(resp.body).toEqual({
					ok: false,
					error: "activity event field sneakyField is not allowed",
				});

				// Unsafe scalar: charset violation AND over-length.
				resp = await post(
					JSON.stringify({ ...base, sessionId: "bad\nnewline" }),
				);
				expect(resp.status).toBe(400);
				resp = await post(
					JSON.stringify({ ...base, eventId: "x".repeat(121) }),
				);
				expect(resp.status).toBe(400);

				// Status must be ok|error.
				resp = await post(JSON.stringify({ ...base, status: "warn" }));
				expect(resp.status).toBe(400);

				// durationMs: negative number and boolean both refuse.
				resp = await post(JSON.stringify({ ...base, durationMs: -5 }));
				expect(resp.status).toBe(400);
				resp = await post(JSON.stringify({ ...base, durationMs: true }));
				expect(resp.status).toBe(400);

				// Over-cap toolInput TRUNCATES (never rejects) and flags it.
				resp = await post(
					JSON.stringify({
						...base,
						toolInput: "z".repeat(5000),
					}),
				);
				expect(resp.status).toBe(202);
				const drained = fx.activityQueue.drain(10);
				const events = drained["events"] as Array<Record<string, unknown>>;
				const truncatedEvent = events[events.length - 1] as Record<
					string,
					unknown
				>;
				expect(String(truncatedEvent["toolInput"]).length).toBe(4096);
				expect(truncatedEvent["toolInputTruncated"]).toBe(true);
				expect(truncatedEvent["truncated"]).toBe(true);
			},
		),
		mk(
			"transport.raft.activity-bounded-queue-drain",
			"raft: telemetry overflow drops OLDEST and counts drops; drain(max) returns FIFO up to max, reports+resets dropped, keeps the remainder; ?max junk falls back to the default clamp",
			async (fx) => {
				await fx.connect({ isReconnect: false });
				const auth = { "x-raft-bridge-token": fx.clientToken as string };
				const mkEvent = (n: number): string =>
					JSON.stringify({
						schema: "raft-activity.v1",
						eventId: `drop-${String(n).padStart(4, "0")}`,
						sessionId: "s",
						hookEventName: "Stop",
						status: "ok",
						occurredAt: "2026-08-25T00:00:00.000Z",
					});

				// Overflow the 500-cap queue by two.
				for (let i = 0; i < 502; i += 1) {
					expect(fx.reportActivity(JSON.parse(mkEvent(i)))).toBe(true);
				}
				expect(fx.activityQueue.size).toBe(500);

				// Partial drain honors ?max=1 and preserves FIFO order of the rest.
				const one = fx.handleActivityDrainGet({
					headers: auth,
					query: { max: "1" },
					rawBody: Buffer.alloc(0),
				});
				const oneBody = one.body as Record<string, unknown>;
				expect(oneBody["schema"]).toBe("raft-activity-drain.v1");
				expect((oneBody["events"] as unknown[]).length).toBe(1);
				expect(oneBody["dropped"]).toBe(2); // reported once, then reset

				// Default clamp: 499 events remain, drain takes exactly 200.
				const again = fx.handleActivityDrainGet({
					headers: auth,
					query: {},
					rawBody: Buffer.alloc(0),
				});
				const againBody = again.body as Record<string, unknown>;
				expect((againBody["events"] as unknown[]).length).toBe(200);
				expect(againBody["dropped"]).toBe(0); // counter was reset by the last drain
				expect(fx.activityQueue.size).toBe(299);

				// Junk ?max falls back to the same default clamp — refill past the
				// cap first (the overflow drops the OLDEST 99 and counts them).
				for (let i = 0; i < 300; i += 1) {
					fx.reportActivity(JSON.parse(mkEvent(1000 + i)));
				}
				expect(fx.activityQueue.size).toBe(500);
				const junk = fx.handleActivityDrainGet({
					headers: auth,
					query: { max: "banana" },
					rawBody: Buffer.alloc(0),
				});
				const junkBody = junk.body as Record<string, unknown>;
				expect((junkBody["events"] as unknown[]).length).toBe(200);
				expect(junkBody["dropped"]).toBe(99);

				// Python-int parity edge: a negative ?max floors at the drain's own
				// limit=1 clamp instead of falling back to the default.
				fx.reportActivity({
					schema: "raft-activity.v1",
					eventId: "neg-max",
					sessionId: "s",
					hookEventName: "Stop",
					status: "ok",
					occurredAt: "2026-08-25T00:00:00.000Z",
				});
				const negative = fx.handleActivityDrainGet({
					headers: auth,
					query: { max: "-5" },
					rawBody: Buffer.alloc(0),
				});
				expect(
					((negative.body as Record<string, unknown>)["events"] as unknown[])
						.length,
				).toBe(1);
			},
		),
		mk(
			"transport.raft.health-unauthenticated-shape",
			"raft: /health serves topology WITHOUT auth (Hermes parity) — status/platform/runtimeSession plus the activity queue size and endpoint map",
			async (fx) => {
				const health = fx.handleHealthGet();
				expect(health.status).toBe(200);
				expect(health.body).toEqual({
					status: "ok",
					platform: "raft",
					runtimeSession: fx.runtimeSession,
					activity: {
						queueSize: 0,
						endpoint: "/activity",
						drainEndpoint: "/activity/drain",
					},
				});
				// Queue size is LIVE data, not a static zero.
				fx.reportActivity({
					schema: "raft-activity.v1",
					eventId: "h1",
					sessionId: "s",
					hookEventName: "Stop",
					status: "ok",
					occurredAt: "2026-08-25T00:00:00.000Z",
				});
				expect(
					(
						(fx.handleHealthGet().body as Record<string, unknown>)[
							"activity"
						] as Record<string, unknown>
					)["queueSize"],
				).toBe(1);
			},
		),
		mk(
			"transport.raft.token-autogeneration-parity",
			"raft: connect() auto-generates the bridge token through the injected entropy source when none is configured; reconnect keeps the SAME token; an explicitly configured token is never replaced",
			async () => {
				let generations = 0;
				const generated = new RaftAdapter({
					secretReader: () => FIXTURE_RAFT_PROFILE,
					tokenHex: () => {
						generations += 1;
						return `gen-${String(generations).padStart(2, "0")}`;
					},
				});
				expect(generated.clientToken).toBeUndefined();
				await generated.connect({ isReconnect: false });
				expect(generated.clientToken).toBe("gen-01");
				await generated.connect({ isReconnect: true });
				expect(generated.clientToken).toBe("gen-01"); // no regeneration
				expect(generations).toBe(1);

				const configured = new RaftAdapter({
					secretReader: () => FIXTURE_RAFT_PROFILE,
					config: { bridge_token: "operator-set" },
					tokenHex: () => {
						throw new Error("must not generate over a configured token");
					},
				});
				await configured.connect({ isReconnect: false });
				expect(configured.clientToken).toBe("operator-set");
			},
		),
		mk(
			"transport.raft.bridge-spawn-plan-data",
			"raft: the CLI bridge spawn contract exists as PURE DATA (the port spawns NO OS children) — exact argv + RAFT_CHANNEL_TOKEN env carrier when RAFT_PROFILE resolves; null (wake-only mode) when it does not",
			async (fx) => {
				const plan = fx.bridgeSpawnPlan("http://127.0.0.1:0/wake");
				expect(plan).not.toBeNull();
				expect(plan?.argv).toEqual([
					"raft",
					"--profile",
					FIXTURE_RAFT_PROFILE,
					"agent",
					"bridge",
					"--wake-adapter",
					"wake-channel",
					"--wake-channel-endpoint",
					"http://127.0.0.1:0/wake",
				]);
				expect(plan?.tokenEnvVar).toBe("RAFT_CHANNEL_TOKEN");

				const profileless = new RaftAdapter({
					secretReader: () => undefined,
				});
				expect(
					profileless.bridgeSpawnPlan("http://127.0.0.1:0/wake"),
				).toBeNull();
			},
		),
		mk(
			"transport.raft.send-noop-zero-outbound-posture",
			"raft: adapter.send is a documented no-op that ALWAYS succeeds with ZERO outbound wire calls — the agent delivers via the raft CLI; both egress doors record through the audited chokepoint without ever leaving the process",
			async (fx) => {
				await fx.connect({ isReconnect: false });
				const result = await fx.send("default", "anything at all");
				expect(result.success).toBe(true);
				expect(fx.counters.outboundWireCalls).toBe(0);
				// Door 2 shares the same posture.
				const door2 = await fx.sendForPlatform("raft", "default", "also here");
				expect(door2.success).toBe(true);
				expect(fx.counters.outboundWireCalls).toBe(0);
				// The chokepoint still AUDITS both admissions (DEC-006 property).
				const audit = fx.doorAudit();
				expect(audit.filter((e) => e.door === "send").length).toBe(1);
				expect(audit.filter((e) => e.door === "send_for_platform").length).toBe(
					1,
				);
			},
		),
	];
}

// ── the suites ──────────────────────────────────────────────────────────────

describe("raft conformance (04 §8 merge gate)", () => {
	it("SHARED applicable rows pass for shape=webhook (streaming family excluded BY THE PROBE)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported, excludedIds } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));
		// The probe must genuinely exclude on this wake-only lane.
		expect(streamsSupported).toBe(false);

		const report = await runConformanceSuite({
			subjectName: "raft",
			shape: "webhook",
			rows: shared,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		// Every non-excluded shared row actually RAN (no silent skip).
		expect(report.rows.length).toBe(all.length - excludedIds.length);
	}, 60_000);

	it("INHERITED webhook transport rows pass over the REAL adapter probes", async () => {
		const world = makeWorld();
		const probe = world.subject.flagsAndTrustProbe();

		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				// The wake endpoint answers synchronously INSIDE the bridge's HTTP
				// window: measure one full accept round-trip.
				await world.adapter.connect({ isReconnect: false });
				const started = Date.now();
				const resp = await world.adapter.handleWakePost({
					headers: {
						"x-raft-bridge-token": world.adapter.clientToken as string,
					},
					rawBody: Buffer.from(JSON.stringify({ eventId: "bw-1" })),
				});
				expect(resp.status).toBe(202);
				return {
					answeredWithinWindowMs: Date.now() - started,
					windowCapMs: 5_000,
				};
			},
		});

		const report = await runConformanceSuite({
			subjectName: "raft-transport",
			shape: "webhook",
			rows: transport,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("raft SHAPE DELTA rows pass through the REAL engine", async () => {
		function freshEngine(): RaftAdapter {
			const scheduler = new ManualScheduler();
			const adapter = new RaftAdapter({
				secretReader: () => FIXTURE_RAFT_PROFILE,
				tokenHex: () => "delta-engine-token",
			});
			adapter.attachStandardGuard(scheduler.spawner);
			void scheduler;
			return adapter;
		}
		const rows = raftDeltaRows(freshEngine);
		const report = await runConformanceSuite({
			subjectName: "raft-deltas",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported, excludedIds } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));

		const world = makeWorld();
		const probe = world.subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				await world.adapter.connect({ isReconnect: false });
				const started = Date.now();
				const resp = await world.adapter.handleWakePost({
					headers: {
						"x-raft-bridge-token": world.adapter.clientToken as string,
					},
					rawBody: Buffer.from(JSON.stringify({ eventId: "full-bw-1" })),
				});
				expect(resp.status).toBe(202);
				return {
					answeredWithinWindowMs: Date.now() - started,
					windowCapMs: 5_000,
				};
			},
		});
		const deltas = raftDeltaRows(() => {
			const scheduler = new ManualScheduler();
			const adapter = new RaftAdapter({
				secretReader: () => FIXTURE_RAFT_PROFILE,
				tokenHex: () => "full-catalog-token",
			});
			adapter.attachStandardGuard(scheduler.spawner);
			return adapter;
		});

		const report = await runConformanceSuite({
			subjectName: "raft-full",
			shape: "webhook",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 60_000);

	it("the gate DETECTS violations: a content-free-gate-defeating mutant fails its own named row", async () => {
		// Mutant: the content-free gate is DEFEATED — bodies that carry a
		// content-shaped field are quietly rewritten into clean hints (as if
		// _has_content_field were stubbed to always-false) while every other
		// input passes through UNTOUCHED. The content-free-contract row must
		// fail BY NAME, and ONLY that row.
		const CONTENT_KEYS = new Set([
			"body",
			"content",
			"message",
			"messages",
			"preview",
			"snippet",
			"text",
		]);
		function carriesContent(value: unknown): boolean {
			if (Array.isArray(value)) return value.some(carriesContent);
			if (value !== null && typeof value === "object") {
				for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
					if (CONTENT_KEYS.has(k.toLowerCase()) || carriesContent(v))
						return true;
				}
			}
			return false;
		}
		function mutantEngine(): RaftAdapter {
			const scheduler = new ManualScheduler();
			const adapter = new RaftAdapter({
				secretReader: () => FIXTURE_RAFT_PROFILE,
				tokenHex: () => "mutant-token",
			});
			adapter.attachStandardGuard(scheduler.spawner);
			const original = adapter.handleWakePost.bind(adapter);
			Object.defineProperty(adapter, "handleWakePost", {
				value: async (input: Parameters<typeof original>[0]) => {
					let rawBody = input.rawBody;
					try {
						const parsed: unknown = JSON.parse(rawBody.toString("utf8"));
						if (carriesContent(parsed)) {
							// THE LIE: admit the smuggled body by scrubbing it.
							rawBody = Buffer.from('{"eventId":"mutant-admitted"}');
						}
					} catch {
						/* untouched — non-JSON keeps its own verdict */
					}
					return original({ ...input, rawBody });
				},
			});
			return adapter;
		}

		const rows = raftDeltaRows(mutantEngine);
		const target = rows.find(
			(r) => r.id === "transport.raft.content-free-contract",
		);
		expect(target).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-raft-content-free",
			shape: "webhook",
			rows: [target as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh engines.
		const others = rows.filter((r) => r.id !== target?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-raft-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 60_000);
});

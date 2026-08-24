// Verify-stage behavior contracts (08 §8): settled window ONLY after actual
// restarts (injected clock); fleet identity sweep states; stale ⇒ escalation;
// unknown never fails. Real-process liveness contracts live in the
// two-process suites.

import { describe, expect, it } from "vitest";
import { ManualClock } from "./testing/manual-clock.js";
import {
	collectFleetVersions,
	fleetHasStaleGateway,
	formatFleetVersionMatrix,
	verifyStage,
	type FleetEntry,
} from "./verify.js";

const NEW_SHA = "a".repeat(40);
const OLD_SHA = "b".repeat(40);

function statusView(pid: number, sha: string | null) {
	return { pid, code_sha: sha, code_version: sha === null ? null : "1.0.0" };
}

describe("collectFleetVersions", () => {
	const homes = [
		{ profile: "default", home: "/h/default" },
		{ profile: "work", home: "/h/work" },
	];

	it("classifies current/stale against the expected sha", () => {
		const fleet = collectFleetVersions(homes, {
			expectedSha: NEW_SHA,
			liveness: () => true,
			readStatus: (home) =>
				home.endsWith("default")
					? statusView(101, NEW_SHA)
					: statusView(202, OLD_SHA),
		});
		expect(fleet).toEqual([
			{
				profile: "default",
				pid: 101,
				codeSha: NEW_SHA,
				codeVersion: "1.0.0",
				state: "current",
			},
			{
				profile: "work",
				pid: 202,
				codeSha: OLD_SHA,
				codeVersion: "1.0.0",
				state: "stale",
			},
		]);
	});

	it("skips homes with dead PIDs or no runtime record at all", () => {
		const fleet = collectFleetVersions(
			homes.concat([{ profile: "ghost", home: "/h/ghost" }]),
			{
				expectedSha: NEW_SHA,
				liveness: (pid) => pid !== 303,
				readStatus: (home) => {
					if (home.endsWith("default")) return statusView(101, NEW_SHA);
					if (home.endsWith("work")) return statusView(303, OLD_SHA);
					return null; // ghost: never started
				},
			},
		);
		expect(fleet.map((e) => e.profile)).toEqual(["default"]);
	});

	it("reports unknown — not stale — when either side lacks a stamp to compare", () => {
		const fleet = collectFleetVersions(homes, {
			expectedSha: NEW_SHA,
			liveness: () => true,
			readStatus: (home) =>
				home.endsWith("default")
					? statusView(101, null)
					: statusView(202, OLD_SHA),
		});
		expect(fleet[0]?.state).toBe("unknown");
		expect(
			collectFleetVersions(homes, {
				expectedSha: null, // updater could not resolve its own HEAD
				liveness: () => true,
				readStatus: () => statusView(101, OLD_SHA),
			})[0]?.state,
		).toBe("unknown");
	});

	it("never raises on a throwing reader — probe failures yield empty list", () => {
		const fleet = collectFleetVersions(homes, {
			expectedSha: NEW_SHA,
			readStatus: () => {
				throw new Error("disk gone");
			},
		});
		expect(fleet).toEqual([]);
	});
});

describe("fleetHasStaleGateway / matrix rendering", () => {
	it("escalates ONLY on provable staleness; unknown entries never fail the run", () => {
		const fleet: FleetEntry[] = [
			{
				profile: "a",
				pid: 1,
				codeSha: OLD_SHA,
				codeVersion: null,
				state: "stale",
			},
			{
				profile: "b",
				pid: 2,
				codeSha: null,
				codeVersion: null,
				state: "unknown",
			},
		];
		expect(fleetHasStaleGateway(fleet)).toBe(true);
		expect(fleetHasStaleGateway([fleet[1] as FleetEntry])).toBe(false);
		expect(fleetHasStaleGateway([])).toBe(false);
	});

	it("renders the version matrix with display-truncated shas", () => {
		const lines = formatFleetVersionMatrix([
			{
				profile: "a",
				pid: 11,
				codeSha: NEW_SHA,
				codeVersion: null,
				state: "current",
			},
			{
				profile: "b",
				pid: 22,
				codeSha: OLD_SHA,
				codeVersion: null,
				state: "stale",
			},
		]);
		expect(lines.some((l) => l.includes("✓ a (pid 11) @ aaaaaaaa"))).toBe(true);
		expect(lines.some((l) => l.includes("STALE"))).toBe(true);
		expect(lines.some((l) => l.startsWith("⚠"))).toBe(true);
	});
});

describe("verifyStage settle window (08 §8 verified condition)", () => {
	it("settles ~2s ONLY when something was actually restarted", async () => {
		const clock = new ManualClock();
		await verifyStage({
			homes: [],
			expectedSha: NEW_SHA,
			restartedSomething: false,
			clock,
		});
		expect(clock.sleeps).toEqual([]); // nothing restarted ⇒ NO settle window

		await verifyStage({
			homes: [],
			expectedSha: NEW_SHA,
			restartedSomething: true,
			clock,
		});
		expect(clock.sleeps).toEqual([2000]); // parity of the ~2s settle window
	});

	it("honors an injected settle window and skips it at zero", async () => {
		const clock = new ManualClock();
		await verifyStage({
			homes: [],
			expectedSha: NEW_SHA,
			restartedSomething: true,
			clock,
			settleWindowMs: 500,
		});
		expect(clock.sleeps).toEqual([500]);

		await verifyStage({
			homes: [],
			expectedSha: NEW_SHA,
			restartedSomething: true,
			clock,
			settleWindowMs: 0,
		});
		expect(clock.sleeps).toEqual([500]);
	});

	it("returns fleet + verdict in one stage result", async () => {
		const clock = new ManualClock();
		const result = await verifyStage({
			homes: [{ profile: "work", home: "/h/work" }],
			expectedSha: NEW_SHA,
			restartedSomething: false,
			clock,
			readStatus: () => statusView(202, OLD_SHA),
			liveness: () => true,
		});
		expect(result.anyStale).toBe(true);
		expect(result.fleet[0]?.state).toBe("stale");
		expect(result.settledMs).toBe(0);
		expect(result.matrixLines.length).toBeGreaterThan(0);
	});
});

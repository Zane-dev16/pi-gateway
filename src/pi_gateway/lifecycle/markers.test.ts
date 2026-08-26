// Behavior contracts for the external drain-request marker (08 §1.2;
// gateway/drain_control.py port): epoch + max-age staleness with LENIENT
// semantics — only a DEFINITE prior-epoch match or a parseable, definitely-
// too-old timestamp is ignored; malformed/contentless markers stay ACTIVE
// (fail-safe toward quiescing); reading never raises.

import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	DRAIN_REQUEST_MAX_AGE_SECONDS,
	clearDrainRequest,
	computeInstantiationEpoch,
	drainMarkerIsStale,
	drainRequestPath,
	drainRequested,
	readDrainRequest,
	resetInstantiationEpochCache,
	writeDrainRequest,
} from "./markers.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-drainreq-"));
	resetInstantiationEpochCache();
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
	resetInstantiationEpochCache();
});

const NOW = 1_750_000_000_000;
const EPOCH = "boot-id:pid1-start";

describe("instantiation epoch (drain_control.py current_instantiation_epoch)", () => {
	it("composes kernel boot id + PID 1 start time", () => {
		const epoch = computeInstantiationEpoch((p) =>
			p === "/proc/sys/kernel/random/boot_id"
				? "boot-id\n"
				: p === "/proc/1/stat"
					? // starttime is tail token index 19 after the LAST ')'.
						`1 (init) S ${"0 ".repeat(18)}12345 other`
					: null,
		);
		expect(epoch).toBe("boot-id:12345");
	});

	it("returns '' when neither source is readable (presence-only fallback)", () => {
		expect(computeInstantiationEpoch(() => null)).toBe("");
	});
});

describe("write/read/clear roundtrip", () => {
	it("writes the full contract payload atomically and reads it back", () => {
		expect(writeDrainRequest(home, { epoch: EPOCH, nowMs: () => NOW })).toBe(
			true,
		);
		const body = readDrainRequest(home);
		expect(body?.["action"]).toBe("drain");
		expect(body?.["principal"]).toBe("drain-control");
		expect(body?.["epoch"]).toBe(EPOCH);
		expect(body?.["suppress_notification"]).toBe(false);

		// Idempotent re-write refreshes requested_at (sanctioned keep-alive).
		expect(
			writeDrainRequest(home, { epoch: EPOCH, nowMs: () => NOW + 5000 }),
		).toBe(true);
		expect(readDrainRequest(home)?.["requested_at"]).toBe(
			new Date(NOW + 5000).toISOString(),
		);

		expect(clearDrainRequest(home)).toBe(true);
		expect(existsSync(drainRequestPath(home))).toBe(false);
		expect(clearDrainRequest(home)).toBe(false); // cancel is idempotent
	});
});

describe("drainRequested staleness contract (NS-570 + #85433)", () => {
	it("same-epoch marker within max-age ⇒ DRAINING (honoured)", () => {
		writeDrainRequest(home, { epoch: EPOCH, nowMs: () => NOW });
		expect(
			drainRequested(home, { epoch: EPOCH, nowMs: () => NOW + 1000 }),
		).toBe(true);
	});

	it("PRIOR-EPOCH marker (survived a machine restart) is ignored leniently", () => {
		writeDrainRequest(home, { epoch: "old-boot:old-init", nowMs: () => NOW });
		expect(
			drainRequested(home, { epoch: EPOCH, nowMs: () => NOW + 1000 }),
		).toBe(false);
		// …and NOT deleted — another instantiation's marker is not ours to drop.
		expect(existsSync(drainRequestPath(home))).toBe(true);
	});

	it("marker older than 3600s is a same-epoch orphan ⇒ ignored; fresh keep-alive still honored", () => {
		expect(DRAIN_REQUEST_MAX_AGE_SECONDS).toBe(3600);
		writeDrainRequest(home, { epoch: EPOCH, nowMs: () => NOW });
		expect(
			drainRequested(home, {
				epoch: EPOCH,
				nowMs: () => NOW + (DRAIN_REQUEST_MAX_AGE_SECONDS + 1) * 1000,
			}),
		).toBe(false);
		// Refreshed exactly at the boundary stays honoured.
		expect(
			drainRequested(home, {
				epoch: EPOCH,
				nowMs: () => NOW + DRAIN_REQUEST_MAX_AGE_SECONDS * 1000,
			}),
		).toBe(true);
	});

	it("malformed/contentless marker STILL reads as drain-active (fail-safe quiesce)", () => {
		writeFileSync(drainRequestPath(home), "{ half-written", { mode: 0o600 });
		expect(readDrainRequest(home)).toEqual({});
		expect(drainRequested(home, { epoch: EPOCH, nowMs: () => NOW })).toBe(true);

		writeFileSync(drainRequestPath(home), "{}", { mode: 0o600 }); // no epoch, no ts
		expect(drainRequested(home, { epoch: EPOCH, nowMs: () => NOW })).toBe(true);
	});

	it("absent marker ⇒ not draining", () => {
		expect(drainRequested(home, { epoch: EPOCH })).toBe(false);
		expect(readDrainRequest(home)).toBeNull();
	});

	it("unparseable requested_at degrades to honoured (only DEFINITE expiry ignores)", () => {
		writeFileSync(
			drainRequestPath(home),
			JSON.stringify({
				action: "drain",
				epoch: EPOCH,
				requested_at: "not-a-date",
			}),
			{ mode: 0o600 },
		);
		expect(
			drainRequested(home, { epoch: EPOCH, nowMs: () => NOW + 10 ** 12 }),
		).toBe(true);
	});

	it("future-dated timestamp (clock skew) is honoured, not expired", () => {
		writeDrainRequest(home, { epoch: EPOCH, nowMs: () => NOW + 60_000 });
		expect(drainRequested(home, { epoch: EPOCH, nowMs: () => NOW })).toBe(true);
	});

	it("empty current epoch disables the epoch check entirely (presence-only)", () => {
		writeDrainRequest(home, { epoch: "old-boot:old-init", nowMs: () => NOW });
		expect(drainRequested(home, { epoch: "", nowMs: () => NOW + 1000 })).toBe(
			true,
		);
	});
});

describe("drainMarkerIsStale unit shape", () => {
	it("either lenient signal suffices", () => {
		expect(
			drainMarkerIsStale(
				{ epoch: "other" },
				{ epoch: EPOCH, nowMs: () => NOW },
			),
		).toBe(true);
		expect(
			drainMarkerIsStale(
				{
					epoch: EPOCH,
					requested_at: new Date(NOW - 4000 * 1000).toISOString(),
				},
				{ epoch: EPOCH, nowMs: () => NOW },
			),
		).toBe(true);
		expect(
			drainMarkerIsStale(
				{ epoch: EPOCH, requested_at: new Date(NOW).toISOString() },
				{ epoch: EPOCH, nowMs: () => NOW },
			),
		).toBe(false);
	});
});

// Receipt behavior contracts (08 §8): every terminal path persists exactly
// one receipt with the real exit code; latest.json pointer updates
// atomically-shaped (temp+rename); bounded prune keeps N.

import {
	existsSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { ManualClock } from "./testing/manual-clock.js";
import {
	LATEST_POINTER_FILENAME,
	RECEIPTS_KEEP_DEFAULT,
	UpdateReceiptWriter,
	exitCodeForOutcome,
	pruneReceipts,
	readLatestPointer,
	receiptsDirFor,
} from "./receipt.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-rcpt-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

describe("UpdateReceiptWriter.finalize", () => {
	it("writes the receipt + latest.json pointer with outcome and exit code", () => {
		const clock = new ManualClock(1_700_000_000);
		const writer = new UpdateReceiptWriter(dir, clock);
		writer.recordStep("plan", true, { install_method: "git" });
		writer.recordSkips("snapshot:default", [
			{
				path: "state.db",
				reason: "file exceeds the 1073741824-byte snapshot cap (9000000 bytes)",
			},
		]);
		const { payload, path } = writer.finalize("partial");
		expect(path).not.toBeNull();
		expect(existsSync(path as string)).toBe(true);
		expect(payload.outcome).toBe("partial");
		// Exit-code discipline: success ⇒ 0, partial/failed ⇒ 1 (08 §8).
		expect(payload.exit_code).toBe(1);
		expect(payload.steps).toHaveLength(1);
		// Skips persist WITH reasons — never silent.
		expect(payload.skips[0]).toMatchObject({
			stage: "snapshot:default",
			path: "state.db",
		});
		const pointer = readLatestPointer(dir);
		expect(pointer?.outcome).toBe("partial");
		expect(pointer?.receipt).toBe((path as string).split("/").pop());
		// Parity finalize_update_receipt: latest.json mirrors the FULL payload
		// (steps/skips included), not a summary pointer.
		expect(pointer?.steps).toEqual(payload.steps);
		expect(pointer?.skips).toEqual(payload.skips);
		expect(pointer?.schema).toBe("pi-update-receipt/1");
	});

	it("is idempotent — one run, one receipt file even if finalized twice", () => {
		const writer = new UpdateReceiptWriter(dir, new ManualClock());
		writer.recordStep("apply", false, { refused: "up-front" });
		const first = writer.finalize("failed");
		const second = writer.finalize("success"); // late caller cannot rewrite history
		expect(second.path).toBe(first.path);
		expect(
			readdirSync(dir).filter((n) => n.startsWith("receipt-")),
		).toHaveLength(1);
	});

	it("refusals and exceptions carry their error text into the receipt", () => {
		const writer = new UpdateReceiptWriter(dir, new ManualClock());
		const reason =
			"update refused: npm-class installs are not updatable in place";
		const { path } = writer.finalize("failed", reason);
		expect(path).not.toBeNull();
		let raw: Record<string, unknown> | null = null;
		try {
			raw = JSON.parse(readFileSync(path as string, "utf8")) as Record<
				string,
				unknown
			>;
		} catch {
			raw = null; // a torn/unreadable receipt must fail the assertion below
		}
		expect(raw).toMatchObject({ error: reason, outcome: "failed" });
	});
});

describe("exitCodeForOutcome", () => {
	it("maps the closed outcome vocabulary onto 0/1/2 (exit-2 refusal convention)", () => {
		expect(exitCodeForOutcome("success")).toBe(0);
		expect(exitCodeForOutcome("partial")).toBe(1);
		expect(exitCodeForOutcome("failed")).toBe(1);
		// Parity hermes_cli/update_receipt.py:finalize_pending_update_receipt:
		// exit 2 ⇒ "refused" (preflight refusals are not failures).
		expect(exitCodeForOutcome("refused")).toBe(2);
	});

	it("a REFUSED finalization records outcome+exit 2 verbatim in receipt and pointer", () => {
		const writer = new UpdateReceiptWriter(dir, new ManualClock());
		writer.recordStep("refusal-gate", false, {
			reason: "update refused: zip-package installs are not updatable in place",
		});
		const { payload } = writer.finalize(
			"refused",
			"update refused: zip-package installs are not updatable in place",
		);
		expect(payload.outcome).toBe("refused");
		expect(payload.exit_code).toBe(2);
		const pointer = readLatestPointer(dir);
		expect(pointer?.outcome).toBe("refused");
		expect(pointer?.exit_code).toBe(2);
	});
});

describe("pruneReceipts", () => {
	function seed(n: number): string[] {
		const names: string[] = [];
		for (let i = 0; i < n; i++) {
			const name = `receipt-2026-01-${String(i + 1).padStart(2, "0")}T00-00-00-000Z-1.json`;
			writeFileSync(join(dir, name), "{}\n");
			names.push(name);
		}
		return names;
	}

	it("keeps the N newest receipts, oldest deleted, latest.json untouched", () => {
		seed(RECEIPTS_KEEP_DEFAULT + 3);
		writeFileSync(join(dir, LATEST_POINTER_FILENAME), "{}\n");
		const deleted = pruneReceipts(dir, RECEIPTS_KEEP_DEFAULT);
		expect(deleted).toBe(3);
		const remaining = readdirSync(dir);
		expect(remaining.filter((n) => n.startsWith("receipt-"))).toHaveLength(
			RECEIPTS_KEEP_DEFAULT,
		);
		expect(existsSync(join(dir, LATEST_POINTER_FILENAME))).toBe(true);
	});

	it("floors keep to 1 and ignores non-receipt files", () => {
		seed(4);
		writeFileSync(join(dir, "operator-notes.txt"), "mine\n");
		pruneReceipts(dir, 0);
		const remaining = readdirSync(dir);
		expect(remaining.filter((n) => n.startsWith("receipt-"))).toHaveLength(1);
		expect(remaining).toContain("operator-notes.txt");
	});

	it("returns 0 for an absent directory without raising", () => {
		expect(pruneReceipts(join(dir, "nope"), 5)).toBe(0);
	});
});

describe("receiptsDirFor", () => {
	it("places receipts under <home>/logs/update_receipts (08 §3)", () => {
		expect(receiptsDirFor("/home/x")).toBe(
			join("/home/x", "logs", "update_receipts"),
		);
	});
});

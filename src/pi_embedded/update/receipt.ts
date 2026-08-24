// receipt.ts — update receipts (08 §3 logging surface + §8 Receipt contract).
//
// "Machine-readable JSON to logs/update_receipts/ (+ latest.json pointer):
// steps (ok/detail), skips WITH reasons, restart outcome, plan, fleet
// snapshot, final outcome ∈ {success, partial, failed}. Command boundary owns
// finalization — early exits (refusals, fetch failures, exceptions) still
// persist a receipt with the real exit code. A begun-but-unwritten receipt is
// a bug."
//
// Hermes anchors (READ-ONLY reference; semantics ported):
//   hermes_cli/update_receipt.py:begin_update_receipt / record_step /
//       record_skip / set_plan / finalize_update_receipt → UpdateReceiptWriter
//   latest.json pointer + atomic write-temp-rename → writeJsonAtomically
//   bounded history prune → pruneReceipts

import {
	mkdirSync,
	readdirSync,
	readFileSync,
	renameSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { join } from "node:path";
import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";

export const UPDATE_RECEIPTS_DIRNAME = join("logs", "update_receipts");
export const LATEST_POINTER_FILENAME = "latest.json";

/** Outcome vocabulary is CLOSED (08 §8). */
export type UpdateOutcome = "success" | "partial" | "failed";

/** Exit-code discipline: success ⇒ 0; partial/failed ⇒ 1 (08 §8). */
export function exitCodeForOutcome(outcome: UpdateOutcome): 0 | 1 {
	return outcome === "success" ? 0 : 1;
}

export interface ReceiptStep {
	stage: string;
	ok: boolean;
	detail: unknown;
}

export interface ReceiptSkip {
	stage: string;
	path?: string;
	reason: string;
}

export interface UpdateReceiptPayload {
	schema: "pi-update-receipt/1";
	started_at: string;
	finished_at: string;
	outcome: UpdateOutcome;
	exit_code: 0 | 1;
	steps: ReceiptStep[];
	skips: ReceiptSkip[];
	plan: unknown;
	restart: unknown;
	fleet: unknown;
	error: string | null;
}

function writeJsonAtomically(path: string, payload: unknown): void {
	// write-temp-rename: a crash mid-write never yields a torn JSON.
	mkdirSync(join(path, ".."), { recursive: true });
	const tmp = `${path}.${process.pid}.tmp`;
	writeFileSync(tmp, `${JSON.stringify(payload, null, 2)}\n`, { mode: 0o600 });
	renameSync(tmp, path);
}

export function receiptsDirFor(home: string): string {
	return join(home, UPDATE_RECEIPTS_DIRNAME);
}

/**
 * Keep only the newest `keep` receipt files in `dir` (latest.json untouched,
 * non-receipt files untouched). Returns count deleted.
 */
export function pruneReceipts(dir: string, keep: number): number {
	if (keep < 1) keep = 1;
	let entries: string[];
	try {
		entries = readdirSync(dir);
	} catch {
		return 0;
	}
	const receipts = entries
		.filter(
			(name) =>
				/^receipt-.*\.json$/.test(name) && name !== LATEST_POINTER_FILENAME,
		)
		.sort()
		.reverse();
	let deleted = 0;
	for (const name of receipts.slice(keep)) {
		try {
			unlinkSync(join(dir, name));
			deleted += 1;
		} catch {
			/* best-effort prune */
		}
	}
	return deleted;
}

/**
 * The receipt accumulator the pipeline carries through EVERY terminal path.
 * Construction starts the clock; finalize() is mandatory-on-all-paths — the
 * pipeline's job is to make early exits impossible without a receipt.
 */
export class UpdateReceiptWriter {
	private readonly steps: ReceiptStep[] = [];
	private readonly skips: ReceiptSkip[] = [];
	private plan: unknown = null;
	private restart: unknown = null;
	private fleet: unknown = null;
	private readonly startedSeconds: number;

	constructor(
		private readonly dir: string,
		private readonly clock: GatewayClock = systemClock,
	) {
		this.startedSeconds = clock.nowSeconds();
	}

	recordStep(stage: string, ok: boolean, detail: unknown = null): void {
		this.steps.push({ stage, ok, detail });
	}

	recordSkips(
		stage: string,
		skips: ReadonlyArray<{ path?: string; reason: string }>,
	): void {
		for (const skip of skips) {
			if (skip.path === undefined) {
				this.skips.push({ stage, reason: skip.reason });
			} else {
				this.skips.push({ stage, path: skip.path, reason: skip.reason });
			}
		}
	}

	setPlan(plan: unknown): void {
		this.plan = plan;
	}

	setRestart(restart: unknown): void {
		this.restart = restart;
	}

	setFleet(fleet: unknown): void {
		this.fleet = fleet;
	}

	/**
	 * Persist the receipt + latest.json pointer; returns the payload with its
	 * exit code. Idempotence guard: a second finalize returns the FIRST
	 * payload — one run, one receipt file.
	 */
	finalize(
		outcome: UpdateOutcome,
		error: string | null = null,
	): { payload: UpdateReceiptPayload; path: string | null } {
		if (this.finalizedPayload !== null) return this.finalizedPayload;
		const finishedSeconds = this.clock.nowSeconds();
		const payload: UpdateReceiptPayload = {
			schema: "pi-update-receipt/1",
			started_at: new Date(this.startedSeconds * 1000).toISOString(),
			finished_at: new Date(finishedSeconds * 1000).toISOString(),
			outcome,
			exit_code: exitCodeForOutcome(outcome),
			steps: [...this.steps],
			skips: [...this.skips],
			plan: this.plan,
			restart: this.restart,
			fleet: this.fleet,
			error,
		};
		const stamp = new Date(finishedSeconds * 1000)
			.toISOString()
			.replace(/[:.]/g, "-");
		const filename = `receipt-${stamp}-${process.pid}.json`;
		try {
			mkdirSync(this.dir, { recursive: true });
			const path = join(this.dir, filename);
			writeJsonAtomically(path, payload);
			writeJsonAtomically(join(this.dir, LATEST_POINTER_FILENAME), {
				receipt: filename,
				outcome,
				exit_code: payload.exit_code,
				finished_at: payload.finished_at,
			});
			pruneReceipts(this.dir, RECEIPTS_KEEP_DEFAULT);
			this.finalizedPayload = { payload, path };
		} catch (error) {
			// Receipt write failure must never mask the pipeline result, but it
			// IS loud (the caller surfaces it alongside the outcome).
			this.finalizedPayload = {
				payload,
				path: null,
			};
			this.writeError = error instanceof Error ? error.message : String(error);
		}
		return this.finalizedPayload;
	}

	private finalizedPayload: {
		payload: UpdateReceiptPayload;
		path: string | null;
	} | null = null;

	writeError: string | null = null;
}

export const RECEIPTS_KEEP_DEFAULT = 20;

/** Read the latest.json pointer (null when absent/corrupt). */
export function readLatestPointer(
	dir: string,
): {
	receipt: string;
	outcome: UpdateOutcome;
	exit_code: number;
	finished_at: string;
} | null {
	try {
		const raw: unknown = JSON.parse(
			readFileSync(join(dir, LATEST_POINTER_FILENAME), "utf8"),
		);
		if (typeof raw === "object" && raw !== null) {
			return raw as {
				receipt: string;
				outcome: UpdateOutcome;
				exit_code: number;
				finished_at: string;
			};
		}
		return null;
	} catch {
		return null;
	}
}

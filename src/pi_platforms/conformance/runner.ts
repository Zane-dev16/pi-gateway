// pi_platforms/conformance/runner — executes a row catalog and produces the
// structured report (encoded vs deferred, pass/fail per row). The merge gate
// for new platforms: an adapter merges only when ALL applicable rows pass
// (04 §8; DEC-002).

import type { Shape } from "./harness.js";
import type { ConformanceRow, RowResult } from "./rows.js";
import { TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";

export interface SuiteReport {
	subjectName: string;
	shape: Shape;
	rows: RowResult[];
	/** Rows required by this shape but NOT YET ENCODED with fixtures. */
	deferred: Array<{ id: string; reason: string }>;
	passed: number;
	failed: number;
	get allApplicablePassed(): boolean;
}

export async function runConformanceSuite(opts: {
	subjectName: string;
	shape: Shape;
	rows: ConformanceRow[];
	/** Fixture-backed transport rows already supplied by the adapter. */
	suppliedTransportRowIds?: ReadonlySet<string> | undefined;
}): Promise<SuiteReport> {
	const rows: RowResult[] = [];
	for (const row of opts.rows) {
		if (row.shapes !== "all" && !row.shapes.has(opts.shape)) continue;
		rows.push(await row.run());
	}
	const required = TRANSPORT_ROW_REQUIREMENTS[opts.shape];
	const supplied = opts.suppliedTransportRowIds ?? new Set();
	const deferred = required
		.filter((id) => !supplied.has(id))
		.map((id) => ({
			id,
			reason:
				"transport fixture not yet supplied — named hook awaits adapter agent",
		}));
	const passed = rows.filter((r) => r.pass).length;
	const failed = rows.length - passed;
	const report: SuiteReport = {
		subjectName: opts.subjectName,
		shape: opts.shape,
		rows,
		deferred,
		passed,
		failed,
		allApplicablePassed: false,
	};
	// Getter assigned post-construction (frozen shape below).
	Object.defineProperty(report, "allApplicablePassed", {
		get: () => failed === 0 && deferred.length === 0,
	});
	return report;
}

export function formatReport(report: SuiteReport): string {
	const lines: string[] = [
		`conformance suite — subject=${report.subjectName} shape=${report.shape}`,
		`rows: ${report.passed}/${report.rows.length} passed, ${report.deferred.length} deferred`,
	];
	for (const r of report.rows) {
		lines.push(
			`  ${r.pass ? "PASS" : "FAIL"}  ${r.id}${r.detail ? ` — ${r.detail}` : ""}`,
		);
	}
	for (const d of report.deferred) {
		lines.push(`  DEFER  ${d.id} — ${d.reason}`);
	}
	return lines.join("\n");
}

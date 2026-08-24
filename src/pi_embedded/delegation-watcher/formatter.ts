// formatter.ts — the self-contained re-injection text for one async-delegation
// completion, and the consolidated multi-delegation batch text.
//
// Port of tools/process_registry.py:_format_async_delegation (reached from
// gateway/run.py:_format_gateway_process_notification's "async_delegation"
// branch) plus run.py:_format_coalesced_async_delegations. The block is
// written to stand entirely on its own — full original task source (goal,
// context, toolsets, role, model), dispatch time, status, complete result
// summary — because the agent receiving it may be deep in unrelated context
// and won't remember why the subagent existed.
//
// Attribution divergence (proposed-DEC note): _delegation_attribution_line
// resolves `sa-*` task ids against Hermes' live subagent registry; Pi has no
// subagent registry seam yet, so every sa-* task id gets the registry-aged-
// out generic line ("Started by subagent …") rather than an anonymous wall.
// Timestamps render in LOCAL time exactly like Python's strftime parity.

/** Human-friendly elapsed string ('18m', '2h3m', '45s') — _format_age parity. */
export function formatAge(seconds: unknown): string {
	const n = typeof seconds === "number" ? seconds : Number(seconds);
	if (!Number.isFinite(n)) return "?";
	const s = Math.max(0, Math.trunc(n));
	if (s < 60) return `${s}s`;
	const mTotal = Math.floor(s / 60);
	const rem = s % 60;
	if (mTotal < 60) return rem === 0 ? `${mTotal}m` : `${mTotal}m${rem}s`;
	const h = Math.floor(mTotal / 60);
	const m = mTotal % 60;
	return m === 0 ? `${h}h` : `${h}h${m}m`;
}

function localTimestamp(epochSeconds: number): string {
	const d = new Date(epochSeconds * 1000);
	const pad = (n: number): string => String(n).padStart(2, "0");
	return (
		`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
		`${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
	);
}

/**
 * One-line provenance for child-originated events (_delegation_attribution_line
 * parity with the aged-out-registry fallback: still attribute generically
 * rather than anonymously). Non-subagent task ids attribute nothing.
 */
export function delegationAttributionLine(
	evt: Record<string, unknown>,
): string | null {
	const taskId = String(evt["task_id"] ?? "");
	if (!taskId.startsWith("sa-")) return null;
	return `Started by subagent ${taskId} (delegate_task).`;
}

function truncMarker(truncated: boolean): string {
	return truncated
		? " [TRUNCATED: hit max_iterations — work may be incomplete]"
		: "";
}

/**
 * Format ONE async-delegation completion event. Port of
 * _format_async_delegation including the fan-out batch branch (`is_batch` or
 * a per-task `results` list renders every subagent's summary in one block).
 */
export function formatAsyncDelegation(
	evt: Record<string, unknown>,
	nowSeconds?: number,
): string {
	const delegId = String(evt["delegation_id"] ?? "unknown");
	const goal = String(evt["goal"] ?? "") || "";
	const context = evt["context"];
	const toolsets = evt["toolsets"];
	const role = String(evt["role"] ?? "") || "leaf";
	const model = String(evt["model"] ?? "") || "?";
	const status = String(evt["status"] ?? "") || "completed";
	const summary = evt["summary"];
	const error = evt["error"];
	const apiCalls = evt["api_calls"] ?? 0;
	const duration = evt["duration_seconds"] ?? "?";
	const truncated =
		evt["truncated"] === true || evt["exit_reason"] === "max_iterations";
	const dispatchedAt = evt["dispatched_at"];
	const completedAt =
		typeof evt["completed_at"] === "number"
			? (evt["completed_at"] as number)
			: (nowSeconds ?? Date.now() / 1000);

	// ----- Batch (fan-out) completion: consolidated multi-task block -------
	const batchResults = evt["results"];
	if (evt["is_batch"] === true || Array.isArray(batchResults)) {
		const results = Array.isArray(batchResults)
			? (batchResults as Array<Record<string, unknown>>)
			: [];
		const goals = Array.isArray(evt["goals"])
			? (evt["goals"] as Array<unknown>)
			: [];
		const n = results.length > 0 ? results.length : goals.length;
		const totalDur = evt["total_duration_seconds"] ?? duration;
		const lines: string[] = [
			`[ASYNC DELEGATION BATCH COMPLETE — ${delegId}]`,
			`A background fan-out of ${n} subagent(s) you dispatched earlier ` +
				"has finished. All ran in parallel and waited on each other; their " +
				"consolidated results are below. You may have moved on since " +
				"dispatching — act on these or re-dispatch if things have changed.",
			"",
		];
		if (typeof dispatchedAt === "number") {
			const age = formatAge(completedAt - dispatchedAt);
			lines.push(`Dispatched: ${localTimestamp(dispatchedAt)} (${age} ago)`);
		}
		if (context) lines.push(`Context you provided: ${String(context)}`);
		if (toolsets) lines.push(`Toolsets: ${joinToolsets(toolsets)}`);
		lines.push(
			`Role: ${role}   Model: ${model}   Total duration: ${String(totalDur)}s`,
		);
		if (error && results.length === 0) {
			lines.push("--- ERROR ---");
			lines.push(`The batch did not complete successfully: ${String(error)}`);
			return lines.join("\n");
		}
		const ordered = [...results].sort(
			(a, b) => numOr0(a["task_index"]) - numOr0(b["task_index"]),
		);
		for (const r of ordered) {
			const idx = numOr0(r["task_index"]);
			const rStatus = String(r["status"] ?? "?");
			const rSummary = r["summary"];
			const rError = r["error"];
			const rGoal =
				idx < goals.length ? String(goals[idx]) : String(r["goal"] ?? "");
			const rTruncated =
				r["truncated"] === true || r["exit_reason"] === "max_iterations";
			const icon = rTruncated
				? "⚠"
				: rStatus === "completed" || rStatus === "success"
					? "✓"
					: "✗";
			lines.push("");
			let header = `--- ${icon} TASK ${idx + 1}/${n}`;
			if (rGoal) header += `: ${rGoal}`;
			header += `  (status=${rStatus}`;
			if (r["api_calls"]) header += `, api_calls=${String(r["api_calls"])}`;
			if (r["duration_seconds"] != null) {
				header += `, ${String(r["duration_seconds"])}s`;
			}
			if (rTruncated) {
				header += ", TRUNCATED: hit max_iterations — work may be incomplete";
			}
			header += ") ---";
			lines.push(header);
			if ((rStatus === "completed" || rStatus === "success") && rSummary) {
				if (rTruncated) {
					lines.push(
						"[TRUNCATED — subagent hit its iteration cap; the " +
							"summary below may be incomplete. Verify before relying " +
							"on it, or re-dispatch the unfinished part.]",
					);
				}
				lines.push(String(rSummary));
			} else if (rSummary) {
				if (rError) lines.push(`(${rStatus}: ${String(rError)})`);
				lines.push("Partial output:");
				lines.push(String(rSummary));
			} else {
				lines.push(
					`(no summary — status=${rStatus}` +
						(rError ? `: ${String(rError)}` : "") +
						")",
				);
			}
			const live = r["live_transcript"];
			if (live) {
				lines.push(
					`Full live transcript (complete tool/assistant trace): ${String(live)}`,
				);
			}
		}
		return lines.join("\n");
	}

	// ----- Single completion ------------------------------------------------
	const age =
		typeof dispatchedAt === "number"
			? ` (${formatAge(completedAt - dispatchedAt)} ago)`
			: "";

	const lines: string[] = [
		`[ASYNC DELEGATION COMPLETE — ${delegId}]`,
		"A background subagent you dispatched earlier has finished. You may " +
			"have moved on since dispatching it; the full task source is below so " +
			"you can act on the result or re-dispatch if things have changed.",
		"",
	];
	if (typeof dispatchedAt === "number") {
		lines.push(`Dispatched: ${localTimestamp(dispatchedAt)}${age}`);
	}
	lines.push(`Original goal: ${goal}`);
	if (context) lines.push(`Context you provided: ${String(context)}`);
	if (toolsets) lines.push(`Toolsets: ${joinToolsets(toolsets)}`);
	lines.push(`Role: ${role}   Model: ${model}`);
	lines.push(
		`Status: ${status}   API calls: ${String(apiCalls)}   Duration: ${String(duration)}s${truncMarker(truncated)}`,
	);
	lines.push("--- RESULT ---");
	if ((status === "completed" || status === "success") && summary) {
		if (truncated) {
			lines.push(
				"[TRUNCATED — subagent hit its iteration cap; the summary below " +
					"may be incomplete. Verify before relying on it, or re-dispatch " +
					"the unfinished part.]",
			);
		}
		lines.push(String(summary));
	} else if (status === "interrupted") {
		lines.push(
			"The subagent was interrupted before completing" +
				(error ? `: ${String(error)}` : "."),
		);
		if (summary) {
			lines.push("Partial output:");
			lines.push(String(summary));
		}
	} else {
		lines.push(
			`The subagent did not complete successfully (status=${status}).` +
				(error ? `\n${String(error)}` : ""),
		);
		if (summary) {
			lines.push("Partial output:");
			lines.push(String(summary));
		}
	}
	return lines.join("\n");
}

/**
 * gateway/run.py:_format_gateway_process_notification, async_delegation
 * branch. Unknown event types format to null (the watcher skips them — other
 * producers own their lanes).
 */
export function formatCompletionNotification(
	evt: Record<string, unknown>,
	nowSeconds?: number,
): string | null {
	if (evt["type"] !== undefined && evt["type"] !== "async_delegation") {
		return null;
	}
	return formatAsyncDelegation(evt, nowSeconds);
}

/**
 * gateway/run.py:_format_coalesced_async_delegations — join per-delegation
 * blocks into ONE consolidated turn (#70300: never flood a session with N
 * synthetic turns for one finishing fan-out).
 */
export function formatCoalescedAsyncDelegations(blocks: string[]): string {
	const header =
		`[IMPORTANT: ${blocks.length} background subagent delegations ` +
		"completed for this session. Treat these results as one " +
		"completion batch and send at most one consolidated user-facing " +
		"response. If a result does not change the current conclusion, " +
		"absorb it silently.]";
	return [header, ...blocks].join("\n\n");
}

function joinToolsets(toolsets: unknown): string {
	if (Array.isArray(toolsets)) return toolsets.map(String).join(", ");
	return String(toolsets);
}

function numOr0(v: unknown): number {
	return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

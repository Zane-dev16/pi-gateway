// dispatcher.ts — ONE kanban dispatch tick (07 §6 tick shape):
//
//   reclaim stale claims → promote ready → atomic claim → spawn the
//   assigned profile's worker.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/kanban_watchers.py::_kanban_dispatcher_watcher  → service.ts loop
//     ("Each tick calls dispatch_once … Per-tick failures don't stop
//      subsequent ticks")
//   hermes_cli/kanban_db.py:_dispatch_once_locked           → dispatchOnce
//     - ready SELECT ordering: `priority DESC, created_at ASC`
//     - max_spawn is a LIVE CONCURRENCY CAP: running cards + this tick's
//       spawns count against it (a per-tick budget would grow concurrency by
//       N every interval on a busy board)
//     - unassigned ready cards land in skipped_unassigned, not an error
//     - claim losing the race ⇒ silently continue to the next card
//     - spawn_fn throw ⇒ recordFailure; breaker auto-blocks at the limit
//   hermes_cli/kanban_db.py:DEFAULT_FAILURE_LIMIT = 2       → types.ts

import type { BoardClient, DispatchResult, SpawnFn } from "./types.js";
import { DEFAULT_FAILURE_LIMIT } from "./types.js";

export interface DispatchOnceOptions {
	/** Epoch seconds for this tick (injected clock read by the caller). */
	nowSeconds: number;
	/** Breaker limit (kanban.failure_limit parity). Default 2. */
	failureLimit?: number;
	/**
	 * Live concurrency cap across the board (parity max_spawn): running
	 * cards + this tick's claims must stay ≤ maxSpawn. null = unbounded.
	 */
	maxSpawn?: number | null;
	/**
	 * Worker spawner. A throw counts as a spawn_failed outcome and feeds
	 * the consecutive-failure breaker; success records nothing further —
	 * completion arrives later via BoardClient.completeCard.
	 */
	spawn: SpawnFn;
}

export const DEFAULT_DISPATCH_INTERVAL_SECONDS = 60;
export const MIN_DISPATCH_INTERVAL_SECONDS = 1;

/**
 * Run one dispatcher tick against the board. All board mutations go through
 * the seam's atomic transitions; this function owns only ORDER and POLICY.
 */
export async function dispatchOnce(
	board: BoardClient,
	opts: DispatchOnceOptions,
): Promise<DispatchResult> {
	const failureLimit = opts.failureLimit ?? DEFAULT_FAILURE_LIMIT;
	const result: DispatchResult = {
		board: board.board,
		reclaimed: [],
		promoted: [],
		spawned: [],
		skippedUnassigned: [],
		autoBlocked: [],
	};

	// 1. Reclaim stale claims (TTL-expired in-progress cards → ready).
	result.reclaimed = await board.reclaimStaleClaims(opts.nowSeconds);

	// 2. Promote todo/blocked cards whose parents finished → ready.
	result.promoted = await board.promoteReady(opts.nowSeconds, failureLimit);

	// 3. Live-concurrency budget: running + new claims ≤ maxSpawn.
	let budget: number | null = null;
	if (opts.maxSpawn !== null && opts.maxSpawn !== undefined) {
		const running = await board.countRunning();
		if (running >= opts.maxSpawn) return result;
		budget = opts.maxSpawn - running;
	}

	// 4. Atomic claim + spawn, highest priority first.
	const ready = await board.listReady();
	for (const card of ready) {
		if (budget !== null && result.spawned.length >= budget) break;
		if (!card.assignee) {
			result.skippedUnassigned.push(card.id);
			continue;
		}
		const claimed = await board.claimCard({
			cardId: card.id,
			lock: `disp:${card.id}:${opts.nowSeconds}`,
			expiresAt: opts.nowSeconds + DEFAULT_CLAIM_TTL_SECONDS,
			nowSeconds: opts.nowSeconds,
		});
		if (claimed === null) continue; // lost race to another racer/claimant
		const assignee: string = card.assignee; // narrowed above
		try {
			await opts.spawn(claimed);
			result.spawned.push({ cardId: claimed.id, assignee });
		} catch (err) {
			const message = err instanceof Error ? err.message : String(err);
			const recorded = await board.recordFailure(
				claimed.id,
				"spawn_failed",
				message,
				{ failureLimit, nowSeconds: opts.nowSeconds },
			);
			if (recorded.blocked) result.autoBlocked.push(claimed.id);
		}
	}
	return result;
}

/**
 * Claim TTL used when the dispatcher itself mints claims. Parity of
 * kanban_db.py DEFAULT_CLAIM_TTL_SECONDS = 15*60 (workers heartbeat/renew
 * via their own toolset; a TTL-bounded claim is what makes stale reclaim
 * sound).
 */
export const DEFAULT_CLAIM_TTL_SECONDS = 15 * 60;

// watcher.ts — the supervised poll-and-deliver loop over the durable
// async-delegation rail. Port of gateway/run.py:_async_delegation_watcher:
//
//   - initial STARTUP DELAY (3s) so platforms finish connecting before the
//     first injection attempt (`await asyncio.sleep(3)` parity);
//   - poll every 2s over the DURABLE backlog (delivery_state='pending',
//     oldest first) — Pi keeps no second in-memory completion lane, the
//     durable row IS the queue, and the rail's atomic claim handshake
//     arbitrates every consumer (watcher ticks, other gateways, future
//     post-turn drains) exactly like the shared queue did;
//   - same-route members COALESCE into ONE forged turn per group (#70300):
//     grouped by full routing key + parent session, never across sessions;
//   - per group: ownership resolution → busy gate (wait for idle end,
//     invariant 1) → claim handshake → forged NEW turn through the NORMAL
//     pipeline → ack ONLY after adapter acceptance;
//   - the tick body is failure-contained: a bad row or broken store NEVER
//     crashes the loop (inner try/except parity).
//
// Boot restore interplay (06 §7.1 restore_undelivered_completions): boot()
// runs the rail's restore pass — recover_abandoned first (Hermes order),
// then the 48h replay-age prune — so rows stranded by a dead owner become
// deliverable 'unknown'-status completions and stale ones converge to
// dropped BEFORE polling starts. Restored rows carry only the IN-MEMORY
// restored=true stamp; this watcher re-delivers them through the normal
// claim path (proving ownership — #64484), which is what makes delivery
// EXACTLY ONCE across restarts: claimed-but-unacked rows from a crashed
// process are re-claimable only via stale takeover (>300s).
//
// Supervision context: in production this watcher is spawned as an
// OPTIONAL-stage service (01 §3.1 stage 8 `embedded_watchers`): startup
// degrades LOUDLY per-service without blocking later stages; stop() joins
// the loop deterministically. This module is the loop only — stage wiring
// belongs to the gateway driver.

import type Database from "better-sqlite3";

import {
	DelegationRail,
	type DelegationRailOptions,
} from "../../pi_gateway/delegation/index.js";
import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";
import {
	CompletionDeliveryEngine,
	type CompletionDeliveryDeps,
	type GroupDisposition,
} from "./delivery.js";
import type { TurnLiveness } from "./idle-gate.js";
import { SessionOwnershipResolver } from "./ownership-resolver.js";
import { listPendingCompletions, type PendingCompletion } from "./pending.js";

/** Poll cadence (run.py:_async_delegation_watcher interval default 2.0s). */
export const DELEGATION_POLL_INTERVAL_MS = 2000;

/**
 * Initial delay before the FIRST tick — platforms must finish connecting
 * before completions inject through them (run.py `asyncio.sleep(3)`).
 */
export const DELEGATION_WATCHER_STARTUP_DELAY_MS = 3000;

export interface DelegationWatcherDeps {
	/** Open state.db handle (rail + reads share ONE connection). */
	db: Database.Database;
	liveness: TurnLiveness;
	/** Forged-turn port into the NORMAL pipeline (see delivery.ts). */
	dispatcher: CompletionDeliveryDeps["dispatcher"];
	/** Routing-entry scope namespace (default '' = unscoped). */
	scope?: string;
	clock?: GatewayClock;
	/**
	 * Pre-built rail override (restart/liveness-injection tests and driver
	 * compositions that share ONE rail instance across engine + watcher).
	 * Default: constructed over `db` with this watcher's clock.
	 */
	rail?: DelegationRail;
	/** Forwarded to the default rail construction (liveness overrides etc.). */
	railOptions?: DelegationRailOptions;
	intervalMs?: number;
	startupDelayMs?: number;
	log?: CompletionDeliveryDeps["log"];
}

export interface DelegationTickReport {
	/** Completions pending at tick start. */
	pending: number;
	delivered: number;
	dropped: number;
	retried: number;
	/** Groups skipped because their target session is mid-turn. */
	busy: number;
	ownedElsewhere: number;
	unformattable: number;
}

export class DelegationWatcher {
	private readonly rail: DelegationRail;
	private readonly db: Database.Database;
	private readonly engine: CompletionDeliveryEngine;
	private readonly clock: GatewayClock;
	private readonly intervalMs: number;
	private readonly startupDelayMs: number;
	private readonly log: CompletionDeliveryDeps["log"];

	private running = false;
	private loopPromise: Promise<void> | null = null;
	private pendingSleepResolvers: Array<() => void> = [];

	constructor(deps: DelegationWatcherDeps) {
		this.db = deps.db;
		const clock = deps.clock ?? systemClock;
		this.clock = clock;
		this.rail =
			deps.rail ?? new DelegationRail(deps.db, { clock, ...deps.railOptions });
		this.engine = new CompletionDeliveryEngine({
			rail: this.rail,
			resolver: new SessionOwnershipResolver(deps.db, {
				scope: deps.scope ?? "",
				clock,
			}),
			liveness: deps.liveness,
			dispatcher: deps.dispatcher,
			clock,
			...(deps.log !== undefined ? { log: deps.log } : {}),
		});
		this.intervalMs = deps.intervalMs ?? DELEGATION_POLL_INTERVAL_MS;
		this.startupDelayMs =
			deps.startupDelayMs ?? DELEGATION_WATCHER_STARTUP_DELAY_MS;
		this.log = deps.log;
	}

	get isRunning(): boolean {
		return this.running;
	}

	exposedRail(): DelegationRail {
		return this.rail;
	}

	/**
	 * Boot-restore pass (restore_undelivered_completions parity): recover
	 * abandoned dispatches + prune replay-stale pendings, then report how many
	 * undelivered completions await re-delivery. Restored rows flow through the
	 * normal claim path afterwards — exactly-once across restarts.
	 */
	async boot(): Promise<{ restored: number }> {
		let restored = 0;
		try {
			restored = await this.rail.restoreUndelivered(() => {
				// Sink receives each restored event; the durable row stays the
				// queue, so re-enqueueing is a no-op here (the poll will see it).
			});
		} catch (err) {
			// Loud degradation: boot restore must never block gateway startup.
			this.log?.warn?.("async-delegation boot restore failed", {
				error: String(err),
			});
		}
		return { restored };
	}

	/**
	 * ONE drain cycle at an explicit instant — never throws (best-effort inner
	 * try/except parity). Tests drive this directly instead of racing timers.
	 */
	async tick(): Promise<DelegationTickReport> {
		const report: DelegationTickReport = {
			pending: 0,
			delivered: 0,
			dropped: 0,
			retried: 0,
			busy: 0,
			ownedElsewhere: 0,
			unformattable: 0,
		};
		try {
			const candidates = listPendingCompletions(this.db);
			report.pending = candidates.length;
			for (const group of this.groupByRoute(candidates)) {
				const r = await this.engine.deliverGroup(group.members);
				bump(report, r.disposition);
			}
		} catch (err) {
			// Outer containment: a broken store must not kill the watcher.
			this.log?.debug?.("async-delegation watcher tick error", {
				error: String(err),
			});
		}
		return report;
	}

	/**
	 * run.py:_async_delegation_group_key parity — coalesce ONLY members sharing
	 * every routing dimension (origin key + parent session). Events for
	 * different sessions never merge into one turn. Order preserved (oldest
	 * first) so a fan-out replays in dispatch order within its group.
	 */
	private groupByRoute(
		candidates: PendingCompletion[],
	): Array<{ key: string; members: PendingCompletion[] }> {
		const groups = new Map<string, PendingCompletion[]>();
		const order: string[] = [];
		for (const c of candidates) {
			const key = `${c.originSession}\u0000${c.parentSessionId ?? ""}`;
			const bucket = groups.get(key);
			if (bucket) bucket.push(c);
			else {
				groups.set(key, [c]);
				order.push(key);
			}
		}
		return order.map((key) => ({
			key,
			members: groups.get(key) as PendingCompletion[],
		}));
	}

	/** Begin the background loop (idempotent). Startup delay precedes tick 1. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.loopPromise = this.loop();
	}

	/** Stop the loop and join it; breaks any in-flight sleep immediately. */
	async stop(): Promise<void> {
		this.running = false;
		const waiters = this.pendingSleepResolvers;
		this.pendingSleepResolvers = [];
		for (const wake of waiters) wake();
		const loop = this.loopPromise;
		this.loopPromise = null;
		if (loop) await loop;
	}

	private async loop(): Promise<void> {
		if (!(await this.sleepInterruptible(this.startupDelayMs))) return;
		while (this.running) {
			await this.tick();
			if (!this.running) break;
			if (!(await this.sleepInterruptible(this.intervalMs))) return;
		}
	}

	/** Clock sleep that stop() can break — never leaves a hung loop behind. */
	private sleepInterruptible(ms: number): Promise<boolean> {
		return new Promise<boolean>((resolvePromise) => {
			let done = false;
			const finish = (woke: boolean): void => {
				if (done) return;
				done = true;
				this.pendingSleepResolvers = this.pendingSleepResolvers.filter(
					(w) => w !== wake,
				);
				resolvePromise(woke);
			};
			const wake = (): void => finish(false); // cancelled by stop()
			this.pendingSleepResolvers.push(wake);
			void this.clock.sleepMs(ms).then(
				() => finish(true),
				() => finish(true),
			);
		});
	}
}

function bump(report: DelegationTickReport, d: GroupDisposition): void {
	switch (d) {
		case "delivered":
			report.delivered++;
			break;
		case "dropped":
			report.dropped++;
			break;
		case "retry":
			report.retried++;
			break;
		case "busy":
			report.busy++;
			break;
		case "owned-elsewhere":
			report.ownedElsewhere++;
			break;
		case "unformattable":
			report.unformattable++;
			break;
		default: {
			const exhaustive: never = d;
			throw new Error(`unknown group disposition ${String(exhaustive)}`);
		}
	}
}

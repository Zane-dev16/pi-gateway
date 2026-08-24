// TEST INFRASTRUCTURE — shared harness for delegation-watcher contracts.
//
// Each test gets an isolated mkdtemp StateStore (production open path: WAL
// ladder + full schema reconcile), a ManualClock-driven rail/resolver/engine,
// a RECORDING dispatcher standing in for the L1-guard composition, and a
// fake liveness map standing in for the turn-lease registry. No wall-clock
// reads, no real child processes: the cross-process durability claims are
// the STORE tests' job (pi_gateway/delegation two-process.test.ts); these
// contracts drive the watcher layer over the same handshake.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { IncomingEvent } from "../../../pi_gateway/guards/index.js";
import { DelegationRail } from "../../../pi_gateway/delegation/index.js";
import { StateStore } from "../../../pi_state/index.js";
import {
	CompletionDeliveryEngine,
	type SyntheticTurnDispatcher,
	SessionOwnershipResolver,
	type TurnLiveness,
	DelegationWatcher,
} from "../index.js";
import { ManualClock } from "./manual-clock.js";

export interface SeededSessionOptions {
	sessionKey?: string;
	parentSessionId?: string | null;
	endedAt?: number | null;
	endReason?: string | null;
	sourcePlatform?: string;
	originJson?: Record<string, unknown> | null;
}

/** Recording dispatcher: captures forged events; can fail on command. */
export class RecordingDispatcher implements SyntheticTurnDispatcher {
	readonly events: IncomingEvent[] = [];
	private failNext: Error | null = null;

	failOnceWith(err: Error): void {
		this.failNext = err;
	}

	async dispatch(event: IncomingEvent): Promise<void> {
		const fail = this.failNext;
		this.failNext = null;
		if (fail) throw fail;
		this.events.push(event);
	}

	texts(): string[] {
		return this.events.map((e) => e.text ?? "");
	}
}

/** Fake liveness: session id → busy flag (the idle gate's test seam). */
export class FakeLiveness implements TurnLiveness {
	readonly busy = new Set<string>();

	isBusy(sessionId: string): boolean {
		return this.busy.has(sessionId);
	}
}

export interface WatcherHarness {
	dir: string;
	dbPath: string;
	store: StateStore;
	clock: ManualClock;
	rail: DelegationRail;
	resolver: SessionOwnershipResolver;
	engine: CompletionDeliveryEngine;
	dispatcher: RecordingDispatcher;
	liveness: FakeLiveness;
	watcher: DelegationWatcher;
	close: () => Promise<void>;
}

export async function openWatcherHarness(
	label = "delegation-watcher",
	opts: { clockStartSeconds?: number } = {},
): Promise<WatcherHarness> {
	const dir = mkdtempSync(join(tmpdir(), `pi-gw-${label}-`));
	const dbPath = join(dir, "state.db");
	const clock = new ManualClock();
	clock.setSeconds(opts.clockStartSeconds ?? 1_775_000_000);
	const store = await StateStore.open(dbPath);
	return buildHarnessOn(dir, dbPath, store, clock);
}

/** Compose watcher-layer objects over an ALREADY-OPEN store (restart tests). */
export async function buildHarnessOn(
	dir: string,
	dbPath: string,
	store: StateStore,
	clock: ManualClock,
): Promise<WatcherHarness> {
	const rail = new DelegationRail(store.db, { clock });
	const resolver = new SessionOwnershipResolver(store.db, { clock });
	const dispatcher = new RecordingDispatcher();
	const liveness = new FakeLiveness();
	const engine = new CompletionDeliveryEngine({
		rail,
		resolver,
		liveness,
		dispatcher,
		clock,
	});
	const watcher = new DelegationWatcher({
		db: store.db,
		liveness,
		dispatcher,
		clock,
	});
	return {
		dir,
		dbPath,
		store,
		clock,
		rail,
		resolver,
		engine,
		dispatcher,
		liveness,
		watcher,
		close: () => store.close(false),
	};
}

// ---------------------------------------------------------------------
// seeding helpers
// ---------------------------------------------------------------------

export async function seedSession(
	h: WatcherHarness,
	id: string,
	opts: SeededSessionOptions = {},
): Promise<void> {
	await h.clock.advance(0); // no-op; keeps call sites uniform
	const origin =
		opts.originJson === undefined
			? JSON.stringify({
					platform: opts.sourcePlatform ?? "telegram",
					chatType: "dm",
					userId: "u1",
					chatId: "100",
				})
			: opts.originJson === null
				? null
				: JSON.stringify(opts.originJson);
	h.store.db
		.prepare(
			`INSERT INTO sessions (id, source, session_key, chat_id, chat_type,
			        thread_id, user_id, parent_session_id, started_at, ended_at,
			        end_reason, origin_json)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   ended_at = excluded.ended_at,
			   end_reason = excluded.end_reason,
			   origin_json = excluded.origin_json`,
		)
		.run(
			id,
			opts.sourcePlatform ?? "telegram",
			opts.sessionKey ?? null,
			"100",
			"dm",
			null,
			"u1",
			opts.parentSessionId ?? null,
			h.clock.nowSeconds(),
			opts.endedAt ?? null,
			opts.endReason ?? null,
			origin,
		);
}

export async function seedRouting(
	h: WatcherHarness,
	sessionKey: string,
	sessionId: string,
): Promise<void> {
	const now = h.clock.nowSeconds();
	h.store.db
		.prepare(
			`INSERT OR REPLACE INTO gateway_routing (scope, session_key, entry_json, updated_at)
			 VALUES ('', ?, ?, ?)`,
		)
		.run(
			sessionKey,
			JSON.stringify({
				session_key: sessionKey,
				session_id: sessionId,
				created_at: now,
				updated_at: now,
				platform: "telegram",
				chat_type: "dm",
			}),
			now,
		);
}

export interface DispatchOptions {
	delegationId: string;
	originSession: string;
	parentSessionId?: string | null;
	goal?: string;
	summary?: string;
	status?: string;
}

/** Full producer flow for one background delegation: dispatch → complete. */
export async function seedCompletion(
	h: WatcherHarness,
	o: DispatchOptions,
): Promise<void> {
	await h.rail.recordDispatch({
		delegationId: o.delegationId,
		originSession: o.originSession,
		parentSessionId: o.parentSessionId ?? null,
		task: { goal: o.goal ?? "" },
	});
	const event = {
		type: "async_delegation",
		delegation_id: o.delegationId,
		session_key: o.originSession,
		parent_session_id: o.parentSessionId ?? null,
		goal: o.goal ?? "",
		status: o.status ?? "completed",
		summary: o.summary ?? `did ${o.delegationId}`,
		dispatched_at: h.clock.nowSeconds() - 60,
		completed_at: h.clock.nowSeconds(),
	};
	await h.rail.publishCompletion({
		delegationId: o.delegationId,
		event,
		result: { status: event.status, summary: event.summary },
	});
}

export function pendingRow(
	h: WatcherHarness,
	delegationId: string,
): {
	delivery_state: string;
	delivery_attempts: number;
	delivery_claim: string | null;
} | null {
	const r = h.store.db
		.prepare(
			"SELECT delivery_state, delivery_attempts, delivery_claim FROM async_delegations WHERE delegation_id = ?",
		)
		.get(delegationId) as
		| {
				delivery_state: string;
				delivery_attempts: number;
				delivery_claim: string | null;
		  }
		| undefined;
	return r ?? null;
}

export { rmSync };

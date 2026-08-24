// ownership-resolver.ts — the DB-fed half of §7.2: pre-flight verdict +
// concrete target resolution for one pending completion.
//
// Port of BOTH consumers of the shared boundary set, kept on ONE code path so
// verdict and routing cannot drift (06 §7.2 invariant 2 — the 2026-08-09
// staging incident, defect #2):
//   gateway/run.py:_classify_completion_target      → classify() (verdict)
//   gateway/run.py:_resolve_async_delegation_session → resolveTarget() (routing)
// The PURE decision table lives in pi_gateway/delegation/ownership.ts
// (classifyCompletionTarget + USER_BOUNDARY_END_REASONS); this module feeds it
// real lookups and owns the routing arms the pure core cannot:
//   - route-owns-lineage check (invariant 3): a stale route is accepted only
//     when its own verified compression tip equals the target;
//   - lineage rebinding via a CAS `advance_compression_session` (gateway/
//     session.py:advance_compression_session — repoint WITHOUT row lifecycle
//     changes; None ⇒ the route moved after our snapshot ⇒ fail closed);
//   - idle-end retarget to the chat's CURRENT session (the routing entry).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/session.py:get_compression_tip  → pi_gateway/resolution
//                                             /compression-tip.ts (imported)
//   hermes gateway_routing store            → gateway_routing rows read here
//   run.py switch_session pin-back (#57498) → RoutingBinder.switchSession
//                                             (pi_embedded/handoff/binder.ts,
//                                             imported NOT modified)
//
// Fail-closed posture (#55578): every unresolved ownership question drops the
// INJECTION (release claim ⇒ retry later ⇒ attempt cap converges), never
// guesses a session; the delegation record keeps the result queryable.

import type Database from "better-sqlite3";

import {
	classifyCompletionTarget,
	type CompletionVerdict,
	type GatewayClock,
	type LineageTip,
	type ParentSnapshot,
} from "../../pi_gateway/delegation/index.js";
import { getCompressionTip } from "../../pi_gateway/resolution/compression-tip.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import { RoutingBinder } from "../handoff/binder.js";

/** sessions-row facts the classifier consumes (no richer than ParentSnapshot). */
interface SessionsRowRaw {
	id: string;
	source: string | null;
	session_key: string | null;
	chat_id: string | null;
	chat_type: string | null;
	thread_id: string | null;
	user_id: string | null;
	parent_session_id: string | null;
	ended_at: number | null;
	end_reason: string | null;
	origin_json: string | null;
}

export interface ResolvedSessionFacts {
	sessionId: string;
	row: SessionsRowRaw;
	/** Adapter SessionSource snapshot persisted at creation (origin_json). */
	source: SessionSource | null;
}

/**
 * The full ownership outcome for one completion.
 *   verdict 'terminal' → dropClaim (honest ack of permanent loss);
 *   verdict 'retry'    → releaseClaim (transient; attempt cap bounds churn);
 *   verdict 'deliver'  → target resolved; dispatch, then completeClaim AFTER
 *                        adapter acceptance. `target` is what the forged turn
 *                        must reach; `routeAction` is the binding work the
 *                        resolver already performed.
 */
export interface OwnershipResolution {
	verdict: CompletionVerdict;
	/** Concrete injection target when verdict === 'deliver'. */
	target: ResolvedSessionFacts | null;
	/** Why — mirrors the resolver's log lines for loud diagnostics. */
	reason: string;
}

export interface OwnershipResolverOptions {
	/** Routing-entry scope namespace (default '' = unscoped). */
	scope?: string;
	/** Injected clock forwarded to the routing binder (tests; default system). */
	clock?: GatewayClock;
}

export class SessionOwnershipResolver {
	private readonly db: Database.Database;
	private readonly binder: RoutingBinder;
	private readonly scope: string;

	constructor(db: Database.Database, opts: OwnershipResolverOptions = {}) {
		this.db = db;
		this.binder = new RoutingBinder(
			db,
			opts.clock ? { clock: opts.clock } : {},
		);
		this.scope = opts.scope ?? "";
	}

	// ------------------------------------------------------------------
	// Lookups (each maps a Hermes session_db call; throws => lookup error)
	// ------------------------------------------------------------------

	/**
	 * Read one session's facts. Returns null for an UNKNOWN row; THROWS when
	 * the DB itself fails (caller maps to the retry arm) — get_session parity.
	 */
	sessionFacts(sessionId: string): ResolvedSessionFacts | null {
		const row = this.db
			.prepare(
				`SELECT id, source, session_key, chat_id, chat_type, thread_id,
				        user_id, parent_session_id, ended_at, end_reason, origin_json
				 FROM sessions WHERE id = ?`,
			)
			.get(sessionId) as SessionsRowRaw | undefined;
		if (!row) return null;
		return { sessionId, row, source: parseOriginJson(row.origin_json) };
	}

	/** Pure-classifier snapshot of one row (null = unknown parent). */
	private snapshot(facts: ResolvedSessionFacts): ParentSnapshot {
		return {
			endedAt: facts.row.ended_at,
			endReason: facts.row.end_reason,
			parentSessionId: facts.sessionId,
		};
	}

	/**
	 * Compression-lineage probe (get_compression_tip + tip row read). A missing
	 * or self-referential tip models mid-rotation (tipSessionId null); an
	 * unreadable TIP ROW leaves endedAt undefined (pure-core retry arm).
	 */
	lineageTip(parentId: string): LineageTip {
		const tipId = getCompressionTip(this.db, parentId);
		if (!tipId || tipId === parentId) return { tipSessionId: null };
		try {
			const tipRow = this.db
				.prepare("SELECT ended_at FROM sessions WHERE id = ?")
				.get(tipId) as { ended_at: number | null } | undefined;
			if (!tipRow) return { tipSessionId: tipId }; // tip row unreadable
			return { tipSessionId: tipId, endedAt: tipRow.ended_at };
		} catch {
			return { tipSessionId: tipId }; // tip row unreadable
		}
	}

	routingEntryOf(sessionKey: string): { session_id: string } | null {
		const entry = this.binder.entryOf(sessionKey, this.scope);
		return entry === null ? null : { session_id: entry.session_id };
	}

	// ------------------------------------------------------------------
	// Pre-flight verdict (_classify_completion_target parity)
	// ------------------------------------------------------------------

	/**
	 * Prove deliverability BEFORE adapter acceptance so the durable ack stays
	 * honest. Throws nothing: DB failure maps to the retry arm here.
	 */
	classify(parentSessionId: string | null): {
		verdict: CompletionVerdict;
		parent: ResolvedSessionFacts | null;
		tip: LineageTip | undefined;
	} {
		let parent: ResolvedSessionFacts | null = null;
		try {
			parent =
				parentSessionId === null ? null : this.sessionFacts(parentSessionId);
		} catch {
			return { verdict: "retry", parent: null, tip: undefined }; // DB unavailable
		}
		if (parent === null)
			return { verdict: "terminal", parent: null, tip: undefined };
		let tip: LineageTip | undefined;
		if (
			parent.row.ended_at !== null &&
			parent.row.end_reason === "compression"
		) {
			try {
				tip = this.lineageTip(parentSessionId ?? "");
			} catch {
				return { verdict: "retry", parent, tip: undefined };
			}
		}
		return {
			verdict: classifyCompletionTarget(this.snapshot(parent), tip),
			parent,
			tip,
		};
	}

	// ------------------------------------------------------------------
	// Full resolution (pre-flight + _resolve_async_delegation_session arms)
	// ------------------------------------------------------------------

	/**
	 * Resolve one completion to its verified owning gateway session. Callers
	 * dispatch ONLY on verdict==='deliver' with target !== null; anything else
	 * is fail-closed (see OwnershipResolution docs for dispositions).
	 */
	resolve(input: {
		originSessionKey: string;
		parentSessionId: string | null;
	}): OwnershipResolution {
		const { originSessionKey, parentSessionId } = input;

		let classified: ReturnType<SessionOwnershipResolver["classify"]>;
		try {
			classified = this.classify(parentSessionId);
		} catch {
			return {
				verdict: "retry",
				target: null,
				reason: "session db unavailable",
			};
		}

		if (classified.verdict !== "deliver") {
			return {
				verdict: classified.verdict,
				target: null,
				reason:
					classified.verdict === "terminal"
						? parentSessionId === null
							? "no parent session recorded"
							: "parent permanently gone (user boundary / unknown)"
						: "transient uncertainty — hold for a later consumer",
			};
		}

		const pinned = classified.parent;
		if (pinned === null) {
			// Unreachable (classify maps unknown parents to terminal) — belt and braces.
			return { verdict: "terminal", target: null, reason: "unknown parent" };
		}

		let entry: { session_id: string } | null = null;
		try {
			entry = this.routingEntryOf(originSessionKey);
		} catch {
			entry = null;
		}
		if (entry === null) {
			return {
				verdict: "retry",
				target: null,
				reason: `no live route for key ${originSessionKey}`,
			};
		}

		const pinnedEnded = pinned.row.ended_at !== null;
		const endReason = String(pinned.row.end_reason ?? "");

		// -- Live parent: deliver pinned; pin the route back onto its owner
		//    when the key drifted (#57498 switch_session pin parity).
		if (!pinnedEnded) {
			if (entry.session_id === pinned.sessionId) {
				return { verdict: "deliver", target: pinned, reason: "live parent" };
			}
			const switched = this.trySwitch(
				originSessionKey,
				entry.session_id,
				pinned.sessionId,
			);
			return switched
				? {
						verdict: "deliver",
						target: pinned,
						reason: `live parent; route re-pinned ${entry.session_id} → ${pinned.sessionId}`,
					}
				: {
						verdict: "retry",
						target: null,
						reason: "could not bind routing key to owning session",
					};
		}

		// -- Idle/timeout/lifecycle end (scale-to-zero norm): deliver to the
		//    chat's CURRENT session. The routing entry IS that session — no
		//    rebinding needed (staging-incident defect #2 arm).
		if (endReason !== "compression") {
			const current = this.safeFacts(entry.session_id);
			return {
				verdict: "deliver",
				target: current ?? pinnedFallback(pinned),
				reason: `${endReason || "idle"}-ended parent; retargeted to chat's current session ${entry.session_id}`,
			};
		}

		// -- Compression-ended parent: follow the verified lineage tip.
		const tipId = classified.tip?.tipSessionId ?? null;
		if (!tipId) {
			return {
				verdict: "retry",
				target: null,
				reason: "compression without a visible continuation (mid-rotation)",
			};
		}
		const tipFacts = this.safeFacts(tipId);
		if (tipFacts === null || tipFacts.row.ended_at !== null) {
			return {
				verdict: "retry",
				target: null,
				reason: `compression continuation ${tipId} is ${
					tipFacts === null ? "unknown" : "ended"
				}`,
			};
		}

		// Route owns the lineage when it sits ON the lineage (pinned or tip) or,
		// for a stale intermediate route, when ITS OWN verified tip equals the
		// target — no recency-based capture of unrelated side-chats (invariant 3).
		const routeOwnsLineage =
			entry.session_id === pinned.sessionId ||
			entry.session_id === tipFacts.sessionId ||
			this.staleRouteTipEquals(entry.session_id, tipFacts.sessionId);
		if (!routeOwnsLineage) {
			return {
				verdict: "retry",
				target: null,
				reason: `lineage ${pinned.sessionId} → ${tipFacts.sessionId} does not own current route ${entry.session_id}`,
			};
		}

		if (entry.session_id !== tipFacts.sessionId) {
			const advanced = this.advanceCompressionSession(
				originSessionKey,
				entry.session_id,
				tipFacts.sessionId,
			);
			if (!advanced) {
				return {
					verdict: "retry",
					target: null,
					reason: "could not bind routing key along compression lineage",
				};
			}
		}
		return {
			verdict: "deliver",
			target: tipFacts,
			reason: `lineage tip ${tipFacts.sessionId}; routing key advanced`,
		};
	}

	// ------------------------------------------------------------------
	// Route-binding arms
	// ------------------------------------------------------------------

	/**
	 * gateway/session.py:advance_compression_session port — CAS-repoint the
	 * route along an ALREADY-VERIFIED compression lineage. No row lifecycle
	 * changes (the compression transaction owns those); updated_at untouched so
	 * a background completion cannot make an idle route look fresh (#85709).
	 * Null when the entry moved after the caller's snapshot ⇒ caller fails closed.
	 */
	private advanceCompressionSession(
		sessionKey: string,
		expectedSessionId: string,
		targetSessionId: string,
	): { session_id: string } | null {
		const entry = this.binder.entryOf(sessionKey, this.scope);
		if (entry === null) return null;
		if (entry.session_id === targetSessionId)
			return { session_id: targetSessionId };
		if (entry.session_id !== expectedSessionId) return null;
		// Lineage sanity: the expected route must still verify forward to the
		// target (gateway/session.py:_heal_compression_tip_locked posture).
		if (getCompressionTip(this.db, expectedSessionId) !== targetSessionId) {
			return null;
		}
		return executeAdvance(
			this.db,
			this.scope,
			sessionKey,
			expectedSessionId,
			targetSessionId,
		);
	}

	/** switch_session pin-back for LIVE parents (ends predecessor 'session_switch'). */
	private trySwitch(
		sessionKey: string,
		_currentSessionId: string,
		targetSessionId: string,
	): boolean {
		void this.binder.switchSession(sessionKey, targetSessionId, this.scope);
		const after = this.binder.entryOf(sessionKey, this.scope);
		return after !== null && after.session_id === targetSessionId;
	}

	/**
	 * Stale-route acceptance test: accept an intermediate compression-ended
	 * route only when its own verified compression tip equals the target.
	 */
	private staleRouteTipEquals(
		routeSessionId: string,
		targetId: string,
	): boolean {
		try {
			const routeRow = this.sessionFacts(routeSessionId);
			if (routeRow === null) return false;
			if (
				routeRow.row.ended_at === null ||
				routeRow.row.end_reason !== "compression"
			) {
				return false;
			}
			return getCompressionTip(this.db, routeSessionId) === targetId;
		} catch {
			return false;
		}
	}

	private safeFacts(sessionId: string): ResolvedSessionFacts | null {
		try {
			return this.sessionFacts(sessionId);
		} catch {
			return null;
		}
	}
}

// --------------------------------------------------------------------
// module helpers
// --------------------------------------------------------------------

function parseOriginJson(raw: string | null): SessionSource | null {
	if (!raw) return null;
	try {
		const parsed: unknown = JSON.parse(raw);
		if (parsed !== null && typeof parsed === "object") {
			const s = parsed as Partial<SessionSource>;
			if (typeof s.platform === "string" && typeof s.chatType === "string") {
				return parsed as SessionSource;
			}
		}
	} catch {
		/* fall through */
	}
	return null;
}

/** Retarget fallback when the current route's row vanished between reads. */
function pinnedFallback(
	pinned: NonNullable<
		ReturnType<SessionOwnershipResolver["classify"]>["parent"]
	>,
): ResolvedSessionFacts {
	return pinned;
}

function executeAdvance(
	db: Database.Database,
	scope: string,
	sessionKey: string,
	expectedSessionId: string,
	targetSessionId: string,
): { session_id: string } | null {
	// Synchronous single-statement CAS inside BEGIN IMMEDIATE (wal.executeWrite
	// parity with every other routing mutation). updated_at deliberately NOT
	// touched (#85709 — store bookkeeping is not user activity).
	const raw = db
		.prepare(
			"SELECT entry_json FROM gateway_routing WHERE scope = ? AND session_key = ?",
		)
		.get(scope, sessionKey) as { entry_json: string } | undefined;
	if (!raw) return null;
	let entry: Record<string, unknown>;
	try {
		const parsed: unknown = JSON.parse(raw.entry_json);
		if (parsed === null || typeof parsed !== "object") return null;
		entry = parsed as Record<string, unknown>;
	} catch {
		return null;
	}
	if (entry["session_id"] !== expectedSessionId) return null;
	entry["session_id"] = targetSessionId;
	const cursor = db
		.prepare(
			"UPDATE gateway_routing SET entry_json = ? WHERE scope = ? AND session_key = ? AND entry_json = ?",
		)
		.run(JSON.stringify(entry), scope, sessionKey, raw.entry_json);
	return cursor.changes === 1 ? { session_id: targetSessionId } : null;
}

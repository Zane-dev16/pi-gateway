// Behavior contracts: compression LIVE-TIP resolution (02-session-and-state.md
// §4.2, hermes_state.py:get_compression_tip) and its integration with the
// cross-process turn lease (§5 / DEC-004: the lease key is the compression-
// lineage ROOT, resolved inside the SAME write transaction as the mutation).
// Roadmap Phase 1 required contract #6.
//
// Asserted by relationship:
//   - The walk follows ONLY children of compression-ended parents; explicit
//     forks (branch/delegate markers, source='tool') are excluded; continuing/
//     live children beat stale closed siblings; timestamps are NEVER trusted
//     (a continuation inserted "before" parent.ended_at is still followed).
//   - Cycle- and depth-bounded read-only walk; input returned when no
//     continuation exists.
//   - Lease acquisition on ANY segment of one lineage contends on ONE row
//     keyed at the ROOT (probeOwner/row query prove conversation_id = root);
//     refresh/release from another segment hit that same root row.

import { describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/store.js";
import { makeTempDir, removeTempDir } from "../../pi_state/testing/harness.js";
import { getCompressionTip } from "./compression-tip.js";

const T0 = 1_750_000_000;

interface Fixture {
	store: StateStore;
	db: StateStore["db"];
	session(row: {
		id: string;
		parent?: string;
		endReason?: string | null;
		source?: string | null;
		modelConfig?: string | null;
		startedAt?: number;
		endedAt?: number | null;
		lastActivityAt?: number | null;
	}): void;
	close(): Promise<void>;
}

async function openFixture(): Promise<Fixture> {
	const dir = makeTempDir("pi-gw-tip-");
	const store = await StateStore.open(`${dir}/state.db`);
	return {
		store,
		db: store.db,
		session: (row) => {
			store.db
				.prepare(
					`INSERT INTO sessions (id, source, parent_session_id, started_at,
					   ended_at, end_reason, last_activity_at, model_config)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				)
				.run(
					row.id,
					row.source ?? "telegram",
					row.parent ?? null,
					row.startedAt ?? T0,
					row.endedAt ?? null,
					row.endReason ?? null,
					row.lastActivityAt ?? null,
					row.modelConfig ?? null,
				);
		},
		close: async () => {
			await store.close();
			removeTempDir(dir);
		},
	};
}

describe("getCompressionTip — live-tip walk (hermes_state.py:get_compression_tip)", () => {
	it("follows the compression-continuation chain to the tip; a chain-free id returns itself", async () => {
		const fx = await openFixture();
		try {
			const root = "s_root";
			const mid = "s_mid";
			const tip = "s_tip";
			// A segment is a continuation iff its PARENT ended with 'compression'
			// — so every intermediate link in the chain is compression-ended.
			fx.session({ id: root, endReason: "compression", endedAt: T0 + 5 });
			fx.session({
				id: mid,
				parent: root,
				endReason: "compression",
				endedAt: T0 + 10,
				lastActivityAt: T0 + 9,
			});
			fx.session({
				id: tip,
				parent: mid,
				endedAt: null,
				lastActivityAt: T0 + 20,
			});

			expect(getCompressionTip(fx.store.db, root)).toBe(tip);
			expect(getCompressionTip(fx.store.db, mid)).toBe(tip);
			expect(getCompressionTip(fx.store.db, tip)).toBe(tip); // no continuation → input
		} finally {
			await fx.close();
		}
	});

	it("excludes explicit forks: branch/delegate markers and source='tool' children are NOT continuations", async () => {
		const fx = await openFixture();
		try {
			const root = "forkroot";
			const branchChild = "s_branch";
			const delegateChild = "s_delegate";
			const toolChild = "s_tool";
			const realTip = "s_real";
			fx.session({ id: root, endReason: "compression", endedAt: T0 + 5 });
			fx.session({
				id: branchChild,
				parent: root,
				modelConfig: JSON.stringify({ _branched_from: root }),
				lastActivityAt: T0 + 999, // would win ordering if wrongly eligible
			});
			fx.session({
				id: delegateChild,
				parent: root,
				modelConfig: JSON.stringify({ _delegate_from: root }),
				lastActivityAt: T0 + 998,
			});
			fx.session({
				id: toolChild,
				parent: root,
				source: "tool",
				lastActivityAt: T0 + 997,
			});
			fx.session({
				id: realTip,
				parent: root,
				lastActivityAt: T0 + 1,
			});

			expect(getCompressionTip(fx.store.db, root)).toBe(realTip);
		} finally {
			await fx.close();
		}
	});

	it("prefers continuing/live children over stale closed siblings; never trusts timestamps", async () => {
		const fx = await openFixture();
		try {
			// Case 1: compression-ended child beats live child beats closed one.
			const r1 = "r1";
			fx.session({ id: r1, endReason: "compression", endedAt: T0 + 5 });
			fx.session({
				id: "r1_closed",
				parent: r1,
				endReason: "agent_close",
				endedAt: T0 + 6,
				lastActivityAt: T0 + 900, // freshest, but stale closed sibling
			});
			fx.session({
				id: "r1_live",
				parent: r1,
				endedAt: null, // still open
				lastActivityAt: T0 + 2,
			});
			fx.session({
				id: "r1_cont",
				parent: r1,
				endReason: "compression",
				endedAt: T0 + 7,
				lastActivityAt: T0 + 1, // oldest activity, highest priority class
			});
			expect(getCompressionTip(fx.store.db, r1)).toBe("r1_cont");

			// Case 2 (no comp child): LIVE child wins over closed sibling even
			// when the closed one has later activity.
			const r2 = "r2";
			fx.session({ id: r2, endReason: "compression", endedAt: T0 + 5 });
			fx.session({
				id: "r2_closed",
				parent: r2,
				endReason: "ws_orphan_reap",
				endedAt: T0 + 6,
				lastActivityAt: T0 + 800,
			});
			fx.session({
				id: "r2_live",
				parent: r2,
				endedAt: null,
				lastActivityAt: T0 + 3,
			});
			expect(getCompressionTip(fx.store.db, r2)).toBe("r2_live");
		} finally {
			await fx.close();
		}
	});

	it("NEVER trusts started_at >= ended_at: a continuation inserted before the parent's ended_at lands is still followed", async () => {
		const fx = await openFixture();
		try {
			// The #lost-messages race: child row lands BEFORE the parent's
			// ended_at write, so child.started_at < parent.ended_at. A
			// timestamp-gated walk follows the WRONG sibling here.
			const root = "race-root";
			fx.session({
				id: root,
				endReason: "compression",
				endedAt: T0 + 100,
			});
			fx.session({
				id: "stale_ws_sibling",
				parent: root,
				endReason: "ws_orphan_reap",
				endedAt: T0 + 150,
				startedAt: T0 + 120, // satisfies any brittle timestamp test
				lastActivityAt: T0 + 140,
			});
			fx.session({
				id: "real_continuation",
				parent: root,
				startedAt: T0 + 50, // BEFORE the parent's ended_at
				lastActivityAt: T0 + 60,
			});
			expect(getCompressionTip(fx.store.db, root)).toBe("real_continuation");
		} finally {
			await fx.close();
		}
	});

	it("orders stale-closed siblings by the THREE-LEG freshest-of recency (_sql_session_last_active), never raw last_activity_at", async () => {
		const fx = await openFixture();
		try {
			const r = "recency-root";
			fx.session({ id: r, endReason: "compression", endedAt: T0 + 5 });

			// Sibling A: NEWER durable heartbeat, but its conversation went quiet
			// long ago (last message at T0+10).
			fx.session({
				id: "hb_newer_msgs_older",
				parent: r,
				endedAt: null,
				lastActivityAt: T0 + 50,
			});
			fx.db
				.prepare(
					"INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'old ask', ?)",
				)
				.run("hb_newer_msgs_older", T0 + 10);

			// Sibling B: OLDER heartbeat that lags its fresh message tail — the
			// rate-limited-heartbeat lag _sql_session_last_active exists for.
			fx.session({
				id: "hb_older_msgs_newer",
				parent: r,
				endedAt: null,
				lastActivityAt: T0 + 20,
			});
			fx.db
				.prepare(
					"INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'fresh ask', ?)",
				)
				.run("hb_older_msgs_newer", T0 + 100);

			// MAX(messages.timestamp) must beat a stale heartbeat: B wins even
			// though A's heartbeat is newer (the old COALESCE(last_activity_at,
			// started_at) ordering picked A here).
			expect(getCompressionTip(fx.db, r)).toBe("hb_older_msgs_newer");
		} finally {
			await fx.close();
		}
	});

	it("freshest-of legs: a NULL heartbeat must not erase the message leg; all-NULL falls back to started_at", async () => {
		const fx = await openFixture();
		try {
			const r = "null-leg-root";
			fx.session({ id: r, endReason: "compression", endedAt: T0 + 5 });

			// Sibling N: NULL heartbeat, fresh messages (aggregate-MAX form must
			// ignore the NULL leg instead of NULL-ing the whole comparison —
			// SQLite scalar max() would return NULL here).
			fx.session({
				id: "null_hb_fresh_msgs",
				parent: r,
				endedAt: null,
				lastActivityAt: null,
				startedAt: T0 + 1, // oldest started_at — must NOT decide
			});
			fx.db
				.prepare(
					"INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'live', ?)",
				)
				.run("null_hb_fresh_msgs", T0 + 90);

			// Sibling M: NULL heartbeat AND no messages ⇒ freshest-of falls
			// through to started_at.
			fx.session({
				id: "all_null_started",
				parent: r,
				endedAt: null,
				lastActivityAt: null,
				startedAt: T0 + 2,
			});

			expect(getCompressionTip(fx.db, r)).toBe("null_hb_fresh_msgs");

			// Pure started_at fallback between two LEG-LESS siblings (separate
			// lineage so the message sibling above cannot dominate).
			const r2 = "null-leg-root-2";
			fx.session({ id: r2, endReason: "compression", endedAt: T0 + 5 });
			fx.session({
				id: "legless_older",
				parent: r2,
				endedAt: null,
				lastActivityAt: null,
				startedAt: T0 + 1,
			});
			fx.session({
				id: "legless_newer",
				parent: r2,
				endedAt: null,
				lastActivityAt: null,
				startedAt: T0 + 2,
			});
			expect(getCompressionTip(fx.db, r2)).toBe("legless_newer");
		} finally {
			await fx.close();
		}
	});

	it("a mid-turn activity heartbeat flips sibling ordering without any new message row", async () => {
		const fx = await openFixture();
		try {
			const r = "touch-root";
			fx.session({ id: r, endReason: "compression", endedAt: T0 + 5 });
			fx.session({
				id: "quiet_tip",
				parent: r,
				endedAt: null,
				startedAt: T0 + 6,
				lastActivityAt: T0 + 50,
			});
			fx.session({
				id: "touched_live",
				parent: r,
				endedAt: null,
				startedAt: T0 + 5, // older start, no heartbeat yet
				lastActivityAt: null, // message-less live continuation
			});
			expect(getCompressionTip(fx.db, r)).toBe("quiet_tip");

			// Mid-turn stamp (hermes touch_session_activity parity): the live
			// continuation is WORKING right now — no message row yet.
			await fx.store.touchSessionActivity("touched_live", { ts: T0 + 200 });
			expect(getCompressionTip(fx.db, r)).toBe("touched_live");
		} finally {
			await fx.close();
		}
	});

	it("cycle-safe and empty-input-safe", async () => {
		const fx = await openFixture();
		try {
			// Parent cycle between two compression segments must terminate.
			// (Rows are inserted parentless first: sessions.parent_session_id
			// carries a real FK, so the cycle edges land via UPDATE.)
			fx.session({ id: "cyc_a", endReason: "compression", endedAt: T0 + 1 });
			fx.session({ id: "cyc_b", endReason: "compression", endedAt: T0 + 2 });
			fx.db
				.prepare("UPDATE sessions SET parent_session_id = ? WHERE id = ?")
				.run("cyc_b", "cyc_a");
			fx.db
				.prepare("UPDATE sessions SET parent_session_id = ? WHERE id = ?")
				.run("cyc_a", "cyc_b");
			const tip = getCompressionTip(fx.store.db, "cyc_a");
			expect(["cyc_a", "cyc_b"]).toContain(tip);

			expect(getCompressionTip(fx.store.db, "")).toBe("");
		} finally {
			await fx.close();
		}
	});
});

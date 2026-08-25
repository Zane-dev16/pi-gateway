// pi_platforms/feishu/dedup — event-subscription replay/dedup state
// (adapter.py:_is_duplicate :4621 + :_card_action_tokens :3050).
//
// Feishu's long-conn client redelivers unacked events after reconnects
// (at-least-once); exactly-once downstream rides a PERSISTED message-id
// seen-set — TTL 24 h, FIFO cap 2048, atomic JSON snapshot written on every
// NEW id and at disconnect (adapter.py:_persist_seen_message_ids :4611,
// _load_seen_message_ids :4575). Card-click tokens dedupe separately with a
// 15-minute TTL (_FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS :244).

import { mkdirSync, writeFileSync, readFileSync, renameSync } from "node:fs";
import { dirname } from "node:path";

export const SEEN_MESSAGE_IDS_FILE = "feishu_seen_message_ids.json";

/** Atomic tmp-file + rename write (adapter.py atomic_json_write parity). */
function atomicWriteJson(path: string, data: unknown): void {
	const tmp = `${path}.tmp`;
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(tmp, JSON.stringify(data), "utf8");
	renameSync(tmp, path);
}

interface SeenEntry {
	/** Epoch ms the id was first seen (0 for migrated legacy entries). */
	at: number;
}

/**
 * The message-id seen-set. `isDuplicate(id)` records AND answers in one call;
 * expired entries prune lazily; the FIFO order list evicts beyond the cap.
 */
export class FeishuSeenMessageStore {
	private readonly seen = new Map<string, SeenEntry>();
	/** Legacy-migrated ids (:4589) — timestamp-less, treated as immortal for
	 * one migration cycle (a bare epoch-0 sentinel is ambiguous under
	 * injected clocks that start at 0). */
	private readonly legacyImmortal = new Set<string>();
	private readonly ttlMs: number;
	private readonly maxEntries: number;
	private readonly nowMs: () => number;
	private dirtySincePersist = false;

	suppressedCount = 0;

	constructor(
		opts: {
			ttlMs?: number | undefined;
			maxEntries?: number | undefined;
			nowMs?: (() => number) | undefined;
			statePath?: string | undefined;
		} = {},
	) {
		this.ttlMs = opts.ttlMs ?? 24 * 60 * 60 * 1000;
		this.maxEntries = Math.max(32, opts.maxEntries ?? 2048);
		this.nowMs = opts.nowMs ?? (() => Date.now());
		if (opts.statePath !== undefined) this.load(opts.statePath);
	}

	isDuplicate(messageId: string): boolean {
		const now = this.nowMs();
		this.pruneExpired(now);
		const prior = this.seen.get(messageId);
		if (
			prior !== undefined &&
			(now - prior.at <= this.ttlMs ||
				(prior.at === 0 && this.legacyImmortal.has(messageId)))
		) {
			this.suppressedCount += 1;
			return true;
		}
		this.seen.delete(messageId); // re-insert to move to the MRU end
		this.legacyImmortal.delete(messageId); // re-recorded ⇒ ages normally
		this.seen.set(messageId, { at: now });
		while (this.seen.size > this.maxEntries) {
			const oldest = this.seen.keys().next();
			if (oldest.done) break;
			this.seen.delete(oldest.value);
		}
		this.dirtySincePersist = true;
		return false;
	}

	get size(): number {
		this.pruneExpired(this.nowMs());
		return this.seen.size;
	}

	private pruneExpired(now: number): void {
		for (const [key, entry] of this.seen) {
			// Epoch-0 entries are LEGACY migrations (:4589 comment) — treated as
			// immortal for one migration cycle, never nuked as expired.
			if (entry.at !== 0 && now - entry.at > this.ttlMs) this.seen.delete(key);
			else break; // insertion-ordered: head is oldest
		}
	}

	/**
	 * Snapshot to `<hermes_home>/feishu_seen_message_ids.json` (atomic).
	 * Wire format (:4611 _persist_seen_message_ids): `{"message_ids": {id:
	 * epoch_seconds}}` — Hermes' loader reads ONLY that key, so a flat map
	 * would be mutually unreadable across implementations.
	 */
	persist(statePath: string): void {
		if (!this.dirtySincePersist && this.seen.size === 0) return;
		atomicWriteJson(statePath, {
			message_ids: Object.fromEntries(
				[...this.seen].map(([id, e]) => [id, e.at / 1000]),
			),
		});
		this.dirtySincePersist = false;
	}

	/**
	 * Loader (:4575 _load_seen_message_ids): reads ONLY payload["message_ids"]
	 * (a bare dict or a LEGACY plain list inside that key — entries get epoch 0,
	 * treated as already-aged), drops TTL-expired ids, caps to the most recent.
	 */
	load(statePath: string): void {
		let raw: unknown;
		try {
			raw = JSON.parse(readFileSync(statePath, "utf8"));
		} catch {
			return; // missing/corrupt file ⇒ cold start (Hermes parity)
		}
		if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return;
		const seenData = (raw as Record<string, unknown>)["message_ids"];
		const now = this.nowMs();
		if (Array.isArray(seenData)) {
			// Backward-compat migration: old format stored a plain id LIST.
			for (const id of seenData) {
				const key = typeof id === "string" ? id.trim() : "";
				if (key === "") continue;
				this.seen.set(key, { at: 0 });
				this.legacyImmortal.add(key);
			}
		} else if (seenData !== null && typeof seenData === "object") {
			for (const [id, ts] of Object.entries(
				seenData as Record<string, unknown>,
			)) {
				if (id.trim() === "") continue;
				const numeric =
					typeof ts === "number" && Number.isFinite(ts) ? ts * 1000 : 0;
				this.seen.set(id, { at: numeric });
				// A saved ts of 0 is equally timestamp-less (:4589 validity).
				if (numeric === 0) this.legacyImmortal.add(id);
			}
		} else {
			return; // no message_ids key ⇒ nothing loadable (Hermes reads only it)
		}
		for (const [id, entry] of [...this.seen]) {
			if (
				entry.at > 0 &&
				now - entry.at > this.ttlMs
			)
				this.seen.delete(id);
		}
		while (this.seen.size > this.maxEntries) {
			const oldest = this.seen.keys().next();
			if (oldest.done) break;
			this.seen.delete(oldest.value);
		}
	}
}

/**
 * A12 card-action token dedup (:3050 _is_card_action_duplicate): TTL 15 min,
 * lazy expiry sweep per call, silent duplicate drop. Protects ONLY the
 * generic synthetic-COMMAND path — approval/update-prompt clicks are
 * protected instead by pending-state POP idempotency (one-shot stores).
 */
export class CardActionTokenStore {
	private readonly tokens = new Map<string, number>();
	private readonly ttlMs: number;
	private readonly nowMs: () => number;

	constructor(opts: { ttlMs?: number | undefined; nowMs?: () => number } = {}) {
		this.ttlMs = opts.ttlMs ?? 15 * 60 * 1000;
		this.nowMs = opts.nowMs ?? (() => Date.now());
	}

	isDuplicate(token: string): boolean {
		const now = this.nowMs();
		for (const [key, at] of this.tokens) {
			if (now - at > this.ttlMs) this.tokens.delete(key);
		}
		if (this.tokens.has(token)) return true;
		this.tokens.set(token, now);
		return false;
	}

	get size(): number {
		return this.tokens.size;
	}
}

// pi_embedded/cron/transcript-sink.ts — pi_state binding for the cron
// session substrate + the mirror appender.
//
// Cron sessions live in state.db like every other conversation (02 §2);
// delivery isolation means the ONLY transcript writes are: the job's own
// turn rows (via the normal runner pipeline) and — when the mirror is
// opted into — ONE cleaned assistant row appended to the ORIGIN session at
// a turn boundary (gateway/mirror.py::mirror_to_session parity; alternation-
// and cache-safe because it lands BETWEEN user turns and never mutates any
// cached prompt).

import { randomUUID } from "node:crypto";
import type { StateStore } from "../../pi_state/index.js";
import type { MirrorAppender } from "./delivery.js";

/** Ensure the per-job cron session row exists (mint-once). */
export function ensureCronSessionRow(
	store: StateStore,
	sessionId: string,
): void {
	void store.withWrite((db) => {
		db.prepare(
			"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'cron', ?)",
		).run(sessionId, Math.floor(Date.now() / 1000));
	});
}

/** MirrorAppender over pi_state: append an assistant turn to a session. */
export function stateStoreMirrorAppender(store: StateStore): MirrorAppender {
	return {
		async appendAssistantTurn(
			sessionId: string,
			text: string,
		): Promise<boolean> {
			try {
				const existing = store.listMessages(sessionId);
				// mirror_to_session only APPENDS to a session that already
				// EXISTS — never mints one (parity of the documented carve-out).
				if (!existing || existing.length === 0) return false;
				await store.appendMessage({
					sessionId,
					role: "assistant",
					content: text,
					apiContent: JSON.stringify({
						role: "assistant",
						content: [{ type: "text", text }],
					}),
				});
				return true;
			} catch {
				return false;
			}
		},
	};
}

/** Session id minted for a fresh origin-session seed test seam. */
export function newOriginSessionId(): string {
	return `sess-${randomUUID().replace(/-/g, "").slice(0, 12)}`;
}

// pi_gateway/outbound/dead-targets.ts — persistent registry of delivery
// targets confirmed unreachable (03-message-routing.md §9.5 "Dead-target
// short-circuit").
//
// Only WHOLE-CHAT deaths are recorded (`forbidden`, chat-level `not_found`);
// thread/topic-level not_found must not mark the entire chat dead. Self-
// healing: any successful send clears the flag. Store is a small JSON file;
// corrupt/unwritable degrades to in-memory-only rather than raising on the
// delivery path.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/dead_targets.py:DeadTargetRegistry → DeadTargetRegistry
//   gateway/dead_targets.py:_DEAD_ERROR_KINDS  → DEAD_ERROR_KINDS

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

/** Error kinds meaning the WHOLE chat is unreachable (never transient/thread-level). */
export const DEAD_ERROR_KINDS: ReadonlySet<string> = new Set([
	"forbidden",
	"not_found",
]);

export function isDeadErrorKind(errorKind: string | null | undefined): boolean {
	return !!errorKind && DEAD_ERROR_KINDS.has(errorKind);
}

function normalizeKey(platform: string, chatId: string): string {
	return `${platform.trim().toLowerCase()}:${chatId.trim()}`;
}

interface DeadEntry {
	platform: string;
	chat_id: string;
	reason: string;
	marked_at: number;
}

export class DeadTargetRegistry {
	private readonly dead = new Map<string, DeadEntry>();

	constructor(private readonly path?: string) {
		this.load();
	}

	/** Best-effort load; corrupt file starts empty. */
	private load(): void {
		if (!this.path) return;
		try {
			const raw = JSON.parse(readFileSync(this.path, "utf8"));
			if (raw == null || typeof raw !== "object" || Array.isArray(raw)) return;
			for (const [key, value] of Object.entries(
				raw as Record<string, unknown>,
			)) {
				if (
					value != null &&
					typeof value === "object" &&
					!Array.isArray(value)
				) {
					this.dead.set(key, value as DeadEntry);
				}
			}
		} catch {
			this.dead.clear();
		}
	}

	/** Atomic tmp+replace flush; failure keeps in-memory state only. */
	private flush(): void {
		if (!this.path) return;
		try {
			mkdirSync(dirname(this.path), { recursive: true });
			writeFileSync(
				this.path,
				JSON.stringify(Object.fromEntries(this.dead), null, 2),
				"utf8",
			);
		} catch {
			// best-effort — never break delivery on persistence failure
		}
	}

	isDead(platform: string, chatId?: string): boolean {
		if (!chatId) return false;
		return this.dead.has(normalizeKey(platform, chatId));
	}

	/** Record a confirmed-dead target. Returns true when newly added. */
	markDead(
		platform: string,
		chatId: string | undefined,
		reason = "",
		nowMs = Date.now(),
	): boolean {
		if (!chatId) return false;
		const key = normalizeKey(platform, chatId);
		const existed = this.dead.has(key);
		this.dead.set(key, {
			platform: platform.trim().toLowerCase(),
			chat_id: String(chatId),
			reason: String(reason).slice(0, 200),
			marked_at: Math.floor(nowMs / 1000),
		});
		this.flush();
		return !existed;
	}

	/** Self-heal: a successful send removes the flag. Returns true when it was set. */
	clear(platform: string, chatId?: string): boolean {
		if (!chatId) return false;
		const key = normalizeKey(platform, chatId);
		if (this.dead.has(key)) {
			this.dead.delete(key);
			this.flush();
			return true;
		}
		return false;
	}

	allDead(): Record<string, DeadEntry> {
		return Object.fromEntries(this.dead);
	}
}

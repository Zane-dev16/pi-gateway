// handoff/adoption.ts — adopt a foreign pi session file into a
// gateway-driven host session (DEC-079).
//
// The gateway attaches to a pi PROCESS, not a session file: adoption loads
// entries through the host SessionManager loader (loadEntriesFromFile — no
// loop reimplementation, DEC-023) and appends the adoptable messages as the
// gateway store's own rows for the host session id, so the next turn replays
// them through the normal seedReplay pipeline. Foreign files are only ever
// READ here; the one write the loader itself may perform is its own
// trailing-newline repair, and only on the lock-winning path.
//
// Disposition is a discriminated union, never scattered booleans: a foreign
// LIVE session drives only under the cross-process SessionDriverLock
// (adopted-live-under-lock, lock held on return); without the lock the
// gateway copies NOTHING and offers read-only context plus takeover
// (readonly-plus-takeover) instead of interleaving writes into the owner's
// loop. The caller owns the held lock's release (next switch, or process
// death which frees it via the fd lifetime).

// Sanctioned deep-import exception (host.ts precedent: the SDK index does
// not export the loader, and this module may not grow a second seam).
// eslint-disable-next-line import/no-unresolved
import { loadEntriesFromFile } from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/dist/core/session-manager.js";

import { isSessionDriveLocked, SessionDriverLock } from "./session-lock.js";

export { isSessionDriveLocked };

/** Minimal structural store seam adoption needs (StateStore satisfies it). */
export interface AdoptionStore {
	/** Own the driven session's row (messages FK on sessions). */
	ensureSession(sessionId: string): Promise<void>;
	appendMessage(message: {
		sessionId: string;
		role: string;
		content: string;
	}): Promise<number>;
	messageCount(sessionId: string): Promise<number>;
}

export type AdoptionDisposition =
	| {
			kind: "adopted-live-under-lock";
			sessionId: string;
			sourceFile: string;
			/** Message entries now driving the session (after this call). */
			entryCount: number;
			/** Held on return — the caller releases on the next switch. */
			lock: SessionDriverLock;
	  }
	| {
			kind: "readonly-plus-takeover";
			sessionId: string;
			sourceFile: string;
			reason: "drive-lock-held";
	  };

export interface AdoptSessionRequest {
	/** Foreign pi session file (<agentDir>/sessions tree, fixture in tests). */
	sessionFile: string;
	/** Host session id to drive (must match the file's header id). */
	sessionId: string;
	/** Directory holding gateway.lock.db; drive sidecars live beside it. */
	lockDir: string;
	store: AdoptionStore;
}

interface AdoptableMessage {
	role: string;
	text: string;
}

/**
 * Adopt one foreign session file. Winner copies entries under the drive
 * lock; loser returns readonly-plus-takeover with ZERO rows written. Repeat
 * adoption is idempotent (row count gates the copy; a crash-partial prefix
 * heals by appending only the missing tail).
 */
export async function adoptSessionFile(
	request: AdoptSessionRequest,
): Promise<AdoptionDisposition> {
	const lock = new SessionDriverLock(request.lockDir, request.sessionId);
	if (!lock.acquire()) {
		return {
			kind: "readonly-plus-takeover",
			sessionId: request.sessionId,
			sourceFile: request.sessionFile,
			reason: "drive-lock-held",
		};
	}
	try {
		const entries = loadEntriesFromFile(request.sessionFile);
		const header = entries[0] as { type?: unknown; id?: unknown } | undefined;
		if (
			header === undefined ||
			header.type !== "session" ||
			typeof header.id !== "string" ||
			header.id !== request.sessionId
		) {
			throw new Error(
				`adoption refuses ${request.sessionFile}: header id does not match ${request.sessionId}`,
			);
		}
		const adoptable: AdoptableMessage[] = [];
		for (const entry of entries) {
			const message = adoptableMessage(
				entry as { type?: unknown; message?: unknown },
			);
			if (message !== null) adoptable.push(message);
		}
		await request.store.ensureSession(request.sessionId);
		const existing = await request.store.messageCount(request.sessionId);
		for (const message of adoptable.slice(
			Math.min(existing, adoptable.length),
		)) {
			await request.store.appendMessage({
				sessionId: request.sessionId,
				role: message.role,
				content: message.text,
			});
		}
		return {
			kind: "adopted-live-under-lock",
			sessionId: request.sessionId,
			sourceFile: request.sessionFile,
			entryCount: Math.max(existing, adoptable.length),
			lock,
		};
	} catch (err) {
		lock.release();
		throw err;
	}
}

/**
 * Adoptable entries: user/assistant messages with extractable text. Tool
 * traffic, thinking changes, compactions, and custom entries stay behind —
 * text-only rows keep strict user/assistant alternation for the replay, and
 * the pre-request repair pass never sees a dangling tool call.
 */
function adoptableMessage(entry: {
	type?: unknown;
	message?: unknown;
}): AdoptableMessage | null {
	if (
		entry.type !== "message" ||
		typeof entry.message !== "object" ||
		entry.message === null
	) {
		return null;
	}
	const message = entry.message as { role?: unknown; content?: unknown };
	if (message.role !== "user" && message.role !== "assistant") return null;
	const text = extractText(message.content);
	if (text === "") return null;
	return { role: message.role, text };
}

/** Host extractTextContent parity: string passes through; blocks join text. */
function extractText(content: unknown): string {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return content
		.filter(
			(block): block is { type: "text"; text: string } =>
				typeof block === "object" &&
				block !== null &&
				(block as { type?: unknown }).type === "text" &&
				typeof (block as { text?: unknown }).text === "string",
		)
		.map((block) => block.text)
		.join(" ");
}

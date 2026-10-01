// pi_gateway/discovery.ts — machine-wide pi session discovery (DEC-079).
//
// Data shapes first: BinderBinding (chat key → host session id, the DEC-079
// use of the routing entry), DiscoveryIndex (resolved path → sessions found
// under <agentDir>/sessions/*/*.jsonl), DiscoveredSession (one header).
//
// Header reads mirror host readSessionHeaderForDiscovery parity (best-effort:
// one corrupt file never blocks the rest; the first parsed entry must be a
// session header carrying a string id). The host seam is private to the SDK,
// so this module carries the same rule over bounded read-only node:fs reads —
// discovery NEVER writes (a full host load can repair trailing newlines, so
// only the lock-winning adoption path may load entries).

import { closeSync, openSync, readSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

/**
 * DEC-079 binder use: a chat key is bound to a HOST session id (not a
 * gateway-minted one). Converters below project the existing RoutingEntry
 * shape onto this use without touching the binder.
 */
export interface BinderBinding {
	chatKey: string;
	hostSessionId: string;
}

/** Project a routing entry onto its DEC-079 binding use. Structural: the
 * binder's RoutingEntry satisfies this without an import (layering: this
 * module may not reach into pi_embedded even for types). */
export function bindingFromRoutingEntry(entry: {
	session_key: string;
	session_id: string;
}): BinderBinding {
	return { chatKey: entry.session_key, hostSessionId: entry.session_id };
}

/** One discovered session: its host id, the working path it belongs to, the
 * file the header came from, and the file mtime ordering recency. */
export interface DiscoveredSession {
	id: string;
	cwd: string;
	file: string;
	mtimeMs: number;
}

/**
 * Path → sessions under it, members newest-first. Only sessions whose
 * header carries a usable cwd are indexed: the map's contract is that every
 * key is a path holding pi sessions (exactly what /switch-path lists).
 */
export type DiscoveryIndex = ReadonlyMap<string, readonly DiscoveredSession[]>;

interface SessionHeaderCandidate {
	id: string;
	cwd: string | null;
}

const HEADER_READ_CHUNK_BYTES = 4096;
const MAX_HEADER_SCAN_BYTES = 1024 * 1024;

/**
 * Bounded read-only header probe. Skips blank and malformed lines like the
 * host loader; the first PARSED entry decides (header with a string id, or
 * rejection). Any I/O or overrun outcome is null, never a throw.
 */
export function readDiscoveryHeader(
	filePath: string,
): SessionHeaderCandidate | null {
	let fd: number;
	try {
		fd = openSync(filePath, "r");
	} catch {
		return null;
	}
	try {
		const buffer = Buffer.allocUnsafe(HEADER_READ_CHUNK_BYTES);
		let pending = "";
		let scannedBytes = 0;
		for (;;) {
			const newlineAt = pending.indexOf("\n");
			if (newlineAt >= 0) {
				const candidate = parseHeaderLine(pending.slice(0, newlineAt));
				if (candidate !== undefined) return candidate;
				pending = pending.slice(newlineAt + 1);
				continue;
			}
			if (scannedBytes >= MAX_HEADER_SCAN_BYTES) return null;
			const toRead = Math.min(
				buffer.length,
				MAX_HEADER_SCAN_BYTES - scannedBytes,
			);
			let bytesRead: number;
			try {
				bytesRead = readSync(fd, buffer, 0, toRead, null);
			} catch {
				return null;
			}
			if (bytesRead === 0) {
				const candidate = parseHeaderLine(pending);
				return candidate ?? null;
			}
			scannedBytes += bytesRead;
			pending += buffer.subarray(0, bytesRead).toString("utf8");
		}
	} finally {
		try {
			closeSync(fd);
		} catch {
			/* best-effort close parity */
		}
	}
}

/**
 * One candidate line: blank or malformed ⇒ keep scanning (undefined); first
 * parsed entry ⇒ header or rejection (null).
 */
function parseHeaderLine(
	line: string,
): SessionHeaderCandidate | null | undefined {
	if (line.trim() === "") return undefined;
	let parsed: unknown;
	try {
		parsed = JSON.parse(line);
	} catch {
		return undefined;
	}
	if (typeof parsed !== "object" || parsed === null) return undefined;
	const entry = parsed as { type?: unknown; id?: unknown; cwd?: unknown };
	if (entry.type !== "session" || typeof entry.id !== "string") return null;
	return {
		id: entry.id,
		cwd: typeof entry.cwd === "string" && entry.cwd !== "" ? entry.cwd : null,
	};
}

function mtimeMsOf(filePath: string): number {
	try {
		return statSync(filePath).mtimeMs;
	} catch {
		return 0;
	}
}

/**
 * Scan <agentDir>/sessions/*\/ *.jsonl into a path-keyed index. Every level
 * is best-effort: a missing sessions root, an unreadable directory, or a
 * corrupt file yields absence, never a throw.
 */
export function buildDiscoveryIndex(agentDir: string): DiscoveryIndex {
	const found = new Map<string, DiscoveredSession[]>();
	let dirs: string[];
	try {
		dirs = readdirSync(join(agentDir, "sessions"));
	} catch {
		return found;
	}
	for (const dir of dirs) {
		const dirPath = join(agentDir, "sessions", dir);
		let files: string[];
		try {
			files = readdirSync(dirPath);
		} catch {
			continue;
		}
		for (const file of files) {
			if (!file.endsWith(".jsonl")) continue;
			const filePath = join(dirPath, file);
			const header = readDiscoveryHeader(filePath);
			if (header === null || header.cwd === null) continue;
			const key = resolve(header.cwd);
			const list = found.get(key) ?? [];
			list.push({
				id: header.id,
				cwd: key,
				file: filePath,
				mtimeMs: mtimeMsOf(filePath),
			});
			found.set(key, list);
		}
	}
	for (const list of found.values()) {
		list.sort((a, b) => b.mtimeMs - a.mtimeMs);
	}
	return found;
}

/** Every path holding pi sessions, sorted. The /switch-path listing. */
export function listDiscoveryPaths(index: DiscoveryIndex): string[] {
	return [...index.keys()].sort();
}

/** Newest session rooted at a path, or null when the path holds none. */
export function mostRecentSessionAtPath(
	index: DiscoveryIndex,
	rawPath: string,
): DiscoveredSession | null {
	return index.get(resolve(rawPath))?.[0] ?? null;
}

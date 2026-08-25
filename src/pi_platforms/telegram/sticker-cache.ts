// pi_platforms/telegram/sticker-cache — vision-described sticker cache (M7),
// ported from the READ-ONLY Hermes reference gateway/sticker_cache.py:
//   ::get_cached_description / ::cache_sticker_description
//     (keyed by Telegram's stable file_unique_id; JSON on disk)
//   ::_save_cache (ATOMIC write: temp file in the SAME directory + rename)
//   ::build_sticker_injection
//     ([The user sent a sticker <emoji> from "<set>"~ It shows: "<desc>" ...])
//   ::build_animated_sticker_injection (animated/video stickers can't be
//     analyzed as static images — emoji-only injection, NO analysis/caching;
//     adapter.py:_handle_sticker branch)
//
// Census-port deltas (both documented in manifest.ts):
//   - the cache FILE lives under an injected mkdtemp directory (mkdtemp
//     isolation rule) instead of ~/.hermes;
//   - reads honor an OPTIONAL TTL measured on the INJECTED clock — Infinity
//     (the default) restores EXACT Hermes semantics where get_cached_description
//     never expires entries; finite ttlMs exists for test mechanics only.

import { mkdtempSync } from "node:fs";
import { readFile, rename, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { TELEGRAM_STICKER_CACHE_TTL_MS } from "./manifest.js";

/** sticker_cache.py::STICKER_VISION_PROMPT — kept concise to save tokens. */
export const STICKER_VISION_PROMPT =
	"Describe this sticker in 1-2 sentences. Focus on what it depicts -- " +
	"character, action, emotion. Be concise and objective.";

export interface CachedStickerDescription {
	description: string;
	emoji: string;
	setName: string;
	cachedAtMs: number;
}

interface CacheFileShape {
	[fileUniqueId: string]: {
		description: string;
		emoji: string;
		set_name: string;
		cached_at: number;
	};
}

export class StickerDescriptionCache {
	private readonly filePath: string;
	private readonly dir: string;

	constructor(
		opts: {
			/** Injected clock (ms). Defaults to Date.now for production use. */
			nowMs?: (() => number) | undefined;
			ttlMs?: number | undefined;
			/** Test/override location; default is a fresh mkdtemp directory. */
			dir?: string | undefined;
		} = {},
	) {
		this.dir = opts.dir ?? mkdtempSync(join(tmpdir(), "pi-tg-stickers-"));
		this.filePath = join(this.dir, "sticker_cache.json");
		this.nowMs = opts.nowMs ?? (() => Date.now());
		this.ttlMs = opts.ttlMs ?? TELEGRAM_STICKER_CACHE_TTL_MS;
	}

	readonly nowMs: () => number;
	readonly ttlMs: number;
	get directory(): string {
		return this.dir;
	}
	get path(): string {
		return this.filePath;
	}

	async load(): Promise<CacheFileShape> {
		try {
			const raw = await readFile(this.filePath, "utf8");
			const parsed = JSON.parse(raw) as unknown;
			return parsed !== null && typeof parsed === "object"
				? (parsed as CacheFileShape)
				: {};
		} catch {
			return {}; // missing/corrupt file ⇒ empty cache (never throw)
		}
	}

	/**
	 * sticker_cache.py::_save_cache atomicity: write a temp file in the SAME
	 * directory, then rename over the target (rename within a filesystem is
	 * atomic). Corrupt prior files are replaced wholesale.
	 */
	private async save(cache: CacheFileShape): Promise<void> {
		const tmpPath = `${this.filePath}.${process.pid}.tmp`;
		await writeFile(tmpPath, JSON.stringify(cache, null, 2), "utf8");
		await rename(tmpPath, this.filePath);
	}

	/** Hit (within TTL) → entry; miss/expired/corrupt → undefined. */
	async getCachedDescription(
		fileUniqueId: string,
	): Promise<CachedStickerDescription | undefined> {
		const cache = await this.load();
		const entry = cache[fileUniqueId];
		if (entry === undefined) return undefined;
		if (this.ttlMs !== Number.POSITIVE_INFINITY) {
			if (this.nowMs() - entry.cached_at > this.ttlMs) return undefined;
		}
		return {
			description: entry.description,
			emoji: entry.emoji,
			setName: entry.set_name,
			cachedAtMs: entry.cached_at,
		};
	}

	async cacheStickerDescription(
		fileUniqueId: string,
		description: string,
		emoji = "",
		setName = "",
	): Promise<void> {
		const cache = await this.load();
		cache[fileUniqueId] = {
			description,
			emoji,
			set_name: setName,
			cached_at: this.nowMs(),
		};
		await this.save(cache);
	}

	/** Entries whose age exceeds the TTL (injected-clock observation). */
	async expiredIds(): Promise<string[]> {
		const cache = await this.load();
		if (this.ttlMs === Number.POSITIVE_INFINITY) return [];
		const now = this.nowMs();
		return Object.keys(cache).filter(
			(id) => now - (cache[id]?.cached_at ?? 0) > this.ttlMs,
		);
	}
}

/**
 * sticker_cache.py::build_sticker_injection — exact warm-style injection
 * format consumed by the agent loop.
 */
export function buildStickerInjection(
	description: string,
	emoji = "",
	setName = "",
): string {
	let context = "";
	if (setName && emoji) context = ` ${emoji} from "${setName}"`;
	else if (emoji) context = ` ${emoji}`;
	return `[The user sent a sticker${context}~ It shows: "${description}" (=^.w.^=)]`;
}

/** sticker_cache.py::build_animated_sticker_injection. */
export function buildAnimatedStickerInjection(emoji = ""): string {
	if (emoji) {
		return (
			`[The user sent an animated sticker ${emoji}~ ` +
			`I can't see animated ones yet, but the emoji suggests: ${emoji}]`
		);
	}
	return "[The user sent an animated sticker~ I can't see animated ones yet]";
}

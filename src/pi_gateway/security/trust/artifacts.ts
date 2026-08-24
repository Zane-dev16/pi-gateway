// pi_gateway/security/trust/artifacts — one-shot authenticated artifact
// constraints: TTL, size cap, exact MIME allowlist, SHA-256 integrity,
// scope binding, traversal-proof ids (06 §8.4; DEC-017 "artifacts
// upload/download with auth + TTL + size/MIME caps").
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/browser_control_artifacts.py ArtifactStore.store (@~280) —
//     size → MIME checks raise BEFORE any disk write; temp + atomic rename;
//   …load (@~350) — verify SHA-256 then consume ONE-SHOT (second load ⇒
//     ArtifactNotFound); checksum mismatch does NOT consume;
//   …_entry_for (@~430) — the target's own expiry raises Expired BEFORE any
//     sweep; scope mismatch refuses cross-principal reads;
//   …_artifact_path (@~455) — ids are server-minted 32-hex, regex-validated
//     and confined to the controlled root (ArtifactTraversal);
//   …_normalize_content_type (@~470) — strip parameters (";"), trim,
//     lowercase; malformed ⇒ "" ⇒ outside the exact allowlist.

import { createHash, randomBytes } from "node:crypto";
import {
	existsSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	realpathSync,
	renameSync,
	statSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";

/** browser_control_artifacts.py DEFAULT_ARTIFACT_TTL_SECONDS = 300. */
export const DEFAULT_ARTIFACT_TTL_MS = 300_000;
/** DEFAULT_MAX_ARTIFACT_BYTES (10 MiB). */
export const DEFAULT_MAX_ARTIFACT_BYTES = 10 * 1024 * 1024;
/** Default EXACT MIME allowlist — unknown/parameterized variants reject. */
export const DEFAULT_ALLOWED_MIME_TYPES: readonly string[] = [
	"application/json",
	"application/pdf",
	"image/gif",
	"image/jpeg",
	"image/png",
	"image/webp",
	"text/plain",
];
/** Length in hex chars of a minted artifact id. */
const ARTIFACT_ID_HEX = 32;
const ARTIFACT_ID_RE = /^[0-9a-f]{32}$/;
const TEMP_SUFFIX = ".tmp";
const FILENAME_LIMIT = 160;

// ── typed failures ───────────────────────────────────────────────────────

export class ArtifactError extends Error {}
export class ArtifactNotFound extends ArtifactError {}
export class ArtifactExpired extends ArtifactError {}
export class ArtifactTooLarge extends ArtifactError {}
export class ArtifactMimeRejected extends ArtifactError {}
export class ArtifactScopeMismatch extends ArtifactError {}
export class ArtifactChecksumMismatch extends ArtifactError {}
export class ArtifactTraversal extends ArtifactError {}

/**
 * Scope identity: ONLY server-derived principal + transport family hash in
 * (session_id deliberately EXCLUDED so HTTP upload → broker dispatch compose;
 * browser_control_artifacts.py:artifact_scope_key parity).
 */
export interface ArtifactScope {
	principal: string;
	transportFamily: string;
}

export function artifactScopeKey(scope: ArtifactScope): string {
	return createHash("sha256")
		.update(`${scope.principal}\u0000${scope.transportFamily}`, "utf8")
		.digest("hex");
}

/** Canonical MIME type or "" for malformed input (parameterized variants die). */
export function normalizeContentType(value: string): string {
	if (typeof value !== "string") return "";
	const canonical = value.trim().split(";")[0];
	return canonical === undefined ? "" : canonical.trim().toLowerCase();
}

/** Display-only filename sanitize; never used as a filesystem path. */
export function boundedFilename(value: string): string {
	if (typeof value !== "string") return "";
	let cleaned = "";
	for (const ch of value.trim()) {
		const code = ch.codePointAt(0);
		if (code === undefined || code < 32) continue; // control chars dropped
		cleaned += ch === "\\" || ch === "/" ? "_" : ch;
	}
	return cleaned.slice(0, FILENAME_LIMIT);
}

// ── fs seam ──────────────────────────────────────────────────────────────

export interface ArtifactFs {
	writeFileAtomic(target: string, data: Buffer): void;
	readFile(path: string): Buffer;
	unlink(path: string): void;
	exists(path: string): boolean;
	/** Basenames of orphaned temp files inside the controlled root. */
	listTempNames(): string[];
	/** Age in ms of one temp file (null when stat fails). */
	tempAgeMs(name: string): number | null;
}

/** node:fs-backed seam over the controlled root (wall-clock mtime domain). */
export class NodeArtifactFs implements ArtifactFs {
	constructor(private readonly root: string) {}

	writeFileAtomic(target: string, data: Buffer): void {
		const temp = `${target}${TEMP_SUFFIX}`;
		writeFileSync(temp, data);
		renameSync(temp, target); // readers never observe partial writes
	}
	readFile(path: string): Buffer {
		return readFileSync(path);
	}
	unlink(path: string): void {
		unlinkSync(path);
	}
	exists(path: string): boolean {
		return existsSync(path);
	}
	listTempNames(): string[] {
		try {
			return readdirSync(this.root).filter((n) => n.endsWith(TEMP_SUFFIX));
		} catch {
			return [];
		}
	}
	tempAgeMs(name: string): number | null {
		try {
			return Date.now() - statSync(join(this.root, name)).mtimeMs;
		} catch {
			return null;
		}
	}
}

// ── receipt + store ──────────────────────────────────────────────────────

export interface ArtifactReceipt {
	artifactId: string;
	sha256: string;
	sizeBytes: number;
	contentType: string;
	filename: string;
	createdAtMs: number;
	expiresAtMs: number;
	ttlMs: number;
	scopeKey: string;
}

interface StoreEntry {
	receipt: ArtifactReceipt;
	path: string;
}

export interface ArtifactStoreOptions {
	/** Controlled root; artifacts never escape it (profile boundary). */
	root: string;
	ttlMs?: number | undefined;
	maxBytes?: number | undefined;
	allowedMimeTypes?: readonly string[] | undefined;
	/** Injected epoch-ms clock. */
	nowMs: () => number;
	fs?: ArtifactFs | undefined;
	mintId?: (() => string) | undefined;
}

export interface ArtifactUpload {
	data: Buffer;
	filename: string;
	contentType: string;
	scope: ArtifactScope;
}

export class ArtifactStore {
	private readonly entries = new Map<string, StoreEntry>();
	private readonly ttlMs: number;
	private readonly maxBytes: number;
	private readonly allowedMimeTypes: ReadonlySet<string>;
	private readonly nowMs: () => number;
	private readonly mintId: () => string;
	private readonly fs: ArtifactFs;
	private readonly resolvedRoot: string;

	constructor(options: ArtifactStoreOptions) {
		this.ttlMs = Math.max(1, options.ttlMs ?? DEFAULT_ARTIFACT_TTL_MS);
		this.maxBytes = Math.max(1, options.maxBytes ?? DEFAULT_MAX_ARTIFACT_BYTES);
		this.allowedMimeTypes = new Set(
			options.allowedMimeTypes ?? DEFAULT_ALLOWED_MIME_TYPES,
		);
		this.nowMs = options.nowMs;
		this.mintId =
			options.mintId ??
			(() => randomBytes(ARTIFACT_ID_HEX / 2).toString("hex"));
		this.resolvedRoot = safeResolveRoot(options.root);
		this.fs = options.fs ?? new NodeArtifactFs(options.root);
		mkdirSync(options.root, { recursive: true });
	}

	get maxArtifactBytes(): number {
		return this.maxBytes;
	}

	get ttl(): number {
		return this.ttlMs;
	}

	get allowedMimes(): readonly string[] {
		return [...this.allowedMimeTypes];
	}

	count(): number {
		return this.entries.size;
	}

	/**
	 * Validate + store one artifact. Size and MIME fail BEFORE any disk
	 * write; each cap is enforced independently (contract-tested).
	 */
	store(upload: ArtifactUpload): ArtifactReceipt {
		const size = upload.data.length;
		if (size > this.maxBytes) {
			throw new ArtifactTooLarge(
				`artifact is ${size} bytes; cap is ${this.maxBytes}`,
			);
		}
		const normalizedType = normalizeContentType(upload.contentType);
		if (!this.allowedMimeTypes.has(normalizedType)) {
			throw new ArtifactMimeRejected(
				`content type ${JSON.stringify(upload.contentType)} is outside the exact allowlist`,
			);
		}
		const now = this.nowMs();
		let artifactId = this.mintId();
		while (
			this.entries.has(artifactId) ||
			this.fs.exists(join(this.resolvedRoot, artifactId))
		) {
			artifactId = this.mintId(); // astronomically unlikely collision retry
		}
		const receipt: ArtifactReceipt = {
			artifactId,
			sha256: createHash("sha256").update(upload.data).digest("hex"),
			sizeBytes: size,
			contentType: normalizedType,
			filename: boundedFilename(upload.filename),
			createdAtMs: now,
			expiresAtMs: now + this.ttlMs,
			ttlMs: this.ttlMs,
			scopeKey: artifactScopeKey(upload.scope),
		};
		const target = join(this.resolvedRoot, artifactId);
		this.entries.set(artifactId, { receipt, path: target });
		try {
			this.fs.writeFileAtomic(target, upload.data);
		} catch (err) {
			this.entries.delete(artifactId);
			throw err instanceof Error ? err : new Error(String(err));
		}
		return receipt;
	}

	/** Existence + TTL + scope check WITHOUT consuming (broker gate parity). */
	validate(artifactId: string, scope: ArtifactScope): ArtifactReceipt {
		return this.entryFor(artifactId, artifactScopeKey(scope)).receipt;
	}

	/**
	 * One-shot download: verify checksum then CONSUME atomically — a second
	 * load of the same id raises ArtifactNotFound.
	 */
	load(
		artifactId: string,
		scope: ArtifactScope,
	): { bytes: Buffer; receipt: ArtifactReceipt } {
		const entry = this.entryFor(artifactId, artifactScopeKey(scope));
		let bytes: Buffer;
		try {
			bytes = this.fs.readFile(entry.path);
		} catch {
			this.entries.delete(artifactId);
			throw new ArtifactNotFound(
				`artifact ${JSON.stringify(artifactId)} is gone`,
			);
		}
		const actualSha = createHash("sha256").update(bytes).digest("hex");
		if (actualSha !== entry.receipt.sha256) {
			throw new ArtifactChecksumMismatch(
				`artifact ${JSON.stringify(artifactId)} failed SHA-256 validation`,
			);
		}
		this.entries.delete(artifactId);
		try {
			this.fs.unlink(entry.path);
		} catch {
			/* TTL sweep retries later (warning parity) */
		}
		return { bytes, receipt: entry.receipt };
	}

	/** Delete every artifact past TTL + stale orphan temps; returns count. */
	pruneExpired(now?: number | undefined): number {
		const at = now ?? this.nowMs();
		let removed = 0;
		for (const [id, entry] of this.entries) {
			if (entry.receipt.expiresAtMs <= at) {
				this.entries.delete(id);
				try {
					this.fs.unlink(entry.path);
				} catch {
					/* next sweep retries */
				}
				removed += 1;
			}
		}
		for (const name of this.fs.listTempNames()) {
			const age = this.fs.tempAgeMs(name);
			if (age !== null && age > this.ttlMs) {
				try {
					this.fs.unlink(join(this.resolvedRoot, name));
				} catch {
					/* temp already removed — sweep is best-effort */
				}
			}
		}
		return removed;
	}

	// ── internals ────────────────────────────────────────────────────────

	private entryFor(artifactId: string, scopeKey: string): StoreEntry {
		this.assertValidId(artifactId);
		const now = this.nowMs();
		let entry = this.entries.get(artifactId);
		if (entry === undefined) {
			this.pruneExpired(now); // expired entries swept only on a MISS lookup
			entry = this.entries.get(artifactId);
		}
		if (entry === undefined) {
			throw new ArtifactNotFound(
				`unknown artifact ${JSON.stringify(artifactId)}`,
			);
		}
		// The target's OWN expiry surfaces as Expired before anything else.
		if (entry.receipt.expiresAtMs <= now) {
			this.entries.delete(artifactId);
			try {
				this.fs.unlink(entry.path);
			} catch {
				/* already gone */
			}
			throw new ArtifactExpired(
				`artifact ${JSON.stringify(artifactId)} expired`,
			);
		}
		if (entry.receipt.scopeKey !== scopeKey) {
			throw new ArtifactScopeMismatch(
				`artifact ${JSON.stringify(artifactId)} is bound to a different scope`,
			);
		}
		return entry;
	}

	private assertValidId(artifactId: string): void {
		if (!ARTIFACT_ID_RE.test(artifactId)) {
			throw new ArtifactTraversal(
				`invalid artifact id ${JSON.stringify(artifactId)}`,
			);
		}
		// Belt-and-braces containment (browser_control_artifacts.py parity:
		// candidate.parent must BE the resolved controlled root).
		const candidate = resolve(this.resolvedRoot, artifactId);
		if (!isAbsolute(candidate) || dirname(candidate) !== this.resolvedRoot) {
			throw new ArtifactTraversal(
				`artifact path escapes root for ${JSON.stringify(artifactId)}`,
			);
		}
	}
}

function safeResolveRoot(root: string): string {
	try {
		return realpathSync(root);
	} catch {
		return resolve(root);
	}
}

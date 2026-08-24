// Behavior contracts for one-shot artifact constraints (06 §8.4;
// browser_control_artifacts.py port): TTL, size, and MIME caps each enforced
// INDEPENDENTLY before any disk write; SHA-256 integrity; scope binding;
// traversal-proof ids; one-shot consumption; injected clock throughout.

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	ArtifactChecksumMismatch,
	ArtifactExpired,
	ArtifactMimeRejected,
	ArtifactNotFound,
	ArtifactScopeMismatch,
	ArtifactStore,
	ArtifactTooLarge,
	ArtifactTraversal,
	NodeArtifactFs,
	artifactScopeKey,
	boundedFilename,
	normalizeContentType,
} from "./index.js";

const SCOPE_A = { principal: "principal-a", transportFamily: "http" };
const SCOPE_B = { principal: "principal-b", transportFamily: "http" };

interface Harness {
	store: ArtifactStore;
	advance(ms: number): void;
	root: string;
	cleanup(): void;
}

function harness(
	overrides: {
		ttlMs?: number;
		maxBytes?: number;
		allowedMimeTypes?: readonly string[];
	} = {},
): Harness {
	let nowMs = 1_000_000;
	const root = mkdtempSync(join(tmpdir(), "pi-trust-artifacts-"));
	const store = new ArtifactStore({
		root,
		nowMs: () => nowMs,
		...overrides,
	});
	return {
		store,
		advance: (ms: number) => {
			nowMs += ms;
		},
		root,
		cleanup: () => rmSync(root, { recursive: true, force: true }),
	};
}
let activeCleanup: (() => void) | null = null;
afterEach(() => {
	activeCleanup?.();
	activeCleanup = null;
});

function tracked(h: Harness): Harness {
	activeCleanup = h.cleanup;
	return h;
}

describe("size cap enforced independently (BEFORE any disk write)", () => {
	it("oversize bytes throw TooLarge even with valid MIME + fresh clock", () => {
		const h = tracked(harness({ maxBytes: 1024 }));
		expect(() =>
			h.store.store({
				data: Buffer.alloc(1025),
				filename: "ok.png",
				contentType: "image/png",
				scope: SCOPE_A,
			}),
		).toThrow(ArtifactTooLarge);
		expect(h.store.count()).toBe(0);
	});

	it("exactly AT the cap stores", () => {
		const h = tracked(harness({ maxBytes: 1024 }));
		const receipt = h.store.store({
			data: Buffer.alloc(1024),
			filename: "edge.pdf",
			contentType: "application/pdf",
			scope: SCOPE_A,
		});
		expect(receipt.sizeBytes).toBe(1024);
	});
});

describe("MIME allowlist enforced independently (exact match)", () => {
	it("unknown base type throws MimeRejected even within size + TTL", () => {
		const h = tracked(harness());
		expect(() =>
			h.store.store({
				data: Buffer.from("x"),
				filename: "evil.exe",
				contentType: "application/x-msdownload",
				scope: SCOPE_A,
			}),
		).toThrow(ArtifactMimeRejected);
		expect(h.store.count()).toBe(0);
	});

	it("parameterized canonical type normalizes down and ADMITS; malformed rejects", () => {
		const h = tracked(harness());
		const receipt = h.store.store({
			data: Buffer.from("x"),
			filename: "a.txt",
			contentType: "Text/Plain; charset=utf-8",
			scope: SCOPE_A,
		});
		expect(receipt.contentType).toBe("text/plain");
		expect(normalizeContentType("  IMAGE/PNG ;q=1")).toBe("image/png");
		expect(normalizeContentType("")).toBe("");
		expect(() =>
			h.store.store({
				data: Buffer.from("x"),
				filename: "b.txt",
				contentType: ";;;garbage",
				scope: SCOPE_A,
			}),
		).toThrow(ArtifactMimeRejected);
	});
});

describe("TTL enforced independently (injected clock)", () => {
	it("live artifact loads; past-TTL raises Expired and is swept", () => {
		const h = tracked(harness({ ttlMs: 300_000 }));
		const receipt = h.store.store({
			data: Buffer.from("payload"),
			filename: "p.json",
			contentType: "application/json",
			scope: SCOPE_A,
		});
		h.advance(299_999);
		expect(h.store.validate(receipt.artifactId, SCOPE_A).artifactId).toBe(
			receipt.artifactId,
		);
		h.advance(1); // expiresAtMs reached
		expect(() => h.store.validate(receipt.artifactId, SCOPE_A)).toThrow(
			ArtifactExpired,
		);
		// Sweep removes the entry AND its file.
		expect(h.store.pruneExpired()).toBe(0); // entryFor already consumed it
		expect(h.store.count()).toBe(0);
	});

	it("pruneExpired bulk-removes past-TTL artifacts by count", () => {
		const h = tracked(harness({ ttlMs: 1000 }));
		for (let i = 0; i < 3; i++) {
			h.store.store({
				data: Buffer.from([i]),
				filename: `f${i}.txt`,
				contentType: "text/plain",
				scope: SCOPE_A,
			});
		}
		h.advance(2000);
		expect(h.store.pruneExpired()).toBe(3);
	});
});

describe("one-shot download + integrity", () => {
	it("load returns bytes then CONSUMES: second load is NotFound", () => {
		const h = tracked(harness());
		const receipt = h.store.store({
			data: Buffer.from("once-only"),
			filename: "shot.txt",
			contentType: "text/plain",
			scope: SCOPE_A,
		});
		const first = h.store.load(receipt.artifactId, SCOPE_A);
		expect(first.bytes.toString()).toBe("once-only");
		expect(first.receipt.sha256).toMatch(/^[0-9a-f]{64}$/);
		expect(() => h.store.load(receipt.artifactId, SCOPE_A)).toThrow(
			ArtifactNotFound,
		);
	});

	it("checksum mismatch does NOT consume (entry survives for forensics)", () => {
		const h = tracked(harness());
		const receipt = h.store.store({
			data: Buffer.from("intact"),
			filename: "c.txt",
			contentType: "text/plain",
			scope: SCOPE_A,
		});
		// Tamper with the bytes behind the store's back.
		writeFileSync(join(h.root, receipt.artifactId), Buffer.from("TAMPERED"));
		expect(() => h.store.load(receipt.artifactId, SCOPE_A)).toThrow(
			ArtifactChecksumMismatch,
		);
		// The entry SURVIVED the failed checksum (not consumed).
		expect(h.store.validate(receipt.artifactId, SCOPE_A).sha256).toBe(
			receipt.sha256,
		);
	});
});

describe("scope binding + traversal-proof ids", () => {
	it("cross-principal load refuses with ScopeMismatch; validate too", () => {
		const h = tracked(harness());
		const receipt = h.store.store({
			data: Buffer.from("secret"),
			filename: "s.txt",
			contentType: "text/plain",
			scope: SCOPE_A,
		});
		for (const op of [
			h.store.validate.bind(h.store),
			h.store.load.bind(h.store),
		]) {
			expect(() => op(receipt.artifactId, SCOPE_B)).toThrow(
				ArtifactScopeMismatch,
			);
		}
		expect(artifactScopeKey(SCOPE_A)).not.toBe(artifactScopeKey(SCOPE_B));
	});

	it("malformed ids raise Traversal — no path escape possible", () => {
		const h = tracked(harness());
		for (const hostile of [
			"../../etc/passwd",
			"",
			"zzz-not-hex",
			`${"a".repeat(31)}G`,
		]) {
			expect(() => h.store.load(hostile, SCOPE_A)).toThrow(ArtifactTraversal);
		}
	});

	it("unknown but well-formed ids are NotFound (not Traversal)", () => {
		const h = tracked(harness());
		expect(() => h.store.load("b".repeat(32), SCOPE_A)).toThrow(
			ArtifactNotFound,
		);
	});
});

describe("filename sanitize + real-fs round trip", () => {
	it("filenames strip separators/control chars and bound to 160 chars", () => {
		expect(boundedFilename("../../etc/passwd")).toBe(".._.._etc_passwd");
		expect(boundedFilename("a\u0000b/c\rd.png")).toBe("ab_cd.png");
		expect(boundedFilename("x".repeat(500))).toHaveLength(160);
	});

	it("NodeArtifactFs atomic write → read → unlink round trip over mkdtemp root", () => {
		const root = mkdtempSync(join(tmpdir(), "pi-trust-artifacts-fs-"));
		activeCleanup = () => rmSync(root, { recursive: true, force: true });
		const fs = new NodeArtifactFs(root);
		const target = join(root, "f".repeat(32));
		fs.writeFileAtomic(target, Buffer.from("atomic"));
		expect(fs.exists(target)).toBe(true);
		expect(fs.readFile(target).toString()).toBe("atomic");
		fs.unlink(target);
		expect(fs.exists(target)).toBe(false);
	});
});

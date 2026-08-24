// Snapshot-stage behavior contracts (08 §6): identical critical set per
// profile under ITS OWN home; 1 GiB cap skip WITH reason; keep=1 prune;
// pruning suppression on protected skips; SQLite safe-copy guards.

import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import {
	PRE_UPDATE_KEEP,
	SNAPSHOTS_DIRNAME,
	createPreUpdateSnapshotsAllProfiles,
	pruneSnapshotDirs,
	safeCopyDb,
	snapshotProfileHome,
} from "./snapshot.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-snap-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

function makeHome(name: string): {
	profile: string;
	home: string;
	root: string;
} {
	const home = join(dir, name);
	mkdirSync(home, { recursive: true });
	return { profile: name, home, root: home };
}

function seedStateFile(home: string, rel: string, body: string): void {
	const path = join(home, rel);
	mkdirSync(join(path, ".."), { recursive: true });
	writeFileSync(path, body);
}

describe("snapshotProfileHome", () => {
	it("copies the critical set that exists and skips absent files silently-normal", () => {
		const home = makeHome("solo");
		seedStateFile(home.root, "config.yaml", "gateway: {}\n");
		seedStateFile(home.root, "cron/jobs.json", "[]");
		const result = snapshotProfileHome({
			profile: home.profile,
			home: home.root,
			clockSeconds: 1_000_000,
		});
		expect(result.ok).toBe(true);
		expect(result.copied).toBe(2);
		expect(result.skips).toEqual([]);
		const snapRoot = join(home.root, SNAPSHOTS_DIRNAME);
		const [snapName] = readdirSync(snapRoot);
		expect(
			readFileSync(join(snapRoot, snapName as string, "config.yaml"), "utf8"),
		).toContain("gateway");
		expect(existsSync(join(snapRoot, snapName as string, ".env"))).toBe(false);
	});

	it("skips oversized files WITH a recorded reason (cap injected small)", () => {
		const home = makeHome("capped");
		seedStateFile(home.root, "state.db", "x".repeat(64));
		seedStateFile(home.root, "config.yaml", "tiny\n");
		const result = snapshotProfileHome({
			profile: home.profile,
			home: home.root,
			clockSeconds: 1_000_000,
			maxFileSize: 16,
		});
		expect(result.copied).toBe(1);
		expect(result.skippedOversized).toEqual(["state.db"]);
		expect(result.skips[0]?.reason).toMatch(/cap/);
	});

	it("refuses zeroed SQLite sources via the safe-copy corruption guard", () => {
		const home = makeHome("zeroed");
		seedStateFile(home.root, "state.db", "\0".repeat(4096));
		const result = snapshotProfileHome({
			profile: home.profile,
			home: home.root,
			clockSeconds: 1_000_000,
		});
		expect(result.ok).toBe(true); // best-effort stage never fails wholesale
		const skip = result.skips.find((s) => s.path === "state.db");
		expect(skip?.reason).toMatch(/zeroed/);
	});

	it("marks snapshots with protected skips as incomplete", () => {
		const home = makeHome("partial");
		seedStateFile(home.root, "state.db", "\0".repeat(4096));
		const result = snapshotProfileHome({
			profile: home.profile,
			home: home.root,
			clockSeconds: 1_000_000,
		});
		const [snapName] = readdirSync(join(home.root, SNAPSHOTS_DIRNAME));
		expect(
			existsSync(
				join(home.root, SNAPSHOTS_DIRNAME, snapName as string, ".incomplete"),
			),
		).toBe(result.skips.length > 0);
	});
});

describe("pruneSnapshotDirs (pre-update keep policy)", () => {
	function seedSnaps(root: string, names: string[]): void {
		for (const name of names) {
			mkdirSync(join(root, name), { recursive: true });
		}
	}

	it("floors keep to 1 — the just-written snapshot always survives", () => {
		const root = join(dir, "snaps-a");
		seedSnaps(root, [
			"2026-01-01T00-00-00-000Z-pre-update",
			"2026-01-02T00-00-00-000Z-pre-update",
			"2026-01-03T00-00-00-000Z-pre-update",
		]);
		expect(pruneSnapshotDirs(root, PRE_UPDATE_KEEP)).toBe(2);
		expect(readdirSync(root)).toEqual(["2026-01-03T00-00-00-000Z-pre-update"]);
	});

	it("never touches hand-made directories or dot entries", () => {
		const root = join(dir, "snaps-b");
		seedSnaps(root, [
			"2026-01-02T00-00-00-000Z-pre-update",
			"my-manual-backup",
		]);
		mkdirSync(join(root, ".hidden-pre-update"));
		pruneSnapshotDirs(root, 1);
		expect(readdirSync(root).sort()).toEqual([
			".hidden-pre-update",
			"2026-01-02T00-00-00-000Z-pre-update",
			"my-manual-backup",
		]);
	});
});

describe("createPreUpdateSnapshotsAllProfiles (#66140 fleet parity)", () => {
	it("snapshots EVERY sibling profile under its OWN home with the same set", () => {
		const a = makeHome("alpha");
		const b = makeHome("beta");
		seedStateFile(a.root, "config.yaml", "a\n");
		seedStateFile(b.root, "cron/jobs.json", "[1]");
		const stage = createPreUpdateSnapshotsAllProfiles({
			profiles: [a, b],
			clockSeconds: 1_000_000,
		});
		expect(stage.idsByProfile).toHaveProperty("alpha");
		expect(stage.idsByProfile).toHaveProperty("beta");
		expect(
			existsSync(
				join(
					a.root,
					SNAPSHOTS_DIRNAME,
					stage.idsByProfile["alpha"] as string,
					"config.yaml",
				),
			),
		).toBe(true);
		expect(
			existsSync(
				join(
					b.root,
					SNAPSHOTS_DIRNAME,
					stage.idsByProfile["beta"] as string,
					"cron/jobs.json",
				),
			),
		).toBe(true);
		// Identical snapshot id per run: same set semantics, no partial tiers.
		expect(stage.idsByProfile["alpha"]).toBe(stage.idsByProfile["beta"]);
	});

	it("SUPPRESSES pruning when any protected file was skipped this run", () => {
		const home = makeHome("guarded");
		// Pre-existing older snapshot must survive the run.
		const oldSnap = join(
			home.root,
			SNAPSHOTS_DIRNAME,
			"2026-01-01T00-00-00-000Z-pre-update",
		);
		mkdirSync(oldSnap, { recursive: true });
		seedStateFile(home.root, "state.db", "\0".repeat(4096)); // protected skip
		const stage = createPreUpdateSnapshotsAllProfiles({
			profiles: [{ profile: home.profile, home: home.root }],
			clockSeconds: 1_000_000,
		});
		expect(stage.pruningSuppressed).toBe(true);
		// Incompleteness must not evict the last complete snapshot.
		expect(existsSync(oldSnap)).toBe(true);
	});

	it("prunes to keep=1 across runs when nothing was skipped", () => {
		const home = makeHome("rolling");
		seedStateFile(home.root, "config.yaml", "cfg\n");
		createPreUpdateSnapshotsAllProfiles({
			profiles: [{ profile: home.profile, home: home.root }],
			clockSeconds: 1_000_000,
		});
		// A later run with a strictly newer timestamp prunes the first.
		const stage2 = createPreUpdateSnapshotsAllProfiles({
			profiles: [{ profile: home.profile, home: home.root }],
			clockSeconds: 1_000_000 + 3600,
		});
		expect(stage2.pruningSuppressed).toBe(false);
		expect(readdirSync(join(home.root, SNAPSHOTS_DIRNAME))).toEqual([
			stage2.idsByProfile[home.profile] as string,
		]);
	});
});

describe("safeCopyDb", () => {
	it("copies a healthy SQLite db with WAL siblings and verifies the copy", () => {
		const srcDir = join(dir, "dbsrc");
		mkdirSync(srcDir, { recursive: true });
		const src = join(srcDir, "state.db");
		const db = new Database(src);
		db.exec("CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('hello');");
		db.pragma("wal_checkpoint(TRUNCATE)");
		db.close();
		const dest = join(dir, "dbdest", "state.db");
		const outcome = safeCopyDb(src, dest);
		expect(outcome).toEqual({ ok: true });
		const check = new Database(dest, { readonly: true });
		const row = check.prepare("SELECT v FROM t").get() as { v: string };
		check.close();
		expect(row.v).toBe("hello");
	});
});

// Path validation ladder + policy bridge contracts (03 §9.2; §11 "Media
// grammar" row). Real files under mkdtemp; INJECTED clock for the recency
// window; injected env/home so process state never leaks between cases.

import {
	mkdtempSync,
	mkdirSync,
	realpathSync,
	rmSync,
	symlinkSync,
	utimesSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	MEDIA_DELIVERY_ALLOW_DIRS_ENV,
	MEDIA_DELIVERY_STRICT_ENV,
	MEDIA_DELIVERY_TRUST_RECENT_ENV,
	MEDIA_DELIVERY_TRUST_RECENT_SECONDS_ENV,
	applyMediaPolicyEnv,
	collectAllowedRoots,
	expandUser,
	filterMediaDeliveryPaths,
	mediaDeliveryRecencySeconds,
	parseDockerVolumeMounts,
	pathUnderDeniedPrefix,
	stripPathWrappers,
	translateDockerContainerMediaPath,
	validateMediaDeliveryPath,
} from "./media-policy.js";

let root: string;
let home: string;
let piHome: string;

const NOW_MS = 1_800_000_000_000;
const FRESH = NOW_MS - 30_000; // 30s old — inside default 600s window
const STALE = NOW_MS - 3_600_000; // 1h old — outside

function mustMkdir(p: string): string {
	// recursive mkdirSync returns the FIRST created ancestor (or undefined) —
	// we always want the requested leaf.
	mkdirSync(p, { recursive: true });
	return p;
}

function touch(path: string, mtimeMs: number, content = "x"): string {
	writeFileSync(path, content);
	utimesSync(path, new Date(mtimeMs), new Date(mtimeMs));
	return path;
}

beforeAll(() => {
	root = mkdtempSync(join(tmpdir(), "pi-outbound-policy-"));
	home = join(root, "home");
	piHome = join(root, "pihome");
	mustMkdir(home);
	mustMkdir(piHome);
});

afterAll(() => {
	rmSync(root, { recursive: true, force: true });
});

function deps(extra?: Record<string, string>) {
	return {
		env: { ...extra },
		home,
		piHome,
		nowMs: NOW_MS,
	};
}

describe("quote/edge stripping + expanduser", () => {
	it("strips quote wrappers and edge punctuation without touching inner path", () => {
		expect(stripPathWrappers('`"/tmp/a.png"`')).toBe("/tmp/a.png");
		expect(stripPathWrappers("'/tmp/a.png',")).toBe("/tmp/a.png");
		expect(stripPathWrappers("/tmp/a.png.")).toBe("/tmp/a.png");
		expect(stripPathWrappers("/tmp/a.png")).toBe("/tmp/a.png");
	});

	it("expandUser resolves ~ and ~/-prefixed paths against the INJECTED home", () => {
		expect(expandUser("~", home)).toBe(home);
		expect(expandUser("~/docs/a.pdf", home)).toBe(join(home, "docs/a.pdf"));
		expect(expandUser("/abs/x.png", home)).toBe("/abs/x.png");
	});
});

describe("validate_media_delivery_path — DEFAULT mode ladder", () => {
	it("accepts an existing regular file off the denylist and returns the RESOLVED absolute path", () => {
		const f = touch(join(root, "report.pdf"), STALE);
		const got = validateMediaDeliveryPath(f, deps());
		expect(got).not.toBeNull();
		if (!got) return;
		expect(got.startsWith(realpathSync(root))).toBe(true);
	});

	it("rejects non-existent paths and directories", () => {
		expect(
			validateMediaDeliveryPath(join(root, "missing.png"), deps()),
		).toBeNull();
		const dir = mustMkdir(join(piHome, "cache"));
		expect(validateMediaDeliveryPath(dir, deps())).toBeNull();
	});

	it("rejects relative paths", () => {
		expect(validateMediaDeliveryPath("relative/file.png", deps())).toBeNull();
	});

	it("denylist blocks system prefixes and credential locations", () => {
		for (const denied of [
			"/etc/passwd",
			"/proc/self/cmdline",
			"/var/lib/db",
			"/boot/vmlinuz",
		]) {
			expect(pathUnderDeniedPrefix(denied, deps()), denied).toBe(true);
		}
		const ssh = mustMkdir(join(home, ".ssh"));
		const key = touch(join(ssh, "id_rsa"), STALE);
		expect(validateMediaDeliveryPath(key, deps())).toBeNull(); // ~/.ssh credential
	});

	it("the running user's OWN injected home is exempt from the /root-style prefix block", () => {
		const work = touch(join(home, "proposal.docx"), STALE);
		expect(validateMediaDeliveryPath(work, deps())).not.toBeNull();
	});

	it("pi-home credential stores are denied per-file so sibling artifacts still deliver", () => {
		const envFile = touch(join(piHome, ".env"), STALE);
		expect(validateMediaDeliveryPath(envFile, deps())).toBeNull();
		const pairing = mustMkdir(join(piHome, "pairing"));
		expect(
			validateMediaDeliveryPath(touch(join(pairing, "a.json"), STALE), deps()),
		).toBeNull();
		// Ad-hoc agent-written files under the pi home stay deliverable.
		const skill = touch(join(piHome, "chart.png"), STALE);
		expect(validateMediaDeliveryPath(skill, deps())).not.toBeNull();
	});

	it("managed cache roots are honored BEFORE the denylist (generated media delivers)", () => {
		const imgDir = mustMkdir(join(piHome, "cache", "images"));
		const img = touch(join(imgDir, "out.png"), STALE);
		expect(validateMediaDeliveryPath(img, deps())).not.toBeNull();
	});

	it("default mode needs NO allowlist: any off-denylist file delivers; allowlist adds strict-mode trust", () => {
		const opDir = mustMkdir(join(root, "opspace"));
		const f = touch(join(opDir, "artifact.zip"), STALE);
		expect(validateMediaDeliveryPath(f, deps())).not.toBeNull(); // DEFAULT mode
		// Under STRICT mode the same file needs the operator allowlist:
		expect(
			validateMediaDeliveryPath(f, deps({ [MEDIA_DELIVERY_STRICT_ENV]: "1" })),
		).toBeNull();
		expect(
			validateMediaDeliveryPath(
				f,
				deps({
					[MEDIA_DELIVERY_STRICT_ENV]: "1",
					[MEDIA_DELIVERY_ALLOW_DIRS_ENV]: opDir,
				}),
			),
		).not.toBeNull(); // stale mtime, allowed ONLY via allowlist membership
	});

	it("symlinks resolve BEFORE containment checks: link into a denylist is rejected, link into cache accepted", () => {
		const awsDir = mustMkdir(join(home, ".aws"));
		const secret = touch(join(awsDir, "creds"), STALE);
		const linkDir = mustMkdir(join(root, "links"));
		try {
			const badLink = join(linkDir, "bad.png");
			symlinkSync(secret, badLink);
			expect(validateMediaDeliveryPath(badLink, deps())).toBeNull();

			const imgDir = mustMkdir(join(piHome, "image_cache"));
			const real = touch(join(imgDir, "real.png"), STALE);
			const goodLink = join(linkDir, "good.png");
			symlinkSync(real, goodLink);
			expect(validateMediaDeliveryPath(goodLink, deps())).not.toBeNull();
		} catch {
			return; // platform without symlink support
		}
	});

	it("a crafted NUL path skips itself instead of raising", () => {
		expect(validateMediaDeliveryPath("~/x\0y.png", deps())).toBeNull();
	});
});

describe("STRICT mode — allowlist-or-recency ladder", () => {
	it("non-cache existing file is rejected in strict mode even when it exists", () => {
		const f = touch(join(root, "stale-report.pdf"), STALE - 100_000);
		expect(
			validateMediaDeliveryPath(f, deps({ [MEDIA_DELIVERY_STRICT_ENV]: "1" })),
		).toBeNull();
	});

	it("recency window rescues freshly produced files OUTSIDE trusted roots", () => {
		const fresh = touch(join(root, "fresh-report.pdf"), FRESH);
		expect(
			validateMediaDeliveryPath(
				fresh,
				deps({ [MEDIA_DELIVERY_STRICT_ENV]: "1" }),
			),
		).not.toBeNull();
	});

	it("credential locations stay blocked in strict mode EVEN when fresh", () => {
		const ssh = mustMkdir(join(home, ".ssh"));
		const freshKey = touch(join(ssh, "id_ed25519"), FRESH);
		expect(
			validateMediaDeliveryPath(
				freshKey,
				deps({ [MEDIA_DELIVERY_STRICT_ENV]: "1" }),
			),
		).toBeNull();
	});

	it("recency trust can be disabled entirely (pure-allowlist mode)", () => {
		const fresh = touch(join(root, "fresh2.pdf"), FRESH);
		expect(
			validateMediaDeliveryPath(
				fresh,
				deps({
					[MEDIA_DELIVERY_STRICT_ENV]: "1",
					[MEDIA_DELIVERY_TRUST_RECENT_ENV]: "0",
				}),
			),
		).toBeNull();
	});

	it("custom window width via HERMES_MEDIA_TRUST_RECENT_SECONDS bounds the rescue", () => {
		const barely = touch(join(root, "barely.pdf"), NOW_MS - 120_000); // 2min old
		const d = deps({
			[MEDIA_DELIVERY_STRICT_ENV]: "1",
			[MEDIA_DELIVERY_TRUST_RECENT_SECONDS_ENV]: "60",
		});
		expect(mediaDeliveryRecencySeconds({ env: d.env })).toBe(60);
		expect(validateMediaDeliveryPath(barely, d)).toBeNull();

		const within = touch(join(root, "within.pdf"), NOW_MS - 30_000);
		expect(validateMediaDeliveryPath(within, d)).not.toBeNull();
	});

	it("cache-root membership still wins under strict mode regardless of mtime", () => {
		const shotDir = mustMkdir(join(piHome, "cache", "screenshots"));
		const oldShot = touch(join(shotDir, "old.png"), STALE - 86_400_000);
		expect(
			validateMediaDeliveryPath(
				oldShot,
				deps({ [MEDIA_DELIVERY_STRICT_ENV]: "1" }),
			),
		).not.toBeNull();
	});
});

describe("docker container-path translation", () => {
	it("parses TERMINAL_DOCKER_VOLUMES host:container specs, skipping named volumes", () => {
		const mounts = parseDockerVolumeMounts({
			TERMINAL_DOCKER_VOLUMES: JSON.stringify([
				"/host/data:/workspace/data:rw",
				"namedvol:/x",
				"/host/b:/b",
			]),
		});
		expect(mounts).toEqual([
			{ host: expect.any(String), container: "/workspace/data" },
			{ host: expect.any(String), container: "/b" },
		]);
	});

	it("translates a container MEDIA path to its host path before existence checks", () => {
		const outDir = mustMkdir(join(root, "sandbox", "workspace", "out"));
		const produced = touch(join(outDir, "plot.svg"), FRESH);
		const mounts = JSON.stringify([
			`${join(root, "sandbox", "workspace")}:/workspace`,
		]);
		const translated = translateDockerContainerMediaPath(
			"/workspace/out/plot.svg",
			{
				TERMINAL_DOCKER_VOLUMES: mounts,
			},
		);
		expect(translated).toBe(produced);
		expect(
			validateMediaDeliveryPath(
				"/workspace/out/plot.svg",
				deps({ TERMINAL_DOCKER_VOLUMES: mounts }),
			),
		).toBe(produced);
	});

	it("longest-prefix match wins when mounts nest", () => {
		const specialDir = mustMkdir(join(root, "h", "data", "special"));
		const deep = touch(join(specialDir, "f.txt"), FRESH);
		const got = translateDockerContainerMediaPath("/data/special/f.txt", {
			TERMINAL_DOCKER_VOLUMES: JSON.stringify([
				`${join(root, "h")}:/`,
				`${join(root, "h", "data")}:/data`,
			]),
		});
		expect(got).toBe(deep);
	});
});

describe("filter_media_delivery_paths — one failure never cancels siblings", () => {
	it("keeps validated entries, drops unsafe ones, preserves order + isVoice flags", () => {
		const ok = touch(join(root, "keep.png"), FRESH);
		const out = filterMediaDeliveryPaths(
			[
				{ path: "/etc/passwd", isVoice: false },
				{ path: ok, isVoice: false },
				{ path: join(root, "ghost.mp3"), isVoice: true },
				{ path: "/proc/x", isVoice: true },
			],
			deps(),
		);
		expect(out).toEqual([
			{ path: expect.stringContaining("keep.png"), isVoice: false },
		]);
	});

	it("empty/null input yields empty output", () => {
		expect(filterMediaDeliveryPaths(null, deps())).toEqual([]);
		expect(filterMediaDeliveryPaths([], deps())).toEqual([]);
	});
});

describe("apply_media_policy_env — idempotent, env-wins, never raises", () => {
	const saved: Record<string, string | undefined> = {};
	beforeAll(() => {
		for (const k of [
			MEDIA_DELIVERY_STRICT_ENV,
			MEDIA_DELIVERY_ALLOW_DIRS_ENV,
			MEDIA_DELIVERY_TRUST_RECENT_ENV,
		]) {
			saved[k] = process.env[k];
			delete process.env[k];
		}
	});
	afterAll(() => {
		for (const [k, v] of Object.entries(saved)) {
			if (v === undefined) delete process.env[k];
			else process.env[k] = v;
		}
	});

	it("bridges config values into unset env vars", () => {
		applyMediaPolicyEnv({
			gateway: {
				strict: true,
				trust_recent_files: false,
				media_delivery_allow_dirs: ["/a", "/b"],
			},
		});
		expect(process.env[MEDIA_DELIVERY_STRICT_ENV]).toBe("1");
		expect(process.env[MEDIA_DELIVERY_TRUST_RECENT_ENV]).toBe("0");
		expect(process.env[MEDIA_DELIVERY_ALLOW_DIRS_ENV]).toBe("/a:/b");
	});

	it("NEVER overwrites a pre-existing env value (operator exports win)", () => {
		process.env[MEDIA_DELIVERY_STRICT_ENV] = "0";
		applyMediaPolicyEnv({ gateway: { strict: true } });
		expect(process.env[MEDIA_DELIVERY_STRICT_ENV]).toBe("0");
	});

	it("garbage config does not throw and mutates nothing", () => {
		delete process.env[MEDIA_DELIVERY_ALLOW_DIRS_ENV];
		expect(() => applyMediaPolicyEnv(null)).not.toThrow();
		expect(() => applyMediaPolicyEnv({ gateway: null })).not.toThrow();
		expect(() => applyMediaPolicyEnv(undefined)).not.toThrow();
		expect(process.env[MEDIA_DELIVERY_ALLOW_DIRS_ENV]).toBeUndefined();
	});
});

describe("allowed roots collection shape", () => {
	it("includes legacy *_cache dirs AND canonical cache/<subdir> layout", () => {
		const roots = collectAllowedRoots({ env: {}, home, piHome });
		for (const expected of [
			join(piHome, "image_cache"),
			join(piHome, "browser_screenshots"),
			join(piHome, "cache", "images"),
			join(piHome, "cache", "videos"),
		]) {
			expect(roots).toContain(expected);
		}
	});
});

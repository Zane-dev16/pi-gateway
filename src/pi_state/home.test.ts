// Behavior contracts: PI_HOME override resolution (01-architecture.md §6).
// Resolution order: context-local override → env (read ONCE) → platform default.

import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	displayPiHome,
	processScopedPiHome,
	resetPiHomeCacheForTests,
	resetPiHomeOverride,
	resolvePiHome,
	runWithPiHomeOverride,
	setPiHomeOverride,
} from "../pi_home.js";

let dir: string;
let savedEnv: string | undefined;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-home-"));
	savedEnv = process.env["PI_HOME"];
	delete process.env["PI_HOME"];
	resetPiHomeCacheForTests();
});

afterEach(() => {
	if (savedEnv === undefined) delete process.env["PI_HOME"];
	else process.env["PI_HOME"] = savedEnv;
	resetPiHomeCacheForTests();
	rmSync(dir, { recursive: true, force: true });
});

describe("01 §6 PI_HOME resolution", () => {
	it("defaults under the user home (~/.pi); win32 shape mirrors Hermes", () => {
		expect(resolvePiHome()).toBe(join(homedir(), ".pi"));
	});

	it("env override wins over default and is read ONCE at first resolution", () => {
		process.env["PI_HOME"] = join(dir, "profile-a");
		const first = resolvePiHome();
		expect(first).toBe(join(dir, "profile-a"));
		// Changing the env AFTER first resolution must NOT change the answer —
		// entrypoint-installed overrides are what make this cache safe.
		process.env["PI_HOME"] = join(dir, "profile-b");
		expect(resolvePiHome()).toBe(first);
		expect(processScopedPiHome()).toBe(first);
	});

	it("context-local override beats env; nests via runWith; reset restores", () => {
		process.env["PI_HOME"] = join(dir, "from-env");
		setPiHomeOverride(join(dir, "ctx"));
		expect(resolvePiHome()).toBe(join(dir, "ctx"));

		const inner = runWithPiHomeOverride(join(dir, "nested"), resolvePiHome);
		expect(inner).toBe(join(dir, "nested"));
		expect(resolvePiHome()).toBe(join(dir, "ctx")); // outer context untouched

		// Async context propagation: promises spawned inside inherit.
		const asyncInner = runWithPiHomeOverride(join(dir, "async"), async () => {
			await new Promise((r) => setTimeout(r, 5));
			return resolvePiHome();
		});
		return asyncInner.then((v) => {
			expect(v).toBe(join(dir, "async"));
			resetPiHomeOverride();
			expect(resolvePiHome()).toBe(join(dir, "from-env"));
		});
	});

	it("display form collapses the user-home prefix to ~", () => {
		process.env["PI_HOME"] = join(homedir(), "somewhere-else");
		resetPiHomeCacheForTests();
		expect(displayPiHome()).toBe("~/somewhere-else");
		process.env["PI_HOME"] = join(dir, "x");
		resetPiHomeCacheForTests();
		expect(displayPiHome()).toBe(join(dir, "x")); // outside home → absolute
	});

	it("unset env with an active_profile marker warns LOUDLY once on stderr", () => {
		const defaultHome = join(homedir(), ".pi");
		const markerPath = join(defaultHome, "active_profile");
		let hadMarker = false;
		let prevContent: string | null = null;
		try {
			hadMarker = existsSync(markerPath);
			if (hadMarker) prevContent = readFileSync(markerPath, "utf8");
			mkdirSync(defaultHome, { recursive: true });
			writeFileSync(markerPath, "work\n");
			const errWrite = process.stderr.write.bind(process.stderr);
			const captured: string[] = [];
			process.stderr.write = ((chunk: unknown) => {
				captured.push(String(chunk));
				return true;
			}) as typeof process.stderr.write;
			try {
				resolvePiHome(); // first call → warning
				resolvePiHome(); // second call → suppressed (one-shot)
			} finally {
				process.stderr.write = errWrite as typeof process.stderr.write;
			}
			const warnings = captured.filter((c) => c.includes("[PI_HOME fallback]"));
			if (!hadMarker) expect(warnings.length).toBeGreaterThanOrEqual(1);
			expect(
				warnings.filter((c) => c.includes('"work"')).length,
			).toBeLessThanOrEqual(1);
		} finally {
			if (hadMarker) {
				writeFileSync(markerPath, prevContent ?? "");
			} else {
				rmSync(markerPath, { force: true });
			}
		}
	});
});

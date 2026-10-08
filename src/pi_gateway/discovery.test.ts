// discovery.test.ts — behavior contracts for machine-wide session
// discovery (DEC-079). Every test builds a real <agentDir>/sessions tree in
// mkdtemp and drives the real scan: headers indexed, corrupt files skipped,
// missing roots empty, recency ordering real.

import {
	mkdtempSync,
	mkdirSync,
	rmSync,
	utimesSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { homedir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	bindingFromRoutingEntry,
	buildDiscoveryIndex,
	listDiscoveryPaths,
	mostRecentSessionAtPath,
	readDiscoveryHeader,
	resolveAgentDir,
} from "./discovery.js";

let agentDir: string;

function sessionLine(id: string, cwd: string): string {
	return (
		JSON.stringify({
			type: "session",
			version: 3,
			id,
			timestamp: new Date().toISOString(),
			cwd,
		}) + "\n"
	);
}

function writeSession(
	safeDir: string,
	file: string,
	lines: string[],
	mtimeMs?: number,
): string {
	mkdirSync(join(agentDir, "sessions", safeDir), { recursive: true });
	const filePath = join(agentDir, "sessions", safeDir, file);
	writeFileSync(filePath, lines.join(""));
	if (mtimeMs !== undefined) {
		const atime = new Date(mtimeMs);
		utimesSync(filePath, atime, atime);
	}
	return filePath;
}

beforeEach(() => {
	agentDir = mkdtempSync(join(tmpdir(), "pi-gw-discovery-"));
	mkdirSync(join(agentDir, "sessions"), { recursive: true });
});

afterEach(() => {
	rmSync(agentDir, { recursive: true, force: true });
});

describe("buildDiscoveryIndex", () => {
	it("indexes headers by resolved path with ids and files", () => {
		writeSession("--proj-a--", "s1.jsonl", [sessionLine("id-a1", "/proj/a")]);
		writeSession("--proj-b--", "s2.jsonl", [sessionLine("id-b1", "/proj/b")]);
		const index = buildDiscoveryIndex(agentDir);
		expect(listDiscoveryPaths(index)).toEqual(["/proj/a", "/proj/b"]);
		expect(index.get("/proj/a")?.map((s) => s.id)).toEqual(["id-a1"]);
		expect(index.get("/proj/b")?.[0]?.file).toContain("s2.jsonl");
	});

	it("skips corrupt files without losing the good ones", () => {
		writeSession("--proj-a--", "good.jsonl", [
			sessionLine("id-good", "/proj/a"),
		]);
		writeSession("--proj-a--", "garbage.jsonl", ["this is not json\n{{{nope"]);
		writeSession("--proj-a--", "empty.jsonl", []);
		writeSession("--proj-a--", "not-a-session.jsonl", [
			JSON.stringify({ type: "message", id: "m1", parentId: null }) + "\n",
		]);
		writeSession("--proj-a--", "no-id.jsonl", [
			JSON.stringify({ type: "session", cwd: "/proj/a" }) + "\n",
		]);
		writeSession("--proj-a--", "notes.txt", [sessionLine("id-txt", "/proj/a")]);
		const index = buildDiscoveryIndex(agentDir);
		expect(index.get("/proj/a")?.map((s) => s.id)).toEqual(["id-good"]);
	});

	it("orders members newest-first and skips headers without a cwd", () => {
		const now = Date.now();
		writeSession(
			"--proj-a--",
			"old.jsonl",
			[sessionLine("id-old", "/proj/a")],
			now - 60_000,
		);
		writeSession(
			"--proj-a--",
			"new.jsonl",
			[sessionLine("id-new", "/proj/a")],
			now,
		);
		writeSession("--proj-a--", "nocwd.jsonl", [
			JSON.stringify({ type: "session", id: "id-nocwd" }) + "\n",
		]);
		const index = buildDiscoveryIndex(agentDir);
		expect(index.get("/proj/a")?.map((s) => s.id)).toEqual([
			"id-new",
			"id-old",
		]);
		expect(mostRecentSessionAtPath(index, "/proj/a")?.id).toBe("id-new");
	});

	it("returns an empty index when the sessions root is missing", () => {
		rmSync(join(agentDir, "sessions"), { recursive: true, force: true });
		expect(listDiscoveryPaths(buildDiscoveryIndex(agentDir))).toEqual([]);
	});

	it("mostRecentSessionAtPath misses on unknown paths", () => {
		writeSession("--proj-a--", "s1.jsonl", [sessionLine("id-a1", "/proj/a")]);
		const index = buildDiscoveryIndex(agentDir);
		expect(mostRecentSessionAtPath(index, "/proj/nowhere")).toBeNull();
	});
});

describe("readDiscoveryHeader", () => {
	it("skips blank and malformed leading lines to the header", () => {
		const filePath = writeSession("--proj-a--", "messy.jsonl", [
			"\n",
			"not json\n",
			sessionLine("id-messy", "/proj/a"),
		]);
		expect(readDiscoveryHeader(filePath)).toEqual({
			id: "id-messy",
			cwd: "/proj/a",
		});
	});

	it("rejects a file whose first entry is not a session", () => {
		const filePath = writeSession("--proj-a--", "msg.jsonl", [
			JSON.stringify({ type: "message", id: "m1", parentId: null }) + "\n",
			sessionLine("id-late", "/proj/a"),
		]);
		expect(readDiscoveryHeader(filePath)).toBeNull();
	});

	it("returns null for missing files instead of throwing", () => {
		expect(readDiscoveryHeader(join(agentDir, "nope.jsonl"))).toBeNull();
	});
});

describe("bindingFromRoutingEntry", () => {
	it("projects the routing entry onto its chat-to-host use", () => {
		// Full routing-entry vocabulary in a variable (a literal would trip
		// the excess-property check; real call sites pass binder values).
		const entry = {
			session_key: "agent:main:telegram:dm:42",
			session_id: "host-session-1",
			created_at: 0,
			updated_at: 0,
			origin: null,
			display_name: null,
			platform: "telegram",
			chat_type: "dm",
		};
		expect(bindingFromRoutingEntry(entry)).toEqual({
			chatKey: "agent:main:telegram:dm:42",
			hostSessionId: "host-session-1",
		});
	});
});

describe("resolveAgentDir", () => {
	const ENV = "PI_CODING_AGENT_DIR";
	let saved: string | undefined;

	beforeEach(() => {
		saved = process.env[ENV];
	});

	afterEach(() => {
		if (saved === undefined) delete process.env[ENV];
		else process.env[ENV] = saved;
	});

	it("honors the override env dir", () => {
		process.env[ENV] = "/tmp/pi-verify-agentdir";
		expect(resolveAgentDir()).toBe("/tmp/pi-verify-agentdir");
	});

	it("expands a leading tilde against the os home", () => {
		process.env[ENV] = "~/pi-agent";
		expect(resolveAgentDir()).toBe(join(homedir(), "pi-agent"));
	});

	it("falls back to the os home agent dir when unset", () => {
		delete process.env[ENV];
		expect(resolveAgentDir()).toBe(join(homedir(), ".pi", "agent"));
	});
});

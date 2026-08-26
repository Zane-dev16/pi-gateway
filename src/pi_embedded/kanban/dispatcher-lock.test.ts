// Behavior contracts for the machine-global kanban dispatcher singleton
// (DEC-057; secops-11). Hermes anchors:
//   gateway/kanban_watchers.py:_acquire_singleton_lock — non-blocking
//     acquisition; "contended" ⇒ caller must NOT dispatch; "unavailable" ⇒
//     config-only control. Ownership rides the OS file-handle lifetime, so a
//     crashed holder auto-releases exactly like fcntl locks.
// Two KanbanDispatcherLock instances on the same path are the in-process
// image of two gateway processes: separate SQLite connections contending on
// one BEGIN IMMEDIATE sidecar.

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	DISPATCHER_LOCK_FILENAME,
	KanbanDispatcherLock,
	sharedKanbanDispatcherLock,
} from "./dispatcher-lock.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-kanban-lock-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

function lockAt(home = dir): KanbanDispatcherLock {
	return new KanbanDispatcherLock(
		join(home, "kanban", DISPATCHER_LOCK_FILENAME),
	);
}

describe("KanbanDispatcherLock — BEGIN IMMEDIATE sidecar", () => {
	it("uncontended acquire ⇒ held + owns(); the sidecar file exists under <home>/kanban", () => {
		const lock = lockAt();
		expect(lock.path).toBe(join(dir, "kanban", DISPATCHER_LOCK_FILENAME));
		expect(lock.acquire()).toBe("held");
		expect(lock.owns()).toBe(true);
		lock.release();
		expect(lock.owns()).toBe(false);
	});

	it("a second gateway contending on the same machine-global path is REFUSED, never blocking", () => {
		const first = lockAt();
		expect(first.acquire()).toBe("held");

		const second = lockAt(); // sibling process image: independent connection
		expect(second.acquire()).toBe("contended");
		expect(second.owns()).toBe(false);

		first.release();

		// Release frees the role for the next acquirer (shutdown parity).
		expect(second.acquire()).toBe("held");
		second.release();
	});

	it("release is idempotent and safe pre-acquire", () => {
		const lock = lockAt();
		expect(() => lock.release()).not.toThrow(); // nothing held yet
		expect(lock.acquire()).toBe("held");
		lock.release();
		expect(() => lock.release()).not.toThrow(); // double release
	});

	it("unresolvable sidecar location ⇒ 'unavailable' (config-only control)", () => {
		// A regular FILE where the kanban dir must be created makes both
		// mkdir and the SQLite open fail — the OSError parity branch.
		const blocker = join(dir, "kanban");
		writeFileSync(blocker, "not a directory", "utf8");
		const lock = lockAt();
		expect(lock.acquire()).toBe("unavailable");
		expect(lock.owns()).toBe(false);
	});
});

describe("sharedKanbanDispatcherLock — one handle per process per path", () => {
	it("every consumer resolving the same path gets THE SAME object", () => {
		const path = join(dir, "kanban", DISPATCHER_LOCK_FILENAME);
		expect(sharedKanbanDispatcherLock(path)).toBe(
			sharedKanbanDispatcherLock(path),
		);
		// Distinct paths stay distinct handles.
		expect(sharedKanbanDispatcherLock(path)).not.toBe(
			sharedKanbanDispatcherLock(`${path}.alt`),
		);
	});

	it("dispatcher holds via the shared handle while the notifier merely consults owns()", () => {
		const path = join(dir, "kanban", DISPATCHER_LOCK_FILENAME);
		const dispatcherView = sharedKanbanDispatcherLock(path);
		const notifierView = sharedKanbanDispatcherLock(path);

		expect(notifierView.owns()).toBe(false); // before dispatch starts
		expect(dispatcherView.acquire()).toBe("held");
		expect(notifierView.owns()).toBe(true); // legacy rows become visible
		dispatcherView.release();
		expect(notifierView.owns()).toBe(false);
	});
});

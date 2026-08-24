// §6 Kanban contracts — the BOARD is a HARD boundary.
//
// Behavior under test:
//   - slug normalization parity (_BOARD_SLUG_RE / _normalize_board_slug)
//   - resolution chain pinned → env → default with fellThrough marking
//   - worker env pinning carries ONE board (defense in depth)
//   - an invalid PINNED slug is a refusal condition for autonomous
//     dispatchers (proposed DEC; see board.ts module header)

import { describe, expect, it } from "vitest";

import {
	BOARD_SLUG_RE,
	DEFAULT_BOARD,
	InvalidBoardSlugError,
	KANBAN_BOARD_ENV,
	normalizeBoardSlug,
	resolveBoardSlug,
	workerBoardEnv,
} from "./board.js";
import { DEFAULT_FAILURE_LIMIT } from "./types.js";

describe("board slug normalization (parity _normalize_board_slug)", () => {
	it("lowercases, trims, and accepts the reference charset", () => {
		expect(normalizeBoardSlug("  Main-Line_2 ")).toBe("main-line_2");
		expect(normalizeBoardSlug(null)).toBeNull();
		expect(normalizeBoardSlug("   ")).toBeNull();
		expect(normalizeBoardSlug("")).toBeNull();
	});

	it("rejects malformed slugs loudly (ValueError parity)", () => {
		for (const bad of [
			"-lead",
			"_lead",
			"has space",
			"sl/ash",
			"x".repeat(65),
		]) {
			expect(() => normalizeBoardSlug(bad)).toThrow(InvalidBoardSlugError);
		}
	});

	it("uppercase input normalizes (lowercase-before-validate parity)", () => {
		expect(normalizeBoardSlug("UPPER")).toBe("upper");
	});

	it("slug regex matches the reference pattern exactly", () => {
		expect(BOARD_SLUG_RE.source).toBe("^[a-z0-9][a-z0-9-_]{0,63}$");
	});
});

describe("resolveBoardSlug — reader resolution chain", () => {
	it("pinned beats env beats default", () => {
		expect(
			resolveBoardSlug({
				pinned: "alpha",
				env: { [KANBAN_BOARD_ENV]: "beta" },
			}),
		).toEqual({
			board: "alpha",
			source: "pinned",
			fellThrough: false,
		});
		expect(resolveBoardSlug({ env: { [KANBAN_BOARD_ENV]: "beta" } })).toEqual({
			board: "beta",
			source: "env",
			fellThrough: false,
		});
		expect(resolveBoardSlug({})).toEqual({
			board: DEFAULT_BOARD,
			source: "default",
			fellThrough: false,
		});
	});

	it("empty/unset slugs fall through WITHOUT the refusal marker", () => {
		const r = resolveBoardSlug({ pinned: "", env: {} });
		expect(r.board).toBe(DEFAULT_BOARD);
		expect(r.fellThrough).toBe(false);
	});

	it("malformed PINNED slug marks fellThrough with the reason (autonomous refusal input)", () => {
		const r = resolveBoardSlug({ pinned: "NOT A SLUG" });
		expect(r.fellThrough).toBe(true);
		expect(r.reason).toContain("not a valid board slug");
	});

	it("malformed ENV slug marks fellThrough identically", () => {
		const r = resolveBoardSlug({ env: { [KANBAN_BOARD_ENV]: "../escape" } });
		expect(r.fellThrough).toBe(true);
		expect(r.board).toBe(DEFAULT_BOARD);
	});
});

describe("hard-boundary pinning (worker env)", () => {
	it("pins exactly one board into every kanban-scoped var", () => {
		const env = workerBoardEnv("sre-board");
		expect(env[KANBAN_BOARD_ENV]).toBe("sre-board");
		expect(Object.keys(env)).toEqual([KANBAN_BOARD_ENV]);
	});
});

describe("dispatcher default constants (07 §6)", () => {
	it("failure limit default is 2 (spin-loop breaker)", () => {
		expect(DEFAULT_FAILURE_LIMIT).toBe(2);
	});
});

// board.ts — the kanban BOARD is a HARD boundary; the TENANT is a soft
// namespace within a board (07 §6: "Board isolation is a HARD boundary:
// workers are pinned via HERMES_KANBAN_BOARD env so they cannot see other
// boards; tenant is a soft namespace WITHIN a board (workspace-path +
// memory-key isolation)").
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/kanban_db.py:_BOARD_SLUG_RE              → BOARD_SLUG_RE
//   hermes_cli/kanban_db.py:_normalize_board_slug       → normalizeBoardSlug
//   hermes_cli/kanban_db.py:get_current_board           → resolveBoardSlug
//     (resolution chain: pinned slug → env → default; malformed slugs fall
//     through with a warning — the reader must never crash)
//   hermes_cli/kanban_db.py:_default_spawn env pinning  → workerBoardEnv
//     (HERMES_KANBAN_BOARD / HERMES_KANBAN_DB / workspaces root all pinned to
//     the SAME board the dispatcher claimed from — defense in depth so a
//     worker can never resolve another board)
//
// Divergence (proposed DEC text): when an EXPLICITLY PINNED board slug fails
// validation or its store cannot be opened, the embedded dispatcher service
// DEGRADES LOUDLY for the whole boot (restart-scoped fix, DEC-013) instead of
// falling through the resolution chain. Hermes' get_current_board falls a
// malformed ENV slug through to `default`, which would let a mis-pinned
// gateway silently dispatch onto the wrong board — exactly the cross-board
// leak the hard boundary exists to prevent. Falling through is correct for an
// interactive CLI reader; it is NOT correct for an autonomous dispatcher.

/** Parity of kanban_db.py DEFAULT_BOARD. */
export const DEFAULT_BOARD = "default";

/** Parity of kanban_db.py:_BOARD_SLUG_RE (1-64 chars, lower alnum/-/_). */
export const BOARD_SLUG_RE = /^[a-z0-9][a-z0-9-_]{0,63}$/;

/** The board-pin environment variable (hard-boundary carrier). */
export const KANBAN_BOARD_ENV = "HERMES_KANBAN_BOARD";

/**
 * Raised for a syntactically invalid board slug (parity of the ValueError in
 * kanban_db.py:_normalize_board_slug).
 */
export class InvalidBoardSlugError extends Error {
	readonly slug: string;
	constructor(slug: string) {
		super(
			`invalid board slug ${JSON.stringify(slug)}: must be 1-64 chars, ` +
				`lowercase alphanumerics / hyphens / underscores, not starting with '-' or '_'`,
		);
		this.name = "InvalidBoardSlugError";
		this.slug = slug;
	}
}

/**
 * Parity of kanban_db.py:_normalize_board_slug: lowercase + strip; null for
 * empty; throws InvalidBoardSlugError on invalid.
 */
export function normalizeBoardSlug(
	slug: string | null | undefined,
): string | null {
	if (slug === null || slug === undefined) return null;
	const s = String(slug).trim().toLowerCase();
	if (!s) return null;
	if (!BOARD_SLUG_RE.test(s)) throw new InvalidBoardSlugError(s);
	return s;
}

export interface BoardResolutionInput {
	/** Explicitly pinned board (config/dispatcher argument). Highest precedence. */
	pinned?: string | null | undefined;
	/** Environment map consulted for KANBAN_BOARD_ENV (tests inject a fake). */
	env?: Record<string, string | undefined> | undefined;
}

export interface BoardResolution {
	board: string;
	/** Where the winning slug came from ("pinned" | "env" | "default"). */
	source: "pinned" | "env" | "default";
	/**
	 * True when a pinned/env slug was requested but unusable and resolution
	 * fell through — callers running an AUTONOMOUS dispatcher must refuse to
	 * start on this (hard-boundary divergence; see module header).
	 */
	fellThrough: boolean;
	reason?: string;
}

/**
 * Reader-style resolution chain (parity get_current_board): pinned → env →
 * default. A malformed pinned or env slug falls through WITH `fellThrough`
 * set so autonomous callers can refuse loudly; an empty/unset slug simply
 * resolves to the default board.
 */
export function resolveBoardSlug(
	input: BoardResolutionInput = {},
): BoardResolution {
	const pinnedRaw = (input.pinned ?? "").toString().trim();
	if (pinnedRaw) {
		try {
			const normed = normalizeBoardSlug(pinnedRaw);
			if (normed)
				return { board: normed, source: "pinned", fellThrough: false };
		} catch (err) {
			if (!(err instanceof InvalidBoardSlugError)) throw err;
			return {
				board: DEFAULT_BOARD,
				source: "default",
				fellThrough: true,
				reason: `pinned board ${JSON.stringify(pinnedRaw)} is not a valid board slug`,
			};
		}
	}
	const envRaw = (input.env?.[KANBAN_BOARD_ENV] ?? "").trim();
	if (envRaw) {
		try {
			const normed = normalizeBoardSlug(envRaw);
			if (normed) return { board: normed, source: "env", fellThrough: false };
		} catch (err) {
			if (!(err instanceof InvalidBoardSlugError)) throw err;
			return {
				board: DEFAULT_BOARD,
				source: "default",
				fellThrough: true,
				reason: `${KANBAN_BOARD_ENV}=${JSON.stringify(envRaw)} is not a valid board slug`,
			};
		}
	}
	return { board: DEFAULT_BOARD, source: "default", fellThrough: false };
}

/**
 * The worker env pin (parity _default_spawn): every kanban-scoped variable
 * resolves to the SAME board the dispatcher claimed the card from, so a
 * worker cannot accidentally see another board even if it re-derives paths.
 */
export function workerBoardEnv(board: string): Record<string, string> {
	return {
		[KANBAN_BOARD_ENV]: board,
	};
}

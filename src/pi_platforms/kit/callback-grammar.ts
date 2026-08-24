// pi_platforms/kit/callback-grammar — the ONE namespaced wire grammar for
// button callbacks across adapters (04-platform-adapters.md §9.1; DEC-016).
//
// One handler routes every button sender by prefix dispatch
// (plugins/platforms/telegram/adapter.py:_handle_callback_query — sole
// CallbackQueryHandler registration). Builder ↔ resolver round-trip is a
// REQUIRED contract per prefix family.
//
// Families (Hermes ground truth):
//   ea:<choice>:<approval_id>     exec approval   — choice ∈ once|session|always|deny
//   sc:<choice>:<confirm_id>      slash confirm   — choice ∈ once|always|cancel
//   cl:<clarify_id>:<idx|other>   clarify         — state KEPT until terminal;
//                                                   `other` flips to free-text capture
//   cp:<i>                        choice picker   (/reasoning, /fast)
//   mp:/mpg:/mpv:/mm:/mc:/mb:/mx:noop / mg:       model-picker nav
//   appr:<id>:approve|deny        WhatsApp-Cloud reply-button REDUCED vocabulary
//                                 into the SAME approval resolver (§9 cross-family)
//
// Cross-family clauses:
//   - ids sized to the STRICTEST cap in scope: Telegram's 64-BYTE callback_data
//     forces short ids (monotonic ints, never uuids). Builders REJECT oversize
//     data at build time.
//   - prefixes are a disjoint namespace: collision is impossible by table.

/** Telegram callback_data strictest cap in scope — every builder enforces it. */
export const CALLBACK_DATA_MAX_BYTES = 64;

export class CallbackDataOverflowError extends Error {
	readonly family: string;
	readonly bytes: number;
	constructor(family: string, data: string) {
		super(
			`callback_data exceeds ${CALLBACK_DATA_MAX_BYTES} bytes (${Buffer.byteLength(data, "utf8")}B): ${data}`,
		);
		this.name = "CallbackDataOverflowError";
		this.family = family;
		this.bytes = Buffer.byteLength(data, "utf8");
	}
}

export type ExecApprovalChoice = "once" | "session" | "always" | "deny";
export const EXEC_APPROVAL_CHOICES: readonly ExecApprovalChoice[] = [
	"once",
	"session",
	"always",
	"deny",
];

export type SlashConfirmChoice = "once" | "always" | "cancel";
export const SLASH_CONFIRM_CHOICES: readonly SlashConfirmChoice[] = [
	"once",
	"always",
	"cancel",
];

const INT_RE = /^\d+$/;

function assertFits(data: string): void {
	if (Buffer.byteLength(data, "utf8") > CALLBACK_DATA_MAX_BYTES) {
		throw new CallbackDataOverflowError(data.split(":", 1)[0] ?? "?", data);
	}
}

/** Monotonic-int id shape enforced everywhere (uuids cannot fit 64 bytes). */
function assertShortId(id: string | number, family: string): string {
	const s = String(id);
	if (!INT_RE.test(s)) {
		throw new Error(
			`${family} ids must be monotonic integers (64-byte cap), got: ${s}`,
		);
	}
	return s;
}

// ── builders ─────────────────────────────────────────────────────────────────

export function buildExecApprovalCallback(
	choice: ExecApprovalChoice,
	approvalId: string | number,
): string {
	const data = `ea:${choice}:${assertShortId(approvalId, "ea")}`;
	assertFits(data);
	return data;
}

export function buildSlashConfirmCallback(
	choice: SlashConfirmChoice,
	confirmId: string | number,
): string {
	const data = `sc:${choice}:${assertShortId(confirmId, "sc")}`;
	assertFits(data);
	return data;
}

/** `idx` is the 0-based choice index; the literal "other" flips to text capture. */
export function buildClarifyCallback(
	clarifyId: string | number,
	idxOrOther: number | "other",
): string {
	const tail =
		idxOrOther === "other" ? "other" : String(Math.trunc(idxOrOther));
	if (tail !== "other" && !INT_RE.test(tail)) {
		throw new Error(`cl idx must be an integer or "other", got: ${tail}`);
	}
	const data = `cl:${assertShortId(clarifyId, "cl")}:${tail}`;
	assertFits(data);
	return data;
}

export function buildChoicePickerCallback(index: number): string {
	const data = `cp:${Math.trunc(index)}`;
	assertFits(data);
	return data;
}

export function buildModelProviderCallback(slug: string): string {
	const data = `mp:${slug}`;
	assertFits(data);
	return data;
}

export function buildModelProviderGroupCallback(groupId: string): string {
	const data = `mpg:${groupId}`;
	assertFits(data);
	return data;
}

export function buildModelPageNavCallback(page: number): string {
	const data = `mpv:${Math.trunc(page)}`;
	assertFits(data);
	return data;
}

export function buildModelMemberCallback(absIndex: number): string {
	const data = `mm:${Math.trunc(absIndex)}`;
	assertFits(data);
	return data;
}

export function buildModelCommitCallback(idx: number): string {
	const data = `mc:${Math.trunc(idx)}`;
	assertFits(data);
	return data;
}

export const MODEL_BACK_CALLBACK = "mb";
export const MODEL_NOOP_CALLBACK = "mx:noop";
export function buildModelGroupNavCallback(groupId: string): string {
	const data = `mg:${groupId}`;
	assertFits(data);
	return data;
}

/** WhatsApp-Cloud reply-button variant into the SAME approval resolver (§9). */
export type WhatsappApprovalChoice = "approve" | "deny";
export function buildWhatsappApprovalCallback(
	id: string | number,
	choice: WhatsappApprovalChoice,
): string {
	const data = `appr:${assertShortId(id, "appr")}:${choice}`;
	assertFits(data);
	return data;
}

// ── parser ───────────────────────────────────────────────────────────────────

export type ParsedCallback =
	| { family: "ea"; choice: ExecApprovalChoice; approvalId: number }
	| { family: "sc"; choice: SlashConfirmChoice; confirmId: number }
	| { family: "cl"; clarifyId: number; idx: number | "other" }
	| { family: "cp"; index: number }
	| { family: "mp"; slug: string }
	| { family: "mpg"; groupId: string }
	| { family: "mpv"; page: number }
	| { family: "mm"; absIndex: number }
	| { family: "mc"; idx: number }
	| { family: "mb" }
	| { family: "mx" }
	| { family: "mg"; groupId: string }
	| { family: "appr"; id: number; choice: WhatsappApprovalChoice }
	| { family: "unknown" };

/**
 * Prefix-dispatch parse — the exact table `_handle_callback_query` walks.
 * Garbage NEVER matches a family (returns unknown); callers must still answer
 * every tap (router's job).
 */
export function parseCallbackData(data: string): ParsedCallback {
	if (data === "mb") return { family: "mb" };
	if (data === "mx:noop") return { family: "mx" };

	const colon = data.indexOf(":");
	if (colon <= 0) return { family: "unknown" };
	const prefix = data.slice(0, colon);
	const rest = data.slice(colon + 1);

	switch (prefix) {
		case "ea": {
			const parts = rest.split(":");
			if (parts.length !== 2) return { family: "unknown" };
			const [choice, rawId] = parts as [string, string];
			if (
				!(EXEC_APPROVAL_CHOICES as readonly string[]).includes(choice) ||
				!INT_RE.test(rawId)
			)
				return { family: "unknown" };
			return {
				family: "ea",
				choice: choice as ExecApprovalChoice,
				approvalId: Number(rawId),
			};
		}
		case "sc": {
			const parts = rest.split(":");
			if (parts.length !== 2) return { family: "unknown" };
			const [choice, rawId] = parts as [string, string];
			if (
				!(SLASH_CONFIRM_CHOICES as readonly string[]).includes(choice) ||
				!INT_RE.test(rawId)
			)
				return { family: "unknown" };
			return {
				family: "sc",
				choice: choice as SlashConfirmChoice,
				confirmId: Number(rawId),
			};
		}
		case "cl": {
			const parts = rest.split(":");
			if (parts.length !== 2) return { family: "unknown" };
			const [rawId, token] = parts as [string, string];
			if (!INT_RE.test(rawId)) return { family: "unknown" };
			if (token === "other")
				return { family: "cl", clarifyId: Number(rawId), idx: "other" };
			if (!INT_RE.test(token)) return { family: "unknown" };
			return { family: "cl", clarifyId: Number(rawId), idx: Number(token) };
		}
		case "cp":
			return INT_RE.test(rest)
				? { family: "cp", index: Number(rest) }
				: { family: "unknown" };
		case "mp":
			return rest.length > 0
				? { family: "mp", slug: rest }
				: { family: "unknown" };
		case "mpg":
		case "mg":
			return rest.length > 0
				? { family: prefix === "mpg" ? "mpg" : "mg", groupId: rest }
				: { family: "unknown" };
		case "mpv":
			return INT_RE.test(rest)
				? { family: "mpv", page: Number(rest) }
				: { family: "unknown" };
		case "mm":
			return INT_RE.test(rest)
				? { family: "mm", absIndex: Number(rest) }
				: { family: "unknown" };
		case "mc":
			return INT_RE.test(rest)
				? { family: "mc", idx: Number(rest) }
				: { family: "unknown" };
		case "appr": {
			const parts = rest.split(":");
			if (parts.length !== 2) return { family: "unknown" };
			const [rawId, choice] = parts as [string, string];
			if (!INT_RE.test(rawId) || (choice !== "approve" && choice !== "deny"))
				return { family: "unknown" };
			return { family: "appr", id: Number(rawId), choice };
		}
		default:
			return { family: "unknown" };
	}
}

/**
 * Namespace-collision impossibility: the family prefix set is disjoint BY
 TABLE — asserted by test over every builder's output. This helper exists for
 that audit, not for runtime dispatch.
 */
export const FAMILY_PREFIXES: readonly string[] = [
	"ea:",
	"sc:",
	"cl:",
	"cp:",
	"mp:",
	"mpg:",
	"mpv:",
	"mm:",
	"mc:",
	"mb",
	"mx:",
	"mg:",
	"appr:",
];

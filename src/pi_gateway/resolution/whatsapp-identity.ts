// pi_gateway/resolution/whatsapp-identity.ts — WhatsApp identity
// canonicalization, ONE alias-collapsing module (02-session-and-state.md §4.3).
//
// WhatsApp's bridge can surface the same human under two JID shapes inside one
// conversation — phone form `15551234567@s.whatsapp.net` and LID form
// `999999999999999@lid` — for a DM chat_id AND for a group member's
// participant_id. If every consumer collapses aliases independently,
// allowlists, session keys, and pairing grants drift apart: the same person
// becomes two sessions and passes authorization under one shape but not the
// other. ALL identity decisions (authz allowlist expansion, session-key
// construction, pairing grants, outbound addressing) MUST resolve through this
// module — a local re-implementation anywhere reopens the fork/deny bug class
// the module exists to close (02 §4.3 invariant + review checklist).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/whatsapp_identity.py:normalize_whatsapp_identifier    → normalizeWhatsappIdentifier
//   gateway/whatsapp_identity.py:expand_whatsapp_aliases          → expandWhatsappAliases
//   gateway/whatsapp_identity.py:canonical_whatsapp_identifier    → canonicalWhatsappIdentifier
//   gateway/whatsapp_identity.py:to_whatsapp_jid                  → toWhatsappJid
//   hermes_constants.py:get_hermes_dir                            → defaultWhatsappSessionDir
//
// Load-bearing mechanics (02 §4.3):
//   - Mapping files are read PER CALL, never cached at startup: the bridge
//     writes new lid-mapping pairs as it learns aliases; a startup snapshot
//     would freeze pre-learning splits into permanent session forks.
//   - The walk is defensive twice over: identifiers failing
//     ^[A-Za-z0-9@.+,-]+$ are skipped from expansion (path-traversal
//     defense-in-depth on the `lid-mapping-{current}` filename join), and an
//     unreadable/corrupt mapping file is skipped with a debug log — never fatal.
//   - Fresh install (no mapping files) degrades to the normalized input.
//     Empty input → empty string, so callers fall through to their
//     no-identifier branches unchanged.

import { readdirSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

import { resolvePiHome } from "../../pi_home.js";

/**
 * WhatsApp JIDs are numeric (or plus-prefixed numeric) with optional `@`, `.`
 * and `:` separators. Pinned to ASCII so full-width digits / Unicode word
 * chars can't sneak through. Parity of
 * gateway/whatsapp_identity.py:_SAFE_IDENTIFIER_RE.
 */
const SAFE_IDENTIFIER_RE = /^[A-Za-z0-9@.+-]+$/;

/**
 * A target that is "just a phone number" — optional leading `+` then digits
 * and the usual human separators. Anything already carrying an `@` is a
 * fully-qualified JID and must pass through toWhatsappJid untouched.
 * Parity of gateway/whatsapp_identity.py:_BARE_PHONE_RE.
 */
const BARE_PHONE_RE = /^\+?[\d\s().-]+$/;

export interface WhatsappIdentityOptions {
	/**
	 * Override the bridge session directory that holds
	 * `lid-mapping-*.json` files. Default resolves per call from PI_HOME
	 * (parity hermes_constants.get_hermes_dir("platforms/whatsapp/session",
	 * "whatsapp/session")). Tests inject a temp dir here.
	 */
	sessionDir?: string;
}

/**
 * Strip WhatsApp JID/LID syntax down to its stable numeric identifier.
 *
 * Accepts any shape the bridge may emit: `"60123456789@s.whatsapp.net"`,
 * `"60123456789:47@s.whatsapp.net"`, `"60123456789@lid"`, or a bare
 * `"+60123456789"` / `"60123456789"`. Returns just the numeric identifier,
 * suitable for equality comparisons. Empty/whitespace input → "".
 */
export function normalizeWhatsappIdentifier(value: unknown): string {
	return (
		(
			String(value ?? "")
				.trim()
				.replace("+", "") // first occurrence only, parity .replace("+", "", 1)
				.split(":", 1)[0] ?? ""
		) // drop device suffix (parity .split(":", 1)[0])
			.split("@", 1)[0] ?? ""
	); // drop JID/LID domain (parity .split("@", 1)[0])
}

/**
 * OUTBOUND inverse (02 §4.3 to_whatsapp_jid row): Baileys' jidDecode crashes
 * on a bare phone number, so sends build fully-qualified JIDs.
 *
 * - "+50766715226" / "507 667-1522 6" → "50766715226@s.whatsapp.net"
 * - fully-qualified JIDs — groups "@g.us", LIDs "@lid",
 *   "status@broadcast" — pass untouched
 * - legacy "user:device@domain" collapses the non-addressable device suffix
 * - anything unrecognizable returns unchanged so the bridge surfaces a
 *   meaningful error rather than us mangling it; empty input → "".
 */
export function toWhatsappJid(value: unknown): string {
	const raw = String(value ?? "");
	if (!raw) return "";
	let normalized = raw.trim();
	// Drop a device suffix before the domain: "user:device@domain" is a legacy
	// Baileys shape whose ":device" part is not addressable — collapse it.
	if (normalized.includes(":") && normalized.includes("@")) {
		const at = normalized.indexOf("@");
		const prefix = normalized.slice(0, at).split(":", 1)[0] ?? "";
		normalized = `${prefix}@${normalized.slice(at + 1)}`;
	}
	if (normalized.includes("@")) return normalized;
	if (BARE_PHONE_RE.test(normalized)) {
		const digits = normalized.replace(/\D+/g, "");
		if (digits) return `${digits}@s.whatsapp.net`;
	}
	return normalized;
}

/**
 * Resolve the bridge session dir per call (NEVER cached — the bridge writes
 * mapping files as it learns aliases). Parity of get_hermes_dir with the
 * whatsapp pair: legacy `<home>/whatsapp/session` wins only when it exists
 * WITH CONTENT (a bare empty stub must not shadow the consolidated layout);
 * otherwise `<home>/platforms/whatsapp/session`.
 */
export function defaultWhatsappSessionDir(home?: string): string {
	const root = home ?? resolvePiHome();
	const legacy = join(root, "whatsapp", "session");
	try {
		if (existsSync(legacy) && readdirSync(legacy).length > 0) return legacy;
	} catch {
		/* unreadable legacy dir → consolidated layout */
	}
	return join(root, "platforms", "whatsapp", "session");
}

function mappingPath(dir: string, id: string, suffix: "" | "_reverse"): string {
	return join(dir, `lid-mapping-${id}${suffix}.json`);
}

function readMappedIdentifier(path: string): string | null {
	try {
		const text = readFileSync(path, "utf8");
		return normalizeWhatsappIdentifier(JSON.parse(text));
	} catch (err) {
		// OSError / JSONDecodeError parity: skip corrupt/unreadable mapping
		// files with a debug log — never fatal (02 §4.3; §12 error table row
		// "WhatsApp alias map unreadable").
		const msg = err instanceof Error ? err.message : String(err);
		console.debug(`whatsapp_identity: failed to read ${path}: ${msg}`);
		return null;
	}
}

/**
 * Transitive BFS over `$PI_HOME/(platforms/)?whatsapp/session/lid-mapping-*.json`
 * (+ `_reverse` twins). The result ALWAYS contains the normalized input
 * itself, so callers can membership-check without a separate fallback branch;
 * an unreadable or hostile-shaped link contributes nothing.
 *
 * Returns an empty set when the identifier normalizes to empty.
 */
export function expandWhatsappAliases(
	identifier: string,
	opts: WhatsappIdentityOptions = {},
): Set<string> {
	const normalized = normalizeWhatsappIdentifier(identifier);
	if (!normalized) return new Set();

	const sessionDir = opts.sessionDir ?? defaultWhatsappSessionDir();
	const resolved = new Set<string>([normalized]); // spec: ALWAYS contains input
	// FIX (Phase 1 resolution contracts): the walk previously pre-seeded
	// `resolved` with the input and reused it as the DEQUEUE guard, so the seed
	// skipped its own mapping reads — no alias was ever expanded, silently
	// reopening the §4.3 fork/deny bug class this module exists to close.
	// Port parity of gateway/whatsapp_identity.py:expand_whatsapp_aliases:
	// visited-at-processing-time (`expanded`) drives termination while
	// `resolved` remains the returned alias set (seeded with the input).
	const expanded = new Set<string>();
	const queue: string[] = [normalized];

	while (queue.length > 0) {
		const current = queue.shift() as string;
		if (!current || expanded.has(current)) continue;
		// Defense-in-depth: reject identifiers that could sneak path
		// separators / traversal segments into the lid-mapping-{current}
		// filename join. Unsafe shapes are excluded from expansion entirely.
		if (!SAFE_IDENTIFIER_RE.test(current)) continue;

		expanded.add(current);
		for (const suffix of ["", "_reverse"] as const) {
			const mapped = readMappedIdentifier(
				mappingPath(sessionDir, current, suffix),
			);
			if (!mapped || resolved.has(mapped)) continue;
			// A hostile-shaped LINK contributes nothing: never followed, never
			// admitted to the alias set (parity: the Python walk drops such ids
			// at dequeue without ever entering `resolved`).
			if (!SAFE_IDENTIFIER_RE.test(mapped)) continue;
			resolved.add(mapped);
			queue.push(mapped);
		}
	}
	return resolved;
}

/**
 * Stable canonical identity across phone-JID/LID variants:
 * min(aliases, key=(len, lexicographic)) — stable numeric-preferred pick.
 * build_session_key uses this for BOTH WhatsApp DM chat_ids AND group
 * participant_ids (gateway/session.py::build_session_key).
 *
 * Fresh install (no mapping files yet) degrades to the normalized input
 * (min over the single-element set). Empty input → "".
 */
export function canonicalWhatsappIdentifier(
	identifier: string,
	opts: WhatsappIdentityOptions = {},
): string {
	const aliases = expandWhatsappAliases(identifier, opts);
	if (aliases.size === 0) return "";
	let best: string | null = null;
	for (const candidate of aliases) {
		if (
			best === null ||
			candidate.length < best.length ||
			(candidate.length === best.length && candidate < best)
		) {
			best = candidate;
		}
	}
	return best as string;
}

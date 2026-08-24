// manifest-yaml.ts — flat-manifest YAML SUBSET parser for hook/plugin
// manifests (07-integrations.md §7; DEC-014).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/hooks.py:discover_and_load      → yaml.safe_load(HOOK.yaml) consumer
//   hermes_cli/plugins.py:_parse_manifest   → fast_safe_load(plugin.yaml) consumer
//
// Runtime reality (DEC-023): this package ships ZERO runtime dependencies
// beyond better-sqlite3, so PyYAML cannot be mirrored wholesale. The manifest
// documents Hermes actually loads are FLAT: scalar keys plus string lists
// (block `- item` or inline `[a, b]`). This parser accepts exactly that
// subset and FAILS LOUD (throws) on anything richer (nested maps, multiline
// scalars) — callers skip the member with a printed reason rather than
// half-interpreting a manifest. Unsupported syntax failing closed is the
// safe direction; full-YAML support can ride a future DEC if a manifest ever
// needs it.
//
// Accepted grammar (superset sketch):
//   document   := (blank | comment | assignment)*
//   assignment := KEY ":" ( inline-list | scalar | <empty> )   # key at column 0
//   <empty>    := followed by zero or more "- item" block lines
//   scalar     := quoted string | number | bool | null/~ | bare word
// Duplicate keys: LAST wins (yaml.safe_load behavior).

/** Thrown on syntactically valid-looking but UNSUPPORTED manifest shapes. */
export class ManifestSyntaxError extends Error {
	constructor(detail: string) {
		super(`unsupported manifest syntax: ${detail}`);
		this.name = "ManifestSyntaxError";
	}
}

/**
 * Parse a flat YAML manifest. Returns `null` for an empty document (comments
 * and blank lines only) — parity of yaml.safe_load returning None, which the
 * discovery paths treat as INVALID and skip loudly.
 */
export function parseFlatYaml(text: string): Record<string, unknown> | null {
	const lines = text.split(/\r\n|\r|\n/);
	const result: Record<string, unknown> = {};
	let sawContent = false;
	let pendingListKey: string | null = null;

	for (let i = 0; i < lines.length; i++) {
		const line = stripComment(lines[i] ?? "");
		if (line.trim().length === 0) continue;
		const trimmed = line.trim();
		sawContent = true;

		// Block list item under the most recent empty-valued key.
		if (trimmed.startsWith("- ")) {
			if (pendingListKey === null) {
				throw new ManifestSyntaxError(
					`list item outside a key block at line ${i + 1}`,
				);
			}
			const current = result[pendingListKey];
			if (!Array.isArray(current)) {
				// First item under `key:` upgrades the null placeholder to a list.
				if (current !== null) {
					throw new ManifestSyntaxError(
						`mixed list/scalar value for key "${pendingListKey}" at line ${i + 1}`,
					);
				}
				result[pendingListKey] = [];
			}
			(result[pendingListKey] as unknown[]).push(parseScalar(trimmed.slice(2)));
			continue;
		}
		if (trimmed === "-") {
			throw new ManifestSyntaxError(`bare list dash at line ${i + 1}`);
		}

		// Assignment line: KEY : rest  (key at column 0 — nested maps unsupported).
		if (/^\s/.test(line)) {
			throw new ManifestSyntaxError(
				`nested mappings unsupported (indented key at line ${i + 1})`,
			);
		}
		const match = /^([^\s:#][^:]*?):(?:\s+(.*))?$/.exec(line);
		if (match === null) {
			throw new ManifestSyntaxError(
				`cannot interpret line ${i + 1}: ${trimmed.slice(0, 60)}`,
			);
		}
		const key = (match[1] ?? "").trim();
		if (key.length === 0) {
			throw new ManifestSyntaxError(`empty key at line ${i + 1}`);
		}
		const rest = (match[2] ?? "").trim();
		pendingListKey = key;
		result[key] = rest.length > 0 ? parseInlineValue(rest) : null; // list items upgrade null → array
		if (rest.length > 0) pendingListKey = null;
	}

	return sawContent ? result : null;
}

/**
 * Remove a trailing comment: `#` at line start or preceded by whitespace,
 * honoring single/double quotes (a `#` inside quotes is data).
 */
function stripComment(line: string): string {
	let inSingle = false;
	let inDouble = false;
	for (let i = 0; i < line.length; i++) {
		const ch = line[i];
		if (ch === "'" && !inDouble) inSingle = !inSingle;
		else if (ch === '"' && !inSingle) inDouble = !inDouble;
		else if (ch === "#" && !inSingle && !inDouble) {
			if (i === 0 || /\s/.test(line[i - 1] ?? "")) return line.slice(0, i);
		}
	}
	return line;
}

/** Parse the right-hand side of `key:` — inline list or scalar. */
function parseInlineValue(rest: string): unknown {
	if (rest.startsWith("[")) return parseInlineList(rest);
	return parseScalar(rest);
}

/** Inline flow list: `[agent:start, session:end]` (quotes respected). */
function parseInlineList(rest: string): unknown[] {
	if (!rest.endsWith("]")) {
		throw new ManifestSyntaxError(`unterminated inline list: ${rest}`);
	}
	const body = rest.slice(1, -1);
	const items: unknown[] = [];
	for (const piece of splitTopLevel(body)) {
		const token = piece.trim();
		if (token.length === 0) continue;
		items.push(parseScalar(token));
	}
	return items;
}

/** Split on commas that sit outside quotes. */
function splitTopLevel(body: string): string[] {
	const parts: string[] = [];
	let current = "";
	let inSingle = false;
	let inDouble = false;
	for (const ch of body) {
		if (ch === "'" && !inDouble) inSingle = !inSingle;
		else if (ch === '"' && !inSingle) inDouble = !inDouble;
		if (ch === "," && !inSingle && !inDouble) {
			parts.push(current);
			current = "";
		} else {
			current += ch;
		}
	}
	parts.push(current);
	return parts;
}

/** Scalar coercion: quotes, numbers, booleans, null/~; otherwise bare word. */
function parseScalar(raw: string): unknown {
	const t = raw.trim();
	if (t.length === 0) return null;
	if (
		(t.startsWith('"') && t.endsWith('"') && t.length >= 2) ||
		(t.startsWith("'") && t.endsWith("'") && t.length >= 2)
	) {
		return t.slice(1, -1);
	}
	const lower = t.toLowerCase();
	if (lower === "null" || lower === "~") return null;
	if (lower === "true") return true;
	if (lower === "false") return false;
	if (/^[+-]?\d+$/.test(t)) return Number.parseInt(t, 10);
	if (/^[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?$/.test(t))
		return Number.parseFloat(t);
	return t;
}

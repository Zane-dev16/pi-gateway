// redact.ts — THE shared redaction point for approval rendering (07 §8.3
// step 2, binding). Tirith's findings are pre-redacted; the RAW command
// string is not — one shared redaction point feeds BOTH the card path and
// the text-fallback path (#48456), applied BEFORE any rendering.
//
// Hermes anchors (READ-ONLY reference):
//   gateway/run.py:_redact_approval_command → redactApprovalCommand
//   gateway/run.py:_GATEWAY_SECRET_PATTERNS → GATEWAY_SECRET_PATTERNS
//
// `force=True` parity: the primary scrubber runs even when security-level
// log redaction is off — approval prompts are chat-facing and must honor
// the "chat responses are scrubbed before delivery" promise unconditionally.

/**
 * Primary credential scrubber (Tirith-grade redaction parity — the caller
 * injects the platform kit's redactor; absent injection degrades to the
 * built-in pattern pass below rather than leaking raw text).
 */
export type PrimarySecretScrub = (text: string) => string;

/**
 * The narrow belt-and-suspenders second pass so nothing the gateway
 * historically caught can regress. Capture group 1 (scheme prefixes like
 * `Bearer `) is preserved; the secret value becomes [REDACTED].
 */
export const GATEWAY_SECRET_PATTERNS: readonly RegExp[] = [
	/\bsk-[A-Za-z0-9][A-Za-z0-9_-]{12,}\b/g,
	/\bgh[pousr]_[A-Za-z0-9_]{20,}\b/g,
	/\bxapp-\d+-[A-Za-z0-9-]{20,}\b/g,
	/\bxox[baprs]-[A-Za-z0-9-]{20,}\b/g,
	/\bhf_[A-Za-z0-9]{20,}\b/g,
	/\bglpat-[A-Za-z0-9_-]{20,}\b/g,
	/\b(Bearer\s+)[A-Za-z0-9._-]{20,}\b/gi,
];

function applyGatewayPatterns(text: string): string {
	let redacted = text;
	for (const pattern of GATEWAY_SECRET_PATTERNS) {
		redacted = redacted.replace(pattern, (_match, group1?: string) =>
			typeof group1 === "string" ? `${group1}[REDACTED]` : "[REDACTED]",
		);
	}
	return redacted;
}

/**
 * Redact credentials from a command BEFORE it goes into an approval prompt.
 * The primary scrubber runs first (force semantics), then the gateway
 * pattern pass; a throwing primary degrades to the pattern pass only — a
 * redactor error must never leak the raw text to chat.
 */
export function redactApprovalCommand(
	command: string,
	primary?: PrimarySecretScrub | undefined,
): string {
	const raw = String(command ?? "");
	let redacted = raw;
	if (primary) {
		try {
			redacted = primary(raw);
		} catch {
			// Fail-soft: fall back to the local pattern pass below.
		}
	}
	return applyGatewayPatterns(redacted);
}

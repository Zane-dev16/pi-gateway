// pi_gateway/security/trust/body-cap — body size caps enforced PRE-PARSE
// (06 §8.3 "requests outside every listed range get 403 BEFORE body parse";
// §8.2/§8.4 body caps; DEC-017 "body-size caps and seen-set bounds").
//
// Order-of-checks contract (webhook.py:_handle_webhook @624 / api_server.py
// MAX_REQUEST_BYTES): declared Content-Length trips the cap WITHOUT reading
// or parsing; a LYING Content-Length trips the cap on the ACTUAL byte count
// after read, still before any parse attempt; only within-cap bodies reach
// the parse seam.

/** msgraph_webhook.py DEFAULT_MAX_BODY_BYTES (@43) = client_max_size (1 MiB). */
export const MSGRAPH_BODY_CAP_BYTES = 1_048_576;
/** webhook.py `_max_body_bytes` default (1 MiB). */
export const WEBHOOK_BODY_CAP_BYTES = 1_048_576;
/** api_server.py MAX_REQUEST_BYTES (@239). */
export const API_SERVER_BODY_CAP_BYTES = 10_000_000;

export type BodyCapFailure = {
	ok: false;
	status: 413;
	phase: "declared-length" | "actual-bytes";
	reason: string;
};

export type BodyCapSuccess<T> = {
	ok: true;
	body: Buffer;
	parsed: T;
};

export interface BodyCapDeps<T> {
	capBytes: number;
	/** Declared length from headers; null when absent/unparseable. */
	declaredContentLength: number | null;
	readBody: () => Promise<Buffer>;
	/**
	 * Parse seam — injected so "oversized body never parsed" is OBSERVABLE:
	 * a parse invocation after a cap rejection fails the contract.
	 */
	parse: (body: Buffer) => T;
}

/**
 * Enforce the body cap at BOTH gates and hand the parsed payload back only
 * for within-cap bodies. Parse errors propagate to the caller (the gate owns
 * SIZING, not syntax).
 */
export async function readBodyWithinCap<T>(
	deps: BodyCapDeps<T>,
): Promise<BodyCapSuccess<T> | BodyCapFailure> {
	const { capBytes } = deps;

	// Gate 1: declared length (auth-before-body; nothing read, nothing parsed).
	if (
		deps.declaredContentLength !== null &&
		deps.declaredContentLength > capBytes
	) {
		return {
			ok: false,
			status: 413,
			phase: "declared-length",
			reason: `declared body ${deps.declaredContentLength} exceeds cap ${capBytes}`,
		};
	}

	const body = await deps.readBody();

	// Gate 2: actual bytes read (defense in depth vs lying Content-Length).
	if (body.length > capBytes) {
		return {
			ok: false,
			status: 413,
			phase: "actual-bytes",
			reason: `body ${body.length} exceeds cap ${capBytes}`,
		};
	}

	return { ok: true, body, parsed: deps.parse(body) };
}

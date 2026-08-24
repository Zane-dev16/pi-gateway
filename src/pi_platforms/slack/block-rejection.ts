// pi_platforms/slack/block-rejection — the block-payload rejection
// classifier whose failure mode is a RETRY WITHOUT BLOCKS (never a dropped
// response).
//
// Hermes anchor (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:_is_block_payload_rejection — "Rich
//   Block Kit output is a progressive enhancement over the plain `text`
//   fallback. If Slack rejects the structured payload as invalid or too
//   large, retrying the same content without blocks is safe and prevents a
//   formatting bug from dropping the whole response." Recoverable codes:
//   invalid_blocks / msg_too_long / too_many_blocks.

export const BLOCK_PAYLOAD_REJECTION_CODES: readonly string[] = [
	"invalid_blocks",
	"msg_too_long",
	"too_many_blocks",
];

/** True when the error is recoverable by removing `blocks` from the payload. */
export function isBlockPayloadRejectionError(errorText: string): boolean {
	return BLOCK_PAYLOAD_REJECTION_CODES.some((code) => errorText.includes(code));
}

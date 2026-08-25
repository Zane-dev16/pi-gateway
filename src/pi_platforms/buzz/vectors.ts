// pi_platforms/buzz/vectors — CONTRACT VECTORS for the NIP-42 crypto port.
//
// PROVENANCE (honesty statement): every vector below was COMPUTED by running
// the READ-ONLY Hermes reference module
// plugins/platforms/buzz/nostr_auth.py locally (python3, fixed created_at and
// fixed 32-byte auxiliary randomness 000102…1f) — running the reference to
// compute test vectors is the documented, honest workflow; NO reference code
// was vendored. Each schnorr signature was ALSO accepted by an independent
// BIP-340 spec verifier, and the reference's schnorr_sign reproduces official
// BIP-340 test vectors 0–3 (bip-0340/test-vectors.csv) byte-exactly before
// any vector was recorded. The TS port must reproduce these bytes exactly;
// a mismatch is a port bug, not a stale fixture.

/** Fixed fixture private key (hex). */
export const FIXED_KEY_HEX =
	"3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f3a7f";

/** The same key bech32-encoded as an nsec (reference charset + polymod). */
export const FIXED_NSEC =
	"nsec18fln5le60ua87wnl8fln5le60ua87wnl8fln5le60ua87wnl8flsuu0qvl";

/** nostr_auth.py:public_key_hex(FIXED_KEY_HEX) — x-only pubkey. */
export const FIXED_PUBKEY_HEX =
	"eac59128dea729c95909cd2bc9aa6232e8e3c66908bda2c283136a43f6165c6b";

/** Known EC anchor: x(G · 1) = generator x (BIP-340 sanity). */
export const GENERATOR_X_HEX =
	"79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

/** Deterministic aux randomness used for BOTH auth-event vectors: 000102…1f. */
export const FIXED_AUX_HEX =
	"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

export interface AuthEventVector {
	challenge: string;
	relayUrl: string;
	authTagJson: "";
	id: string;
	pubkey: string;
	createdAt: number;
	tags: string[][];
	sig: string;
}

export interface TaggedAuthEventVector {
	authTagJson: string;
	id: string;
	sig: string;
}

const CHALLENGE = "chal-token-4f8a2b";
const RELAY = "wss://relay.example";
const CREATED_AT = 1700000000;

/** build_auth_event WITHOUT BUZZ_AUTH_TAG (fixed clock + aux ⇒ deterministic). */
export const AUTH_EVENT_PLAIN: AuthEventVector = {
	challenge: CHALLENGE,
	relayUrl: RELAY,
	authTagJson: "",
	id: "ea0c654d2b2d3046f1b4a8a44608ba25eddf8c6603f327db2ab1859d6aa47c38",
	pubkey: FIXED_PUBKEY_HEX,
	createdAt: CREATED_AT,
	tags: [
		["relay", RELAY],
		["challenge", CHALLENGE],
	],
	sig: "a5f4cb1e4cf8fa77ad5e13e0112736ecade7e7d5bd54ea1f9b1580b5925485467cb61a69c14286a79df25e92c6304976323df73d84bf5da3ac50d95c0317cbba",
};

/** Valid four-string owner-attestation tag JSON (NIP-OA class). */
export const AUTH_TAG_JSON =
	'["auth", "a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8", "comm-uuid-1234", "ownersig"]';

/**
 * build_auth_event WITH the valid auth tag — tag appended VERBATIM after
 * relay/challenge; id and sig change accordingly.
 */
export const AUTH_EVENT_TAGGED: TaggedAuthEventVector = {
	authTagJson: AUTH_TAG_JSON,
	id: "7830bc55423b262fc8fb315fd9e488dedcbe487b11471be4eafaa7364e11f63e",
	sig: "b8d8743837f0247a84a06e2aef6c124a7730cf2252433434b7f36a485bf5bbc5a8bc9d453f681fd375cea3e65801b9699434339045b6eada4fb70d1ebc44aa23",
};

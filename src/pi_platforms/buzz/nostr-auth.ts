// pi_platforms/buzz/nostr-auth — dependency-free Nostr signing for Buzz
// NIP-42 WebSocket authentication. FULL SEMANTIC PORT of the READ-ONLY Hermes
// reference plugins/platforms/buzz/nostr_auth.py (port semantics with
// file:symbol anchors; no code vendored).
//
// Anchor map (reference → port):
//   FIELD_ORDER / CURVE_ORDER / GENERATOR / BECH32_CHARSET  → same-name consts
//   _bech32_polymod / _bech32_hrp_expand                    → bech32Polymod / bech32HrpExpand
//   _decode_nsec (strictness ladder)                        → decodeNsecBytes
//   decode_private_key (nsec1… or 64-hex, 1≤k<n)            → decodePrivateKeyScalar
//   _point_add / _point_multiply (affine bigint EC)         → pointAdd / pointMultiply
//   _tagged_hash sha256(tag‖tag‖msg)                        → taggedHash
//   public_key_hex                                           → publicKeyHex
//   schnorr_sign (BIP-340 exact, aux override)               → schnorrSign
//   build_auth_event (kind 22242 + optional BUZZ_AUTH_TAG)    → buildAuthEvent
//   adapter.py:hex_to_npub / npub_to_hex / _normalize_user_ref → hexToNpub / npubToHex / normalizeUserRef
//
// Deterministic overrides (auxHex/createdAt) exist for CONTRACT vectors only —
// production callers omit them exactly like the reference defaults.

import { createHash, randomBytes } from "node:crypto";

/** nostr_auth.py:FIELD_ORDER — secp256k1 field prime. */
export const FIELD_ORDER = 2n ** 256n - 2n ** 32n - 977n;
/** nostr_auth.py:CURVE_ORDER — group order n. */
export const CURVE_ORDER =
	0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141n;
/** nostr_auth.py:GENERATOR — the curve base point G. */
export const GENERATOR: readonly [bigint, bigint] = [
	0x79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798n,
	0x483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8n,
];
/** nostr_auth.py:BECH32_CHARSET. */
export const BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

type Point = readonly [bigint, bigint] | null;

function sha256(...chunks: Buffer[]): Buffer {
	const h = createHash("sha256");
	for (const c of chunks) h.update(c);
	return h.digest();
}

/** nostr_auth.py:_bech32_polymod. */
export function bech32Polymod(values: readonly number[]): number {
	const generators = [
		0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3,
	];
	let checksum = 1;
	for (const value of values) {
		const top = checksum >> 25;
		checksum = ((checksum & 0x1ffffff) << 5) ^ value;
		for (let i = 0; i < 5; i++) {
			if ((top >> i) & 1) checksum ^= generators[i] as number;
		}
	}
	return checksum;
}

/** nostr_auth.py:_bech32_hrp_expand. */
export function bech32HrpExpand(hrp: string): number[] {
	return [
		...[...hrp].map((c) => c.charCodeAt(0) >> 5),
		0,
		...[...hrp].map((c) => c.charCodeAt(0) & 31),
	];
}

/**
 * nostr_auth.py:_decode_nsec — STRICT decode: case-uniformity, `nsec` hrp,
 * charset validation, polymod===1 checksum, 5→8 bit regrouping that REJECTS
 * non-zero padding, and an exact 32-byte payload.
 */
export function decodeNsecBytes(value: string): Buffer {
	if (value.toLowerCase() !== value && value.toUpperCase() !== value) {
		throw new Error("nsec cannot mix upper- and lowercase");
	}
	const normalized = value.toLowerCase();
	const separator = normalized.lastIndexOf("1");
	if (separator < 1 || separator + 7 > normalized.length) {
		throw new Error("invalid nsec encoding");
	}
	const hrp = normalized.slice(0, separator);
	if (hrp !== "nsec") {
		throw new Error("private key must use the nsec prefix");
	}
	const data: number[] = [];
	for (const char of normalized.slice(separator + 1)) {
		const index = BECH32_CHARSET.indexOf(char);
		if (index < 0) throw new Error("invalid character in nsec");
		data.push(index);
	}
	if (bech32Polymod([...bech32HrpExpand(hrp), ...data]) !== 1) {
		throw new Error("invalid nsec checksum");
	}

	let accumulator = 0n;
	let bits = 0;
	const decoded: number[] = [];
	for (const value5 of data.slice(0, -6)) {
		accumulator = (accumulator << 5n) | BigInt(value5);
		bits += 5;
		while (bits >= 8) {
			bits -= 8;
			decoded.push(Number((accumulator >> BigInt(bits)) & 0xffn));
		}
	}
	if (bits > 0 && (accumulator & ((1n << BigInt(bits)) - 1n)) !== 0n) {
		throw new Error("non-zero nsec padding");
	}
	if (decoded.length !== 32) {
		throw new Error("nsec must encode exactly 32 bytes");
	}
	return Buffer.from(decoded);
}

/**
 * nostr_auth.py:decode_private_key — accepts an nsec bech32 string or a hex
 * private key; enforces 32 bytes and 1 ≤ k < CURVE_ORDER.
 */
export function decodePrivateKeyScalar(value: string): bigint {
	const raw = value.trim();
	let keyBytes: Buffer;
	if (raw.toLowerCase().startsWith("nsec1")) {
		keyBytes = decodeNsecBytes(raw);
	} else if (/^[0-9a-fA-F]*$/.test(raw) && raw.length % 2 === 0) {
		keyBytes = Buffer.from(raw, "hex");
		if (keyBytes.length !== 32) {
			throw new Error("private key must be 32 bytes");
		}
	} else {
		throw new Error("private key must be 64 hex characters or nsec");
	}
	const key = BigInt(`0x${keyBytes.toString("hex")}`);
	if (!(1n <= key && key < CURVE_ORDER)) {
		throw new Error("private key is outside the secp256k1 range");
	}
	return key;
}

function modPow(base: bigint, exponent: bigint, modulus: bigint): bigint {
	let result = 1n;
	let b = ((base % modulus) + modulus) % modulus;
	let e = exponent;
	while (e > 0n) {
		if (e & 1n) result = (result * b) % modulus;
		b = (b * b) % modulus;
		e >>= 1n;
	}
	return result;
}

function fieldInv(a: bigint): bigint {
	return modPow(a, FIELD_ORDER - 2n, FIELD_ORDER);
}

/** nostr_auth.py:_point_add — affine addition incl. doubling, None = ∞. */
export function pointAdd(left: Point, right: Point): Point {
	if (left === null) return right;
	if (right === null) return left;
	const [x1, y1] = left;
	const [x2, y2] = right;
	let slope: bigint;
	if (x1 === x2) {
		if ((y1 + y2) % FIELD_ORDER === 0n) return null;
		slope =
			(((3n * x1 * x1) % FIELD_ORDER) * fieldInv((2n * y1) % FIELD_ORDER)) %
			FIELD_ORDER;
	} else {
		slope =
			(((y2 - y1) % FIELD_ORDER) *
				fieldInv((((x2 - x1) % FIELD_ORDER) + FIELD_ORDER) % FIELD_ORDER)) %
			FIELD_ORDER;
	}
	slope = ((slope % FIELD_ORDER) + FIELD_ORDER) % FIELD_ORDER;
	const x3 = (slope * slope - x1 - x2) % FIELD_ORDER;
	const y3 = (slope * (x1 - x3) - y1) % FIELD_ORDER;
	return [
		((x3 % FIELD_ORDER) + FIELD_ORDER) % FIELD_ORDER,
		((y3 % FIELD_ORDER) + FIELD_ORDER) % FIELD_ORDER,
	];
}

/** nostr_auth.py:_point_multiply — double-and-add over GENERATOR default. */
export function pointMultiply(
	scalar: bigint,
	point: Point = GENERATOR as Point,
): Point {
	let result: Point = null;
	let addend = point;
	let s = scalar;
	while (s > 0n) {
		if (s & 1n) result = pointAdd(result, addend);
		addend = pointAdd(addend, addend);
		s >>= 1n;
	}
	return result;
}

/** nostr_auth.py:_tagged_hash — sha256(tag‖tag‖payload). */
export function taggedHash(tag: string, payload: Buffer): Buffer {
	const tagHash = sha256(Buffer.from(tag, "utf8"));
	return sha256(tagHash, tagHash, payload);
}

function intTo32Bytes(v: bigint): Buffer {
	return Buffer.from(v.toString(16).padStart(64, "0"), "hex");
}

/** nostr_auth.py:public_key_hex — x-only BIP-340 pubkey for a key string. */
export function publicKeyHex(privateKey: string): string {
	const secret = decodePrivateKeyScalar(privateKey);
	const point = pointMultiply(secret);
	if (point === null) throw new Error("invalid private key"); // pragma-covered unreachable
	return point[0].toString(16).padStart(64, "0");
}

export interface SchnorrOptions {
	/** Deterministic 64-char-hex auxiliary randomness (TEST vectors only). */
	auxHex?: string | undefined;
}

/**
 * nostr_auth.py:schnorr_sign — EXACT BIP-340: masked-secret nonce derivation,
 * even-y adjustments of secret and nonce, tagged challenge, rx‖s signature.
 * Default auxiliary randomness is cryptographically random 32 bytes.
 */
export function schnorrSign(
	message: Buffer,
	privateKey: string,
	opts: SchnorrOptions = {},
): Buffer {
	if (message.length !== 32) {
		throw new Error("BIP-340 signs a 32-byte message");
	}
	const secret = decodePrivateKeyScalar(privateKey);
	const publicPoint = pointMultiply(secret);
	if (publicPoint === null) throw new Error("invalid private key");
	const publicX = intTo32Bytes(publicPoint[0]);
	const adjustedSecret =
		publicPoint[1] % 2n === 0n ? secret : CURVE_ORDER - secret;

	let aux: Buffer;
	if (opts.auxHex !== undefined) {
		aux = Buffer.from(opts.auxHex, "hex");
	} else {
		aux = randomBytes(32);
	}
	if (aux.length !== 32) {
		throw new Error("auxiliary randomness must be 32 bytes");
	}
	const masked = Buffer.from(
		intTo32Bytes(adjustedSecret).map(
			(b, i) => b ^ ((taggedHash("BIP0340/aux", aux) as Buffer)[i] as number),
		),
	);
	const nonce =
		BigInt(
			`0x${taggedHash("BIP0340/nonce", Buffer.concat([masked, publicX, message])).toString("hex")}`,
		) % CURVE_ORDER;
	if (nonce === 0n) throw new Error("BIP-340 produced a zero nonce");
	const noncePoint = pointMultiply(nonce);
	if (noncePoint === null)
		throw new Error("BIP-340 produced an invalid nonce point");
	const adjustedNonce = noncePoint[1] % 2n === 0n ? nonce : CURVE_ORDER - nonce;
	const nonceX = intTo32Bytes(noncePoint[0]);
	const challenge =
		BigInt(
			`0x${taggedHash("BIP0340/challenge", Buffer.concat([nonceX, publicX, message])).toString("hex")}`,
		) % CURVE_ORDER;
	const signatureScalar =
		(adjustedNonce + challenge * adjustedSecret) % CURVE_ORDER;
	return Buffer.concat([nonceX, intTo32Bytes(signatureScalar)]);
}

export interface AuthEvent {
	id: string;
	pubkey: string;
	created_at: number;
	kind: 22242;
	tags: string[][];
	content: "";
	sig: string;
}

export interface BuildAuthEventOptions {
	privateKey: string;
	challenge: string;
	relayUrl: string;
	/** Optional BUZZ_AUTH_TAG JSON — EXACT four-string ["auth",…] tag. */
	authTagJson?: string | undefined;
	/** Deterministic clock override (tests). Default int(now seconds). */
	createdAt?: number | undefined;
	/** Deterministic aux override (test vectors). */
	auxHex?: string | undefined;
}

/**
 * nostr_auth.py:build_auth_event — kind-22242 NIP-42 auth event with tags
 [["relay",url],["challenge",challenge]] plus the optional owner-attestation
 tag appended VERBATIM when BUZZ_AUTH_TAG parses to a four-string auth tag.
 Serialization is compact JSON [0,pubkey,created_at,22242,tags,""]; id =
 sha256(serialized); sig = BIP-340 over the id.
 */
export function buildAuthEvent(opts: BuildAuthEventOptions): AuthEvent {
	const tags: string[][] = [
		["relay", opts.relayUrl],
		["challenge", opts.challenge],
	];
	if ((opts.authTagJson ?? "").trim().length > 0) {
		let authTag: unknown;
		try {
			authTag = JSON.parse(opts.authTagJson as string);
		} catch {
			throw new Error("BUZZ_AUTH_TAG is not valid JSON");
		}
		if (
			!Array.isArray(authTag) ||
			authTag.length !== 4 ||
			authTag[0] !== "auth" ||
			!authTag.every((part) => typeof part === "string")
		) {
			throw new Error("BUZZ_AUTH_TAG must be a four-string auth tag");
		}
		tags.push(authTag as string[]);
	}

	const pubkey = publicKeyHex(opts.privateKey);
	const timestamp =
		opts.createdAt === undefined
			? Math.floor(Date.now() / 1000)
			: Math.trunc(opts.createdAt);
	const serialized = Buffer.from(
		JSON.stringify([0, pubkey, timestamp, 22242, tags, ""]),
		"utf8",
	);
	const eventId = sha256(serialized);
	return {
		id: eventId.toString("hex"),
		pubkey,
		created_at: timestamp,
		kind: 22242,
		tags,
		content: "",
		sig: schnorrSign(eventId, opts.privateKey, {
			auxHex: opts.auxHex,
		}).toString("hex"),
	};
}

// ── adapter.py bech32 helpers (npub codecs + user-ref normalization) ────────

/** adapter.py:_convertbits — regroup bit streams; pad=false rejects residue. */
function convertBits(
	data: readonly number[],
	fromBits: number,
	toBits: number,
	pad: boolean,
): number[] | null {
	let acc = 0;
	let bits = 0;
	const ret: number[] = [];
	const maxv = (1 << toBits) - 1;
	for (const value of data) {
		if (value < 0 || value >> fromBits > 0) return null;
		acc = (acc << fromBits) | value;
		bits += fromBits;
		while (bits >= toBits) {
			bits -= toBits;
			ret.push((acc >> bits) & maxv);
		}
	}
	if (pad) {
		if (bits > 0) ret.push((acc << (toBits - bits)) & maxv);
	} else if (bits >= fromBits || ((acc << (toBits - bits)) & maxv) !== 0) {
		return null;
	}
	return ret;
}

/** adapter.py:hex_to_npub — 64-char hex pubkey → npub1… bech32 (or null). */
export function hexToNpub(pubkeyHex: string): string | null {
	if (!/^[0-9a-fA-F]*$/.test(pubkeyHex) || pubkeyHex.length % 2 !== 0)
		return null;
	const raw = Buffer.from(pubkeyHex, "hex");
	if (raw.length !== 32) return null;
	const data = convertBits([...raw], 8, 5, true);
	if (data === null) return null;
	const values = [...bech32HrpExpand("npub"), ...data];
	const polymod = bech32Polymod([...values, 0, 0, 0, 0, 0, 0]) ^ 1;
	const checksum = [0, 1, 2, 3, 4, 5].map(
		(i) => (polymod >> (5 * (5 - i))) & 31,
	);
	return (
		"npub1" + [...data, ...checksum].map((d) => BECH32_CHARSET[d]).join("")
	);
}

/** adapter.py:npub_to_hex — npub1… bech32 → 64-char hex pubkey (or null). */
export function npubToHex(npub: string): string | null {
	const lowered = npub.trim().toLowerCase();
	if (!lowered.startsWith("npub1")) return null;
	const dataPart = lowered.slice("npub1".length);
	const data: number[] = [];
	for (const c of dataPart) {
		const index = BECH32_CHARSET.indexOf(c);
		if (index < 0) return null;
		data.push(index);
	}
	if (bech32Polymod([...bech32HrpExpand("npub"), ...data]) !== 1) return null;
	const decoded = convertBits(data.slice(0, -6), 5, 8, false);
	if (decoded === null || decoded.length !== 32) return null;
	return Buffer.from(decoded).toString("hex");
}

const HEX_PUBKEY_RE = /^[0-9a-f]{64}$/;

/**
 * adapter.py:_normalize_user_ref — user reference (hex pubkey or npub) →
 * lowercase hex, or null when neither form matches.
 */
export function normalizeUserRef(ref: string): string | null {
	const trimmed = (ref ?? "").trim().toLowerCase();
	if (trimmed.length === 0) return null;
	if (trimmed.startsWith("npub1")) return npubToHex(trimmed);
	if (HEX_PUBKEY_RE.test(trimmed)) return trimmed;
	return null;
}

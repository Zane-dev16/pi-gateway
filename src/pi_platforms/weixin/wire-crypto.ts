// pi_platforms/weixin/wire-crypto — AES-128-ECB helpers for iLink context and
// CDN media payloads, ported from Hermes gateway/platforms/weixin.py.
//
// Hermes anchors:
//   weixin.py:_pkcs7_pad / _aes128_ecb_encrypt / _aes128_ecb_decrypt —
//     PKCS#7 padding with FULL-block pad bytes; decrypt tolerates unpadded
//     trailing data (returns as-is when the tail is not valid padding).
//   weixin.py:_aes_padded_size — ((size + 1 + 15) // 16) * 16.
//   weixin.py:_parse_aes_key — 16 raw bytes, or 32 ASCII hex chars → bytes.

import { createDecipheriv, createCipheriv } from "node:crypto";

export function pkcs7Pad(data: Buffer, blockSize = 16): Buffer {
	const padLen = blockSize - (data.length % blockSize);
	return Buffer.concat([data, Buffer.alloc(padLen, padLen)]);
}

export function aes128EcbEncrypt(plaintext: Buffer, key: Buffer): Buffer {
	if (key.length !== 16) {
		throw new Error(`AES-128 key must be 16 bytes, got ${key.length}`);
	}
	const cipher = createCipheriv("aes-128-ecb", key, null);
	cipher.setAutoPadding(false);
	return Buffer.concat([cipher.update(pkcs7Pad(plaintext)), cipher.final()]);
}

export function aes128EcbDecrypt(ciphertext: Buffer, key: Buffer): Buffer {
	if (key.length !== 16) {
		throw new Error(`AES-128 key must be 16 bytes, got ${key.length}`);
	}
	const decipher = createDecipheriv("aes-128-ecb", key, null);
	decipher.setAutoPadding(false);
	const padded = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
	if (padded.length === 0) return padded;
	const padLen = padded[padded.length - 1]!;
	if (
		padLen >= 1 &&
		padLen <= 16 &&
		padded.subarray(padded.length - padLen).every((b) => b === padLen)
	) {
		return padded.subarray(0, padded.length - padLen);
	}
	return padded;
}

/** ((size + 1 + 15) // 16) * 16 — always ≥ one full pad block. */
export function aesPaddedSize(size: number): number {
	return Math.floor((size + 1 + 15) / 16) * 16;
}

/** Parse base64 aes keys: 16 raw bytes, or 32 ASCII hex chars (weixin.py). */
export function parseAesKey(aesKeyB64: string): Buffer {
	const decoded = Buffer.from(aesKeyB64, "base64");
	if (decoded.length === 16) return decoded;
	if (decoded.length === 32) {
		const text = decoded.toString("ascii");
		if (/^[0-9a-fA-F]+$/.test(text)) return Buffer.from(text, "hex");
	}
	throw new Error(
		`unexpected aes_key format (${decoded.length} decoded bytes)`,
	);
}

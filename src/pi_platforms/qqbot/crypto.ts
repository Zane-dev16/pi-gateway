// pi_platforms/qqbot/crypto — AES-256-GCM scan-to-configure credential
// decryption, ported from Hermes gateway/platforms/qqbot/crypto.py.
//
// Hermes anchors:
//   qqbot/crypto.py:generate_bind_key — 256-bit random AES key, base64;
//     handed to create_bind_task so the server encrypts the bot's
//     client_secret before returning it (secret never travels plaintext).
//   qqbot/crypto.py:decrypt_secret — base64(IV[12] ‖ ciphertext ‖ tag[16]),
//     AESGCM decrypt with NO associated data.

import {
	createCipheriv,
	createDecipheriv,
	randomBytes,
	type CipherGCMTypes,
} from "node:crypto";

/** Generate a 256-bit random AES key returned as base64 (crypto.py:generate_bind_key). */
export function generateBindKey(): string {
	return randomBytes(32).toString("base64");
}

export class SecretDecryptError extends Error {
	constructor(
		message: string,
		readonly phase: "decode" | "authenticate",
	) {
		super(message);
		this.name = "SecretDecryptError";
	}
}

/**
 * Decrypt a base64 AES-256-GCM ciphertext (crypto.py:decrypt_secret).
 * Layout after base64 decode: IV(12) ‖ ciphertext(N) ‖ authTag(16).
 * Tampered inputs fail AUTHENTICATION (GCM tag) — never silent garbage.
 */
export function decryptSecret(
	encryptedBase64: string,
	keyBase64: string,
): string {
	let key: Buffer;
	let raw: Buffer;
	try {
		key = Buffer.from(keyBase64, "base64");
		raw = Buffer.from(encryptedBase64, "base64");
	} catch (err) {
		throw new SecretDecryptError(
			`base64 decode failed: ${String(err)}`,
			"decode",
		);
	}
	if (key.length !== 32) {
		throw new SecretDecryptError(
			`AES-256 key must be 32 bytes, got ${key.length}`,
			"decode",
		);
	}
	if (raw.length < 12 + 16) {
		throw new SecretDecryptError(
			`ciphertext too short (${raw.length} bytes; need IV+tag ≥ 28)`,
			"decode",
		);
	}
	const iv = raw.subarray(0, 12);
	const ciphertextWithTag = raw.subarray(12);
	const decipher = createDecipheriv("aes-256-gcm" as CipherGCMTypes, key, iv);
	const tagLength = 16;
	const tag = ciphertextWithTag.subarray(ciphertextWithTag.length - tagLength);
	const body = ciphertextWithTag.subarray(
		0,
		ciphertextWithTag.length - tagLength,
	);
	decipher.setAuthTag(tag);
	try {
		return Buffer.concat([decipher.update(body), decipher.final()]).toString(
			"utf8",
		);
	} catch {
		throw new SecretDecryptError(
			"GCM authentication failed — ciphertext or key tampered",
			"authenticate",
		);
	}
}

/** Encrypt helper for FIXTURES ONLY — mirrors the bind-server side of the flow. */
export function encryptSecretForFixture(
	plaintext: string,
	keyBase64: string,
): string {
	const key = Buffer.from(keyBase64, "base64");
	const iv = randomBytes(12);
	const cipher = createCipheriv("aes-256-gcm" as CipherGCMTypes, key, iv);
	const body = Buffer.concat([
		cipher.update(plaintext, "utf8"),
		cipher.final(),
	]);
	return Buffer.concat([iv, body, cipher.getAuthTag()]).toString("base64");
}

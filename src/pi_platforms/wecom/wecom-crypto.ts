// pi_platforms/wecom/wecom-crypto — WeCom BizMsgCrypt-compatible AES-CBC
// callback crypto, ported from the READ-ONLY Hermes reference
// (plugins/platforms/wecom/wecom_crypto.py; semantics ported, cited not
// vendored) onto node:crypto.
//
// Wire format (wecom_crypto.py:WXBizMsgCrypt):
//   signature  = sha1(sorted([token, timestamp, nonce, encrypt]).join("")).hex
//                compared against `msg_signature` (_sha1_signature @~50)
//   key        = base64decode(encoding_aes_key + "=") — 43 chars ⇒ 32 bytes
//                ⇒ AES-256-CBC, iv = key[:16] (@__init__)
//   plaintext  = random(16) || htonl(len(xml)) BE || xml || receive_id,
//                padded to a 32-byte PKCS7 block (@_encrypt_bytes/PKCS7Encoder)
//
// DEVIATION NOTE (improvement, DEC-017 posture — recorded per DEC-026, not
// silently): the source compares the SHA1 signature with Python `!=`
// (wecom_crypto.py decrypt: "if expected != msg_signature"). The port routes
// EVERY secret-material comparison through kit secureCompare — constant-time
// by contract. Same accept/reject verdicts; no timing oracle.

import { createDecipheriv, createCipheriv, randomBytes } from "node:crypto";
import { createHash } from "node:crypto";
import { secureCompare } from "../kit/index.js";

export const WECOM_PKCS7_BLOCK_SIZE = 32;
/** wecom_crypto.py:WXBizMsgCrypt.__init__ guard. */
export const WECOM_ENCODING_AES_KEY_LENGTH = 43;

export class WeComCryptoError extends Error {}
export class WeComSignatureError extends WeComCryptoError {}
export class WeComDecryptError extends WeComCryptoError {}

/** _sha1_signature parity: sorted-join then hex sha1. */
export function sha1Signature(
	token: string,
	timestamp: string,
	nonce: string,
	encrypt: string,
): string {
	return createHash("sha1")
		.update([...[token, timestamp, nonce, encrypt]].sort().join(""), "utf8")
		.digest("hex");
}

function deriveKey(encodingAesKey: string): Buffer {
	if (!encodingAesKey)
		throw new WeComCryptoError("encoding_aes_key is required");
	if (encodingAesKey.length !== WECOM_ENCODING_AES_KEY_LENGTH) {
		throw new WeComCryptoError(
			`encoding_aes_key must be ${WECOM_ENCODING_AES_KEY_LENGTH} chars`,
		);
	}
	const key = Buffer.from(`${encodingAesKey}=`, "base64");
	if (key.length !== 32) {
		throw new WeComCryptoError("encoding_aes_key must decode to 32 bytes");
	}
	return key;
}

/** PKCS7Encoder.encode parity: ALWAYS pad 1..32 bytes to a 32-byte block. */
export function pkcs7Encode(text: Buffer): Buffer {
	const amountToPad =
		WECOM_PKCS7_BLOCK_SIZE - (text.length % WECOM_PKCS7_BLOCK_SIZE);
	const pad = Buffer.alloc(amountToPad, amountToPad);
	return Buffer.concat([text, pad]);
}

/** PKCS7Encoder.decode parity: validate THEN strip the 32-byte-block padding. */
export function pkcs7Decode(decrypted: Buffer): Buffer {
	if (decrypted.length === 0) {
		throw new WeComDecryptError("empty decrypted payload");
	}
	const pad = decrypted[decrypted.length - 1] as number;
	if (pad < 1 || pad > WECOM_PKCS7_BLOCK_SIZE) {
		throw new WeComDecryptError("invalid PKCS7 padding");
	}
	const tail = decrypted.subarray(decrypted.length - pad);
	for (const byte of tail) {
		if (byte !== pad) throw new WeComDecryptError("malformed PKCS7 padding");
	}
	return decrypted.subarray(0, decrypted.length - pad);
}

/**
 * Minimal BizMsgCrypt-compatible helper (WXBizMsgCrypt parity). receive_id is
 * the corp_id bound INTO every encrypted payload and verified on decrypt.
 */
export class WxBizMsgCrypt {
	readonly token: string;
	readonly receiveId: string;
	private readonly key: Buffer;
	private readonly iv: Buffer;

	constructor(token: string, encodingAesKey: string, receiveId: string) {
		if (!token) throw new WeComCryptoError("token is required");
		if (!receiveId) throw new WeComCryptoError("receive_id is required");
		this.token = token;
		this.receiveId = receiveId;
		this.key = deriveKey(encodingAesKey);
		this.iv = this.key.subarray(0, 16);
	}

	/** verify_url parity: decrypt the echostr challenge, return its plain text. */
	verifyUrl(
		msgSignature: string,
		timestamp: string,
		nonce: string,
		echostr: string,
	): string {
		return this.decrypt(msgSignature, timestamp, nonce, echostr).toString(
			"utf8",
		);
	}

	decrypt(
		msgSignature: string,
		timestamp: string,
		nonce: string,
		encrypt: string,
	): Buffer {
		// Constant-time compare (see DEVIATION note — source used !=).
		if (
			!secureCompare(
				sha1Signature(this.token, timestamp, nonce, encrypt),
				msgSignature ?? "",
			)
		) {
			throw new WeComSignatureError("signature mismatch");
		}
		let cipherText: Buffer;
		try {
			cipherText = Buffer.from(encrypt, "base64");
		} catch (err) {
			throw new WeComDecryptError(
				`invalid base64 payload: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
		let plain: Buffer;
		let content: Buffer;
		try {
			const decipher = createDecipheriv("aes-256-cbc", this.key, this.iv);
			decipher.setAutoPadding(false); // PKCS7 over 32-byte blocks, manual
			plain = Buffer.concat([decipher.update(cipherText), decipher.final()]);
			content = pkcs7Decode(plain).subarray(16); // skip 16-byte random prefix
			const xmlLength = content.readUInt32BE(0);
			const xmlContent = content.subarray(4, 4 + xmlLength);
			const receiveId = content.subarray(4 + xmlLength).toString("utf8");
			if (receiveId !== this.receiveId) {
				throw new WeComDecryptError("receive_id mismatch");
			}
			return xmlContent;
		} catch (err) {
			if (err instanceof WeComCryptoError) throw err;
			throw new WeComDecryptError(
				`decrypt failed: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
	}

	encrypt(plaintext: string, nonce?: string, timestamp?: string): string {
		const usedNonce = nonce ?? randomBytes(5).toString("hex");
		const usedTimestamp = timestamp ?? String(Math.floor(Date.now() / 1000));
		const encrypt = this.encryptBytes(Buffer.from(plaintext, "utf8"));
		const signature = sha1Signature(
			this.token,
			usedTimestamp,
			usedNonce,
			encrypt,
		);
		return (
			`<xml><Encrypt><![CDATA[${encrypt}]]></Encrypt>` +
			`<MsgSignature>${signature}</MsgSignature>` +
			`<TimeStamp>${usedTimestamp}</TimeStamp>` +
			`<Nonce>${usedNonce}</Nonce></xml>`
		);
	}

	private encryptBytes(raw: Buffer): string {
		const randomPrefix = randomBytes(16);
		const lenPrefix = Buffer.alloc(4);
		lenPrefix.writeUInt32BE(raw.length, 0);
		const payload = Buffer.concat([
			randomPrefix,
			lenPrefix,
			raw,
			Buffer.from(this.receiveId, "utf8"),
		]);
		const cipher = createCipheriv("aes-256-cbc", this.key, this.iv);
		cipher.setAutoPadding(false); // manual 32-byte-block PKCS7
		const encrypted = Buffer.concat([
			cipher.update(pkcs7Encode(payload)),
			cipher.final(),
		]);
		return encrypted.toString("base64");
	}
}

// ── minimal XML tag extraction (defusedxml posture parity) ──────────────────

const NAMED_ENTITIES: Readonly<Record<string, string>> = Object.freeze({
	"&lt;": "<",
	"&gt;": ">",
	"&quot;": '"',
	"&apos;": "'",
	"&amp;": "&",
});

/**
 * Extract one tag's text from an XML document WITHOUT any general XML parser
 * and WITHOUT dynamic regex compilation:
 *   - no DOCTYPE/entity DECLARATIONS are honored at all (billion-laughs /
 *     XXE posture parity of defusedxml in callback_adapter.py), only the five
 *     predefined entities are decoded;
 *   - CDATA-wrapped values are returned VERBATIM;
 *   - combined with the 64 KB pre-parse body cap, unauthenticated POSTs can
 *     force bounded work only.
 * Returns null when the tag is absent.
 */
export function extractXmlTag(xml: string, tag: string): string | null {
	if (!/^[A-Za-z_][A-Za-z0-9_.-]*$/.test(tag)) return null;
	const openLiteral = `<${tag}`;
	let cursor = 0;
	while (cursor <= xml.length) {
		const openIndex = xml.indexOf(openLiteral, cursor);
		if (openIndex < 0) return null;
		const afterOpen = openIndex + openLiteral.length;
		const next = xml[afterOpen];
		// A real open tag for THIS name — reject prefix matches (<Msg vs <MsgId).
		if (
			next !== ">" &&
			next !== " " &&
			next !== "\t" &&
			next !== "\n" &&
			next !== "\r"
		) {
			cursor = afterOpen;
			continue;
		}
		const gtIndex = xml.indexOf(">", afterOpen);
		if (gtIndex < 0) return null;
		const bodyStart = gtIndex + 1;
		const closeLiteral = `</${tag}>`;
		const closeIndex = xml.indexOf(closeLiteral, bodyStart);
		if (closeIndex < 0) return null;
		let value = xml.slice(bodyStart, closeIndex);
		if (
			value.length >= 12 &&
			value.startsWith("<![CDATA[") &&
			value.endsWith("]]>")
		) {
			return value.slice(9, -3);
		}
		value = value.replace(
			/&(lt|gt|quot|apos|amp);/g,
			(m) => NAMED_ENTITIES[m] ?? m,
		);
		return value;
	}
	return null;
}

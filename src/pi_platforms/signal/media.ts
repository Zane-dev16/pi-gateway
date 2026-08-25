// pi_platforms/signal/media — attachment byte-sniffing + historical Signal
// ext→mime table, ported from signal.py helpers (_guess_extension /
// _is_image_ext / _is_audio_ext / _ext_to_mime) with the audio-container
// disambiguations signal.py delegates to tools/audio_container.py inlined as
// data (READ-ONLY reference; semantics ported, no code vendored).
//
// Android voice notes arrive as raw ADTS AAC frames sharing the 0xFF 0xFx
// sync word with MP3; bit-1 layout disambiguates (ADTS packs ID/layer/
// protection_absent into bits 3-0 with layer always 0; MP3 frames have
// ID=1 and layer ∈ {1,2,3}).
//
// AAC→M4A remux parity note: upstream shells out to ffmpeg when present and
// passes raw ADTS through when absent ("graceful no-op"). The adapter owns an
// INJECTED remux seam defaulting to the no-ffmpeg pass-through path — no OS
// child is ever spawned from this port.

export interface SniffedContainer {
	container: "mp3" | "aac-adts" | "wav" | "m4a" | "m4b";
	ext: string;
}

/** tools/audio_container.py subset signal.py actually consumes. */
export function sniffAudioContainer(data: Buffer): SniffedContainer | null {
	if (data.length < 4) return null;
	const b0 = data.subarray(0, 1)[0];
	const b1 = data.subarray(1, 2)[0];
	if (b0 === 0xff && b1 !== undefined && (b1 & 0xe0) === 0xe0) {
		const idBit = (b1 & 0x08) >> 3; // MPEG version bit: 1 = MPEG-1
		const layer = (b1 & 0x06) >> 1; // layer bits: 0 = reserved
		if (layer === 0) return { container: "aac-adts", ext: ".aac" };
		if (idBit === 1) return { container: "mp3", ext: ".mp3" };
		return { container: "aac-adts", ext: ".aac" };
	}
	if (
		data.subarray(0, 4).toString("latin1") === "RIFF" &&
		data.subarray(8, 12).toString("latin1") === "WAVE"
	) {
		return { container: "wav", ext: ".wav" };
	}
	const brand = data.subarray(4, 8).toString("latin1");
	if (brand === "M4A ") return { container: "m4a", ext: ".m4a" };
	if (brand === "M4B ") return { container: "m4b", ext: ".m4a" };
	return null;
}

/**
 * Guess file extension from magic bytes (signal.py:_guess_extension).
 * Falls through to the audio-container sniffer, then zip, then ".bin".
 */
export function guessExtension(data: Buffer): string {
	const first = data.subarray(0, 1)[0];
	const second = data.subarray(1, 2)[0];
	if (
		data.length >= 4 &&
		data.subarray(0, 4).toString("latin1") === "\u0089PNG"
	)
		return ".png";
	if (first === 0xff && second === 0xd8) return ".jpg";
	if (data.length >= 4 && data.subarray(0, 4).toString("latin1") === "GIF8")
		return ".gif";
	if (
		data.length >= 12 &&
		data.subarray(0, 4).toString("latin1") === "RIFF" &&
		data.subarray(8, 12).toString("latin1") === "WEBP"
	)
		return ".webp";
	if (data.length >= 4 && data.subarray(0, 4).toString("latin1") === "%PDF")
		return ".pdf";
	const container = sniffAudioContainer(data);
	if (container !== null) return container.ext;
	if (first === 0x50 && second === 0x4b) return ".zip";
	return ".bin";
}

export function isImageExt(ext: string): boolean {
	return [".jpg", ".jpeg", ".png", ".gif", ".webp"].includes(ext.toLowerCase());
}

export function isAudioExt(ext: string): boolean {
	return [".mp3", ".wav", ".ogg", ".m4a", ".aac"].includes(ext.toLowerCase());
}

/**
 * Historical Signal ext→mime table (media_cache.py:DEFAULT_EXT_TO_MIME,
 * byte-identical per its parity contract); unknown → application/octet-stream.
 */
const EXT_TO_MIME: Readonly<Record<string, string>> = Object.freeze({
	".jpg": "image/jpeg",
	".jpeg": "image/jpeg",
	".png": "image/png",
	".gif": "image/gif",
	".webp": "image/webp",
	".ogg": "audio/ogg",
	".mp3": "audio/mpeg",
	".wav": "audio/wav",
	".m4a": "audio/mp4",
	".aac": "audio/aac",
	".mp4": "video/mp4",
	".pdf": "application/pdf",
	".zip": "application/zip",
});

export function extToMime(ext: string): string {
	return EXT_TO_MIME[ext.toLowerCase()] ?? "application/octet-stream";
}

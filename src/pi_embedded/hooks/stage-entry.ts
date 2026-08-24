// stage-entry.ts — embedded extensions' OPTIONAL-STAGE entry (01 §3.1 stage 8
// `embedded_watchers` slot; DEC-040 registration wiring).
//
// startupEmbeddedExtensions NEVER throws: hook/plugin discovery failing
// degrades the SUBSYSTEM loudly internally and yields an empty-but-functional
// snapshot — so the mapped outcome is always ok:true (the per-subsystem
// degrade logs carry the loudness, exactly per its own contract). Extensions
// are discovered once per boot (DEC-013): there is nothing to stop, so no
// handle. Layering: lifecycle shape mirrored via ../service-entry.js.

import {
	startupEmbeddedExtensions,
	type EmbeddedExtensionsSnapshot,
	type StartupEmbeddedExtensionsOptions,
} from "./startup.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";

/** Registration name of the embedded-extensions service entry. */
export const EMBEDDED_EXTENSIONS_SERVICE_NAME = "embedded-extensions";

export interface ExtensionsServiceEntry extends EmbeddedServiceEntry {
	/** Snapshot from the most recent start() (null before the first run). */
	readonly lastSnapshot: Readonly<EmbeddedExtensionsSnapshot> | null;
}

export function extensionsServiceEntry(
	options: StartupEmbeddedExtensionsOptions,
): ExtensionsServiceEntry {
	let last: EmbeddedExtensionsSnapshot | null = null;
	return {
		name: EMBEDDED_EXTENSIONS_SERVICE_NAME,
		get lastSnapshot() {
			return last;
		},
		async start() {
			last = await startupEmbeddedExtensions(options);
			return { ok: true };
		},
	};
}

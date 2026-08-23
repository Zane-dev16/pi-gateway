// TEST INFRASTRUCTURE — isolation helpers. Every test isolates its DB/home
// dirs to mkdtemp paths under os.tmpdir() (flake discipline).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export function makeTempDir(prefix: string): string {
	return mkdtempSync(join(tmpdir(), prefix));
}

export function removeTempDir(dir: string): void {
	try {
		rmSync(dir, { recursive: true, force: true });
	} catch {
		/* disposable temp */
	}
}

// TEST INFRASTRUCTURE — resolve-hook mapping TypeScript-style ".js" specifiers
// to ".ts" files when running production sources under RAW Node (the spawned
// child processes used by the two-process contention contracts). Vitest and
// tsc resolve these natively; only bare Node needs this bridge.
//
// Loaded via: node --import <this file> child-driver.ts
import { registerHooks } from "node:module";

registerHooks({
	resolve(specifier, context, nextResolve) {
		try {
			return nextResolve(specifier, context);
		} catch (err) {
			if (
				err &&
				typeof err === "object" &&
				err.code === "ERR_MODULE_NOT_FOUND" &&
				specifier.endsWith(".js")
			) {
				const alt = specifier.slice(0, -3) + ".ts";
				try {
					return nextResolve(alt, context);
				} catch {
					/* fall through to the original error */
				}
			}
			throw err;
		}
	},
});

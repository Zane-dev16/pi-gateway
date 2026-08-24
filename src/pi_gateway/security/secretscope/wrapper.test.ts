// Behavior contracts for the CANONICAL wrapper (06 §3.2; port of
// plugins/platforms/feishu/adapter.py:_get_scoped_secret) and the kit
// secret-reader seam.
//
// 06 §10 "Wrapper copy fidelity": the canonical wrapper is loaded through the
// REAL discovery path — this repo's sanctioned import surface, the package
// barrel (index.js), exactly as production consumers import it — and asserted
// to keep BOTH behaviors: fail-closed on a SCOPED miss (default, never env)
// and the SANCTIONED env fallback on the UNSCOPED default-profile path.
// The grep gate (gate.test.ts) proves any hand-rolled COPY of the wrapper
// with an after-miss fallback is detected.
//
// Kit seam compatibility is proven against a structural mirror of
// src/pi_platforms/kit/registration.ts's ScopedSecretReader (declared there
// as `(name: string) => string | undefined`). The mirror — not a direct kit
// import — keeps pi_gateway downward-only per 01 §5.3 (rank 3 may not import
// rank 4); TypeScript structural typing makes every value produced here
// assignable to the real kit type.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { resolvePiHome } from "../../../pi_home.js";
import {
	getScopedSecret,
	kitScopedSecretReader,
	secretMappingFromRecord,
	setMultiplexActive,
	setSecretScope,
	resetSecretScope,
	runInSecretScope,
	withProfileRuntimeScope,
	type SecretMapping,
} from "./index.js";
// REAL discovery path: consumers import through the barrel, never deep paths.
import * as engine from "./index.js";

/** Structural mirror of kit/registration.ts ScopedSecretReader. */
type ScopedSecretReader = (name: string) => string | undefined;

const scope: SecretMapping = secretMappingFromRecord({
	BOT_TOKEN: "scoped-token",
});

const POISONED = ["BOT_TOKEN", "UNSCOPED_ONLY_KEY", "API_SERVER_PORT"];
beforeEach(() => {
	process.env.BOT_TOKEN = "poison-env-token";
	process.env.UNSCOPED_ONLY_KEY = "default-profile-env-value";
	process.env.API_SERVER_PORT = "8443";
});
afterEach(() => {
	setMultiplexActive(false);
	for (const k of POISONED) delete process.env[k];
});

describe("wrapper copy fidelity (06 §10 row 5)", () => {
	it("barrel re-exports THE canonical wrapper (single discovery surface)", () => {
		expect(engine.getScopedSecret).toBe(getScopedSecret);
		expect(engine.kitScopedSecretReader).toBe(kitScopedSecretReader);
	});

	it("SCOPED miss under multiplex → declared default; poisoned env NEVER read", () => {
		setMultiplexActive(true);
		const seen = runInSecretScope(scope, () =>
			getScopedSecret("ABSENT_FROM_SCOPE", "declared-default"),
		);
		expect(seen).toBe("declared-default");
		// stronger: a name present in env but absent from the scope stays closed
		const borrowed = runInSecretScope(scope, () =>
			getScopedSecret("BOT_TOKEN"),
		);
		expect(borrowed).toBe("scoped-token"); // not poison-env-token
		const missed = runInSecretScope(scope, () =>
			getScopedSecret("UNSCOPED_ONLY_KEY"),
		);
		expect(missed).toBeUndefined(); // NOT default-profile-env-value
	});

	it("UNSCOPED under multiplex → SANCTIONED fallback serves process env (default-profile path)", () => {
		setMultiplexActive(true);
		expect(getScopedSecret("UNSCOPED_ONLY_KEY")).toBe(
			"default-profile-env-value",
		);
		expect(getScopedSecret("TOTALLY_ABSENT", "fb")).toBe("fb");
	});

	it("multiplex OFF → overlay semantics through the same wrapper", () => {
		setMultiplexActive(false);
		expect(getScopedSecret("BOT_TOKEN")).toBe("poison-env-token"); // scoped-miss fallthrough
		const token = setSecretScope(scope);
		try {
			expect(getScopedSecret("BOT_TOKEN")).toBe("scoped-token");
		} finally {
			resetSecretScope(token);
		}
	});
});

describe("kit seam compatibility (pi_platforms/kit/registration.ts)", () => {
	it("reader is structurally assignable to the kit ScopedSecretReader shape", () => {
		const reader: ScopedSecretReader = kitScopedSecretReader();
		expect(typeof reader).toBe("function");
	});

	it("enablement routing: scoped hit resolves own value; scoped miss ⇒ undefined ⇒ LOUD disable", () => {
		setMultiplexActive(true);
		const reader: ScopedSecretReader = kitScopedSecretReader();
		const underScope = runInSecretScope(scope, () => ({
			hit: reader("BOT_TOKEN"),
			miss: reader("ABSENT_FROM_SCOPE"),
		}));
		expect(underScope.hit).toBe("scoped-token"); // profile's OWN value
		// undefined is exactly what drives the kit's `secret_missing` loud
		// disable — the reader must NEVER borrow env to keep an adapter alive.
		expect(underScope.miss).toBeUndefined();
	});

	it("default-profile registration (unscoped, multiplex ON) falls back to ITS OWN env values", () => {
		setMultiplexActive(true);
		const reader = kitScopedSecretReader();
		expect(reader("UNSCOPED_ONLY_KEY")).toBe("default-profile-env-value");
	});
});

describe("withProfileRuntimeScope (gateway/run.py::_profile_runtime_scope port)", () => {
	it("installs home override + secret scope; body sees both; both unwind", () => {
		setMultiplexActive(true);
		const seen = withProfileRuntimeScope("/tmp/profile-b-home", scope, () => {
			// the home half of the scope pair is visible through THE accessor
			expect(resolvePiHome()).toBe("/tmp/profile-b-home");
			return { tok: getScopedSecret("BOT_TOKEN") };
		});
		expect(seen.tok).toBe("scoped-token");
		// home override fully unwound after the body
		expect(resolvePiHome()).not.toBe("/tmp/profile-b-home");
	});

	it("BOTH tokens reset on unwind even when the body throws (06 §9 hygiene row)", () => {
		setMultiplexActive(true);
		const outerToken = setSecretScope(
			secretMappingFromRecord({ OUTER_KEY: "outer" }),
		);
		expect(() =>
			withProfileRuntimeScope("/tmp/profile-c-home", scope, () => {
				expect(getScopedSecret("BOT_TOKEN")).toBe("scoped-token");
				throw new Error("turn crashed mid-dispatch");
			}),
		).toThrow("turn crashed mid-dispatch");
		// OUTER scope survived intact (its own key still resolves)...
		expect(getScopedSecret("OUTER_KEY")).toBe("outer");
		// ...while the CRASHED inner scope is fully unwound: BOT_TOKEN absent
		// from the outer scope fails CLOSED through the wrapper (default
		// undefined) — the poisoned env value is NOT borrowed.
		expect(getScopedSecret("BOT_TOKEN")).toBeUndefined();
		resetSecretScope(outerToken);
		expect(currentScopeOrThrow()).toBeUndefined();
	});

	it("nested runtime scopes unwind in reverse order without cross-talk", () => {
		setMultiplexActive(false);
		withProfileRuntimeScope(
			"/tmp/outer-home",
			secretMappingFromRecord({ K: "outer" }),
			() => {
				withProfileRuntimeScope(
					"/tmp/inner-home",
					secretMappingFromRecord({ K: "inner" }),
					() => {
						expect(getScopedSecret("K")).toBe("inner");
					},
				);
				expect(getScopedSecret("K")).toBe("outer"); // inner fully unwound
			},
		);
		expect(currentSecretScopeValue()).toBeUndefined();
	});
});

// tiny helpers keeping the describe bodies readable
import { currentSecretScope } from "./index.js";
function currentScopeOrThrow(): SecretMapping | undefined {
	return currentSecretScope();
}
function currentSecretScopeValue(): SecretMapping | undefined {
	return currentSecretScope();
}

// Behavior contracts for the fail-closed PROFILE-SCOPED env reader
// (06 §4 item 1; roadmap Phase-4 "profile-scoped env reads"). The binding
// property: a profile's env resolution NEVER borrows process env — poisoned
// values are invisible in BOTH directions — and unset-in-profile reads yield
// the declared default or raise, per DEC-003/DEC-009.

import { afterEach, describe, expect, it } from "vitest";

import {
	UnscopedSecretError,
	isMultiplexActive,
	runInSecretScope,
	secretMappingFromRecord,
	setMultiplexActive,
} from "../secretscope/index.js";
import {
	ProfileEnvMissingError,
	currentProfileEnv,
	profileEnvFor,
} from "./index.js";

const POISON = "POISONED-FROM-PROCESS-ENV";

const poisonedVars: string[] = [];
function poison(name: string): void {
	poisonedVars.push(name);
	process.env[name] = POISON;
}

afterEach(() => {
	for (const name of poisonedVars.splice(0)) delete process.env[name];
	setMultiplexActive(false); // suite baseline; individual tests may flip
});

describe("profile-env never borrows process env (poisoned-env property)", () => {
	it("unset-in-profile get returns the declared default — never process.env", () => {
		poison("MULTIPLEX_TEST_VAR");
		const env = profileEnvFor(secretMappingFromRecord({}));
		expect(env.get("MULTIPLEX_TEST_VAR", "declared-default")).toBe(
			"declared-default",
		);
		expect(env.get("MULTIPLEX_TEST_VAR")).toBeUndefined();
	});

	it("unset-in-profile require RAISES ProfileEnvMissingError even when process.env holds the name", () => {
		poison("MULTIPLEX_TEST_TOKEN");
		const env = profileEnvFor(secretMappingFromRecord({ OTHER: "x" }));
		expect(() => env.require("MULTIPLEX_TEST_TOKEN")).toThrow(
			ProfileEnvMissingError,
		);
	});

	it("present-in-profile keys win with presence parity (empty string is a HIT)", () => {
		poison("MULTIPLEX_TEST_EMPTY");
		const env = profileEnvFor(
			secretMappingFromRecord({ A: "own-value", B: "" }),
		);
		expect(env.get("A")).toBe("own-value");
		expect(env.get("B", "default")).toBe(""); // "" is present, not missing
		expect(env.get("MULTIPLEX_TEST_EMPTY", "default")).toBe("default");
	});

	it("currentProfileEnv over the ALS scope ignores process.env on BOTH sides of the multiplex flag", () => {
		poison("MULTIPLEX_TEST_SHARED");
		for (const active of [true, false]) {
			setMultiplexActive(active);
			const hit = runInSecretScope(
				secretMappingFromRecord({ MULTIPLEX_TEST_SHARED: "profile-owns-this" }),
				() => currentProfileEnv().get("MULTIPLEX_TEST_SHARED"),
			);
			expect(hit).toBe("profile-owns-this");

			// Scoped miss → declared default; the poisoned value NEVER surfaces.
			const miss = runInSecretScope(
				secretMappingFromRecord({ UNRELATED: "1" }),
				() =>
					currentProfileEnv().get("MULTIPLEX_TEST_SHARED", "scoped-default"),
			);
			expect(miss).toBe("scoped-default");
		}
	});

	it("no scope installed ⇒ every read raises UnscopedSecretError (fail closed in every mode)", () => {
		poison("MULTIPLEX_TEST_ORPHAN");
		expect(isMultiplexActive()).toBe(false);
		for (const active of [true, false]) {
			setMultiplexActive(active);
			expect(() => currentProfileEnv().get("MULTIPLEX_TEST_ORPHAN")).toThrow(
				UnscopedSecretError,
			);
			expect(() =>
				currentProfileEnv().require("MULTIPLEX_TEST_ORPHAN"),
			).toThrow(UnscopedSecretError);
		}
	});
});

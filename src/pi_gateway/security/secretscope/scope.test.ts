// Behavior contracts for the secret scope CONTEXT primitive (06 §3;
// agent/secret_scope.py parity). Contracts here: token discipline, hygiene
// under exceptions, async-context propagation, and the #86905-class
// cross-profile isolation property (interleaved tasks never see each other's
// scope). Env reads are NOT exercised here — resolve.test.ts owns the
// resolution table.

import { describe, expect, it } from "vitest";
import {
	currentSecretScope,
	isMultiplexActive,
	resetSecretScope,
	runInSecretScope,
	secretMappingFromRecord,
	setMultiplexActive,
	setSecretScope,
	UnscopedSecretError,
	type SecretMapping,
} from "./index.js";

const profileA: SecretMapping = secretMappingFromRecord({
	TELEGRAM_BOT_TOKEN: "token-a",
});
const profileB: SecretMapping = secretMappingFromRecord({
	TELEGRAM_BOT_TOKEN: "token-b",
});

describe("secret scope tokens (agent/secret_scope.py:set_secret_scope parity)", () => {
	it("set/reset round-trip restores the previous mapping; undefined clears", () => {
		expect(currentSecretScope()).toBeUndefined();
		const t1 = setSecretScope(profileA);
		expect(currentSecretScope()).toBe(profileA);
		const t2 = setSecretScope(profileB); // nested install
		expect(currentSecretScope()).toBe(profileB);
		resetSecretScope(t2);
		expect(currentSecretScope()).toBe(profileA);
		resetSecretScope(t1);
		expect(currentSecretScope()).toBeUndefined();

		// "Pass None to clear": clearing on TOP of an installed scope, then
		// unwinding restores the base scope (reverse-order discipline).
		const base = setSecretScope(profileA);
		const clear = setSecretScope(undefined);
		expect(currentSecretScope()).toBeUndefined();
		resetSecretScope(clear);
		expect(currentSecretScope()).toBe(profileA);
		resetSecretScope(base);
		expect(currentSecretScope()).toBeUndefined();
	});

	it("double reset is caller MISUSE and throws instead of corrupting siblings", () => {
		const token = setSecretScope(profileA);
		resetSecretScope(token);
		expect(() => resetSecretScope(token)).toThrow(/already consumed/);
	});

	it("out-of-order reset throws (reverse-order discipline is enforced)", () => {
		const outer = setSecretScope(profileA);
		const inner = setSecretScope(profileB);
		// outer's token was SURPASSED by the inner install — resetting it now
		// is the stale-token misuse (parity of ContextVar.reset ValueError).
		expect(() => resetSecretScope(outer)).toThrow(
			/stale token|reverse install order/,
		);
		// inner still valid — clean unwind from the broken state
		resetSecretScope(inner);
		resetSecretScope(outer);
		expect(currentSecretScope()).toBeUndefined();
	});
});

describe("runInSecretScope hygiene under exceptions (06 §9 scope-reset row)", () => {
	it("restores the outer scope on normal return", () => {
		const outer = setSecretScope(profileA);
		const seen = runInSecretScope(profileB, () => currentSecretScope());
		expect(seen).toBe(profileB);
		expect(currentSecretScope()).toBe(profileA);
		resetSecretScope(outer);
	});

	it("reset runs on unwind EVEN WHEN the scoped body throws", () => {
		const outer = setSecretScope(profileA);
		expect(() =>
			runInSecretScope(profileB, () => {
				throw new Error("boom inside scope");
			}),
		).toThrow("boom inside scope");
		// outer scope state pristine after the exception
		expect(currentSecretScope()).toBe(profileA);
		resetSecretScope(outer);
		expect(currentSecretScope()).toBeUndefined();
	});

	it("no-scope outer state equally pristine when body throws", () => {
		expect(() =>
			runInSecretScope(profileA, () => {
				throw new Error("kaboom");
			}),
		).toThrow("kaboom");
		expect(currentSecretScope()).toBeUndefined();
	});
});

describe("async-context propagation (ctxvar ≙ AsyncLocalStorage)", () => {
	it("scope survives await/set continuations inside its dynamic extent", async () => {
		await runInSecretScope(profileA, async () => {
			await new Promise((r) => setTimeout(r, 5));
			expect(currentSecretScope()).toBe(profileA);
			await Promise.resolve();
			expect(currentSecretScope()).toBe(profileA);
		});
	});

	it("#86905 shape: interleaved tasks with DIFFERENT profiles never cross-contaminate", async () => {
		// 60 interleaved turns alternating profiles; every turn must resolve
		// ONLY its own token despite yields letting other tasks run mid-flight.
		const turn = async (
			scope: SecretMapping,
			expected: string,
			label: string,
		) => {
			await runInSecretScope(scope, async () => {
				for (let i = 0; i < 10; i++) {
					await new Promise((r) => setTimeout(r, 0)); // yield to the sibling
					const token = currentSecretScope()?.get("TELEGRAM_BOT_TOKEN");
					expect(token, `${label} leaked at round ${i}`).toBe(expected);
				}
			});
		};
		await Promise.all([
			turn(profileA, "token-a", "profileA"),
			turn(profileB, "token-b", "profileB"),
			turn(profileA, "token-a", "profileA-2"),
			turn(profileB, "token-b", "profileB-2"),
		]);
		expect(currentSecretScope()).toBeUndefined();
	});
});

describe("multiplex-active flag (deployment mode, not task-local)", () => {
	it("set/is round-trip; flag is global, not per-context", async () => {
		try {
			setMultiplexActive(true);
			expect(isMultiplexActive()).toBe(true);
			await runInSecretScope(profileA, async () => {
				await Promise.resolve();
				expect(isMultiplexActive()).toBe(true); // same value in nested ctx
			});
		} finally {
			setMultiplexActive(false);
		}
		expect(isMultiplexActive()).toBe(false);
	});
});

describe("UnscopedSecretError (fail-closed signal)", () => {
	it("is an Error subclass with an actionable message naming the fix", () => {
		const err = new UnscopedSecretError("TELEGRAM_BOT_TOKEN");
		expect(err).toBeInstanceOf(Error);
		expect(err.name).toBe("UnscopedSecretError");
		expect(err.message).toContain("TELEGRAM_BOT_TOKEN");
		expect(err.message).toContain("setSecretScope");
	});
});

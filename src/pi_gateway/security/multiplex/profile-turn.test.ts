// Behavior contracts for the per-turn isolation combinator (06 §4 + the
// 06 §9 "scope reset hygiene" row): home override, secret scope, and profile
// stamp install together and reset in REVERSE order even when the body
// throws — including exceptions crossing NESTED profile boundaries.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	currentSecretScope,
	secretMappingFromRecord,
} from "../secretscope/index.js";
import { resolvePiHome } from "../../../pi_home.js";
import {
	currentProfileEnv,
	currentProfileTurn,
	withProfileIsolation,
} from "./index.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-turn-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

function makeProfile(name: string, env?: Record<string, string>): string {
	const home = join(dir, name);
	mkdirSync(home, { recursive: true });
	if (env) {
		writeFileSync(
			join(home, ".env"),
			Object.entries(env)
				.map(([k, v]) => `${k}=${v}`)
				.join("\n"),
		);
	}
	return home;
}

describe("withProfileIsolation installs the full per-turn context", () => {
	it("home override + secret scope + profile stamp resolve together inside the turn", () => {
		const homeA = makeProfile("a", { TELEGRAM_BOT_TOKEN: "tok-a" });
		withProfileIsolation({ profile: "a", home: homeA }, () => {
			expect(resolvePiHome()).toBe(homeA);
			expect(currentProfileTurn()).toEqual({ profile: "a", home: homeA });
			expect(currentSecretScope()?.get("TELEGRAM_BOT_TOKEN")).toBe("tok-a");
			expect(currentProfileEnv().require("TELEGRAM_BOT_TOKEN")).toBe("tok-a");
		});
	});

	it("builds the secret scope from <home>/.env WITHOUT touching process.env", () => {
		process.env.MULTIPLEX_TURN_LEAK = "from-process-env";
		try {
			const home = makeProfile("leakless", { OWN: "yes" });
			withProfileIsolation({ profile: "leakless", home }, () => {
				expect(currentSecretScope()?.get("OWN")).toBe("yes");
				expect(
					currentSecretScope()?.get("MULTIPLEX_TURN_LEAK"),
				).toBeUndefined();
			});
		} finally {
			delete process.env.MULTIPLEX_TURN_LEAK;
		}
	});

	it("refuses blank profile names and blank homes (fail closed)", () => {
		expect(() =>
			withProfileIsolation({ profile: "  ", home: dir }, () => {}),
		).toThrow(/without a\s+profile name/);
		expect(() =>
			withProfileIsolation({ profile: "x", home: "   " }, () => {}),
		).toThrow(/no home/);
	});

	it("resolves relative homes to absolute (stable cache-scope keys)", () => {
		const home = makeProfile("abs");
		withProfileIsolation({ profile: "abs", home }, () => {
			expect(currentProfileTurn()?.home.startsWith("/")).toBe(true);
		});
	});
});

describe("scope-reset hygiene under exceptions (06 §9 row)", () => {
	it("exception inside a profile-A op resets ALL THREE layers; global state stays clean", () => {
		const homeA = makeProfile("boom", { K: "v" });
		const before = { home: resolvePiHome(), scope: currentSecretScope() };
		expect(() =>
			withProfileIsolation({ profile: "a", home: homeA }, () => {
				throw new Error("turn failed mid-flight");
			}),
		).toThrow("turn failed mid-flight");

		expect(resolvePiHome()).toBe(before.home);
		expect(currentSecretScope()).toBe(before.scope);
		expect(currentSecretScope()?.get("K")).toBeUndefined();
		expect(currentProfileTurn()).toBeUndefined();
	});

	it("nested boundary: profile-B op throwing inside profile-A leaves A fully intact", () => {
		const homeA = makeProfile("outer", { WHO: "A" });
		const homeB = makeProfile("inner", { WHO: "B" });

		const caught = withProfileIsolation({ profile: "a", home: homeA }, () => {
			const outerTurn = currentProfileTurn();
			try {
				withProfileIsolation({ profile: "b", home: homeB }, () => {
					throw new Error("inner B failure");
				});
			} catch (err) {
				// After B's unwind, EVERYTHING resolves to A again.
				expect((err as Error).message).toBe("inner B failure");
				expect(currentProfileTurn()).toEqual(outerTurn);
				expect(resolvePiHome()).toBe(homeA);
				expect(currentSecretScope()?.get("WHO")).toBe("A");
				return "recovered-in-A";
			}
			return "not-reached";
		});
		expect(caught).toBe("recovered-in-A");

		// And after A unwinds normally, nothing leaks.
		expect(currentProfileTurn()).toBeUndefined();
		expect(currentSecretScope()).toBeUndefined();
	});

	it("exception crossing BOTH boundaries still cleans every layer", () => {
		const homeA = makeProfile("cross-a", { K: "A" });
		const homeB = makeProfile("cross-b", { K: "B" });
		expect(() =>
			withProfileIsolation({ profile: "a", home: homeA }, () =>
				withProfileIsolation({ profile: "b", home: homeB }, () => {
					throw new Error("both");
				}),
			),
		).toThrow("both");
		expect(currentSecretScope()?.get("K")).toBeUndefined();
		expect(currentProfileTurn()).toBeUndefined();
	});

	it("explicit mappings override the .env build (and empty scopes stay empty)", () => {
		const home = makeProfile("explicit", { FROM_ENV_FILE: "yes" });
		const explicit = secretMappingFromRecord({ EXPLICIT_ONLY: "1" });
		withProfileIsolation({ profile: "e", home, secrets: explicit }, () => {
			expect(currentSecretScope()?.get("EXPLICIT_ONLY")).toBe("1");
			expect(currentSecretScope()?.get("FROM_ENV_FILE")).toBeUndefined();
		});
		withProfileIsolation(
			{ profile: "empty", home, secrets: secretMappingFromRecord({}) },
			() => {
				expect(currentSecretScope()?.size).toBe(0);
			},
		);
	});

	it("async interleaving: concurrent turns for different profiles never cross scopes", async () => {
		const homeA = makeProfile("async-a", { WHO: "A" });
		const homeB = makeProfile("async-b", { WHO: "B" });

		const probe = async (
			profile: string,
			home: string,
		): Promise<string | undefined> =>
			withProfileIsolation({ profile, home }, async () => {
				await new Promise((r) => setTimeout(r, 5));
				expect(currentProfileTurn()?.profile).toBe(profile);
				await new Promise((r) => setTimeout(r, 5));
				return currentSecretScope()?.get("WHO");
			});

		const [a, b] = await Promise.all([probe("a", homeA), probe("b", homeB)]);
		expect(a).toBe("A");
		expect(b).toBe("B");
	});
});

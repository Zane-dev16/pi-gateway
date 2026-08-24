// POISONED-ENV behavior contracts (06 §10 rows "Profile-scoped allowlists",
// "Scoped-miss fail-closed", "Multiplex-OFF overlay"; roadmap Phase-4 exit
// criterion verbatim): "profile B's sender denied while profile A's allowlist
// would admit" — and vice versa. Runs the REAL §2.1 decision chain with
// per-profile scopes installed; process env is poisoned in both directions so
// any after-a-scoped-miss fallback would flip these tests red.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { getScopedSecret, setMultiplexActive } from "../secretscope/index.js";
import {
	isUserAuthorized,
	platformGateEnv,
	type AuthzDecisionRecord,
	type AuthzSource,
} from "../authz/index.js";
import { withProfileIsolation } from "./index.js";

let dir: string;
const poisonedVars: string[] = [];

function poison(name: string, value: string): void {
	poisonedVars.push(name);
	process.env[name] = value;
}

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-poison-"));
});

afterEach(() => {
	for (const name of poisonedVars.splice(0)) delete process.env[name];
	setMultiplexActive(false);
	rmSync(dir, { recursive: true, force: true });
});

function makeProfile(name: string, env: Record<string, string>): string {
	const home = join(dir, name);
	mkdirSync(home, { recursive: true });
	writeFileSync(
		join(home, ".env"),
		Object.entries(env)
			.map(([k, v]) => `${k}=${v}`)
			.join("\n"),
	);
	return home;
}

interface ProfileRef {
	profile: string;
	home: string;
}

/** Run one multiplexed inbound turn for `profile` and authorize a sender. */
function authorize(
	ctx: ProfileRef,
	source: Omit<AuthzSource, "profile">,
): AuthzDecisionRecord {
	return withProfileIsolation(ctx, () =>
		isUserAuthorized({ ...source, profile: ctx.profile }),
	);
}

describe("poisoned process env cannot leak across profiles (#86905/#72348)", () => {
	it("profile B's sender DENIED while profile A's allowlist would admit — and vice versa", () => {
		setMultiplexActive(true);
		poison("TELEGRAM_ALLOWED_USERS", "attacker");
		poison("TELEGRAM_ALLOW_ALL_USERS", "true");
		poison("GATEWAY_ALLOWED_USERS", "attacker");

		const profileA: ProfileRef = {
			profile: "a",
			home: makeProfile("a", { TELEGRAM_ALLOWED_USERS: "alice,bob" }),
		};
		const profileB: ProfileRef = {
			profile: "b",
			home: makeProfile("b", { TELEGRAM_ALLOWED_USERS: "carol" }),
		};

		const aliceOnA = authorize(profileA, {
			platform: "telegram",
			userId: "alice",
			chatType: "dm",
		});
		expect(aliceOnA.allowed).toBe(true);
		expect(aliceOnA.reasonCode).toBe("allowlist_union");

		// Alice's id exists ONLY in A's scope. B must deny her even though the
		// poisoned PROCESS-env allowlist ("attacker") and A's own list both sit
		// one fallthrough away.
		const aliceOnB = authorize(profileB, {
			platform: "telegram",
			userId: "alice",
			chatType: "dm",
		});
		expect(aliceOnB.allowed).toBe(false);
		expect(aliceOnB.gate).toBe(10);
		expect(aliceOnB.reasonCode).toBe("default_deny");

		// Vice versa: carol admits on B only.
		expect(
			authorize(profileB, {
				platform: "telegram",
				userId: "carol",
				chatType: "dm",
			}).allowed,
		).toBe(true);
		expect(
			authorize(profileA, {
				platform: "telegram",
				userId: "carol",
				chatType: "dm",
			}).allowed,
		).toBe(false);

		// The attacker id lives ONLY in process env — no profile admits it.
		for (const ctx of [profileA, profileB]) {
			const r = authorize(ctx, {
				platform: "telegram",
				userId: "attacker",
				chatType: "dm",
			});
			expect(r.allowed).toBe(false);
			expect(r.gate).toBe(10);
		}
	});

	it("ALLOW_ALL flag poisoned into process env never flips a scoped profile open", () => {
		setMultiplexActive(true);
		poison("TELEGRAM_ALLOW_ALL_USERS", "1");
		const strict: ProfileRef = {
			profile: "strict",
			home: makeProfile("strict", {}),
		};
		const r = authorize(strict, {
			platform: "telegram",
			userId: "anyone",
			chatType: "dm",
		});
		expect(r.allowed).toBe(false);
		expect(r.reasonCode).toBe("default_deny");
	});

	it("scoped-miss gate reads return the DECLARED default (#72348 shape), not the env value", () => {
		setMultiplexActive(true);
		poison("DISCORD_ALLOW_BOTS", "all");
		const nobots: ProfileRef = {
			profile: "nobots",
			home: makeProfile("nobots", {}),
		};

		// Under the scope, the missing ALLOW_BOTS var resolves to the declared
		// default "none" → the poisoned env value must not admit bot senders.
		const r = withProfileIsolation(nobots, () =>
			isUserAuthorized({
				profile: nobots.profile,
				platform: "discord",
				chatType: "dm",
				isBot: true, // no user id at all
			}),
		);
		expect(r.allowed).toBe(false);
		expect(r.reasonCode).toBe("no_user_id");

		// Direct accessor view of the same property:
		withProfileIsolation(nobots, () => {
			expect(platformGateEnv("DISCORD_ALLOW_BOTS", "none")).toBe("none");
		});
	});
});

describe("the two LEGITIMATE env paths stay reachable (over-correction guard)", () => {
	it("multiplex-OFF overlay: a scoped miss falls through to env (cron overlay semantics)", () => {
		// Overlay fallthrough is legal ONLY when multiplex is OFF.
		process.env.MULTIPLEX_OVERLAY_VAR = "from-process-overlay";
		try {
			const overlaid = withProfileIsolation(
				{
					profile: "cron",
					home: dir,
					secrets: new Map([["OTHER", "x"]]),
				},
				() => getScopedSecret("MULTIPLEX_OVERLAY_VAR", "declared"),
			);
			expect(overlaid).toBe("from-process-overlay");
		} finally {
			delete process.env.MULTIPLEX_OVERLAY_VAR;
		}
	});

	it("unscoped DEFAULT-profile path under ACTIVE multiplex still reads its OWN process env (DEC-009 sanctioned)", () => {
		setMultiplexActive(true);
		poison("TELEGRAM_ALLOWED_USERS", "default-profile-owner");
		// NO scope installed → UnscopedSecretError → the canonical wrapper's
		// sanctioned catch serves process env (the default profile's own value).
		expect(getScopedSecret("TELEGRAM_ALLOWED_USERS")).toBe(
			"default-profile-owner",
		);
	});
});

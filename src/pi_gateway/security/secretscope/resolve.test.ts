// Behavior contracts for the fail-closed RESOLUTION TABLE (06 §3, verified
// order of agent/secret_scope.py::get_secret). Every fail-closed row is
// proven by POISONING process env with the would-be value: if the resolver
// ever consulted env on a scoped path, the poison surfaces and the test
// fails. 06 §10 rows: scoped-miss fail-closed · unscoped raise · multiplex-OFF
// overlay · global-env carve-out.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	getSecret,
	isGlobalEnv,
	secretMappingFromRecord,
	setMultiplexActive,
	setSecretScope,
	resetSecretScope,
	runInSecretScope,
	UnscopedSecretError,
	type SecretMapping,
} from "./index.js";

const scope: SecretMapping = secretMappingFromRecord({
	TELEGRAM_BOT_TOKEN: "scoped-token",
	EMPTY_BUT_PRESENT: "",
});

const POISONED: Record<string, string> = {
	TELEGRAM_BOT_TOKEN: "POISON-ENV-VALUE",
	EMPTY_BUT_PRESENT: "POISON-ENV-VALUE",
	TERMINAL_BACKEND: "poison-terminal",
	API_SERVER_PORT: "9999",
	PI_TELEGRAM_BATCH_DELAY: "77",
	PI_HOME: "poison-pi-home",
	API_SERVER_KEY: "poison-api-key",
	GATEWAY_RELAY_SECRET: "poison-relay-secret",
};

beforeEach(() => {
	for (const [k, v] of Object.entries(POISONED)) process.env[k] = v;
});

afterEach(() => {
	setMultiplexActive(false);
	for (const k of Object.keys(POISONED)) delete process.env[k];
});

describe("scoped-miss fail-closed (06 §10 row 1; DEC-003)", () => {
	it("scope + multiplex ON + key absent → declared DEFAULT returned; poisoned env NEVER observed", () => {
		setMultiplexActive(true);
		const seen = runInSecretScope(scope, () =>
			getSecret("FEISHU_APP_SECRET", "declared-default"),
		);
		// FEISHU_APP_SECRET is poisoned nowhere — prove the stronger case: a
		// name whose value EXISTS in env still resolves to the default.
		process.env.FEISHU_APP_SECRET = "would-be-borrowed-value";
		try {
			const borrowed = runInSecretScope(scope, () =>
				getSecret("FEISHU_APP_SECRET", "declared-default"),
			);
			expect(borrowed).toBe("declared-default");
		} finally {
			delete process.env.FEISHU_APP_SECRET;
		}
		expect(seen).toBe("declared-default");
	});

	it("scoped miss with NO declared default → undefined (still never env)", () => {
		setMultiplexActive(true);
		process.env.UNCONFIGURED_KEY = "env-only-value";
		try {
			const seen = runInSecretScope(scope, () => getSecret("UNCONFIGURED_KEY"));
			expect(seen).toBeUndefined();
		} finally {
			delete process.env.UNCONFIGURED_KEY;
		}
	});

	it("scoped HIT wins over both env and default; empty string is a HIT", () => {
		setMultiplexActive(true);
		const seen = runInSecretScope(scope, () =>
			getSecret("TELEGRAM_BOT_TOKEN", "default"),
		);
		expect(seen).toBe("scoped-token"); // not POISON-ENV-VALUE
		const empty = runInSecretScope(scope, () =>
			getSecret("EMPTY_BUT_PRESENT", "default"),
		);
		expect(empty).toBe(""); // parity: `val is not None` — present-empty ≠ miss
	});
});

describe("unscoped raise (06 §10 row 2)", () => {
	it("multiplex ON + no scope → UnscopedSecretError even when env holds the value", () => {
		setMultiplexActive(true);
		expect(() => getSecret("TELEGRAM_BOT_TOKEN")).toThrow(UnscopedSecretError);
		try {
			getSecret("TELEGRAM_BOT_TOKEN");
		} catch (err) {
			expect((err as Error).message).toContain("TELEGRAM_BOT_TOKEN");
			expect((err as Error).message).toContain("setSecretScope");
		}
	});

	it("multiplex OFF + no scope → legacy os.getenv path returns the env value", () => {
		setMultiplexActive(false);
		expect(getSecret("TELEGRAM_BOT_TOKEN")).toBe("POISON-ENV-VALUE");
		expect(getSecret("ABSENT_KEY", "fallback")).toBe("fallback");
	});
});

describe("multiplex-OFF overlay semantics (06 §10 row 3)", () => {
	it("scope installed + OFF + scoped miss → falls through to env (cron .env-overlay parity)", () => {
		setMultiplexActive(false);
		// CRON_API_KEY exists ONLY in env (systemd/secret-manager injection);
		// the cron scheduler installs a scope around every job.
		process.env.CRON_API_KEY = "env-injected-credential";
		try {
			const seen = runInSecretScope(scope, () => getSecret("CRON_API_KEY"));
			expect(seen).toBe("env-injected-credential");
		} finally {
			delete process.env.CRON_API_KEY;
		}
	});

	it("the SAME call FAILS CLOSED once multiplex flips ON — mode decides, not the scope", () => {
		setMultiplexActive(true);
		process.env.CRON_API_KEY = "env-injected-credential";
		try {
			const seen = runInSecretScope(scope, () =>
				getSecret("CRON_API_KEY", "dflt"),
			);
			expect(seen).toBe("dflt");
		} finally {
			delete process.env.CRON_API_KEY;
		}
	});
});

describe("global-env carve-out (06 §3.1 / §10 row 4)", () => {
	it("deployment knobs read env EVEN under scope+multiplex ON (poison proves source)", () => {
		setMultiplexActive(true);
		const seen = runInSecretScope(scope, () => ({
			listener: getSecret("API_SERVER_PORT"),
			prefix: getSecret("TERMINAL_BACKEND"),
			tuning: getSecret("PI_TELEGRAM_BATCH_DELAY"),
			runtimeKnob: getSecret("PI_HOME"),
		}));
		expect(seen.listener).toBe("9999");
		expect(seen.prefix).toBe("poison-terminal");
		expect(seen.tuning).toBe("77");
		expect(seen.runtimeKnob).toBe("poison-pi-home"); // set below
	});

	it("credentials are NOT carved out even when env-poisoned: API_SERVER_KEY / GATEWAY_RELAY_SECRET stay scoped", () => {
		setMultiplexActive(true);
		const seen = runInSecretScope(scope, () => ({
			apiKey: getSecret("API_SERVER_KEY", "default"),
			relaySecret: getSecret("GATEWAY_RELAY_SECRET", "default"),
		}));
		expect(seen.apiKey).toBe("default"); // NOT poison-api-key
		expect(seen.relaySecret).toBe("default"); // NOT poison-relay-secret
	});

	it("carve-out is exact-name OR prefix; near-misses stay profile-scoped", () => {
		expect(isGlobalEnv("API_SERVER_PORT")).toBe(true);
		expect(isGlobalEnv("TERMINAL_SHELL")).toBe(true); // prefix
		expect(isGlobalEnv("PATH")).toBe(true);
		expect(isGlobalEnv("API_SERVER_KEY")).toBe(false); // credential!
		expect(isGlobalEnv("GATEWAY_RELAY_URL")).toBe(true);
		expect(isGlobalEnv("GATEWAY_RELAY_SECRET")).toBe(false); // credential!
		expect(isGlobalEnv("TELEGRAM_BOT_TOKEN")).toBe(false); // PI_TELEGRAM_ prefix ≠ TELEGRAM_*
		expect(isGlobalEnv("MY_TERMINAL_SETTINGS")).toBe(false); // prefix anchors at START
	});
});

describe("token discipline inside resolution", () => {
	it("nested scopes: inner hit shadows outer; after inner reset outer visible again", () => {
		setMultiplexActive(false);
		const outer = setSecretScope(secretMappingFromRecord({ KEY: "outer" }));
		const inner = setSecretScope(secretMappingFromRecord({ KEY: "inner" }));
		expect(getSecret("KEY")).toBe("inner");
		resetSecretScope(inner);
		expect(getSecret("KEY")).toBe("outer");
		resetSecretScope(outer);
		// overlay: no scope left → poisoned env value resolves (multiplex OFF)
		expect(getSecret("TELEGRAM_BOT_TOKEN")).toBe("POISON-ENV-VALUE");
		expect(getSecret("KEY", "fallback")).toBe("fallback");
	});
});

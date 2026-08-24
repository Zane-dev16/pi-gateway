// Behavior contracts for profile .env loading WITHOUT process-env mutation
// (06 §3: build_profile_secret_scope "parses <home>/.env WITHOUT touching
// os.environ"). Parser parity corpus mirrors agent/secret_scope.py:
// load_env_file + hermes_cli/config.py:_parse_env_value semantics.

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	buildProfileSecretScope,
	getSecret,
	loadEnvFile,
	parseEnvValue,
	resetSecretScope,
	setSecretScope,
	stripInlineComment,
} from "./index.js";
import { setMultiplexActive } from "./index.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "secretscope-home-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
	setMultiplexActive(false);
	delete process.env.SCOPE_ENVFILE_MARKER;
});

function writeEnv(content: string): string {
	writeFileSync(join(home, ".env"), content);
	return join(home, ".env");
}

describe("loadEnvFile parser (parity of _parse_env_value + _strip_inline_comment)", () => {
	it("parses KEY=VALUE, export prefixes, full-line comments, blank lines", () => {
		const map = loadEnvFile(
			writeEnv(
				[
					"# leading comment",
					"",
					"PLAIN=value",
					"export EXPORTED=ev",
					"  SPACED_KEY = spaced value  ",
				].join("\n"),
			),
		);
		expect(map.get("PLAIN")).toBe("value");
		expect(map.get("EXPORTED")).toBe("ev");
		expect(map.get("SPACED_KEY")).toBe("spaced value");
	});

	it('double quotes reverse \\" and \\\\ escapes; single quotes are literal', () => {
		const map = loadEnvFile(
			writeEnv(
				[
					`DB_PASS="he said \\"hi\\" \\\\ done"`,
					"SINGLE='lit # not-comment \" kept'",
					'MIXED="unterminated',
				].join("\n"),
			),
		);
		expect(map.get("DB_PASS")).toBe('he said "hi" \\ done');
		expect(map.get("SINGLE")).toBe('lit # not-comment " kept');
		// unterminated quote: left as-is (lenient, unlike dotenv's hard error)
		expect(map.get("MIXED")).toBe('"unterminated');
	});

	it("inline comments: whitespace-before-# truncates unquoted; quoted keep through close quote", () => {
		const map = loadEnvFile(
			writeEnv(
				[
					"A=foo#bar", // no whitespace before # → kept verbatim
					"B=value # trailing comment",
					`C="has # inside" # real comment`,
					"D=#leading",
					"E=junk # two # hashes",
				].join("\n"),
			),
		);
		expect(map.get("A")).toBe("foo#bar");
		expect(map.get("B")).toBe("value");
		expect(map.get("C")).toBe("has # inside");
		expect(map.get("D")).toBe("#leading");
		expect(map.get("E")).toBe("junk");
	});

	it("BOM-prefixed files resolve the FIRST key (utf-8-sig parity)", () => {
		const map = loadEnvFile(writeEnv("\uFEFFFIRST_KEY=1\nSECOND=2"));
		expect(map.has("\uFEFFFIRST_KEY")).toBe(false);
		expect(map.get("FIRST_KEY")).toBe("1");
		expect(map.get("SECOND")).toBe("2");
	});

	it("missing / unreadable file → empty mapping, never a throw", () => {
		expect(loadEnvFile(join(home, "does-not-exist.env"))).toEqual(new Map());
	});

	it("CRLF line endings parse cleanly", () => {
		const map = loadEnvFile(writeEnv("A=1\r\nB=2\r\n"));
		expect(map.get("A")).toBe("1");
		expect(map.get("B")).toBe("2");
	});
});

describe("process-env isolation (the whole point of the scope dict)", () => {
	it("loading NEVER mutates process env; values live only in the mapping", () => {
		process.env.SCOPE_ENVFILE_MARKER = "untouched";
		writeEnv("SCOPE_ENVFILE_MARKER=overwritten-by-naive-loaders\nOTHER=x");
		const map = loadEnvFile(join(home, ".env"));
		expect(process.env.SCOPE_ENVFILE_MARKER).toBe("untouched");
		expect(map.get("SCOPE_ENVFILE_MARKER")).toBe(
			"overwritten-by-naive-loaders",
		);
	});
});

describe("buildProfileSecretScope (agent/secret_scope.py:build_profile_secret_scope)", () => {
	it("copies profile secrets; genuinely-global names are EXCLUDED from the copy", () => {
		writeEnv(
			[
				"TELEGRAM_BOT_TOKEN=tok",
				"API_SERVER_PORT=8080", // listener setting — global, excluded
				"PATH=/usr/bin", // OS var — global, excluded
				"TERMINAL_BACKEND=local", // prefix family — global, excluded
				"API_SERVER_KEY=key", // credential — INCLUDED despite API_SERVER_ shape
				"GATEWAY_RELAY_URL=https://relay", // routing stamp — excluded
				"GATEWAY_RELAY_SECRET=s3cret", // credential — included
			].join("\n"),
		);
		const scope = buildProfileSecretScope(home);
		expect(scope.get("TELEGRAM_BOT_TOKEN")).toBe("tok");
		expect(scope.get("API_SERVER_KEY")).toBe("key");
		expect(scope.get("GATEWAY_RELAY_SECRET")).toBe("s3cret");
		expect(scope.has("API_SERVER_PORT")).toBe(false);
		expect(scope.has("PATH")).toBe(false);
		expect(scope.has("TERMINAL_BACKEND")).toBe(false);
		expect(scope.has("GATEWAY_RELAY_URL")).toBe(false);

		// End-to-end: the built scope FAILS CLOSED for an excluded name under
		// multiplex even though the .env listed it — getSecret reads globals
		// from process env only.
		setMultiplexActive(true);
		let token: ReturnType<typeof setSecretScope> | undefined;
		token = setSecretScope(scope);
		try {
			expect(getSecret("API_SERVER_PORT")).toBeUndefined(); // not in env either
			expect(getSecret("TELEGRAM_BOT_TOKEN")).toBe("tok");
		} finally {
			if (token !== undefined) {
				resetSecretScope(token);
				token = undefined;
			}
		}
	});

	it("returns a FRESH map each call (callers may mutate their copy safely)", () => {
		writeEnv("K=v");
		const a = buildProfileSecretScope(home);
		const b = buildProfileSecretScope(home);
		expect(a).not.toBe(b);
		expect(a.get("K")).toBe(b.get("K"));
	});
});

describe("unit parity of the comment/quote helpers", () => {
	it("stripInlineComment + parseEnvValue compose like the Python pair", () => {
		expect(stripInlineComment("x # c")).toBe("x");
		expect(stripInlineComment(`"q v" # c`)).toBe(`"q v"`);
		expect(parseEnvValue(`"a \\" b"`)).toBe('a " b');
		expect(parseEnvValue("'raw'")).toBe("raw");
		expect(parseEnvValue(" plain ")).toBe("plain");
	});
});

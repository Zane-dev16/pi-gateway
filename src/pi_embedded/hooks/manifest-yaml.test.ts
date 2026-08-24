// Behavior contracts for the flat-manifest YAML subset parser
// (src/pi_embedded/hooks/manifest-yaml.ts).
//
// Normative basis: 07-integrations.md §7 — hook discovery reads HOOK.yaml
// (name + events), plugin discovery reads plugin.yaml; both are FLAT scalar +
// string-list documents in Hermes (gateway/hooks.py:discover_and_load,
// hermes_cli/plugins.py:_parse_manifest). The parser must accept exactly the
// shapes real manifests use, return null for empty documents (yaml.safe_load
// None parity → "invalid" skip), and FAIL LOUD on richer syntax rather than
// half-interpret it.

import { describe, expect, it } from "vitest";
import { ManifestSyntaxError, parseFlatYaml } from "./manifest-yaml.js";

describe("parseFlatYaml — accepted manifest shapes", () => {
	it("HOOK.yaml shape: scalars + block list", () => {
		const doc = parseFlatYaml(
			[
				"name: my-hook",
				"description: Watches things",
				"",
				"events:",
				"  - agent:start",
				"  - session:end",
			].join("\n"),
		);
		expect(doc).toEqual({
			name: "my-hook",
			description: "Watches things",
			events: ["agent:start", "session:end"],
		});
	});

	it("inline flow list form for events", () => {
		const doc = parseFlatYaml("name: h\nevents: [agent:start, command:*]\n");
		expect(doc?.events).toEqual(["agent:start", "command:*"]);
	});

	it("quoted scalars keep special characters and colons", () => {
		const doc = parseFlatYaml(
			`description: "Hello: world #1"\nname: 'quoted-name'\n`,
		);
		expect(doc?.description).toBe("Hello: world #1");
		expect(doc?.name).toBe("quoted-name");
	});

	it("numbers, booleans, nulls coerce like yaml.safe_load", () => {
		const doc = parseFlatYaml(
			"version: 2\ncount: -3\nratio: 0.5\nflag: true\noff: false\nnothing: null\nempty: ~\n",
		);
		expect(doc).toEqual({
			version: 2,
			count: -3,
			ratio: 0.5,
			flag: true,
			off: false,
			nothing: null,
			empty: null,
		});
	});

	it("full-line and trailing comments stripped; # inside quotes is data", () => {
		const doc = parseFlatYaml(
			[
				"# leading comment",
				"name: h  # trailing comment",
				`description: "hash # inside"`,
				"# tail comment",
			].join("\n"),
		);
		expect(doc?.name).toBe("h");
		expect(doc?.description).toBe("hash # inside");
	});

	it("duplicate keys: last wins (safe_load parity)", () => {
		const doc = parseFlatYaml("name: first\nname: second\n");
		expect(doc?.name).toBe("second");
	});

	it("plugin.yaml realistic sample parses field-for-field", () => {
		const doc = parseFlatYaml(
			[
				"name: acme-provider",
				"version: 1.2.3",
				"kind: model-provider",
				"description: ACME models",
				"tags: [llm, gateway]",
			].join("\n"),
		);
		expect(doc).toEqual({
			name: "acme-provider",
			version: "1.2.3", // not a valid YAML float ⇒ stays a string (safe_load parity)
			kind: "model-provider",
			description: "ACME models",
			tags: ["llm", "gateway"],
		});
	});
});

describe("parseFlatYaml — rejection behavior is LOUD or null, never silent garbage", () => {
	it("comments/blank-only document → null (safe_load None parity)", () => {
		expect(parseFlatYaml("# just a comment\n\n   \n")).toBeNull();
		expect(parseFlatYaml("")).toBeNull();
	});

	it("`key:` with no items → null value (not [])", () => {
		const doc = parseFlatYaml("events:\nname: h\n");
		expect(doc?.events).toBeNull();
		expect(doc?.name).toBe("h");
	});

	it("nested mappings throw ManifestSyntaxError (unsupported subset)", () => {
		expect(() =>
			parseFlatYaml(
				"config_schema:\n  type: object\n  properties:\n    x: 1\n",
			),
		).toThrow(ManifestSyntaxError);
	});

	it("stray list item outside any key throws", () => {
		expect(() => parseFlatYaml("- orphan\n")).toThrow(
			/list item outside a key block/,
		);
	});

	it("unterminated inline list throws", () => {
		expect(() => parseFlatYaml("events: [agent:start\n")).toThrow(
			ManifestSyntaxError,
		);
	});

	it("uninterpretable line throws with line number", () => {
		expect(() => parseFlatYaml("just words no colon\n")).toThrow(/line 1/);
	});
});

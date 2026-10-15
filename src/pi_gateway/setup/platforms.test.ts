// pi_gateway/setup/platforms.test — registry behavior contracts. Asserts
// status derivation and the PI_GATEWAY_PLATFORMS filter against literal
// values, never prompt strings.

import { describe, expect, it } from "vitest";
import {
	allowAllVarFor,
	filterPlatforms,
	PLATFORM_REGISTRY,
	statusOf,
	telegramTokenFormatOk,
	type VarReader,
} from "./platforms.js";

function reader(vars: Record<string, string>): VarReader {
	return (name) => vars[name];
}

describe("statusOf", () => {
	it("reports configured when every required var is present", () => {
		const spec = PLATFORM_REGISTRY.find((p) => p.name === "telegram");
		if (spec === undefined) throw new Error("telegram row missing");
		const report = statusOf(spec, reader({ TELEGRAM_BOT_TOKEN: "t" }));
		expect(report.status).toBe("configured");
		expect(report.missing).toEqual([]);
	});

	it("reports partially configured when some required var is missing", () => {
		const spec = PLATFORM_REGISTRY.find((p) => p.name === "slack");
		if (spec === undefined) throw new Error("slack row missing");
		const report = statusOf(
			spec,
			reader({ SLACK_BOT_TOKEN: "x" }),
		);
		expect(report.status).toBe("partially configured");
		expect(report.missing).toEqual(["SLACK_APP_TOKEN"]);
	});

	it("reports not configured when no required var is present", () => {
		const spec = PLATFORM_REGISTRY.find((p) => p.name === "discord");
		if (spec === undefined) throw new Error("discord row missing");
		const report = statusOf(spec, reader({}));
		expect(report.status).toBe("not configured");
		expect(report.missing).toEqual(["DISCORD_BOT_TOKEN"]);
	});

	it("treats blank values as missing", () => {
		const spec = PLATFORM_REGISTRY.find((p) => p.name === "telegram");
		if (spec === undefined) throw new Error("telegram row missing");
		const report = statusOf(spec, reader({ TELEGRAM_BOT_TOKEN: "   " }));
		expect(report.status).toBe("not configured");
	});
});

describe("filterPlatforms", () => {
	it("returns every row when the filter is empty", () => {
		expect(filterPlatforms("").specs).toHaveLength(PLATFORM_REGISTRY.length);
		expect(filterPlatforms(undefined).warnings).toEqual([]);
	});

	it("filters to the named rows", () => {
		const { specs, warnings } = filterPlatforms("telegram,discord");
		expect(specs.map((s) => s.name)).toEqual(["telegram", "discord"]);
		expect(warnings).toEqual([]);
	});

	it("warns on unknown names and ignores them", () => {
		const { specs, warnings } = filterPlatforms("telegram,nope");
		expect(specs.map((s) => s.name)).toEqual(["telegram"]);
		expect(warnings).toHaveLength(1);
	});
});

describe("telegramTokenFormatOk", () => {
	it("accepts BotFather-shaped tokens", () => {
		expect(telegramTokenFormatOk(`123456:${"a".repeat(35)}`)).toBe(true);
	});

	it("rejects short or malformed tokens", () => {
		expect(telegramTokenFormatOk("not-a-token")).toBe(false);
		expect(telegramTokenFormatOk(`123:${"a".repeat(10)}`)).toBe(false);
		expect(telegramTokenFormatOk("")).toBe(false);
	});
});

describe("allowAllVarFor", () => {
	it("returns the opt-out var for matrix platforms", () => {
		expect(allowAllVarFor("telegram")).toBe("TELEGRAM_ALLOW_ALL_USERS");
	});
});

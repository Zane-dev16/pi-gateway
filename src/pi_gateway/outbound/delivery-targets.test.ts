// Multi-target routing precedence contracts (03 §9.5; §11 "Multi-target
// routing" row). Table-driven over the target-string grammar AND the
// explicit > home > origin > local precedence ladder. Pure functions only.

import { describe, expect, it } from "vitest";
import {
	deliveryTargetToString,
	type DestinationSource,
	parseDeliveryTarget,
	resolveDeliveryRouting,
} from "./delivery-targets.js";

const ORIGIN_DM = { platform: "telegram", chatId: "-100777" };
const ORIGIN_THREADED = {
	platform: "telegram",
	chatId: "-100777",
	threadId: "55",
};

describe("target-string grammar (DeliveryTarget.parse)", () => {
	const table: Array<[string, string, string | undefined]> = [
		["origin", "origin", undefined],
		["local", "local", undefined],
		["telegram", "telegram", undefined], // home channel: no chat id
		["telegram:123456", "telegram:123456", undefined],
		["TELEGRAM:123456", "telegram:123456", undefined], // platform case-insensitive
		["slack:C123:T1", "slack:C123:T1", undefined],
	];

	it.each(table)("parse %s → %s", (input, expected) => {
		expect(deliveryTargetToString(parseDeliveryTarget(input))).toBe(expected);
	});

	it("chat/thread ids keep their original case while the platform lowercases", () => {
		const t = parseDeliveryTarget("Slack:ABC-def/!:Thread9");
		expect(t.platform).toBe("slack");
		expect(t.chatId).toBe("ABC-def/!");
		expect(t.threadId).toBe("Thread9");
		expect(t.isExplicit).toBe(true);
	});

	it("origin resolves against the RECORDED origin; falls back LOCAL when none", () => {
		expect(parseDeliveryTarget("origin", ORIGIN_DM)).toEqual({
			platform: "telegram",
			chatId: "-100777",
			isOrigin: true,
			isExplicit: false,
		});
		const threaded = parseDeliveryTarget("origin", ORIGIN_THREADED);
		expect(threaded.threadId).toBe("55");
		const orphan = parseDeliveryTarget("origin", null);
		expect(orphan.platform).toBe("local");
		expect(orphan.isOrigin).toBe(true);
	});

	it("garbage targets degrade to LOCAL (never throw)", () => {
		for (const garbage of ["", "   ", "not a target!!", "::", "telegram:"]) {
			expect(parseDeliveryTarget(garbage).platform).toBe("local");
		}
	});

	it("plugin platforms are dynamic members (_missing_ parity): irc resolves as a platform home", () => {
		expect(deliveryTargetToString(parseDeliveryTarget("irc"))).toBe("irc");
	});
});

describe("precedence ladder — table-driven (explicit > home > origin > local)", () => {
	const HOME = { platform: "telegram", chatId: "home-1" };

	const cases: Array<{
		name: string;
		inputs: Parameters<typeof resolveDeliveryRouting>[0];
		expectedSource: DestinationSource;
		expectedPlatform: string;
	}> = [
		{
			name: "explicit beats everything",
			inputs: {
				explicitTargets: ["discord:99"],
				homeChannel: HOME,
				origin: ORIGIN_DM,
			},
			expectedSource: "explicit_target",
			expectedPlatform: "discord",
		},
		{
			name: "home channel when no explicit target",
			inputs: { homeChannel: HOME, origin: ORIGIN_DM },
			expectedSource: "home_channel",
			expectedPlatform: "telegram",
		},
		{
			name: "origin when neither target nor home",
			inputs: { origin: ORIGIN_DM },
			expectedSource: "origin",
			expectedPlatform: "telegram",
		},
		{
			name: "local floor when nothing recorded",
			inputs: {},
			expectedSource: "local",
			expectedPlatform: "local",
		},
		{
			name: "explicit 'origin' keyword still counts as EXPLICIT level",
			inputs: {
				explicitTargets: ["origin"],
				homeChannel: HOME,
				origin: ORIGIN_DM,
			},
			expectedSource: "explicit_target",
			expectedPlatform: "telegram",
		},
		{
			name: "empty explicit list ⇒ fall through to home channel",
			inputs: { explicitTargets: [], homeChannel: HOME, origin: ORIGIN_DM },
			expectedSource: "home_channel",
			expectedPlatform: "telegram",
		},
	];

	it.each(cases.map((c) => [c.name, c] as const))("%s", (_name, c) => {
		const resolved = resolveDeliveryRouting(c.inputs);
		expect(resolved).toHaveLength(1);
		expect(resolved[0]?.source).toBe(c.expectedSource);
		expect(resolved[0]?.target.platform).toBe(c.expectedPlatform);
	});

	it("multiple explicit targets fan out in GIVEN order (every target kept)", () => {
		const resolved = resolveDeliveryRouting({
			explicitTargets: ["discord:9", "local", "telegram:7:t2"],
		});
		expect(resolved.map((r) => deliveryTargetToString(r.target))).toEqual([
			"discord:9",
			"local",
			"telegram:7:t2",
		]);
		expect(resolved.every((r) => r.source === "explicit_target")).toBe(true);
	});

	it("an explicit 'origin' with NO recorded origin degrades to LOCAL inside an explicit slot", () => {
		const resolved = resolveDeliveryRouting({
			explicitTargets: ["origin"],
			origin: null,
		});
		expect(resolved[0]?.target.platform).toBe("local");
		expect(resolved[0]?.source).toBe("explicit_target");
	});

	it("resolution is PURE: identical inputs give structurally identical output", () => {
		const inputs = {
			explicitTargets: ["telegram:5"],
			homeChannel: HOME,
			origin: ORIGIN_DM,
		};
		expect(resolveDeliveryRouting(inputs)).toEqual(
			resolveDeliveryRouting(inputs),
		);
	});
});

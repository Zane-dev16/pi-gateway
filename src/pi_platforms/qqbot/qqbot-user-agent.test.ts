// pi_platforms/qqbot/qqbot-user-agent.test.ts — descriptive User-Agent
// contract (adjudication cn-13, Hermes anchor utils.py:build_user_agent set
// on gateway-url GET, every _api_request and the interaction ACK).
//
// The UA rides EVERY authenticated REST leg; assertions pin presence AND the
// vendor-required descriptive shape (`QQBotAdapter/<version> (…)`), not a
// snapshot of runtime tokens.

import { describe, expect, it } from "vitest";
import { makeQQWorld } from "./qqbot-fixture.js";
import { eventually } from "./eventually.js";
import { QQBOT_USER_AGENT } from "./manifest.js";

describe("qqbot User-Agent on authenticated legs", () => {
	const UA_SHAPE = /^QQBotAdapter\/\d+\.\d+\.\d+ \(.+\)$/;

	it("carries the UA on the gateway-url GET leg", async () => {
		const world = makeQQWorld({ name: "qb-ua-gateway" });
		await world.connectAndAwaitLive();

		const gatewayCalls = world.gateway.callsOf("gateway");
		expect(gatewayCalls.length).toBeGreaterThanOrEqual(1);
		for (const call of gatewayCalls) {
			expect(call.method).toBe("GET");
			expect(call.headers?.["User-Agent"]).toMatch(UA_SHAPE);
			expect(call.headers?.["User-Agent"]).toBe(QQBOT_USER_AGENT);
			expect(call.headers?.["Authorization"]).toMatch(/^QQBot /);
		}
	});

	it("carries the UA on every apiRequest leg (text sends + media uploads)", async () => {
		const world = makeQQWorld({ name: "qb-ua-api" });
		const { engine, gateway } = world;
		await world.connectAndAwaitLive();
		engine.chatTypeMap.set("u_ua", "c2c");

		await engine.sendImage("u_ua", "https://x.example/a.png");

		for (const key of ["files", "messages:c2c"] as const) {
			const calls = gateway.callsOf(key);
			expect(calls.length).toBeGreaterThanOrEqual(1);
			for (const call of calls) {
				expect(call.headers?.["User-Agent"]).toBe(QQBOT_USER_AGENT);
				expect(call.headers?.["Content-Type"]).toBe("application/json");
				expect(call.headers?.["Authorization"]).toMatch(/^QQBot /);
			}
		}
	});

	it("carries the UA on the interaction ACK PUT leg", async () => {
		const world = makeQQWorld({ name: "qb-ua-ack" });
		const { engine, gateway } = world;
		await world.connectAndAwaitLive();

		gateway.pushDispatch("INTERACTION_CREATE", {
			id: "it-ua-1",
			chat_type: 2,
			user_openid: "u_ua",
			data: {
				resolved: { button_data: "zz:bogus:data" }, // ACKed then logged-and-dropped
			},
		});
		await eventually(() =>
			engine.interactionAcks.some((a) => a.id === "it-ua-1"),
		);

		const acks = gateway.callsOf("interactions");
		expect(acks).toHaveLength(1);
		expect(acks[0]!.method).toBe("PUT");
		expect(acks[0]!.path).toContain("/interactions/it-ua-1");
		expect(acks[0]!.headers?.["User-Agent"]).toBe(QQBOT_USER_AGENT);
		expect(acks[0]!.headers?.["Authorization"]).toMatch(/^QQBot /);
	});

	it("exposes a Hermes-shaped build_user_agent constant", () => {
		// utils.py:build_user_agent format:
		//   QQBotAdapter/<version> (<runtime>; <os>; <product>)
		expect(QQBOT_USER_AGENT).toMatch(UA_SHAPE);
		expect(QQBOT_USER_AGENT.startsWith(`QQBotAdapter/`)).toBe(true);
	});
});

// guard-wiring-slash.test.ts — DEC-078 idle-path slash behavior contracts.
//
// The production messageHandler resolves slash intake between the authz
// allow and the runner turn: recognized commands dispatch (or deny) locally,
// plain text AND unknown "/foo" reach the turn on byte-identical original
// bytes. These contracts drive the REAL handler with a recording stub runner
// and assert which texts reached the turn — never registry internals.

import { describe, expect, it } from "vitest";
import type { TurnOutcome } from "../pi_agent_core/runner-types.js";
import { createBuiltinCommandRegistry } from "../pi_gateway/commands/builtins.js";
import { buildIdleExecutors } from "../pi_gateway/commands/idle-executors.js";
import type { IncomingEvent } from "../pi_gateway/guards/index.js";
import { SLASH_ACCESS_DISABLED } from "../pi_gateway/guards/slash-access.js";
import {
	buildProductionMessageHandler,
	type ChatTurnRunner,
} from "./guard-wiring.js";

function stubRunner(): {
	texts: string[];
	runner: ChatTurnRunner;
} {
	const texts: string[] = [];
	return {
		texts,
		runner: {
			handleTurn: async (request: {
				sessionId: string;
				routingKey: string;
				text: string;
			}): Promise<TurnOutcome> => {
				texts.push(request.text);
				return {
					exitReason: "finalized",
					finalText: `TURN:${request.text}`,
					iterations: 1,
					repairs: 0,
					userRowId: 1,
					assistantRowId: 2,
					usage: null,
				};
			},
		},
	};
}

function event(
	text: string,
	extra?: Partial<IncomingEvent>,
): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: {
			platform: "test",
			chatType: "dm",
			userId: "pleb",
			chatId: "c1",
		},
		metadata: { gateway_session_key: "sess" },
		...extra,
	};
}

const ALLOW_ALL = () => ({
	allowed: true,
	gate: 0,
	reasonCode: "allowlisted_test",
	platform: "test",
	userId: "pleb",
	chatId: "c1",
});

const CTX = {
	task: {
		result: Promise.resolve(),
		isDone: () => true,
		cancel: () => {},
		cancelRequested: () => false,
	},
	throwIfCancelled: () => {},
};

describe("idle slash path (DEC-078)", () => {
	it("plain text reaches the turn byte-identical", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
		});
		const reply = await handler(event("hello world"), CTX);
		expect(reply).toBe("TURN:hello world");
		expect(stub.texts).toEqual(["hello world"]);
	});

	it("unknown /foo queues as text byte-identical (never an error reply)", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
		});
		const reply = await handler(event("/Foo MixedCase-thing  x=1"), CTX);
		expect(reply).toBe("TURN:/Foo MixedCase-thing  x=1");
		expect(stub.texts).toEqual(["/Foo MixedCase-thing  x=1"]);
	});

	it("/help answers locally from pi rows with zero model calls", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
		});
		const reply = await handler(event("/help"), CTX);
		expect(typeof reply).toBe("string");
		expect(reply).toContain("`/compact");
		expect(reply).toContain("`/new");
		expect(reply).not.toContain("subscription");
		expect(stub.texts).toEqual([]);
	});

	it("known commands with no local executor fall through on original bytes", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
		});
		const reply = await handler(event("/compact focus the tail"), CTX);
		expect(reply).toBe("TURN:/compact focus the tail");
		expect(stub.texts).toEqual(["/compact focus the tail"]);
	});

	it("/compact with the runner seam compacts locally and consumes no turn", async () => {
		const stub = stubRunner();
		const compacted: string[] = [];
		const handler = buildProductionMessageHandler({
			runner: {
				...stub.runner,
				compactSession: async (sessionId: string) => {
					compacted.push(sessionId);
					return {
						summary: "condensed talk",
						tokensBefore: 42424,
					};
				},
			},
			isAuthorized: ALLOW_ALL,
		});
		const reply = await handler(event("/compact focus the tail"), CTX);
		if (typeof reply !== "string") throw new Error("/compact must reply");
		expect(reply).toContain("Compacted 42424 tokens");
		expect(reply).toContain("condensed talk");
		expect(compacted).toEqual(["sess"]);
		expect(stub.texts).toEqual([]);
	});

	it("allowGatewayControl:false treats slash as plain text", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
		});
		const reply = await handler(
			event("/help", { allowGatewayControl: false }),
			CTX,
		);
		expect(reply).toBe("TURN:/help");
		expect(stub.texts).toEqual(["/help"]);
	});

	it("gated commands deny non-admins without running a turn; unknown stays ungated", async () => {
		const stub = stubRunner();
		const gated = {
			enabled: true,
			adminUserIds: new Set<string>(["admin"]),
			userAllowedCommands: new Set<string>(),
		};
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
			slashIdle: {
				resolve: (raw: string | null | undefined) =>
					createBuiltinCommandRegistry().resolve(raw),
				executors: buildIdleExecutors(
					createBuiltinCommandRegistry().rows(),
				),
				rows: createBuiltinCommandRegistry().rows(),
				policyOf: () => gated,
			},
		});
		const denied = await handler(event("/new fresh start"), CTX);
		expect(denied).toContain("⛔ /new is admin-only here.");
		// Unknown names are plain text and are NEVER gated.
		const through = await handler(event("/frobnicate the thing"), CTX);
		expect(through).toBe("TURN:/frobnicate the thing");
		expect(stub.texts).toEqual(["/frobnicate the thing"]);
	});

	it("gating disabled leaves recognized commands on the turn path", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			isAuthorized: ALLOW_ALL,
			slashIdle: {
				resolve: (raw: string | null | undefined) =>
					createBuiltinCommandRegistry().resolve(raw),
				executors: buildIdleExecutors(
					createBuiltinCommandRegistry().rows(),
				),
				rows: createBuiltinCommandRegistry().rows(),
				policyOf: () => SLASH_ACCESS_DISABLED,
			},
		});
		const reply = await handler(event("/new fresh start"), CTX);
		expect(reply).toBe("TURN:/new fresh start");
		expect(stub.texts).toEqual(["/new fresh start"]);
	});
});

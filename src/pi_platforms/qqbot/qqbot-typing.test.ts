// pi_platforms/qqbot/qqbot-typing.test.ts — input_notify behavior contract
// (adjudication cn-4, Hermes anchor adapter.py:send_typing):
//
//   - msg_type=6 body {input_notify:{input_type:1,input_second:60},
//     msg_id:last_inbound, msg_seq} posted to /v2/users/{id}/messages.
//   - C2C-ONLY: group/guild chats never emit typing.
//   - Requires a captured inbound message id (lastMsgIdByChat).
//   - Debounced to one request per ~50s per chat; the debounce stamp lands
//     ONLY on success, so a failed notify may retry immediately.

import { describe, expect, it } from "vitest";
import { QQBotAdapter } from "./qqbot-adapter.js";
import { FakeQQGateway } from "./fake-qq-gateway.js";
import { eventually } from "./eventually.js";
import {
	QQ_MSG_TYPE_INPUT_NOTIFY,
	QQ_TYPING_DEBOUNCE_MS,
	QQBOT_USER_AGENT,
} from "./manifest.js";

function c2cDispatch(
	messageId: string,
	userOpenid: string,
	text: string,
): [string, Record<string, unknown>] {
	return [
		"C2C_MESSAGE_CREATE",
		{
			id: messageId,
			content: text,
			author: { user_openid: userOpenid },
			timestamp: "2026-08-25T00:00:00+08:00",
		},
	];
}

interface TypingRig {
	gateway: FakeQQGateway;
	engine: QQBotAdapter;
	advanceMs(ms: number): void;
}

function makeTypingRig(name: string): TypingRig {
	void name;
	const gateway = new FakeQQGateway();
	let now = 1_000_000;
	const engine = new QQBotAdapter({
		appId: "typing-app",
		clientSecret: "typing-secret",
		groupPolicy: "open", // group traffic must REACH intake (typing stays C2C-only)
		rest: {
			request: async (method, path, body, headers) => {
				// Subject-capture parity: strip the REST base before the fake.
				const base = "https://api.sgroup.qq.com";
				const bare = path.startsWith(base) ? path.slice(base.length) : path;
				return gateway.handleRest(
					method,
					bare,
					Buffer.isBuffer(body) ? {} : (body ?? {}),
					headers,
				);
			},
		},
		wsFactory: gateway,
		nowMs: () => now,
		// Default timer-based sleep: a NO-OP sleep would turn the heartbeat
		// loop into a hot microtask spin and starve the test runner.
	});
	engine.attachStandardGuard();
	return {
		gateway,
		engine,
		advanceMs(ms: number) {
			now += ms;
		},
	};
}

async function live(rig: TypingRig): Promise<void> {
	await rig.engine.connect({ isReconnect: false });
	await eventually(() => rig.engine.isLive);
	await eventually(() => rig.engine.sessionId !== null);
}

describe("qqbot send_typing (msg_type=6 input_notify)", () => {
	it("posts the vendor body driven by the captured last inbound message id", async () => {
		const rig = makeTypingRig("qb-typing-body");
		await live(rig);

		// No inbound yet → no typing target, no call.
		await rig.engine.sendTyping("u_typing");
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(0);

		rig.gateway.pushDispatch(...c2cDispatch("in-1", "u_typing", "hello"));
		await eventually(() => rig.engine.lastMsgIdByChat.has("u_typing"));

		await rig.engine.sendTyping("u_typing");
		const calls = rig.gateway.callsOf("messages:c2c");
		expect(calls).toHaveLength(1);
		expect(calls[0]!.method).toBe("POST");
		expect(calls[0]!.path).toBe("/v2/users/u_typing/messages");
		const body = calls[0]!.body;
		expect(body["msg_type"]).toBe(QQ_MSG_TYPE_INPUT_NOTIFY);
		expect(body["msg_id"]).toBe("in-1");
		expect(body["input_notify"]).toEqual({
			input_type: 1,
			input_second: 60,
		});
		expect(typeof body["msg_seq"]).toBe("number");
	});

	it("debounces repeats inside ~50s and re-emits only after the window", async () => {
		const rig = makeTypingRig("qb-typing-debounce");
		await live(rig);
		rig.gateway.pushDispatch(...c2cDispatch("in-2", "u_deb", "hi"));
		await eventually(() => rig.engine.lastMsgIdByChat.has("u_deb"));

		await rig.engine.sendTyping("u_deb"); // first → wire
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(1);

		rig.advanceMs(QQ_TYPING_DEBOUNCE_MS - 10_000); // still inside window
		await rig.engine.sendTyping("u_deb");
		rig.advanceMs(9_999); // 1ms short of refresh — still suppressed
		await rig.engine.sendTyping("u_deb");
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(1);

		rig.advanceMs(1_001); // past 50s since the successful stamp
		await rig.engine.sendTyping("u_deb");
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(2);
	});

	it("is C2C-ONLY and stamps the debounce on SUCCESS only", async () => {
		const rig = makeTypingRig("qb-typing-c2c-only");
		await live(rig);

		// Group chat with a captured id NEVER emits typing.
		rig.gateway.pushDispatch("GROUP_AT_MESSAGE_CREATE", {
			id: "g-in-1",
			content: "hi",
			group_openid: "g_typ",
			author: { member_openid: "u_member" },
		});
		await eventually(() => rig.engine.lastMsgIdByChat.has("g_typ"));
		await rig.engine.sendTyping("g_typ");
		expect(rig.gateway.callsOf("messages:group")).toHaveLength(0);

		// C2C failure does NOT consume the debounce window: the next attempt
		// (even immediately) retries after the scripted failure clears.
		rig.gateway.pushDispatch(...c2cDispatch("in-3", "u_fail", "yo"));
		await eventually(() => rig.engine.lastMsgIdByChat.has("u_fail"));
		rig.gateway.script("messages:c2c", { kind: "fail", message: "boom" });
		await rig.engine.sendTyping("u_fail");
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(1); // attempted, failed

		await rig.engine.sendTyping("u_fail"); // immediate retry allowed
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(2);
		const retryBody = rig.gateway.callsOf("messages:c2c")[1]!.body;
		expect(retryBody["msg_type"]).toBe(QQ_MSG_TYPE_INPUT_NOTIFY);

		// …and the SUCCESSFUL retry stamps the window: an immediate third is
		// suppressed under the injected clock.
		await rig.engine.sendTyping("u_fail");
		expect(rig.gateway.callsOf("messages:c2c")).toHaveLength(2);

		// Every typing leg carries the descriptive User-Agent (cn-13 parity).
		for (const call of rig.gateway.callsOf("messages:c2c")) {
			expect(call.headers?.["User-Agent"]).toBe(QQBOT_USER_AGENT);
		}
	});
});

// pi_platforms/qqbot/qqbot-onboard.test.ts — scan-to-configure QR
// registration behavior contract (adjudication cn-10, Hermes anchors
// gateway/platforms/qqbot/onboard.py + constants.py):
//
//   - create_bind_task posts {key: <generated aes key>} with portal headers
//     (Accept + Content-Type + User-Agent) and surfaces retcode≠0 / missing
//     task_id as errors.
//   - poll_bind_result posts {task_id} and maps the vendor data envelope.
//   - qr_register: COMPLETED decrypts client_secret LOCALLY under the
//     generated key (secret never travels plaintext); EXPIRED refreshes the
//     task up to 3 times; deadline exhaust ⇒ null; create failure ⇒ null.
//   - The fake portal validates wire shapes exactly like q.qq.com: a create
//     without a key and a poll without a task_id are rejected.

import { describe, expect, it } from "vitest";
import {
	buildConnectUrl,
	createBindTask,
	pollBindResult,
	qrRegister,
} from "./onboard.js";
import { encryptSecretForFixture, generateBindKey } from "./crypto.js";
import { FakeQQGateway } from "./fake-qq-gateway.js";
import type { QQRestTransport } from "./qqbot-adapter.js";
import {
	QQ_BIND_STATUS_COMPLETED,
	QQ_BIND_STATUS_EXPIRED,
	QQ_BIND_STATUS_PENDING,
	QQ_ONBOARD_CREATE_PATH,
	QQ_ONBOARD_POLL_PATH,
	QQBOT_USER_AGENT,
} from "./manifest.js";

function portalRig(): { gateway: FakeQQGateway; rest: QQRestTransport } {
	const gateway = new FakeQQGateway();
	const rest: QQRestTransport = {
		request: async (method, path, body, headers) =>
			gateway.handleRest(
				method,
				path,
				Buffer.isBuffer(body) ? {} : (body ?? {}),
				headers,
			),
	};
	return { gateway, rest };
}

/** Monotonic clock stepping 1s per read with an instant recorded sleep. */
function steppedClock(stepMs = 1000): {
	nowMs: () => number;
	sleepMs: (ms: number) => Promise<void>;
	slept: () => number[];
} {
	let now = 0;
	const sleeps: number[] = [];
	return {
		nowMs: () => {
			now += stepMs;
			return now;
		},
		sleepMs: async (ms) => {
			sleeps.push(ms);
		},
		slept: () => sleeps,
	};
}

describe("qqbot QR onboard endpoints (q.qq.com lite bind)", () => {
	it("create_bind_task posts a fresh AES key and returns task_id + key", async () => {
		const { gateway, rest } = portalRig();

		const created = await createBindTask(rest);

		expect(created.taskId).toBe("bind-task-1");
		expect(() => Buffer.from(created.aesKey, "base64")).not.toThrow();
		expect(Buffer.from(created.aesKey, "base64")).toHaveLength(32); // AES-256

		const calls = gateway.callsOf("onboard:create");
		expect(calls).toHaveLength(1);
		expect(calls[0]!.path).toContain(QQ_ONBOARD_CREATE_PATH);
		expect(typeof calls[0]!.body["key"]).toBe("string");
		// Portal headers: q.qq.com requires Accept + identifying UA.
		expect(calls[0]!.headers?.["Accept"]).toBe("application/json");
		expect(calls[0]!.headers?.["Content-Type"]).toBe("application/json");
		expect(calls[0]!.headers?.["User-Agent"]).toBe(QQBOT_USER_AGENT);
	});

	it("rejects retcode≠0 creates and missing-task_id polls like the vendor", async () => {
		// Scripted vendor retcode failure on create.
		const { gateway, rest } = portalRig();
		gateway.script("onboard:create", {
			kind: "ok",
			body: { retcode: 310010, msg: "quota exceeded" },
		});
		await expect(createBindTask(rest)).rejects.toThrow("quota exceeded");

		// Malformed poll (no task_id) is REJECTED by the fake vendor contract.
		const resp = await rest.request(
			"POST",
			`https://q.qq.com${QQ_ONBOARD_POLL_PATH}`,
			{},
			{ "Content-Type": "application/json" },
		);
		expect(resp.body["retcode"]).not.toBe(0);
	});

	it("poll_bind_result maps status/appid/encrypted_secret/user_openid", async () => {
		const { gateway, rest } = portalRig();
		const sealed = encryptSecretForFixture("s3cret", generateBindKey());
		gateway.script("onboard:poll", {
			kind: "ok",
			body: {
				retcode: 0,
				data: {
					status: QQ_BIND_STATUS_COMPLETED,
					bot_appid: "app-42",
					bot_encrypt_secret: sealed,
					user_openid: "scanner-openid",
				},
			},
		});

		const poll = await pollBindResult(rest, "task-x");

		expect(poll).toEqual({
			status: QQ_BIND_STATUS_COMPLETED,
			botAppid: "app-42",
			encryptedSecret: sealed,
			userOpenid: "scanner-openid",
		});
		expect(gateway.callsOf("onboard:poll")[0]!.body).toEqual({
			task_id: "task-x",
		});
	});

	it("qr_register completes: pending → completed decrypts the secret locally", async () => {
		const clock = steppedClock();
		const { gateway, rest } = portalRig();
		gateway.script(
			"onboard:poll",
			{
				kind: "ok",
				body: { retcode: 0, data: { status: QQ_BIND_STATUS_PENDING } },
			},
			{
				kind: "ok",
				body: { retcode: 0, data: { status: QQ_BIND_STATUS_PENDING } },
			},
			// NOTE: completed body needs the CREATE-side key — captured below.
		);

		// Run the flow in two phases so we can seal the secret under the real
		// generated key: phase 1 captures the create call, then we script the
		// completed poll with the ciphertext BEFORE polling reaches it.
		let capturedKey: string | null = null;
		const capturingRest: QQRestTransport = {
			request: async (method, path, body, headers) => {
				if (
					method === "POST" &&
					String(path).includes(QQ_ONBOARD_CREATE_PATH)
				) {
					capturedKey = String((body as Record<string, unknown>)["key"]);
					gateway.script("onboard:poll", {
						kind: "ok",
						body: {
							retcode: 0,
							data: {
								status: QQ_BIND_STATUS_COMPLETED,
								bot_appid: "app-7",
								bot_encrypt_secret: encryptSecretForFixture(
									"client-secret-λ",
									capturedKey,
								),
								user_openid: "openid-scan",
							},
						},
					});
				}
				return rest.request(method, path, body, headers);
			},
		};

		const result = await qrRegister(capturingRest, {
			timeoutS: 600,
			...clock,
		});

		expect(result).toEqual({
			appId: "app-7",
			clientSecret: "client-secret-λ",
			userOpenid: "openid-scan",
		});
		// Poll pacing rode the injected interval (two pending waits).
		expect(clock.slept().length).toBeGreaterThanOrEqual(2);
		for (const wait of clock.slept()) expect(wait).toBe(2000);
	});

	it("qr_register refreshes on EXPIRED up to three times then gives up", async () => {
		const clock = steppedClock();
		const { gateway, rest } = portalRig();
		// Every poll answers EXPIRED — four tasks max (initial + 3 refreshes).
		gateway.script(
			"onboard:poll",
			{
				kind: "ok",
				body: { retcode: 0, data: { status: QQ_BIND_STATUS_EXPIRED } },
			},
			{
				kind: "ok",
				body: { retcode: 0, data: { status: QQ_BIND_STATUS_EXPIRED } },
			},
			{
				kind: "ok",
				body: { retcode: 0, data: { status: QQ_BIND_STATUS_EXPIRED } },
			},
			{
				kind: "ok",
				body: { retcode: 0, data: { status: QQ_BIND_STATUS_EXPIRED } },
			},
		);

		const result = await qrRegister(rest, { timeoutS: 600, ...clock });

		expect(result).toBeNull();
		const creates = gateway.callsOf("onboard:create");
		expect(creates).toHaveLength(4); // initial + _MAX_REFRESHES refreshes
		const taskIds = new Set(creates.map((c) => c.body["key"]));
		expect(taskIds.size).toBe(4); // every refresh generates a FRESH key/task
	});

	it("qr_register returns null when the create leg fails or the deadline passes", async () => {
		const failing = portalRig();
		failing.gateway.script("onboard:create", {
			kind: "fail",
			message: "portal down",
		});
		await expect(
			qrRegister(failing.rest, { timeoutS: 60, ...steppedClock() }),
		).resolves.toBeNull();

		// Deadline exhaust while pending ⇒ null (never hangs past timeoutS).
		const pending = portalRig();
		const clock = steppedClock(30_000); // 30s per clock read → 600s burns fast
		await expect(
			qrRegister(pending.rest, { timeoutS: 600, ...clock }),
		).resolves.toBeNull();
	});

	it("buildConnectUrl renders the QR target with the encoded task id", () => {
		expect(buildConnectUrl("task id/1&2")).toBe(
			"https://q.qq.com/qqbot/openclaw/connect.html?task_id=task%20id%2F1%262&_wv=2&source=pi-gateway",
		);
	});
});

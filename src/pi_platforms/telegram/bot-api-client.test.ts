// pi_platforms/telegram/bot-api-client.test — behavior contracts for the REAL
// Telegram Bot API HTTP transport (DEC-077). Every case drives the client
// against a loopback stub HTTP server (hermetic, temp ports): getUpdates
// mapping + offset floor + drop discard, 409/retry_after mapping, arg sets,
// raw-registry routing, and the production egress binding. No timing
// asserts, no network, no token in repo (TESTTOKEN is a stub-only fixture).

import {
	createServer,
	type IncomingMessage,
	type Server,
	type ServerResponse,
} from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import {
	bindTelegramProductionTransport,
	HttpTelegramBotApi,
	type TelegramProductionEgressTarget,
} from "./bot-api-client.js";
import { TELEGRAM_ALLOWED_UPDATES } from "./manifest.js";
import { extractRetryAfterSeconds } from "../kit/index.js";
import {
	TelegramConflictError,
	TelegramTransportError,
} from "./telegram-fake-server.js";

const TOKEN = "TESTTOKEN";

interface SeenRequest {
	method: string;
	apiMethod: string;
	bodyJson: unknown;
}

type StubRoute = (
	seen: SeenRequest,
) =>
	| { status: number; json: unknown }
	| Promise<{ status: number; json: unknown }>;

const stubs = new Map<string, StubRoute>();
const seen: SeenRequest[] = [];

let server: Server | null = null;
let baseUrl = "";

async function startStub(): Promise<string> {
	if (server !== null) return baseUrl;
	server = createServer((req: IncomingMessage, res: ServerResponse) => {
		const url = new URL(req.url ?? "/", "http://127.0.0.1");
		const chunks: Buffer[] = [];
		req.on("data", (c: Buffer) => chunks.push(c));
		req.on("end", () => {
			const bodyText = Buffer.concat(chunks).toString("utf8");
			let bodyJson: unknown = null;
			try {
				bodyJson = bodyText === "" ? null : (JSON.parse(bodyText) as unknown);
			} catch {
				bodyJson = null;
			}
			const prefix = `/bot${TOKEN}/`;
			const apiMethod = url.pathname.startsWith(prefix)
				? url.pathname.slice(prefix.length)
				: `??${url.pathname}`;
			const record: SeenRequest = {
				method: req.method ?? "",
				apiMethod,
				bodyJson,
			};
			seen.push(record);
			const route = stubs.get(apiMethod);
			void (async () => {
				const out =
					route !== undefined
						? await route(record)
						: { status: 404, json: { ok: false, description: "Not Found" } };
				res.writeHead(out.status, { "Content-Type": "application/json" });
				res.end(JSON.stringify(out.json));
			})();
		});
	});
	await new Promise<void>((resolve) => {
		server?.listen(0, "127.0.0.1", () => resolve());
	});
	const addr = server.address();
	if (typeof addr === "object" && addr !== null) {
		baseUrl = `http://127.0.0.1:${String(addr.port)}`;
	}
	return baseUrl;
}

afterEach(() => {
	stubs.clear();
	seen.length = 0;
});

function on(apiMethod: string, route: StubRoute): void {
	stubs.set(apiMethod, route);
}

function ok(result: unknown): { status: number; json: unknown } {
	return { status: 200, json: { ok: true, result } };
}

function clientAt(url: string): HttpTelegramBotApi {
	return new HttpTelegramBotApi({ token: TOKEN, baseUrl: url });
}

function lastSeen(apiMethod: string): SeenRequest {
	const match = [...seen].reverse().find((r) => r.apiMethod === apiMethod);
	if (match === undefined) throw new Error(`no request seen at ${apiMethod}`);
	return match;
}

async function caught(promise: Promise<unknown>): Promise<unknown> {
	return promise.then(
		() => null,
		(e: unknown) => e,
	);
}

function bodyOf(rec: SeenRequest): Record<string, unknown> {
	if (typeof rec.bodyJson !== "object" || rec.bodyJson === null) {
		throw new Error(`non-object body at ${rec.apiMethod}`);
	}
	return rec.bodyJson as Record<string, unknown>;
}

function messageUpdate(id: number, text: string): unknown {
	return {
		update_id: id,
		message: {
			message_id: id + 100,
			from: { id: 7001, is_bot: false, first_name: "Ada" },
			chat: { id: 111, type: "private" },
			date: 1760000000,
			text,
		},
	};
}

function callbackUpdate(id: number): unknown {
	return {
		update_id: id,
		callback_query: {
			id: "cbq-9",
			from: { id: 7002, is_bot: false, first_name: "Bo" },
			message: {
				message_id: 42,
				chat: { id: 111, type: "private" },
				date: 1760000000,
			},
			data: "tap:1",
		},
	};
}

describe("bot-api-client — getUpdates mapping + registry", () => {
	it("returns engine views and serves raw kinds through rawUpdateFor", async () => {
		const url = await startStub();
		on("getUpdates", () =>
			ok([messageUpdate(5001, "hello"), callbackUpdate(5002)]),
		);
		const client = clientAt(url);
		const token = client.openSession();
		const batch = await client.getUpdates({
			sessionToken: token,
			offset: 5001,
			timeoutMs: 10_000,
		});
		expect(batch.updates).toEqual([
			{ updateId: 5001, chatId: "111", senderId: "7001", text: "hello" },
			{ updateId: 5002, chatId: "111", senderId: "7002", text: "" },
		]);
		const body = bodyOf(lastSeen("getUpdates"));
		expect(body["offset"]).toBe(5001);
		expect(body["timeout"]).toBe(10);
		expect(body["allowed_updates"]).toEqual([...TELEGRAM_ALLOWED_UPDATES]);
		// Kind routing: the engine view carries no callback text; the raw
		// registry carries the callback payload.
		expect(client.rawUpdateFor("5002")?.callback_query?.data).toBe("tap:1");
		expect(client.rawUpdateFor(5001)?.message?.text).toBe("hello");
		expect(client.rawUpdateFor(9999)).toBeUndefined();
	});

	it("converts fractional poll timeouts up to whole seconds", async () => {
		const url = await startStub();
		on("getUpdates", () => ok([]));
		const client = clientAt(url);
		const token = client.openSession();
		await client.getUpdates({
			sessionToken: token,
			offset: 1,
			timeoutMs: 1500,
		});
		expect(bodyOf(lastSeen("getUpdates"))["timeout"]).toBe(2);
	});
});

describe("bot-api-client — offset floor + drop_pending_updates", () => {
	it("drop fetches-and-discards one quick poll and advances past stale ids", async () => {
		const url = await startStub();
		on("getUpdates", () =>
			ok([messageUpdate(5001, "stale"), messageUpdate(5002, "stale")]),
		);
		const client = clientAt(url);
		const token = client.openSession();
		const dropped = await client.getUpdates({
			sessionToken: token,
			offset: 5001,
			timeoutMs: 10_000,
			dropPendingUpdates: true,
		});
		expect(dropped.updates).toEqual([]);
		expect(bodyOf(lastSeen("getUpdates"))["timeout"]).toBe(0);
		// Discarded updates never become routable and never redeliver: the
		// next poll carries the advanced offset.
		expect(client.rawUpdateFor(5001)).toBeUndefined();
		await client.getUpdates({
			sessionToken: token,
			offset: 5001,
			timeoutMs: 10_000,
		});
		expect(bodyOf(lastSeen("getUpdates"))["offset"]).toBe(5003);
	});

	it("commitOffset advances the floor for later polls", async () => {
		const url = await startStub();
		on("getUpdates", () => ok([]));
		const client = clientAt(url);
		const token = client.openSession();
		client.commitOffset(token, 6000);
		await client.getUpdates({
			sessionToken: token,
			offset: 1,
			timeoutMs: 1000,
		});
		expect(bodyOf(lastSeen("getUpdates"))["offset"]).toBe(6000);
	});
});

describe("bot-api-client — poll error classes", () => {
	it("HTTP 409 becomes TelegramConflictError (engine conflict ladder)", async () => {
		const url = await startStub();
		on("getUpdates", () => ({
			status: 409,
			json: {
				ok: false,
				error_code: 409,
				description:
					"Conflict: terminated by other getUpdates request; make sure that only one bot instance is running",
			},
		}));
		const client = clientAt(url);
		const err = await caught(
			client.getUpdates({
				sessionToken: client.openSession(),
				offset: 1,
				timeoutMs: 1000,
			}),
		);
		expect(err).toBeInstanceOf(TelegramConflictError);
		expect((err as Error).name).toBe("TelegramConflictError");
	});

	it("unreachable host becomes TelegramTransportError (recovery ladder)", async () => {
		const client = clientAt("http://127.0.0.1:1");
		const err = await caught(
			client.getUpdates({
				sessionToken: client.openSession(),
				offset: 1,
				timeoutMs: 50,
			}),
		);
		expect(err).toBeInstanceOf(TelegramTransportError);
	});

	it("closeSessions aborts a parked long-poll into the transport class", async () => {
		const url = await startStub();
		on(
			"getUpdates",
			() => new Promise<{ status: number; json: unknown }>(() => {}),
		);
		const client = clientAt(url);
		const pending = client.getUpdates({
			sessionToken: client.openSession(),
			offset: 1,
			timeoutMs: 30_000,
		});
		client.closeSessions();
		const err = await caught(pending);
		expect(err).toBeInstanceOf(TelegramTransportError);
	});
});

describe("bot-api-client — identity + webhook housekeeping", () => {
	it("getMe/getWebhookInfo map; failures throw the transport class", async () => {
		const url = await startStub();
		on("getMe", () => ok({ id: 1, username: "pi_gateway_bot" }));
		on("getWebhookInfo", () => ok({ pending_update_count: 3 }));
		const client = clientAt(url);
		expect(await client.getMe()).toEqual({ username: "pi_gateway_bot" });
		expect(await client.getWebhookInfo()).toEqual({ pending_update_count: 3 });

		on("getMe", () => ({
			status: 401,
			json: { ok: false, error_code: 401, description: "Unauthorized" },
		}));
		const err = await caught(client.getMe());
		expect(err).toBeInstanceOf(TelegramTransportError);
	});

	it("deleteWebhook posts the drop flag verbatim; failure throws", async () => {
		const url = await startStub();
		on("deleteWebhook", () => ok(true));
		const client = clientAt(url);
		expect(await client.deleteWebhook({ drop_pending_updates: false })).toEqual(
			{
				ok: true,
			},
		);
		expect(bodyOf(lastSeen("deleteWebhook"))).toEqual({
			drop_pending_updates: false,
		});
		on("deleteWebhook", () => ({
			status: 500,
			json: { ok: false, error_code: 500, description: "Internal Error" },
		}));
		expect(
			(await caught(client.deleteWebhook())) instanceof TelegramTransportError,
		).toBe(true);
	});

	it("housekeeping maps command/topic shapes and throws on transport failure", async () => {
		const url = await startStub();
		on("setMyCommands", () => ok(true));
		on("setMyShortDescription", () => ok(true));
		on("createForumTopic", () => ok({ message_thread_id: 3001 }));
		on("editForumTopic", () => ok(true));
		const client = clientAt(url);
		expect(
			await client.setMyCommands({
				commands: [{ command: "start", description: "boot" }],
				scope: { type: "default" },
			}),
		).toEqual({ ok: true });
		expect(bodyOf(lastSeen("setMyCommands"))["scope"]).toEqual({
			type: "default",
		});
		expect(
			await client.createForumTopic({ chat_id: 111, name: "ops" }),
		).toEqual({ message_thread_id: 3001 });
		on("createForumTopic", () => ({
			status: 400,
			json: {
				ok: false,
				error_code: 400,
				description: "Bad Request: topic exists",
			},
		}));
		expect(
			(await caught(
				client.createForumTopic({ chat_id: 111, name: "ops" }),
			)) instanceof TelegramTransportError,
		).toBe(true);
	});
});

describe("bot-api-client — send paths never throw: 429 maps the flood shape", () => {
	it("sendMessage posts the arg set and maps message_id", async () => {
		const url = await startStub();
		on("sendMessage", () =>
			ok({ message_id: 42, chat: { id: 111 }, date: 1, text: "hi" }),
		);
		const client = clientAt(url);
		const res = await client.sendMessage({
			chat_id: 111,
			text: "hi",
			parse_mode: "MarkdownV2",
		});
		expect(res).toEqual({ success: true, messageId: "42" });
		expect(bodyOf(lastSeen("sendMessage"))).toEqual({
			chat_id: 111,
			text: "hi",
			parse_mode: "MarkdownV2",
		});
	});

	it("send failures ride SendResult with fake-identical retry_after text", async () => {
		const url = await startStub();
		on("sendMessage", () => ({
			status: 429,
			json: {
				ok: false,
				error_code: 429,
				description: "Too Many Requests: retry after 3",
				parameters: { retry_after: 3 },
			},
		}));
		on("sendPhoto", () => ({
			status: 429,
			json: {
				ok: false,
				error_code: 429,
				description: "Too Many Requests: retry after 7",
				parameters: { retry_after: 7 },
			},
		}));
		const client = clientAt(url);
		const text = await client.sendMessage({ chat_id: 111, text: "hi" });
		expect(text.success).toBe(false);
		expect(text.error).toBe("Too Many Requests: retry after 3");
		expect(text.retryAfter).toBe(3);
		// THE adapter parse site reads exactly this text.
		expect(extractRetryAfterSeconds(text.error)).toBe(3);
		const photo = await client.sendPhoto({ chat_id: 111, photo: "f" });
		expect(photo).toEqual({
			success: false,
			error: "Too Many Requests: retry after 7",
			retryAfter: 7,
		});
	});

	it("draft/delete/reaction/callback lanes map success and flood", async () => {
		const url = await startStub();
		on("sendMessageDraft", () => ok(true));
		on("deleteMessage", () => ok(true));
		on("answerCallbackQuery", () => ok(true));
		on("setMessageReaction", () => ({
			status: 429,
			json: {
				ok: false,
				error_code: 429,
				description: "Too Many Requests: retry after 2",
				parameters: { retry_after: 2 },
			},
		}));
		on("sendChatAction", () => ok(true));
		const client = clientAt(url);
		expect(
			await client.sendMessageDraft({
				chat_id: 111,
				draft_id: 9,
				text: "preview",
			}),
		).toEqual({ success: true });
		expect(bodyOf(lastSeen("sendMessageDraft"))).toMatchObject({
			chat_id: 111,
			draft_id: 9,
		});
		expect(
			await client.deleteMessage({ chat_id: 111, message_id: 42 }),
		).toEqual({
			success: true,
		});
		expect(
			await client.answerCallbackQuery({
				callback_query_id: "cbq-9",
				text: "ok",
			}),
		).toEqual({ success: true });
		expect(
			await client.setMessageReaction({
				chat_id: 111,
				message_id: 42,
				reaction: "👀",
			}),
		).toEqual({
			success: false,
			error: "Too Many Requests: retry after 2",
			retryAfter: 2,
		});
		expect(await client.sendChatAction("111")).toEqual({ success: true });
		expect(
			await client.sendChatActionEx({ chat_id: 111, action: "typing" }),
		).toEqual({ success: true });
	});

	it("editMessageText maps the shaped result; failures throw for the ladder", async () => {
		const url = await startStub();
		on("editMessageText", () =>
			ok({ message_id: 42, chat: { id: 111 }, date: 1, text: "v2" }),
		);
		const client = clientAt(url);
		const edited = await client.editMessageText({
			chat_id: 111,
			message_id: 42,
			text: "v2",
			parse_mode: "MarkdownV2",
		});
		expect(edited.message_id).toBe(42);
		expect(edited.text).toBe("v2");
		on("editMessageText", () => ({
			status: 400,
			json: {
				ok: false,
				error_code: 400,
				description: "Bad Request: message is not modified",
			},
		}));
		const err = await caught(
			client.editMessageText({ chat_id: 111, message_id: 42, text: "v2" }),
		);
		expect(err).toBeInstanceOf(TelegramTransportError);
		// The adapter not-modified no-op reads this text.
		expect((err as Error).message.toLowerCase()).toContain("not modified");
	});

	it("rich lanes ride SendResult so capability errors latch downstream", async () => {
		const url = await startStub();
		on("sendRichMessage", () => ({
			status: 404,
			json: {
				ok: false,
				error_code: 404,
				description: "Not Found: method not found",
			},
		}));
		const client = clientAt(url);
		const res = await client.sendRichMessage({ chat_id: 111 });
		expect(res.success).toBe(false);
		expect((res.error ?? "").toLowerCase()).toContain("method not found");
	});
});

describe("bot-api-client — production egress binding", () => {
	function boundTarget(): TelegramProductionEgressTarget {
		return {
			wireTransmitSend: () => Promise.resolve({ success: false }),
			wireTransmitDraft: () => Promise.resolve({ success: false }),
			wireTransmitDraftFinal: () => Promise.resolve({ success: false }),
			wireTransmitRich: () => Promise.resolve({ success: false }),
			editTransmit: () => Promise.resolve({ success: false }),
		};
	}

	it("bindTelegramProductionTransport drives HTTP methods per lane", async () => {
		const url = await startStub();
		on("sendMessage", () =>
			ok({ message_id: 7, chat: { id: 111 }, date: 1, text: "t" }),
		);
		on("editMessageText", () =>
			ok({ message_id: 7, chat: { id: 111 }, date: 1, text: "t" }),
		);
		on("sendMessageDraft", () => ok(true));
		on("sendRichMessage", () => ok({ message_id: 8 }));
		const client = clientAt(url);
		const target = boundTarget();
		bindTelegramProductionTransport(target, client);

		const sent = await target.wireTransmitSend("111", "t", {
			chat_id: 111,
			text: "t",
			parse_mode: "MarkdownV2",
		} as never);
		expect(sent).toEqual({ success: true, messageId: "7" });
		expect(bodyOf(lastSeen("sendMessage"))).toMatchObject({
			chat_id: 111,
			text: "t",
			parse_mode: "MarkdownV2",
		});

		const edited = await target.editTransmit("111", "7", "t2", {
			parse_mode: "MarkdownV2",
		} as never);
		expect(edited.success).toBe(true);
		expect(bodyOf(lastSeen("editMessageText"))).toMatchObject({ text: "t2" });

		const draft = await target.wireTransmitDraft({
			chatId: "111",
			draftId: 3,
			content: "preview",
			metadata: {},
		});
		expect(draft).toEqual({ success: true });
		expect(bodyOf(lastSeen("sendMessageDraft"))).toMatchObject({
			chat_id: 111,
			draft_id: 3,
			text: "preview",
		});

		const sealed = await target.wireTransmitDraftFinal({
			chatId: "111",
			draftId: 3,
			content: "final preview",
			metadata: {},
		});
		expect(sealed).toEqual({ success: true });

		const rich = await target.wireTransmitRich("raw *md*", {
			chat_id: 111,
		} as never);
		expect(rich.success).toBe(true);
		expect(lastSeen("sendRichMessage")).toBeDefined();
	});

	it("binding converts network death into SendResult failure, never a throw", async () => {
		const client = clientAt("http://127.0.0.1:1");
		const target = boundTarget();
		bindTelegramProductionTransport(target, client);
		const res = await target.wireTransmitSend("111", "t", {
			chat_id: 111,
			text: "t",
		} as never);
		expect(res.success).toBe(false);
	});
});

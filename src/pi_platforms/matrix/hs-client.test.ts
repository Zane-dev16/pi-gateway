// pi_platforms/matrix/hs-client.test — behavior contracts for the REAL
// Matrix CS-API HTTP transport (DEC-073). Every case drives the client
// against a loopback stub HTTP server (hermetic, temp ports): sync mapping,
// send shape, 429 retry_after honored, auth header present, auth/epoch error
// classes, and the production egress binding. No timing asserts, no network.

import {
	createServer,
	type IncomingMessage,
	type Server,
	type ServerResponse,
} from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import {
	bindMatrixProductionTransport,
	HttpMatrixHomeserver,
	type MatrixProductionEgressTarget,
} from "./hs-client.js";
import { extractMatrixRetryAfterSeconds } from "./matrix-adapter.js";

interface SeenRequest {
	method: string;
	path: string;
	query: URLSearchParams;
	auth: string | undefined;
	contentType: string | undefined;
	bodyText: string;
	bodyJson: unknown;
}

type StubRoute = (seen: SeenRequest) => { status: number; json: unknown };

const exactStubs = new Map<string, StubRoute>();
const prefixStubs: Array<{ method: string; prefix: string; route: StubRoute }> =
	[];
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
			const record: SeenRequest = {
				method: req.method ?? "",
				path: url.pathname,
				query: url.searchParams,
				auth:
					typeof req.headers["authorization"] === "string"
						? req.headers["authorization"]
						: undefined,
				contentType:
					typeof req.headers["content-type"] === "string"
						? req.headers["content-type"]
						: undefined,
				bodyText,
				bodyJson,
			};
			seen.push(record);
			const exact = exactStubs.get(`${record.method} ${record.path}`);
			const prefixed = prefixStubs.find(
				(p) => p.method === record.method && record.path.startsWith(p.prefix),
			);
			const route = exact ?? prefixed?.route;
			const out =
				route !== undefined
					? route(record)
					: { status: 404, json: { errcode: "M_NOT_FOUND", error: "no stub" } };
			res.writeHead(out.status, { "Content-Type": "application/json" });
			res.end(JSON.stringify(out.json));
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
	exactStubs.clear();
	prefixStubs.length = 0;
	seen.length = 0;
});

/** Exact method+path stub (fixed paths: sync, login, whoami, typing …). */
function on(method: string, path: string, route: StubRoute): void {
	exactStubs.set(`${method} ${path}`, route);
}

/** Prefix stub (paths with per-send txn segments: send/reaction/redact). */
function onPrefix(method: string, prefix: string, route: StubRoute): void {
	prefixStubs.push({ method, prefix, route });
}

function ok(json: unknown): { status: number; json: unknown } {
	return { status: 200, json };
}

function clientAt(url: string, token = "syt_test_token"): HttpMatrixHomeserver {
	return new HttpMatrixHomeserver({ baseUrl: url, accessToken: token });
}

function lastSeen(path: string): SeenRequest {
	const match = [...seen].reverse().find((r) => r.path === path);
	if (match === undefined) throw new Error(`no request seen at ${path}`);
	return match;
}

async function caught(promise: Promise<unknown>): Promise<unknown> {
	return promise.then(
		() => null,
		(e: unknown) => e,
	);
}

const SYNC_PATH = "/_matrix/client/v3/sync";

function syncPayload(nextBatch: string, events: unknown[] = []): unknown {
	return {
		next_batch: nextBatch,
		rooms: {
			join: {
				"!room:stub.example": { timeline: { events } },
			},
			invite: {},
		},
	};
}

function textEvent(id: string, body: string): unknown {
	return {
		event_id: id,
		sender: "@alice:stub.example",
		origin_server_ts: 1_700_000_000_000,
		type: "m.room.message",
		content: { msgtype: "m.text", body },
	};
}

describe("hs-client — sync long-poll mapping", () => {
	it("returns mapped events with room context from the join key", async () => {
		const url = await startStub();
		on("GET", SYNC_PATH, () =>
			ok(
				syncPayload("s7", [textEvent("$e1", "hello"), textEvent("$e2", "hi")]),
			),
		);
		const client = clientAt(url);
		const result = await client.sync({ since: null, timeoutMs: 30_000 });
		if (!("next_batch" in result)) throw new Error("expected sync response");
		expect(result.next_batch).toBe("s7");
		const events = result.rooms.join["!room:stub.example"]?.timeline.events;
		expect(events).toHaveLength(2);
		expect(events?.[0]).toMatchObject({
			eventId: "$e1",
			roomId: "!room:stub.example",
			sender: "@alice:stub.example",
			originServerTsMs: 1_700_000_000_000,
			type: "m.room.message",
		});
		expect((events?.[0]?.content as Record<string, unknown>)["body"]).toBe(
			"hello",
		);
		// Auth header present on sync.
		expect(lastSeen(SYNC_PATH).auth).toBe("Bearer syt_test_token");
		// Initial sync carries no since; timeout rides the query.
		expect(lastSeen(SYNC_PATH).query.get("since")).toBeNull();
		expect(lastSeen(SYNC_PATH).query.get("timeout")).toBe("30000");
	});

	it("passes since + full_state through on incremental syncs", async () => {
		const url = await startStub();
		on("GET", SYNC_PATH, () => ok(syncPayload("s8")));
		const client = clientAt(url);
		await client.sync({ since: "s7", timeoutMs: 1000, fullState: true });
		const q = lastSeen(SYNC_PATH).query;
		expect(q.get("since")).toBe("s7");
		expect(q.get("full_state")).toBe("true");
		expect(q.get("timeout")).toBe("1000");
	});

	it("soft-logout arrives as a sync-error object (immediate-fatal parity)", async () => {
		const url = await startStub();
		on("GET", SYNC_PATH, () => ({
			status: 401,
			json: { errcode: "M_UNKNOWN_TOKEN", error: "Invalid access token" },
		}));
		const client = clientAt(url);
		const result = await client.sync({ since: "s7", timeoutMs: 1000 });
		if (!("message" in result)) throw new Error("expected sync error object");
		expect(result.message.toLowerCase()).toContain("m_unknown_token");
	});

	it("unknown since-token throws the epoch-death class", async () => {
		const url = await startStub();
		on("GET", SYNC_PATH, () => ({
			status: 400,
			json: { errcode: "M_UNKNOWN_SYNC_TOKEN", error: "stale token" },
		}));
		const client = clientAt(url);
		const err = await caught(client.sync({ since: "s0_0", timeoutMs: 1000 }));
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).name).toBe("MatrixUnknownSyncTokenError");
	});
});

describe("hs-client — send / reaction / typing / receipt shapes", () => {
	it("send PUTs the content dict to …/send/m.room.message/{txn}", async () => {
		const url = await startStub();
		onPrefix(
			"PUT",
			"/_matrix/client/v3/rooms/!room%3Astub.example/send/m.room.message/",
			() => ok({ event_id: "$sent1" }),
		);
		const client = clientAt(url);
		const eventId = await client.sendMessage("!room:stub.example", {
			msgtype: "m.text",
			body: "hello wire",
			"m.mentions": { user_ids: ["@bob:stub.example"] },
		});
		expect(eventId).toBe("$sent1");
		const put = [...seen].reverse().find((r) => r.method === "PUT");
		if (put === undefined) throw new Error("no PUT seen");
		expect(put.path).toMatch(
			/^\/_matrix\/client\/v3\/rooms\/!room%3Astub\.example\/send\/m\.room\.message\/.+/,
		);
		expect(put.auth).toBe("Bearer syt_test_token");
		expect(put.contentType).toContain("application/json");
		expect(put.bodyJson).toEqual({
			msgtype: "m.text",
			body: "hello wire",
			"m.mentions": { user_ids: ["@bob:stub.example"] },
		});
	});

	it("reaction PUTs the annotation relation; typing/receipt/redact hit their endpoints", async () => {
		const url = await startStub();
		onPrefix("PUT", "/_matrix/client/v3/rooms/!r%3Ax/send/m.reaction/", () =>
			ok({ event_id: "$rxn" }),
		);
		on("PUT", "/_matrix/client/v3/rooms/!r%3Ax/typing/%40b%3Ax", () => ok({}));
		on("POST", "/_matrix/client/v3/rooms/!r%3Ax/receipt/m.read/%24e", () =>
			ok({}),
		);
		onPrefix("PUT", "/_matrix/client/v3/rooms/!r%3Ax/redact/%24e/", () =>
			ok({}),
		);
		const client = clientAt(url);
		expect(await client.sendReaction("!r:x", "$e", "👀")).toBe("$rxn");
		const reaction = [...seen]
			.reverse()
			.find((r) => r.path.includes("/send/m.reaction/"));
		expect(reaction?.bodyJson).toEqual({
			"m.relates_to": { rel_type: "m.annotation", event_id: "$e", key: "👀" },
		});
		expect(reaction?.auth).toBe("Bearer syt_test_token");

		await client.setTyping("!r:x", "@b:x", 30_000);
		expect(
			lastSeen("/_matrix/client/v3/rooms/!r%3Ax/typing/%40b%3Ax").bodyJson,
		).toEqual({
			typing: true,
			timeout: 30_000,
		});
		await client.setTyping("!r:x", "@b:x", 0); // stop_typing parity
		expect(
			[...seen]
				.reverse()
				.find(
					(r) => r.path === "/_matrix/client/v3/rooms/!r%3Ax/typing/%40b%3Ax",
				)?.bodyJson,
		).toEqual({ typing: false, timeout: 0 });

		await client.sendReadReceipt("!r:x", "$e");
		expect(
			lastSeen("/_matrix/client/v3/rooms/!r%3Ax/receipt/m.read/%24e").method,
		).toBe("POST");
		await client.redactEvent("!r:x", "$e");
		expect(
			[...seen].reverse().find((r) => r.path.includes("/redact/%24e/"))?.method,
		).toBe("PUT");
	});
});

describe("hs-client — 429 retry_after honored", () => {
	it("setTyping surfaces M_LIMIT_EXCEEDED with retry_after_ms the adapter parses", async () => {
		const url = await startStub();
		on("PUT", "/_matrix/client/v3/rooms/!r%3Ax/typing/%40b%3Ax", () => ({
			status: 429,
			json: {
				errcode: "M_LIMIT_EXCEEDED",
				error: "Too many requests",
				retry_after_ms: 2000,
			},
		}));
		const client = clientAt(url);
		const err = await caught(client.setTyping("!r:x", "@b:x", 30_000));
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).message).toContain("M_LIMIT_EXCEEDED");
		expect((err as Error).message).toContain("retry_after_ms=2000");
		// THE adapter honor-once site parses exactly this text.
		expect(extractMatrixRetryAfterSeconds((err as Error).message)).toBe(2);
	});
});

describe("hs-client — transport errors", () => {
	it("unreachable homeserver throws the transport class on sync", async () => {
		const client = clientAt("http://127.0.0.1:1");
		const err = await caught(client.sync({ since: null, timeoutMs: 50 }));
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).name).toBe("MatrixTransportError");
	});
});

describe("hs-client — identity / join / login", () => {
	it("whoami resolves the mxid; join posts the room; login captures the session token", async () => {
		const url = await startStub();
		on("GET", "/_matrix/client/v3/account/whoami", () =>
			ok({ user_id: "@bot:stub.example", device_id: "DEV1" }),
		);
		on("POST", "/_matrix/client/v3/join/!r%3Ax", () => ok({ room_id: "!r:x" }));
		on("POST", "/_matrix/client/v3/login", (rec) => {
			expect(rec.auth).toBeUndefined(); // login is unauthenticated
			expect(rec.bodyJson).toMatchObject({
				type: "m.login.password",
				password: "pw",
			});
			return ok({
				user_id: "@bot:stub.example",
				device_id: "DEVLOGIN",
				access_token: "syt_session_token",
			});
		});
		const authed = new HttpMatrixHomeserver({ baseUrl: url });
		const me = await authed.whoami();
		expect(me).toEqual({ user_id: "@bot:stub.example", device_id: "DEV1" });
		expect(await authed.joinRoom("!r:x")).toEqual({ room_id: "!r:x" });

		const pw = new HttpMatrixHomeserver({ baseUrl: url });
		const logged = await pw.login({
			identifier: "@bot:stub.example",
			password: "pw",
		});
		expect(logged.user_id).toBe("@bot:stub.example");
		// The captured session token authenticates subsequent calls.
		on("GET", "/_matrix/client/v3/account/whoami", () =>
			ok({ user_id: "@bot:stub.example", device_id: "DEVLOGIN" }),
		);
		await pw.whoami();
		expect(lastSeen("/_matrix/client/v3/account/whoami").auth).toBe(
			"Bearer syt_session_token",
		);
	});
});

describe("hs-client — production egress binding", () => {
	it("bindMatrixProductionTransport PUTs metadata.event_content and returns the event id", async () => {
		const url = await startStub();
		onPrefix(
			"PUT",
			"/_matrix/client/v3/rooms/!room%3Astub.example/send/m.room.message/",
			() => ok({ event_id: "$bound1" }),
		);
		const client = clientAt(url);
		const target: MatrixProductionEgressTarget = {
			wireTransmitSend: () => Promise.resolve({ success: false }),
			wireTransmitEdit: () => Promise.resolve({ success: false }),
		};
		bindMatrixProductionTransport(target, client);
		const content = {
			msgtype: "m.text",
			body: "bound send",
			"m.mentions": { user_ids: ["@b:x"] },
		};
		const result = await target.wireTransmitSend(
			"!room:stub.example",
			"bound send",
			{ event_content: content } as never,
		);
		expect(result.success).toBe(true);
		expect(typeof result.messageId).toBe("string");
		const put = [...seen].reverse().find((r) => r.method === "PUT");
		expect(put?.bodyJson).toEqual(content);
		expect(put?.auth).toBe("Bearer syt_test_token");
	});
});

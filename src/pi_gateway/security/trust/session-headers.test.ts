// Behavior contracts for api_server opt-in session headers (06 §8.4;
// api_server.py:_extract_session_key port): Session-Key REQUIRES API-key
// auth (rejected otherwise — NEVER anonymous), control-character injection
// and over-length headers reject, absence adopts nothing (stateless default).

import { describe, expect, it } from "vitest";
import {
	MAX_SESSION_HEADER_LEN,
	extractOptInSessionHeaders,
	extractSessionKeyHeader,
} from "./index.js";

describe("X-Hermes-Session-Key requires API-key auth", () => {
	it("key WITHOUT configured API key ⇒ 403 gateway_auth_error, never anonymous", () => {
		const verdict = extractOptInSessionHeaders(
			{ "x-hermes-session-key": "memory-scope-A" },
			{ apiKeyConfigured: false },
		);
		expect(verdict).toEqual({
			ok: false,
			status: 403,
			error: expect.stringMatching(/requires API key authentication/),
			errorType: "gateway_auth_error",
		});
	});

	it("key WITH configured API key adopts the scope", () => {
		expect(
			extractOptInSessionHeaders(
				{ "x-hermes-session-key": "scope" },
				{ apiKeyConfigured: true },
			),
		).toEqual({ ok: true, sessionId: null, sessionKey: "scope" });
	});
});

describe("header hygiene (echo-path injection defense)", () => {
	it("control characters [\\r\\n\\x00] reject with 400", () => {
		for (const hostile of ["a\rb", "a\nb", "a\u0000b"]) {
			const verdict = extractOptInSessionHeaders(
				{ "x-hermes-session-key": hostile },
				{ apiKeyConfigured: true },
			);
			expect(verdict.ok === false && verdict.status).toBe(400);
			expect(verdict.ok === false && verdict.errorType).toBe(
				"invalid_request_error",
			);
		}
	});

	it("over-length keys reject with 400 at _MAX_SESSION_HEADER_LEN", () => {
		expect(MAX_SESSION_HEADER_LEN).toBe(256);
		const verdict = extractOptInSessionHeaders(
			{ "x-hermes-session-key": "k".repeat(257) },
			{ apiKeyConfigured: true },
		);
		expect(verdict.ok === false && verdict.status).toBe(400);
		expect(
			extractOptInSessionHeaders(
				{ "x-hermes-session-key": "k".repeat(256) },
				{ apiKeyConfigured: true },
			).ok,
		).toBe(true); // exactly AT the cap is legal
	});

	it("session-id carries the same hygiene (injection + length)", () => {
		const injected = extractOptInSessionHeaders(
			{ "x-hermes-session-id": "id\ninjected: evil" },
			{ apiKeyConfigured: false },
		);
		expect(injected.ok === false && injected.status).toBe(400);
		const tooLong = extractOptInSessionHeaders(
			{ "x-hermes-session-id": "i".repeat(300) },
			{ apiKeyConfigured: false },
		);
		expect(tooLong.ok === false && tooLong.status).toBe(400);
	});
});

describe("opt-in semantics", () => {
	it("absent headers adopt NOTHING (stateless default)", () => {
		for (const apiKeyConfigured of [false, true]) {
			expect(extractOptInSessionHeaders({}, { apiKeyConfigured })).toEqual({
				ok: true,
				sessionId: null,
				sessionKey: null,
			});
		}
	});

	it("empty-string headers are treated as absent", () => {
		expect(
			extractOptInSessionHeaders(
				{ "x-hermes-session-id": "", "x-hermes-session-key": "  " },
				{ apiKeyConfigured: true },
			),
		).toEqual({ ok: true, sessionId: null, sessionKey: null });
	});

	it("BOTH present: id adoption + memory scoping together; KEY gate runs first", () => {
		expect(
			extractOptInSessionHeaders(
				{
					"x-hermes-session-id": "sess-9",
					"x-hermes-session-key": "scope-1",
				},
				{ apiKeyConfigured: true },
			),
		).toEqual({ ok: true, sessionId: "sess-9", sessionKey: "scope-1" });
		// Even a VALID id cannot slip through when the key lacks auth.
		const verdict = extractOptInSessionHeaders(
			{
				"x-hermes-session-id": "sess-9",
				"x-hermes-session-key": "scope-1",
			},
			{ apiKeyConfigured: false },
		);
		expect(verdict.ok === false && verdict.status).toBe(403);
	});

	it("session-id alone does NOT require API-key auth (spec attaches the gate to Key)", () => {
		expect(
			extractOptInSessionHeaders(
				{ "x-hermes-session-id": "sess-7" },
				{ apiKeyConfigured: false },
			),
		).toEqual({ ok: true, sessionId: "sess-7", sessionKey: null });
	});
});

describe("extractSessionKeyHeader — the key-only lane core (api-5)", () => {
	it("mirrors _parse_session_key_header: absent/blank adopt nothing", () => {
		expect(extractSessionKeyHeader({}, { apiKeyConfigured: true })).toEqual({
			ok: true,
			sessionKey: null,
		});
		expect(
			extractSessionKeyHeader(
				{ "x-hermes-session-key": "   " },
				{ apiKeyConfigured: false },
			),
		).toEqual({ ok: true, sessionKey: null });
	});

	it("no API key configured ⇒ 403 gateway_auth_error (never anonymous)", () => {
		const verdict = extractSessionKeyHeader(
			{ "x-hermes-session-key": "scope" },
			{ apiKeyConfigured: false },
		);
		expect(verdict).toEqual({
			ok: false,
			status: 403,
			error: expect.stringMatching(/requires API key authentication/),
			errorType: "gateway_auth_error",
		});
	});

	it("injection chars and over-length reject with the Hermes 400 bodies", () => {
		const injected = extractSessionKeyHeader(
			{ "x-hermes-session-key": "a\u0000b" },
			{ apiKeyConfigured: true },
		);
		expect(injected).toEqual({
			ok: false,
			status: 400,
			error: "Invalid session key",
			errorType: "invalid_request_error",
		});
		const tooLong = extractSessionKeyHeader(
			{ "x-hermes-session-key": "k".repeat(257) },
			{ apiKeyConfigured: true },
		);
		expect(tooLong).toEqual({
			ok: false,
			status: 400,
			error: "Session key too long",
			errorType: "invalid_request_error",
		});
	});

	it("valid keys are adopted trimmed; the pair helper composes this core", () => {
		expect(
			extractSessionKeyHeader(
				{ "x-hermes-session-key": "  scope-7  " },
				{ apiKeyConfigured: true },
			),
		).toEqual({ ok: true, sessionKey: "scope-7" });
	});
});

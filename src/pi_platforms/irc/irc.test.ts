// pi_platforms/irc/irc.test — A19 INJECTION-SAFETY SANITIZER contracts with
// FULL MUTATION COVERAGE plus the IRC-family behavior contracts that are not
// already encoded as conformance rows.
//
// Mutation discipline (workspace hard rule): every sanitizer contract below
// is executed TWICE — once against the REAL implementation (must pass) and
// once against a deliberate MUTANT of exactly one sanitizer (MUST FAIL its
// named contract). A contract that survives its mutant would be a
// change-detector-shaped no-op.

import { describe, expect, it } from "vitest";

import {
	SANITIZER_CONTRACTS,
	realSanitizers,
	extractNick,
	isCtcp,
	isCtcpAction,
	parseIrcMessage,
	type SanitizerImpls,
} from "./sanitize.js";
import { FakeIrcServer } from "./fake-irc-server.js";
import { IrcAdapter } from "./irc-adapter.js";

/** Run ONE named contract against an impl bundle; returns null when it PASSES
 * and the violation message when it THROWS. */
function runContract(name: string, impls: SanitizerImpls): string | null {
	const contract = SANITIZER_CONTRACTS.find((c) => c.name === name);
	if (contract === undefined) throw new Error(`unknown contract ${name}`);
	try {
		contract.run(impls);
		return null;
	} catch (err) {
		return err instanceof Error ? err.message : String(err);
	}
}

function mutant(over: Partial<SanitizerImpls>): SanitizerImpls {
	return { ...realSanitizers, ...over };
}

describe("A19 sanitizer contracts — real implementation", () => {
	for (const contract of SANITIZER_CONTRACTS) {
		it(`real impls pass contract "${contract.name}"`, () => {
			const violation = runContract(contract.name, realSanitizers);
			expect(violation).toBeNull();
		});
	}
});

describe("A19 sanitizer contracts — MUTANTS MUST FAIL their named contract", () => {
	it("mutant: control-char stripping disabled → crlf-nul-stripped FAILS", () => {
		const violation = runContract(
			"crlf-nul-stripped",
			mutant({ stripIrcControlChars: (t) => t }),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("control byte survived");
	});

	it("mutant: strips only \\n (leaves bare \\r) → crlf-nul-stripped FAILS", () => {
		const violation = runContract(
			"crlf-nul-stripped",
			mutant({ stripIrcControlChars: (t) => t.replaceAll("\n", " ") }),
		);
		expect(violation).not.toBeNull();
	});

	it("mutant: bold markers kept → markdown-stripped-to-plain FAILS", () => {
		const violation = runContract(
			"markdown-stripped-to-plain",
			mutant({
				stripMarkdownForIrc: (t) => t.replace(/__(.+?)__/g, "$1"),
			}),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("** survived");
	});

	it("mutant: links stripped entirely (drops the url leg) → markdown FAILS", () => {
		const violation = runContract(
			"markdown-stripped-to-plain",
			mutant({
				stripMarkdownForIrc: (t) => t.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1"),
			}),
		);
		expect(violation).not.toBeNull();
	});

	it("mutant: target check allows spaces → injection-target-rejected FAILS", () => {
		const violation = runContract(
			"injection-target-rejected",
			mutant({
				safeIrcTarget: (t) =>
					/[\r\n\x00]/u.test(t ?? "") ? null : (t ?? null),
			}),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("hostile target accepted");
	});

	it("mutant: target check over-rejects CLEAN targets → same contract FAILS", () => {
		const violation = runContract(
			"injection-target-rejected",
			mutant({ safeIrcTarget: () => null }),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("clean target passes");
	});

	it("mutant: collision ladder always appends _1 (regresses) → ladder contract FAILS", () => {
		const violation = runContract(
			"nick-ladder-bounded-and-shaped",
			mutant({ nextNickOnCollision: (configured) => `${configured}_1` }),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("underscore suffix");
	});

	it("mutant: ACTION conversion drops the nick → ctcp contract FAILS", () => {
		const violation = runContract(
			"ctcp-action-converted-others-dropped",
			mutant({ ctcpActionText: (text) => `* ${text.slice(8, -1)}` }),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("ACTION conversion shape");
	});

	it("mutant: splitter ignores BYTE budgets (chars only) → byte-budget contract FAILS on astral content", () => {
		const violation = runContract(
			"splitter-respects-byte-budget",
			mutant({
				splitMessageForIrc: (content, target, userLimit) => {
					const limit = Math.min(
						userLimit,
						510 - `PRIVMSG ${target} :`.length - 2,
					);
					const out: string[] = [];
					let rest = content;
					while (rest.length > limit) {
						out.push(rest.slice(0, limit));
						rest = rest.slice(limit);
					}
					if (rest.trim().length > 0 || out.length === 0) out.push(rest);
					return out;
				},
			}),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("protocol budget");
	});
});

describe("line-protocol helpers (parse/nick/CTCP classification)", () => {
	it("parses prefix/command/trailing exactly like _parse_irc_message", () => {
		const parsed = parseIrcMessage(":alice!u@h PRIVMSG #chan :hello world");
		expect(parsed.prefix).toBe("alice!u@h");
		expect(parsed.command).toBe("PRIVMSG");
		expect(parsed.params).toEqual(["#chan", "hello world"]);

		const bare = parseIrcMessage("PING :server-token");
		expect(bare.command).toBe("PING");
		expect(bare.params).toEqual(["server-token"]);

		const colonOnly = parseIrcMessage(":weird");
		expect(colonOnly).toEqual({ prefix: "weird", command: "", params: [] });

		// adapter.py:104 bug-for-bug (`if trailing:`): an EMPTY trailing is NOT
		// pushed — "PRIVMSG botnick :" parses to params=[botnick], which the
		// PRIVMSG gate drops (<2 params). Pushing text="" instead would let an
		// empty turn pass the DM gates and dispatch an empty reply.
		const emptyTrailing = parseIrcMessage(":bob!u@h PRIVMSG botnick :");
		expect(emptyTrailing.command).toBe("PRIVMSG");
		expect(emptyTrailing.params).toEqual(["botnick"]);
	});

	it("extracts nicks and classifies CTCP payloads", () => {
		expect(extractNick("alice!user@host")).toBe("alice");
		expect(extractNick("plainnick")).toBe("plainnick");
		expect(isCtcpAction("\x01ACTION waves\x01")).toBe(true);
		expect(isCtcpAction("\x01VERSION\x01")).toBe(false);
		expect(isCtcp("\x01PING 123\x01")).toBe(true);
		expect(isCtcp("plain text")).toBe(false);
	});

	// ══════════════════════════════════════════════════════════════════════
	// Wire identity parity (plugins/platforms/irc/adapter.py :: connect :219 /
	// disconnect :258): the registration realname and the QUIT reason are vendor-
	// visible strings — BYTE-IDENTICAL to Hermes.
	// ══════════════════════════════════════════════════════════════════════

	describe("wire identity parity (adapter.py::connect / disconnect)", () => {
		it('USER realname is "Hermes Agent"; QUIT reason is "Hermes Agent shutting down"', async () => {
			const server = new FakeIrcServer();
			const adapter = new IrcAdapter({
				fakeServer: server,
				secretReader: (k) =>
					k === "IRC_SERVER"
						? server.address
						: k === "IRC_CHANNEL"
							? "#hermes"
							: k === "IRC_NICKNAME"
								? "hermes-bot"
								: undefined,
			});
			expect(await adapter.connect({ isReconnect: false })).toBe(true);
			expect(server.receivedLines).toContain(
				`USER hermes-bot 0 * :Hermes Agent`,
			);

			await adapter.disconnect();
			expect(server.receivedLines[server.receivedLines.length - 1]).toBe(
				"QUIT :Hermes Agent shutting down",
			);
		});
	});
});

// ═════════════════════════════════════════════════════════════════
// Whitespace-only outbound (adapter.py:359/:297 parity): _split_message
// yields [""] and send() transmits ONE 'PRIVMSG <target> :' empty-trailing
// line. Short-circuiting success would silently DROP a message the reference
// puts on the wire.
// ═════════════════════════════════════════════════════════════════

describe("whitespace-only outbound transmits the empty-trailing PRIVMSG", () => {
	it("whitespace-only content puts ONE empty-trailing line on the wire", async () => {
		const { makeIrcWorld, CHANNEL } = await import("./irc-world.js");
		const w = makeIrcWorld({ name: "irc-blank" });
		await w.connectAndAwaitLive();

		const result = await w.subject.sendThroughDoor1(CHANNEL, "   \n\t ");
		expect(result.success).toBe(true);
		const sends = w.wire.sendsOf(CHANNEL);
		expect(sends.length).toBe(1);
		expect(sends[0]?.content).toBe("");
	});

	it("markdown-stripped content keeps its bytes — only PURE whitespace collapses to the empty-trailing shape", async () => {
		const { makeIrcWorld, CHANNEL } = await import("./irc-world.js");
		const w = makeIrcWorld({ name: "irc-blank-md" });
		await w.connectAndAwaitLive();

		// "**__**" strips (vendor order) to "__" — NOT whitespace, so it ships
		// verbatim exactly like Hermes format_message output would.
		const result = await w.subject.sendThroughDoor1(CHANNEL, "**__**");
		expect(result.success).toBe(true);
		const sends = w.wire.sendsOf(CHANNEL);
		expect(sends.length).toBe(1);
		expect(sends[0]?.content).toBe("__");
	});

	it("the paced deliverText lane collapses blank content to the SAME empty-trailing shape", async () => {
		const { makeIrcWorld, CHANNEL } = await import("./irc-world.js");
		const w = makeIrcWorld({ name: "irc-blank-paced" });
		await w.connectAndAwaitLive();

		const results = await w.subject.deliverLongText(CHANNEL, "   ");
		expect(results.length).toBe(1);
		expect(results[0]?.success).toBe(true);
		const sends = w.wire.sendsOf(CHANNEL);
		expect(sends.length).toBe(1);
		expect(sends[0]?.content).toBe("");
	});
});

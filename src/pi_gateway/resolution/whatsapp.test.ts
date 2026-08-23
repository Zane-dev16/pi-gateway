// Behavior contracts: WhatsApp identity canonicalization — ONE alias-
// collapsing module (02-session-and-state.md §4.3). Roadmap Phase 1 required
// contract #3.
//
// Asserted by relationship:
//   - Alias flip MID-STREAM converges: a conversation whose identifier flips
//     phone-JID ↔ LID form after the bridge learns the mapping resolves ALL
//     messages under ONE canonical session key (DM chat_id AND group
//     participant slot).
//   - Mapping files are read PER CALL (no startup snapshot): learning a
//     mapping between two calls changes expansion results on the next call.
//   - Defensive walk: hostile-shaped links contribute nothing; corrupt/
//     unreadable mapping files are skipped, never fatal; fresh installs
//     degrade to the normalized input; empty input → "".
//   - Outbound inverse (to_whatsapp_jid) round-trips the canonical form.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/store.js";
import {
	canonicalWhatsappIdentifier,
	defaultWhatsappSessionDir,
	expandWhatsappAliases,
	normalizeWhatsappIdentifier,
	toWhatsappJid,
} from "./whatsapp-identity.js";
import { buildSessionKey } from "./session-key.js";

const PHONE = "15551234567";
const PHONE_JID = `${PHONE}@s.whatsapp.net`;
const LID = "999999999999999"; // realistic LIDs are longer than phone numbers
const LID_JID = `${LID}@lid`;

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-waid-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

/** The bridge writes `lid-mapping-{id}[_reverse].json` as it learns aliases. */
function bridgeLearns(phoneId: string, lidId: string): void {
	writeFileSync(
		join(dir, `lid-mapping-${phoneId}.json`),
		JSON.stringify(lidId),
	);
	writeFileSync(
		join(dir, `lid-mapping-${lidId}_reverse.json`),
		JSON.stringify(phoneId),
	);
}

describe("normalizeWhatsappIdentifier", () => {
	it("strips JID/LID/device/plus syntax down to the bare numeric id", () => {
		expect(normalizeWhatsappIdentifier(PHONE_JID)).toBe(PHONE);
		expect(normalizeWhatsappIdentifier(`${PHONE}:47@s.whatsapp.net`)).toBe(
			PHONE,
		);
		expect(normalizeWhatsappIdentifier(LID_JID)).toBe(LID);
		expect(normalizeWhatsappIdentifier(`+${PHONE}`)).toBe(PHONE);
		expect(normalizeWhatsappIdentifier(PHONE)).toBe(PHONE);
		expect(normalizeWhatsappIdentifier(`  ${PHONE_JID} `)).toBe(PHONE);
	});

	it("empty/whitespace input → empty string", () => {
		expect(normalizeWhatsappIdentifier("")).toBe("");
		expect(normalizeWhatsappIdentifier("   ")).toBe("");
		expect(normalizeWhatsappIdentifier(undefined)).toBe("");
	});
});

describe("toWhatsappJid — outbound inverse", () => {
	it("bare phones (with the usual human separators) build fully-qualified JIDs", () => {
		expect(toWhatsappJid("+50766715226")).toBe("50766715226@s.whatsapp.net");
		expect(toWhatsappJid("50766715226")).toBe("50766715226@s.whatsapp.net");
		expect(toWhatsappJid("507 667-1522 6")).toBe("50766715226@s.whatsapp.net");
	});

	it("fully-qualified JIDs pass untouched; legacy device suffixes collapse", () => {
		expect(toWhatsappJid("group-id@g.us")).toBe("group-id@g.us");
		expect(toWhatsappJid(LID_JID)).toBe(LID_JID);
		expect(toWhatsappJid("status@broadcast")).toBe("status@broadcast");
		expect(toWhatsappJid(`${PHONE}:47@s.whatsapp.net`)).toBe(
			`${PHONE}@s.whatsapp.net`,
		);
	});

	it('unrecognizable input returns unchanged so the bridge surfaces a real error; empty → ""', () => {
		expect(toWhatsappJid("not-a-jid")).toBe("not-a-jid");
		expect(toWhatsappJid("")).toBe("");
	});
});

describe("expandWhatsappAliases — transitive per-call walk", () => {
	it("ALWAYS contains the normalized input; fresh install (no mapping files) degrades to it", () => {
		const fresh = mkdtempSync(join(tmpdir(), "pi-gw-wafresh-"));
		try {
			expect(expandWhatsappAliases(PHONE_JID, { sessionDir: fresh })).toEqual(
				new Set([PHONE]),
			);
			// no sessionDir at all must not throw either
			const expanded = expandWhatsappAliases(PHONE, {
				sessionDir: join(fresh, "missing"),
			});
			expect(expanded).toEqual(new Set([PHONE]));
		} finally {
			rmSync(fresh, { recursive: true, force: true });
		}
	});

	it("walks transitively through forward and reverse twins", () => {
		bridgeLearns(PHONE, LID);
		const otherLid = "888888888888888";
		writeFileSync(
			join(dir, `lid-mapping-${LID}.json`),
			JSON.stringify(otherLid),
		);

		const fromPhone = expandWhatsappAliases(PHONE_JID, { sessionDir: dir });
		expect(fromPhone.has(PHONE)).toBe(true);
		expect(fromPhone.has(LID)).toBe(true);
		expect(fromPhone.has(otherLid)).toBe(true); // second hop

		const fromLid = expandWhatsappAliases(LID_JID, { sessionDir: dir });
		expect(fromLid.has(PHONE)).toBe(true); // reverse twin edge
		expect([...fromLid].every((id) => fromPhone.has(id))).toBe(true);
	});

	it("mapping files are read PER CALL: learning between two calls changes the next result (no startup snapshot)", () => {
		// Pre-learning: LID is alone in the world.
		expect(expandWhatsappAliases(LID_JID, { sessionDir: dir })).toEqual(
			new Set([LID]),
		);
		bridgeLearns(PHONE, LID); // the bridge writes pairs mid-conversation
		// Next call sees it immediately — a frozen startup snapshot would not.
		const post = expandWhatsappAliases(LID_JID, { sessionDir: dir });
		expect(post.has(PHONE)).toBe(true);
		expect(post.has(LID)).toBe(true);
	});

	it("corrupt and unreadable mapping files are skipped — never fatal", () => {
		writeFileSync(join(dir, `lid-mapping-${PHONE}.json`), "{not json!!");
		mkdirSync(join(dir, `lid-mapping-${PHONE}_reverse.json`)); // EISDIR on read
		const expanded = expect(() =>
			expandWhatsappAliases(PHONE, { sessionDir: dir }),
		);
		expanded.not.toThrow();
		expect(expandWhatsappAliases(PHONE, { sessionDir: dir })).toEqual(
			new Set([PHONE]),
		);
	});

	it("hostile-shaped links contribute nothing (path-traversal defense-in-depth)", () => {
		writeFileSync(
			join(dir, `lid-mapping-${PHONE}.json`),
			JSON.stringify("../../etc/passwd"),
		);
		const expanded = expandWhatsappAliases(PHONE, { sessionDir: dir });
		expect(expanded).toEqual(new Set([PHONE])); // link dropped entirely
		// A traversal-shaped SEED is never followed into the filesystem either.
		const seeded = expandWhatsappAliases("../../oops", { sessionDir: dir });
		expect(seeded).toEqual(new Set(["../../oops"]));
	});

	it("empty input → empty set (callers fall through to their no-identifier branches)", () => {
		expect(expandWhatsappAliases("", { sessionDir: dir }).size).toBe(0);
	});
});

describe("canonicalWhatsappIdentifier — stable min(len, lexicographic) pick", () => {
	it("prefers the shorter numeric form across learned aliases", () => {
		bridgeLearns(PHONE, LID);
		expect(canonicalWhatsappIdentifier(PHONE_JID, { sessionDir: dir })).toBe(
			PHONE,
		);
		expect(canonicalWhatsappIdentifier(LID_JID, { sessionDir: dir })).toBe(
			PHONE,
		);
	});

	it("breaks equal lengths lexicographically; degrades to normalized input when nothing is known", () => {
		const a = "1111111111";
		const b = "2222222222";
		writeFileSync(join(dir, `lid-mapping-${a}.json`), JSON.stringify(b));
		expect(canonicalWhatsappIdentifier(a, { sessionDir: dir })).toBe(a);

		const fresh = mkdtempSync(join(tmpdir(), "pi-gw-wafresh2-"));
		try {
			expect(
				canonicalWhatsappIdentifier(`+${PHONE}`, { sessionDir: fresh }),
			).toBe(PHONE);
		} finally {
			rmSync(fresh, { recursive: true, force: true });
		}
	});

	it('empty input → ""', () => {
		expect(canonicalWhatsappIdentifier("", { sessionDir: dir })).toBe("");
	});
});

describe("§4.3 convergence — alias flip mid-stream resolves under ONE session key", () => {
	it("WhatsApp DM chat_id flips phone→LID mid-conversation; every message lands on the SAME canonical key", async () => {
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			const opts = { whatsapp: { sessionDir: dir } };
			const keyFor = (chatIdJid: string) =>
				buildSessionKey(
					{ platform: "whatsapp", chatType: "dm", chatId: chatIdJid },
					{},
					undefined,
					opts,
				);

			// Turn 1..3 arrive in phone form BEFORE any mapping exists.
			const earlyKeys = [PHONE_JID, PHONE_JID, PHONE_JID].map(keyFor);
			const expectedKey = `agent:main:whatsapp:dm:${PHONE}`;
			for (const key of earlyKeys) expect(key).toBe(expectedKey);

			// The bridge LEARNS the pairing; later turns surface as LID.
			bridgeLearns(PHONE, LID);
			const lateKeys = [LID_JID, LID_JID, LID_JID].map(keyFor);
			for (const key of lateKeys) expect(key).toBe(expectedKey);

			// Persisted history: all six messages resolve under ONE row.
			const findSession = store.db.prepare(
				"SELECT id FROM sessions WHERE session_key = ?",
			);
			const insertSession = store.db.prepare(
				"INSERT INTO sessions (id, source, session_key, started_at) VALUES (?, 'whatsapp', ?, ?)",
			);
			const insertMessage = store.db.prepare(
				"INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', ?, ?)",
			);
			let msgNo = 0;
			for (const key of [...earlyKeys, ...lateKeys]) {
				msgNo += 1;
				const found = findSession.get(key) as { id: string } | undefined;
				let sessionId = found?.id;
				if (!sessionId) {
					sessionId = [
						"20260823_120000_wa",
						String(msgNo).padStart(5, "0"),
					].join("");
					insertSession.run(sessionId, key, 1_750_000_000 + msgNo);
				}
				insertMessage.run(
					sessionId,
					["msg ", String(msgNo)].join(""),
					1_750_000_000 + msgNo,
				);
			}

			const rows = store.db
				.prepare(
					"SELECT COUNT(*) AS n FROM sessions WHERE session_key LIKE 'agent:main:whatsapp:dm:%'",
				)
				.get() as { n: number };
			expect(rows.n).toBe(1); // ONE canonical session — no fork
		} finally {
			await store.close();
		}
	});

	it("group participant slot flips forms mid-stream; isolation keys stay converged", async () => {
		const GROUP = "12036302@g.us";
		const opts = { whatsapp: { sessionDir: dir } };
		const keyFor = (participantJid: string) =>
			buildSessionKey(
				{
					platform: "whatsapp",
					chatType: "group",
					chatId: GROUP,
					userIdAlt: participantJid,
				},
				{},
				undefined,
				opts,
			);

		// Participant arrives by phone JID pre-learning…
		const beforeFlip = keyFor(PHONE_JID);
		expect(beforeFlip).toBe(`agent:main:whatsapp:group:${GROUP}:${PHONE}`);

		bridgeLearns(PHONE, LID);

		// …and by LID afterwards: same canonical participant → SAME key.
		const keys = [LID_JID, PHONE_JID, LID_JID].map(keyFor);
		for (const key of keys) expect(key).toBe(beforeFlip);
	});

	it("allowlist-style membership matches ANY known form of the sender (authz consumer contract)", () => {
		bridgeLearns(PHONE, LID);
		const knownFormsOfSender = expandWhatsappAliases(LID_JID, {
			sessionDir: dir,
		});
		// Authorization expands its allowlist entry through the SAME module…
		const allowlisted = expandWhatsappAliases(`+${PHONE}`, {
			sessionDir: dir,
		});
		// …so an arrival in EITHER shape matches.
		expect(knownFormsOfSender.has(LID)).toBe(true);
		expect(knownFormsOfSender.has(PHONE)).toBe(true);
		// superset check without ES2025 Set methods (lib pinned to ES2022):
		expect([...knownFormsOfSender].every((id) => allowlisted.has(id))).toBe(
			true,
		);
	});

	it("outbound addressing builds a sendable JID from either stored form", () => {
		bridgeLearns(PHONE, LID);
		const canonical = canonicalWhatsappIdentifier(LID_JID, {
			sessionDir: dir,
		});
		expect(canonical).toBe(PHONE);
		expect(toWhatsappJid(canonical)).toBe(PHONE_JID);
		expect(toWhatsappJid(LID_JID)).toBe(LID_JID); // already-qualified passes
	});
});

describe("defaultWhatsappSessionDir (hermes_constants.py:get_hermes_dir parity)", () => {
	it("legacy <home>/whatsapp/session wins only WITH content; consolidated layout otherwise", () => {
		const home = mkdtempSync(join(tmpdir(), "pi-gw-wahome-"));
		try {
			// No dirs at all → consolidated layout.
			expect(defaultWhatsappSessionDir(home)).toBe(
				join(home, "platforms", "whatsapp", "session"),
			);

			// Bare empty legacy stub must NOT shadow the consolidated layout.
			mkdirSync(join(home, "whatsapp", "session"), { recursive: true });
			expect(defaultWhatsappSessionDir(home)).toBe(
				join(home, "platforms", "whatsapp", "session"),
			);

			// Legacy dir with CONTENT wins.
			writeFileSync(join(home, "whatsapp", "session", "x.json"), "{}");
			expect(defaultWhatsappSessionDir(home)).toBe(
				join(home, "whatsapp", "session"),
			);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});
});

// CONFORMANCE WIRING — the Buzz community-relay census port vs the executable
// 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="polling" against the REAL
//      kit-built BuzzSubject. Applicability is COMPUTED from capability data:
//      the streaming family applies only when supportsDraftStreaming() holds —
//      the CLI plane has no native draft lanes, so those rows are excluded BY
//      THE PROBE, never by a hardcoded skip (supportsAsyncDelivery stays TRUE;
//      the exclusion keys on draft streaming alone).
//   2. The INHERITED polling transport rows (TRANSPORT_ROW_REQUIREMENTS.polling)
//      run as REAL buzz fixtures through makePollingRows(makeRealBuzzFixture());
//      each row title carries its vendor-class mapping verbatim.
//   3. Fresh buzz shape-delta rows execute against REAL engines + the injected
//      FakeBuzzCli seam: NIP-42 crypto CONTRACT VECTORS (reference-computed,
//      independently BIP-340-verified), bech32/npub codecs, normalizeUserRef,
//      BUZZ_AUTH_TAG validation, CLI secret hygiene (env-only carriage), the
//      CLI JSON error contract, the connect refusal ladder, sweep semantics,
//      mention gating + allow-list normalization, transport-mode resolution,
//      send command shapes, DM-discovery cadence, self-echo suppression, and
//      the pinned commit-first ack window.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a kind-filter-defeating mutant (defineProperty) fails
//      ITS OWN named row alone while every other row passes.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makePollingRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import {
	makeBuzzSubject,
	FIXTURE_BUZZ_RELAY,
	FIXTURE_BUZZ_NSEC,
} from "../buzz/buzz-subject.js";
import {
	makeBuzzWorld,
	makeRealBuzzFixture,
	ALICE,
	BOB,
	MAIN_CHANNEL,
	DM_CHANNEL,
	type BuzzWorld,
} from "../buzz/buzz-world.js";
import { FakeBuzzCli } from "../buzz/cli-wire.js";
import {
	BuzzAdapter,
	cliErrorMessage,
	parseJsonList,
	resolveCliPath,
} from "../buzz/buzz-adapter.js";
import {
	bech32HrpExpand,
	bech32Polymod,
	BECH32_CHARSET,
	buildAuthEvent,
	CURVE_ORDER,
	decodePrivateKeyScalar,
	hexToNpub,
	normalizeUserRef,
	npubToHex,
	publicKeyHex,
} from "../buzz/nostr-auth.js";
import {
	AUTH_EVENT_PLAIN,
	AUTH_EVENT_TAGGED,
	FIXED_AUX_HEX,
	FIXED_KEY_HEX,
	FIXED_NSEC,
	FIXED_PUBKEY_HEX,
	GENERATOR_X_HEX,
} from "../buzz/vectors.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const cli = new FakeBuzzCli({
		relayUrl: FIXTURE_BUZZ_RELAY,
		selfPubkey: FIXED_PUBKEY_HEX,
		selfDisplayName: "PiBot",
	});
	cli.addChannel(MAIN_CHANNEL, "General");
	void opts.streamIsMessageChatIds; // no native lanes exist on this plane
	return makeBuzzSubject({
		wire: new FakePlatformWire(),
		cli,
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	draftStreamingSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const draftStreamingSupported =
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	return {
		draftStreamingSupported,
		excludedIds: [...STREAMING_ROW_IDS],
	};
}

/** Vendor-class mapping documented IN the inherited row titles (mission spec). */
const BUZZ_POLLING_ROW_TITLES: Record<string, string> = {
	"transport.polling.outage-reconnect-preserves-queue":
		"buzz [CLI-outage mapping]: persistent CLI failures mid-sweep are contained; the fake-relay queue persists SERVER-SIDE; resumed sweeps + a full reconnect re-seed deliver held events EXACTLY ONCE downstream (seen-set dedupe across inclusive refetches)",
	"transport.polling.held-inbound-redispatch":
		"buzz [commit-first ack-window — SOURCE-PINNED ordering]: a fetched-but-UNCOMMITTED batch held across a pre-commit crash REDISPATCHES exactly once on the next inclusive-since sweep; dedupe keeps single delivery",
	"transport.polling.conflict-zombie-eviction":
		"buzz [CLASS DELTA — a stateless request/response CLI cannot hold a server-side polling session, so no 409/zombie exists]: the REAL bounds are pinned instead — a duplicate consumer refuses FATAL via the scoped identity lock, and the survivor's reconnect RESEEDS, dropping backlog (drop_pending_updates parity)",
	"transport.polling.heartbeat-escalation":
		'buzz [CLASS DELTA — Buzz has NO heartbeats; the escalation analog is the CLI TIMEOUT LADDER]: two consecutive rc-124 timeouts surface the {"error":"timeout"} stderr contract, latch NOTHING fatal, and the sweep loop SURVIVES to resume delivery',
};

// ── buzz shape-delta rows (executed over REAL engines) ───────────────────────

interface DeltaEngine {
	engine: BuzzAdapter;
	cli: FakeBuzzCli;
	scheduler: ManualScheduler;
	world: BuzzWorld;
}

function freshDeltaEngine(): DeltaEngine {
	const world = makeBuzzWorld();
	return {
		engine: world.engine,
		cli: world.cli,
		scheduler: world.scheduler,
		world,
	};
}

/** Test-side minimal nsec bech32 encoder (uses EXPORTED primitives). */
function bech32EncodeNsec(data5: number[]): string {
	const values = [...bech32HrpExpand("nsec"), ...data5];
	const polymod = bech32Polymod([...values, 0, 0, 0, 0, 0, 0]) ^ 1;
	const checksum = [0, 1, 2, 3, 4, 5].map(
		(i) => (polymod >> (5 * (5 - i))) & 31,
	);
	return (
		"nsec1" + [...data5, ...checksum].map((d) => BECH32_CHARSET[d]).join("")
	);
}

function convertBits8to5(bytes: number[], pad = true): number[] {
	let acc = 0;
	let bits = 0;
	const ret: number[] = [];
	const maxv = 31;
	for (const v of bytes) {
		acc = (acc << 8) | v;
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			ret.push((acc >> bits) & maxv);
		}
	}
	if (pad && bits > 0) ret.push((acc << (5 - bits)) & maxv);
	return ret;
}

function buzzDeltaRows(newEngine: () => DeltaEngine): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: DeltaEngine) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["polling"]),
		run: async () => {
			const fx = newEngine();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["polling"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["polling"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"transport.buzz.nip42-crypto-vectors",
			"buzz NIP-42 crypto CONTRACT VECTORS (computed FROM the reference implementation, independently BIP-340-verified): strict nsec decode rejects mixed case/bad encoding/bad hrp/bad charset/bad checksum/non-zero padding/wrong length/out-of-range scalars with the exact reference verdicts; pubkey derivation and BOTH deterministic auth events reproduce the reference id+sig byte-exactly",
			async () => {
				// Valid decode: nsec and hex agree; derivation matches the vector.
				expect(decodePrivateKeyScalar(FIXED_NSEC)).toBe(
					BigInt(`0x${FIXED_KEY_HEX}`),
				);
				expect(decodePrivateKeyScalar(FIXED_KEY_HEX)).toBe(
					BigInt(`0x${FIXED_KEY_HEX}`),
				);
				expect(publicKeyHex(FIXED_NSEC)).toBe(FIXED_PUBKEY_HEX);
				expect(publicKeyHex(FIXED_KEY_HEX)).toBe(FIXED_PUBKEY_HEX);

				// Mixed case refused.
				const mixed = `N${FIXED_NSEC.slice(1)}`;
				expect(() => decodePrivateKeyScalar(mixed)).toThrow(
					"nsec cannot mix upper- and lowercase",
				);
				// Encoding too short / no separator.
				expect(() => decodePrivateKeyScalar("nsec1")).toThrow(
					"invalid nsec encoding",
				);
				// Non-nsec1, non-hex garbage takes the hex path verdict.
				expect(() => decodePrivateKeyScalar("1abcdef")).toThrow(
					"private key must be 64 hex characters or nsec",
				);
				// A later embedded '1' moves the hrp off `nsec`.
				expect(() => decodePrivateKeyScalar("nsec1a1b2c3d4e5")).toThrow(
					"private key must use the nsec prefix",
				);
				// Invalid charset character.
				const badChar = `${FIXED_NSEC.slice(0, -1)}b`; // 'b' ∉ BECH32_CHARSET
				expect(() => decodePrivateKeyScalar(badChar)).toThrow(
					"invalid character in nsec",
				);
				// Broken checksum (single-char substitution, charset-valid).
				const badChecksum = `${FIXED_NSEC.slice(0, -1)}m`;
				expect(badChecksum).not.toBe(FIXED_NSEC);
				expect(() => decodePrivateKeyScalar(badChecksum)).toThrow(
					"invalid nsec checksum",
				);
				// Non-zero padding: valid checksum, dirty trailing bits.
				const bytes31 = Array.from({ length: 31 }, (_, i) => (i * 7 + 1) % 256);
				const data5 = convertBits8to5(bytes31);
				data5[data5.length - 1] = data5[data5.length - 1]! | 0b1; // dirty pad bits
				expect(() => decodePrivateKeyScalar(bech32EncodeNsec(data5))).toThrow(
					"non-zero nsec padding",
				);
				// Wrong payload length (31 bytes, cleanly encoded).
				const clean31 = convertBits8to5(bytes31);
				expect(() => decodePrivateKeyScalar(bech32EncodeNsec(clean31))).toThrow(
					"nsec must encode exactly 32 bytes",
				);
				// Out-of-range scalars: zero and ≥ curve order.
				expect(() => decodePrivateKeyScalar("00".repeat(32))).toThrow(
					"private key is outside the secp256k1 range",
				);
				expect(() => decodePrivateKeyScalar(CURVE_ORDER.toString(16))).toThrow(
					"private key is outside the secp256k1 range",
				);
				// Non-hex garbage.
				expect(() => decodePrivateKeyScalar("zzzz")).toThrow(
					"private key must be 64 hex characters or nsec",
				);

				// Deterministic auth event WITHOUT auth tag: id + sig byte-exact.
				const plain = buildAuthEvent({
					privateKey: FIXED_NSEC,
					challenge: AUTH_EVENT_PLAIN.challenge,
					relayUrl: AUTH_EVENT_PLAIN.relayUrl,
					authTagJson: "",
					createdAt: AUTH_EVENT_PLAIN.createdAt,
					auxHex: FIXED_AUX_HEX,
				});
				expect(plain.id).toBe(AUTH_EVENT_PLAIN.id);
				expect(plain.pubkey).toBe(AUTH_EVENT_PLAIN.pubkey);
				expect(plain.created_at).toBe(AUTH_EVENT_PLAIN.createdAt);
				expect(plain.kind).toBe(22242);
				expect(plain.tags).toEqual(AUTH_EVENT_PLAIN.tags);
				expect(plain.sig).toBe(AUTH_EVENT_PLAIN.sig);

				// WITH the valid owner-attestation tag: appended VERBATIM.
				const tagged = buildAuthEvent({
					privateKey: FIXED_NSEC,
					challenge: AUTH_EVENT_PLAIN.challenge,
					relayUrl: AUTH_EVENT_PLAIN.relayUrl,
					authTagJson: AUTH_EVENT_TAGGED.authTagJson,
					createdAt: AUTH_EVENT_PLAIN.createdAt,
					auxHex: FIXED_AUX_HEX,
				});
				expect(tagged.id).toBe(AUTH_EVENT_TAGGED.id);
				expect(tagged.sig).toBe(AUTH_EVENT_TAGGED.sig);
				expect(tagged.tags[2]).toEqual(
					JSON.parse(AUTH_EVENT_TAGGED.authTagJson),
				);

				// Generator anchor: x(G·1) = generator x.
				expect(publicKeyHex("01".padStart(64, "0"))).toBe(GENERATOR_X_HEX);
			},
		),

		mk(
			"transport.buzz.bech32-and-userref-codecs",
			"buzz bech32 npub codecs: hex↔npub round-trips (incl. the fixture identity); rejects wrong lengths/non-hex/foreign prefixes/invalid charset/tampered checksums; normalizeUserRef accepts npub|lowercase-hex, normalizes case, drops everything else",
			async () => {
				const npub = hexToNpub(FIXED_PUBKEY_HEX);
				expect(npub).not.toBeNull();
				expect(npub?.startsWith("npub1")).toBe(true);
				expect(npubToHex(npub as string)).toBe(FIXED_PUBKEY_HEX);
				// Round-trips across arbitrary payloads.
				for (const hex of [FIXED_KEY_HEX, GENERATOR_X_HEX, "ab".repeat(32)]) {
					expect(npubToHex(hexToNpub(hex) as string)).toBe(hex.toLowerCase());
				}
				// Rejection matrix.
				expect(hexToNpub(FIXED_PUBKEY_HEX.slice(0, -2))).toBeNull(); // 62 chars
				expect(hexToNpub(`zz${FIXED_PUBKEY_HEX.slice(2)}`)).toBeNull(); // non-hex
				expect(hexToNpub(`${FIXED_KEY_HEX}00`)).toBeNull(); // 33 bytes
				const lastChar = (npub as string).at(-1) as string;
				const replacement = lastChar === "q" ? "p" : "q";
				const tampered = `${(npub as string).slice(0, -1)}${replacement}`;
				expect(tampered).not.toBe(npub); // checksum break
				expect(npubToHex(tampered)).toBeNull();
				expect(npubToHex("nostr1abcdef")).toBeNull(); // foreign hrp
				expect(npubToHex("npub1biobiobiobio")).toBeNull(); // invalid charset
				expect(npubToHex("npub1qqqq")).toBeNull(); // truncated payload

				// normalizeUserRef matrix.
				expect(normalizeUserRef(`  ${npub}  `)).toBe(FIXED_PUBKEY_HEX);
				expect(normalizeUserRef(FIXED_PUBKEY_HEX.toUpperCase())).toBe(
					FIXED_PUBKEY_HEX,
				);
				expect(normalizeUserRef(ALICE)).toBe(ALICE);
				expect(normalizeUserRef("")).toBeNull();
				expect(normalizeUserRef("not-a-key")).toBeNull();
				expect(normalizeUserRef(`${ALICE}00`)).toBeNull(); // 66 chars
				expect(normalizeUserRef(tampered)).toBeNull();

				// resolveCliPath chain: configured-existing → PATH → ~/bin/buzz → "".
				expect(resolveCliPath("/bin/buzz", { fileExists: () => true })).toBe(
					"/bin/buzz",
				);
				expect(
					resolveCliPath("/missing/buzz", { fileExists: () => false }),
				).toBe("");
				expect(
					resolveCliPath("", {
						which: () => "/usr/bin/buzz",
						fileExists: () => false,
					}),
				).toBe("/usr/bin/buzz");
				expect(
					resolveCliPath("", { fileExists: (p) => p === "~/bin/buzz" }),
				).toBe("~/bin/buzz");
				expect(resolveCliPath("", {})).toBe("");
			},
		),

		mk(
			"transport.buzz.auth-tag-validation-matrix",
			'buzz BUZZ_AUTH_TAG handling: whitespace-only is IGNORED; a valid four-string ["auth",…] tag appends VERBATIM; non-JSON raises exactly "BUZZ_AUTH_TAG is not valid JSON"; wrong shapes (short/long/non-string/wrong-kind/non-array) raise exactly "BUZZ_AUTH_TAG must be a four-string auth tag"',
			async () => {
				const base = {
					privateKey: FIXED_NSEC,
					challenge: "c",
					relayUrl: "wss://r",
					createdAt: 1_700_000_000,
					auxHex: FIXED_AUX_HEX,
				};
				// Whitespace-only ≡ absent (reference strips before checking).
				const blank = buildAuthEvent({ ...base, authTagJson: "   \t " });
				const absent = buildAuthEvent(base);
				expect(blank.id).toBe(absent.id);
				// Valid tag appends verbatim (already vector-checked; shape here).
				const tagged = buildAuthEvent({
					...base,
					authTagJson: '["auth","owner","comm","sig"]',
				});
				expect(tagged.tags.at(-1)).toEqual(["auth", "owner", "comm", "sig"]);
				// Error matrix — exact reference wording.
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: "{not json]" }),
				).toThrow("BUZZ_AUTH_TAG is not valid JSON");
				const FOUR_STRING = "BUZZ_AUTH_TAG must be a four-string auth tag";
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: '["auth","a","b"]' }),
				).toThrow(FOUR_STRING);
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: '["auth","a","b","c","d"]' }),
				).toThrow(FOUR_STRING);
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: '["auth","a","b",5]' }),
				).toThrow(FOUR_STRING);
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: '["attest","a","b","c"]' }),
				).toThrow(FOUR_STRING);
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: '"auth"' }),
				).toThrow(FOUR_STRING);
				expect(() =>
					buildAuthEvent({ ...base, authTagJson: '{"tag":"auth"}' }),
				).toThrow(FOUR_STRING);
			},
		),

		mk(
			"transport.buzz.cli-secret-hygiene",
			"buzz CLI SECRET HYGIENE: across connect/sweeps/sends/reactions/discovery the private key NEVER appears in argv (executor argv capture proves it); the secret travels ENV-ONLY — every invocation carries BUZZ_RELAY_URL + BUZZ_PRIVATE_KEY exactly like _exec_buzz's subprocess environment",
			async (fx) => {
				const { engine, cli } = fx;
				await engine.connect({ isReconnect: false });
				await engine.pollSweep();
				await engine.send(MAIN_CHANNEL, "hygiene probe");
				await fx.scheduler.runToEnd();
				cli.advanceClock(1);
				await engine.pollSweep();
				await fx.scheduler.runToEnd();

				expect(cli.calls.length).toBeGreaterThan(3);
				expect(cli.argvContains(FIXTURE_BUZZ_NSEC)).toBe(false);
				expect(cli.argvContains(FIXED_KEY_HEX)).toBe(false);
				expect(cli.argvContains(FIXED_PUBKEY_HEX.slice(0, 16))).toBe(false); // derived material stays off argv too
				expect(
					cli.allCallsCarryEnv(FIXTURE_BUZZ_RELAY, FIXTURE_BUZZ_NSEC),
				).toBe(true);
				// Every argv element is a plain command word — never a key-shaped blob.
				for (const call of cli.calls) {
					for (const arg of call.args) {
						expect(arg.includes("nsec1")).toBe(false);
						expect(arg.startsWith("3a7f")).toBe(false);
					}
				}
			},
		),

		mk(
			"transport.buzz.cli-error-contract",
			'buzz CLI JSON error contract: {error,message} renders "<category>: <detail> (exit N)"; missing message/object falls back to RAW stderr; empty stderr falls back to the exit-code sentence; the rc-124 timeout shape classifies as "timeout: … timed out after 30s (exit 124)"; parseJsonList is tolerant of junk/non-arrays/non-objects',
			async () => {
				expect(
					cliErrorMessage(
						JSON.stringify({
							error: "not_found",
							message: "no such channel: x",
						}),
						3,
					),
				).toBe("not_found: no such channel: x (exit 3)");
				// Missing error category defaults to "error".
				expect(cliErrorMessage(JSON.stringify({ message: "boom" }), 1)).toBe(
					"error: boom (exit 1)",
				);
				// Object WITHOUT message → raw stderr fallback.
				expect(cliErrorMessage("{}", 1)).toBe("{}");
				// Plain text → raw fallback.
				expect(cliErrorMessage("plain boom\n", 7)).toBe("plain boom");
				// Empty → exit-code sentence.
				expect(cliErrorMessage("", 9)).toBe("buzz CLI failed with exit code 9");
				// Timeout contract (rc 124 from _exec_buzz's kill ladder).
				expect(
					cliErrorMessage(
						JSON.stringify({
							error: "timeout",
							message: "buzz messages timed out after 30s",
						}),
						124,
					),
				).toBe("timeout: buzz messages timed out after 30s (exit 124)");

				// parseJsonList tolerance.
				expect(parseJsonList("junk")).toEqual([]);
				expect(parseJsonList('[1,"two",null]')).toEqual([]);
				expect(parseJsonList('{"a":1}')).toEqual([]);
				expect(parseJsonList("")).toEqual([]);
				expect(parseJsonList('[{"id":"e1"},42,"x",null]')).toEqual([{ id: "e1" }]);
			},
		),

		mk(
			"transport.buzz.connect-refusal-ladder",
			"buzz connect refusal ladder (FATAL, ordered, non-retryable): relay URL missing ⇒ [config_missing] BUZZ_RELAY_URL; CLI binary unresolvable ⇒ [cli_missing]; private key missing ⇒ [config_missing] BUZZ_PRIVATE_KEY; profile-fetch failure maps retryable ⇔ exit code 2; an empty profile payload ⇒ connect_failed 'returned no profile'",
			async () => {
				// Present-but-empty env keeps adapters ENABLED while ladder checks
				// fire (resolveEnablement refuses only UNDEFINED secrets).
				const enabledSecrets = (over: Record<string, string>) => (k: string) =>
					k in over
						? over[k]
						: k === "BUZZ_PRIVATE_KEY"
							? FIXTURE_BUZZ_NSEC
							: k === "BUZZ_RELAY_URL"
								? FIXTURE_BUZZ_RELAY
								: undefined;

				// 1. Relay URL missing (checked FIRST even with everything else gone).
				const noRelay = new BuzzAdapter({
					config: { cli_path: "/bin/buzz" },
					pathProbes: { fileExists: () => true },
					secretReader: enabledSecrets({ BUZZ_RELAY_URL: "" }),
				});
				await expect(noRelay.connect({ isReconnect: false })).resolves.toBe(false);
				expect(noRelay.fatalEvents[0]?.code).toBe("config_missing");
				expect(noRelay.fatalEvents[0]?.retryable).toBe(false);
				expect(noRelay.fatalEvents[0]?.detail).toContain("BUZZ_RELAY_URL");

				// 2. CLI missing.
				const noCli = new BuzzAdapter({
					secretReader: enabledSecrets({}),
				});
				await expect(noCli.connect({ isReconnect: false })).resolves.toBe(false);
				expect(noCli.fatalEvents[0]?.code).toBe("cli_missing");
				expect(noCli.fatalEvents[0]?.retryable).toBe(false);

				// 3. Private key missing (empty env; no credentials seams).
				const noKey = new BuzzAdapter({
					config: { cli_path: "/bin/buzz" },
					pathProbes: { fileExists: () => true },
					secretReader: enabledSecrets({ BUZZ_PRIVATE_KEY: "" }),
				});
				await expect(noKey.connect({ isReconnect: false })).resolves.toBe(false);
				expect(noKey.fatalEvents[0]?.code).toBe("config_missing");
				expect(noKey.fatalEvents[0]?.detail).toContain("BUZZ_PRIVATE_KEY");

				// 4. Profile fetch failure: retryable ⇔ exit code 2.
				const world = makeBuzzWorld({ name: "buzz-refusal-profile" });
				world.cli.scriptError("denied", "key not a member", 2);
				await expect(world.engine.connect({ isReconnect: false })).resolves.toBe(
					false,
				);
				expect(world.engine.fatalEvents[0]?.code).toBe("connect_failed");
				expect(world.engine.fatalEvents[0]?.retryable).toBe(true);
				expect(world.engine.fatalEvents[0]?.detail).toContain(
					"denied: key not a member (exit 2)",
				);

				// 5. Profile payload WITHOUT a pubkey ⇒ connect_failed, retryable.
				const emptyProfile = makeBuzzWorld({ name: "buzz-refusal-empty" });
				emptyProfile.cli.scriptFailure(() => ({
					code: 0,
					stdout: JSON.stringify([{ display_name: "ghost" }]),
					stderr: "",
				}));
				await expect(
					emptyProfile.engine.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(emptyProfile.engine.fatalEvents[0]?.code).toBe("connect_failed");
				expect(emptyProfile.engine.fatalEvents[0]?.retryable).toBe(true);
			},
		),


		mk(
			"transport.buzz.sweep-semantics",
			"buzz sweep semantics: ONLY kind-9 chat events dispatch (membership/housekeeping kinds commit-and-drop — with @mention payloads so ONLY the kind filter can suppress them); polls request --limit 50 with an INCLUSIVE --since watermark; seen-set dedupe keeps single delivery across refetches; the seen cap evicts FIFO at 500 (OrderedDict popitem(last=false) parity)",
			async (fx) => {
				const { engine, cli, world } = fx;
				await engine.connect({ isReconnect: false });
				await engine.pollSweep();
				await fx.scheduler.runToEnd();

				// Kind filter: one event per settled cycle (no burst merging).
				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot membership noise",
					kind: 44100,
				});
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(world.subject.turns()).toEqual([]);

				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot housekeeping noise",
					kind: 42,
				});
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(world.subject.turns()).toEqual([]);

				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot real chat",
				});
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(world.subject.turns()).toContain("real chat");

				// Fetch limit + inclusive watermark ride the messages-get argv.
				const lastGet = cli.calls
					.filter((c) => c.args[0] === "messages" && c.args[1] === "get")
					.at(-1)?.args ?? [];
				expect(lastGet).toContain("--limit");
				expect(lastGet[lastGet.indexOf("--limit") + 1]).toBe("50");
				expect(lastGet).toContain("--since");

				// Watermark skips OLD events: below-lastTs content never arrives.
				const staleTs = cli.nowSeconds - 5;
				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot stale beyond watermark",
					createdAt: staleTs,
				});
				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot fresh after",
				});
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(world.subject.turns()).not.toContain("stale beyond watermark");
				expect(world.engine.inboundEventLog.some((e) => e.text === "fresh after")).toBe(true);

				// Dedupe: the same event processed twice commits + dispatches once.
				const state = engine.channelState.get(MAIN_CHANNEL)!;
				const dup = {
					id: "dup-1",
					kind: 9,
					pubkey: ALICE,
					content: "@PiBot dup",
					created_at: cli.nowSeconds,
					tags: [],
				};
				const beforeDispatches = world.engine.inboundEventLog.length;
				await engine.handleEvent(MAIN_CHANNEL, state, dup);
				await engine.handleEvent(MAIN_CHANNEL, state, dup);
				await fx.scheduler.runToEnd();
				expect(world.engine.inboundEventLog.length - beforeDispatches).toBe(1);

				// Cap eviction: 505 cheap commits ⇒ trim to exactly 500, OLDEST gone
				// (source trims after each pollChannel batch — mirrored here).
				for (let i = 0; i < 505; i += 1) {
					await engine.handleEvent(MAIN_CHANNEL, state, {
						id: `evict-${String(i).padStart(4, "0")}`,
						kind: 0, // commits seen, then drops at the kind filter
						pubkey: ALICE,
						content: "",
						created_at: cli.nowSeconds,
						tags: [],
					});
				}
				engine.trimSeen(state);
				expect(state.seen.size).toBe(500);
				expect(state.seen.has("evict-0000")).toBe(false); // oldest evicted
				expect(state.seen.has("evict-0504")).toBe(true); // newest retained
			},
		),


		mk(
			"transport.buzz.gating-matrix",
			"buzz mention gating: channels REQUIRE a mention by DEFAULT (display-name word-boundary @PiBot/PiBot, hex pubkey, npub all qualify; PiBotX does NOT) and leading mentions strip cleanly; DMs ALWAYS dispatch without mentions; require_mention=false parses from config AND the env false-token set; the allow-list normalizes npub↔hex and DROPS invalid entries; an EMPTY allow-list means the filter is OFF (source truth)",
			async () => {
				const world = makeBuzzWorld({ name: "buzz-gating" });
				const { engine, cli, subject } = world;
				cli.addDm(DM_CHANNEL, { alsoChannel: false }); // dms-list-native
				await world.connectAndAwaitLive();

				// One event per settled cycle — each dispatch is its own turn.
				const cycle = async (content: string): Promise<void> => {
					cli.advanceClock(1);
					cli.pushEvent(MAIN_CHANNEL, { pubkey: ALICE, content });
					await world.sweep();
				};

				await cycle("plain chatter"); // unmentioned ⇒ dropped
				await cycle("@PiBot help me"); // mention ⇒ stripped to clean text
				await cycle("yo PiBot assist"); // bare display-name word qualifies
				await cycle("this PiBotX thing"); // word boundary: NOT a mention
				await cycle(`ping ${FIXED_PUBKEY_HEX} please`); // hex mention
				await cycle(`np ${hexToNpub(FIXED_PUBKEY_HEX)} hi`); // npub mention

				const turns = subject.turns();
				expect(turns).not.toContain("plain chatter");
				expect(turns).toContain("help me"); // leading @PiBot stripped
				expect(turns).toContain("yo PiBot assist"); // mid-prose mention kept
				expect(turns.some((t) => t.includes("PiBotX"))).toBe(false);
				expect(turns.some((t) => t.startsWith("ping "))).toBe(true);
				expect(turns.some((t) => t.startsWith("np "))).toBe(true);

				// DMs ALWAYS dispatch without mentions — demonstrated on a
				// dms-list-native conversation (seeded chat_type=dm at connect).
				cli.advanceClock(1);
				cli.pushEvent(DM_CHANNEL, { pubkey: ALICE, content: "dm plain text" });
				await world.sweep();
				expect(engine.channelState.get(DM_CHANNEL)?.chatType).toBe("dm");
				expect(subject.turns()).toContain("dm plain text");

				// A DM LEAKED through channels list seeds GROUP (source truth) and
				// latches via the #68871 p-tag discriminator on the first
				// mention-free direct message — which then dispatches ungated.
				const LEAKED = "dm-leaked-bb7f22";
				cli.addChannel(LEAKED, "DM");
				await engine.connect({ isReconnect: true }); // pick up new watch-set entry
				expect(engine.channelState.get(LEAKED)?.chatType).toBe("group");
				cli.advanceClock(1);
				cli.pushEvent(LEAKED, {
					pubkey: ALICE,
					content: "psst, no mention here",
					tags: [["p", FIXED_PUBKEY_HEX]],
				});
				await world.sweep();
				expect(engine.dmLatches).toContain(LEAKED);
				expect(engine.channelState.get(LEAKED)?.chatType).toBe("dm");
				expect(subject.turns()).toContain("psst, no mention here");

				// require_mention=false parses from config AND the env false-token set.
				const openAdapter = new BuzzAdapter({
					config: { cli_path: "/bin/buzz", require_mention: false },
					pathProbes: { fileExists: () => true },
					secretReader: (k) =>
						k === "BUZZ_PRIVATE_KEY"
							? FIXTURE_BUZZ_NSEC
							: k === "BUZZ_RELAY_URL"
								? FIXTURE_BUZZ_RELAY
								: undefined,
				});
				expect(openAdapter.requireMention).toBe(false);
				const offViaEnv = new BuzzAdapter({
					config: { cli_path: "/bin/buzz" },
					pathProbes: { fileExists: () => true },
					secretReader: (k) =>
						k === "BUZZ_REQUIRE_MENTION"
							? "Off"
							: k === "BUZZ_PRIVATE_KEY"
								? FIXTURE_BUZZ_NSEC
								: k === "BUZZ_RELAY_URL"
									? FIXTURE_BUZZ_RELAY
									: undefined,
				});
				expect(offViaEnv.requireMention).toBe(false);
				void engine;

				// Allow-list normalization + invalid-entry dropping + filter-off.
				const listed = new BuzzAdapter({
					config: {
						cli_path: "/bin/buzz",
						allowed_users: [
							hexToNpub(ALICE) ?? "",
							BOB.toUpperCase(),
							"garbage-entry",
							"",
						],
					},
					pathProbes: { fileExists: () => true },
					secretReader: (k) =>
						k === "BUZZ_PRIVATE_KEY"
							? FIXTURE_BUZZ_NSEC
							: k === "BUZZ_RELAY_URL"
								? FIXTURE_BUZZ_RELAY
								: undefined,
				});
				expect(listed.allowedPubkeys.size).toBe(2);
				expect(listed.allowedPubkeys.has(ALICE)).toBe(true); // npub → hex
				expect(listed.allowedPubkeys.has(BOB)).toBe(true); // case-folded hex
				const emptyFilter = new BuzzAdapter({
					config: { cli_path: "/bin/buzz", allowed_users: [] },
					pathProbes: { fileExists: () => true },
					secretReader: (k) =>
						k === "BUZZ_PRIVATE_KEY"
							? FIXTURE_BUZZ_NSEC
							: k === "BUZZ_RELAY_URL"
								? FIXTURE_BUZZ_RELAY
								: undefined,
				});
				expect(emptyFilter.allowedPubkeys.size).toBe(0); // FILTER OFF entirely
			},
		),


		mk(
			"transport.buzz.transport-mode-resolution",
			"buzz transport-mode resolution: auto/websocket/poll accepted (case-folded), junk resolves to AUTO; websocket-required with an unavailable handshake fails connect FATAL ws_auth_failed (retryable) and disconnects; auto FALLS BACK TO POLL; a successful WS start flips lastTransportUsed without the poll loop; transport=poll NEVER attempts the WS",
			async () => {
				const baseSecrets = (k: string) =>
					k === "BUZZ_PRIVATE_KEY"
						? FIXTURE_BUZZ_NSEC
						: k === "BUZZ_RELAY_URL"
							? FIXTURE_BUZZ_RELAY
							: undefined;

				// Junk ⇒ auto.
				const junk = new BuzzAdapter({
					config: { cli_path: "/bin/buzz", transport: "carrier-pigeon" },
					pathProbes: { fileExists: () => true },
					secretReader: baseSecrets,
				});
				expect(junk.transportMode).toBe("auto");
				// Case-folded valid mode.
				const upper = new BuzzAdapter({
					config: { cli_path: "/bin/buzz", transport: "POLL" },
					pathProbes: { fileExists: () => true },
					secretReader: baseSecrets,
				});
				expect(upper.transportMode).toBe("poll");

				// websocket-required + unavailable ⇒ FATAL retryable ws_auth_failed.
				const wsCli = new FakeBuzzCli({ relayUrl: FIXTURE_BUZZ_RELAY });
				wsCli.addChannel(MAIN_CHANNEL, "General");
				const wsRequired = new BuzzAdapter({
					config: { cli_path: "/bin/buzz", transport: "websocket" },
					pathProbes: { fileExists: () => true },
					secretReader: baseSecrets,
					executor: wsCli.executor(),
					wsStarter: async () => false,
				});
				await expect(
					wsRequired.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(wsRequired.fatalEvents[0]?.code).toBe("ws_auth_failed");
				expect(wsRequired.fatalEvents[0]?.retryable).toBe(true);
				expect(wsRequired.connectedOnce).toBe(false);

				// auto falls back to poll when the WS cannot start.
				const autoWorld = makeBuzzWorld({ name: "buzz-mode-auto" });
				autoWorld.engine.wsStarter = async () => false;
				await expect(
					autoWorld.engine.connect({ isReconnect: false }),
				).resolves.toBe(true);
				expect(autoWorld.engine.lastTransportUsed).toBe("poll");
				expect(autoWorld.engine.pollLoopActive).toBe(true);

				// A successful WS start owns inbound; the poll loop stays OFF.
				const wsWorld = makeBuzzWorld({ name: "buzz-mode-ws" });
				wsWorld.engine.wsStarter = async () => true;
				await expect(
					wsWorld.engine.connect({ isReconnect: false }),
				).resolves.toBe(true);
				expect(wsWorld.engine.lastTransportUsed).toBe("websocket");
				expect(wsWorld.engine.pollLoopActive).toBe(false);

				// poll NEVER attempts the WS (starter would explode if called).
				const pollOnly = new BuzzAdapter({
					config: { cli_path: "/bin/buzz", transport: "poll" },
					pathProbes: { fileExists: () => true },
					secretReader: baseSecrets,
				});
				pollOnly.wsStarter = async () => {
					throw new Error("WS must not be attempted with transport=poll");
				};
				// No executor wired ⇒ the identity step fails AFTER no WS attempt.
				await expect(pollOnly.connect({ isReconnect: false })).resolves.toBe(false);
				expect(pollOnly.lastTransportUsed).toBeNull();
			},
		),


		mk(
			"transport.buzz.send-command-shapes",
			"buzz sends (REAL CLI plane): channels AND DMs share ONE command shape ['messages','send','--channel',<target>,'--content','-'] with content riding stdin (DM target = conversation ref — source truth); reply targets append --reply-to; failures surface '<cat>: <msg> (exit N)' with retryable ⇔ exit 2; accepted:false ⇒ failed SendResult; the sent event id lands in the seen-set (belt-and-braces echo suppression)",
			async (fx) => {
				const { engine, cli } = fx;
				// REAL CLI send semantics: drop the subject's harness egress binding.
				engine.wireTransmitSend = undefined;
				cli.addDm(DM_CHANNEL);
				await engine.connect({ isReconnect: false });

				// Channel send.
				const ok = await engine.send(MAIN_CHANNEL, "hello channel");
				expect(ok.success).toBe(true);
				expect(ok.messageId).toMatch(/^evt\d+$/);
				const sendCalls = cli.calls.filter(
					(c) => c.args[0] === "messages" && c.args[1] === "send",
				);
				expect(sendCalls.length).toBeGreaterThanOrEqual(1);
				const channelArgs = sendCalls[0]?.args ?? [];
				expect(channelArgs).toEqual([
					"messages",
					"send",
					"--channel",
					MAIN_CHANNEL,
					"--content",
					"-",
				]);
				expect(sendCalls[0]?.input).toBe("hello channel");


				// DM send: SAME shape, different target.
				const dmOk = await engine.send(DM_CHANNEL, "psst dm");
								expect(dmOk.success).toBe(true);
				const dmCall = cli.calls
					.filter((c) => c.args[0] === "messages" && c.args[1] === "send")
					.find((c) => c.args.includes(DM_CHANNEL));
								expect(dmCall?.input).toBe("psst dm");
				expect(
					dmCall?.args.filter((a) => a !== MAIN_CHANNEL && a !== DM_CHANNEL),
				).toEqual(["messages", "send", "--channel", "--content", "-"]);

				// Reply targets ride METADATA through the kit door (thread_id is the
				// reference key; reply_to_message_id the kit-standard one).
				const replyMeta = await engine.send(MAIN_CHANNEL, "meta reply", undefined, {
					thread_id: "t1",
				});
				expect(replyMeta.success).toBe(true);
				const replyKit = await engine.send(MAIN_CHANNEL, "kit reply", undefined, {
					reply_to_message_id: "r9",
				});
				expect(replyKit.success).toBe(true);
				const replyArgs = cli.calls
					.filter((c) => c.args.includes("--reply-to"))
					.map((c) => c.args);
				expect(
					replyArgs.some((a) => a[a.indexOf("--reply-to") + 1] === "t1"),
				).toBe(true);
				expect(
					replyArgs.some((a) => a[a.indexOf("--reply-to") + 1] === "r9"),
				).toBe(true);

				// Failure surfaces the parsed CLI error; retryable ⇔ rc 2.
				cli.scriptError("denied", "not a member", 2);
				const denied = await engine.send(MAIN_CHANNEL, "will fail");
				expect(denied.success).toBe(false);
				expect(denied.error).toBe("denied: not a member (exit 2)");
				expect(denied.retryable).toBe(true);
				cli.scriptError("busy", "relay busy", 1);
				const busy = await engine.send(MAIN_CHANNEL, "will fail once more");
				expect(busy.retryable).toBe(false);

				// Empty content refuses locally.
				const empty = await engine.send(MAIN_CHANNEL, "");
				expect(empty.success).toBe(false);
				expect(empty.error).toBe("Empty message");

				// Echo suppression: the sent id sits in the channel seen-set.
				const state = engine.channelState.get(MAIN_CHANNEL)!;
				expect(state.seen.has(ok.messageId as string)).toBe(true);
			},
		),

		mk(
			"transport.buzz.dm-discovery-cadence",
			"buzz DM discovery runs every 5th sweep (dms list + channels-list fallback); a DM opened MID-RUN enters with a FRESH watermark (lastTs 0) so its history dispatches from the beginning exactly once; startup DMs are seeded like channels and replay NOTHING",
			async (fx) => {
				const { engine, cli, world } = fx;
				await engine.connect({ isReconnect: false }); // seed-time discovery: no DM registered
				const dmsListsBefore = cli.calls.filter(
					(c) => c.args[0] === "dms",
				).length;

				// Sweeps 1–4: NO dms list.
				for (let i = 0; i < 4; i += 1) {
					await engine.pollSweep();
					await fx.scheduler.runToEnd();
				}
				expect(cli.calls.filter((c) => c.args[0] === "dms").length).toBe(
					dmsListsBefore,
				);

				// Sweep 5: discovery fires (still nothing to find).
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(cli.calls.filter((c) => c.args[0] === "dms").length).toBe(
					dmsListsBefore + 1,
				);

				// Mid-run DM opens; a message waits inside.
				cli.addDm(DM_CHANNEL);
				cli.advanceClock(2);
				cli.pushEvent(DM_CHANNEL, { pubkey: ALICE, content: "first-ever dm" });

				// Sweeps 6–9: quiet; sweep 10: discovery picks the DM up fresh.
				for (let i = 0; i < 4; i += 1) {
					await engine.pollSweep();
					await fx.scheduler.runToEnd();
				}
				await engine.pollSweep(); // sweep 10 — % 5 === 0
				await fx.scheduler.runToEnd();
				expect(engine.channelState.has(DM_CHANNEL)).toBe(true);
				expect(engine.channelState.get(DM_CHANNEL)?.chatType).toBe("dm");

				// NEXT sweep fetches it (lastTs 0 ⇒ no --since) and dispatches ONCE.
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(
					world.subject.turns().filter((t) => t === "first-ever dm").length,
				).toBe(1);

				// Total dms-list invocations: exactly two discoveries (sweeps 5 & 10).
				expect(cli.calls.filter((c) => c.args[0] === "dms").length).toBe(
					dmsListsBefore + 2,
				);

				// Classification units: real metadata rules OUT a DM; bare "DM" +
				// empty description (and unconfigured absence) trust the latch.
				expect(engine.mayReclassifyAsDm(MAIN_CHANNEL)).toBe(false); // named room
				expect(engine.mayReclassifyAsDm("unknown-id")).toBe(true); // unconfigured
			},
		),

		mk(
			"transport.buzz.self-echo-suppression",
			"buzz self-echo suppression: events authored by OUR OWN pubkey (derived from the identity key) never dispatch even when mention-shaped — the poll loop skips them AND sends mark their event id seen; the 👀 reaction best-effort fires after each human dispatch",
			async (fx) => {
				const { engine, cli, world } = fx;
				await engine.connect({ isReconnect: false });
				await engine.pollSweep();
				await fx.scheduler.runToEnd();

				// Own-authored message never dispatches…
				cli.advanceClock(1);
				const own = cli.pushEvent(MAIN_CHANNEL, {
					pubkey: FIXED_PUBKEY_HEX,
					content: "@PiBot talking to myself",
				});
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(world.subject.turns()).toEqual([]);
				expect(
					engine.inboundEventLog.every(
						(e) => e.source?.userId !== FIXED_PUBKEY_HEX,
					),
				).toBe(true);
				expect(engine.channelState.get(MAIN_CHANNEL)?.seen.has(own.id)).toBe(
					true,
				);

				// …while a human mention-shaped message flows and gets 👀.
				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot human words",
				});
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				expect(world.subject.turns()).toContain("human words");
				const reactions = cli.calls.filter(
					(c) => c.args[0] === "reactions" && c.args[1] === "add",
				);
				expect(reactions.length).toBeGreaterThanOrEqual(1);
				expect(reactions.at(-1)?.args).toContain("👀");
			},
		),


		mk(
			"transport.buzz.ack-window-commit-first",
			"buzz ACK WINDOW (complement leg, SOURCE-PINNED): a crash BETWEEN the seen-commit and dispatch leaves the event CONSUMED-but-undelivered — later sweeps NEVER redispatch it (at-most-once downstream), while the uncommitted remainder of the batch flows normally on recovery",
			async (fx) => {
				const { engine, cli, world } = fx;
				await engine.connect({ isReconnect: false });
				await engine.pollSweep();
				await fx.scheduler.runToEnd();

				let crashedOnce = false;
				engine.hooks = {
					beforeDispatch: (event) => {
						if (!crashedOnce && event.text === "c1") {
							crashedOnce = true;
							throw new Error("simulated crash between commit and dispatch");
						}
					},
				};
				cli.advanceClock(1);
				const c1 = cli.pushEvent(MAIN_CHANNEL, {
					pubkey: ALICE,
					content: "@PiBot c1",
				});
				cli.advanceClock(1);
				cli.pushEvent(MAIN_CHANNEL, { pubkey: ALICE, content: "@PiBot c2" });
				await engine.pollSweep(); // c1 committed-then-crashed; c2 untouched
				await fx.scheduler.runToEnd();
				expect(engine.sweepErrors).toBe(1);
				// c1 was COMMITTED (seen-set) despite never dispatching.
				const state = engine.channelState.get(MAIN_CHANNEL)!;
				expect(state.seen.has(c1.id)).toBe(true);

				engine.hooks = undefined; // recovery
				await engine.pollSweep();
				await fx.scheduler.runToEnd();
				const turns = world.subject.turns();
				expect(turns).not.toContain("c1"); // NEVER redispatched — at-most-once
				expect(turns.filter((t) => t === "c2").length).toBe(1); // uncommitted tail flows once
			},
		),
	];
}

// ── the suites ──────────────────────────────────────────────────────────────

describe("buzz conformance (04 §8 merge gate)", () => {
	it("SHARED applicable rows pass for shape=polling (streaming family excluded BY THE PROBE)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { draftStreamingSupported, excludedIds } = computeApplicability();
		const shared = draftStreamingSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));
		// The probe must genuinely exclude: the CLI plane has no native
		// draft lanes even though supportsAsyncDelivery is TRUE.
		expect(draftStreamingSupported).toBe(false);

		const report = await runConformanceSuite({
			subjectName: "buzz",
			shape: "polling",
			rows: shared,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		// Every non-excluded shared row actually RAN (no silent skip).
		expect(report.rows.length).toBe(all.length - excludedIds.length);
	}, 60_000);

	it("INHERITED polling transport rows pass over the REAL buzz fixtures (vendor-class mappings carried in titles)", async () => {
		const required = TRANSPORT_ROW_REQUIREMENTS.polling;
		const inherited = makePollingRows(makeRealBuzzFixture()).map((r) => ({
			...r,
			title: BUZZ_POLLING_ROW_TITLES[r.id] ?? r.title,
		}));
		expect(inherited.map((r) => r.id).sort()).toEqual([...required].sort());

		const report = await runConformanceSuite({
			subjectName: "buzz-transport",
			shape: "polling",
			rows: inherited,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 60_000);

	it("buzz SHAPE DELTA rows pass through REAL engines (crypto contract vectors, codecs, CLI contracts, sweep + gating semantics, ack windows)", async () => {
		const rows = buzzDeltaRows(freshDeltaEngine);
		const report = await runConformanceSuite({
			subjectName: "buzz-deltas",
			shape: "polling",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 120_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { draftStreamingSupported, excludedIds } = computeApplicability();
		const shared = draftStreamingSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));

		const transport = makePollingRows(makeRealBuzzFixture()).map((r) => ({
			...r,
			title: BUZZ_POLLING_ROW_TITLES[r.id] ?? r.title,
		}));
		const deltas = buzzDeltaRows(freshDeltaEngine);

		const report = await runConformanceSuite({
			subjectName: "buzz-full",
			shape: "polling",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set([
				"transport.polling.outage-reconnect-preserves-queue",
				"transport.polling.held-inbound-redispatch",
				"transport.polling.conflict-zombie-eviction",
				"transport.polling.heartbeat-escalation",
			]),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 120_000);

	it("the gate DETECTS violations: a kind-filter-defeating MUTANT (defineProperty) fails ITS OWN named row alone", async () => {
		function mutantEngine(): DeltaEngine {
			const fx = freshDeltaEngine();
			const original = fx.engine.handleEvent.bind(fx.engine);
			Object.defineProperty(fx.engine, "handleEvent", {
				value: async (
					channelId: string,
					state: Parameters<typeof original>[1],
					event: Record<string, unknown>,
				) => {
					// THE LIE: every event masquerades as kind 9 (filter defeated).
					return original(channelId, state, { ...event, kind: 9 });
				},
			});
			return fx;
		}

		const rows = buzzDeltaRows(mutantEngine);
		const target = rows.find((r) => r.id === "transport.buzz.sweep-semantics");
		expect(target).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-buzz-kind-filter",
			shape: "polling",
			rows: [target as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER delta rows still pass on their own fresh engines.
		const others = rows.filter((r) => r.id !== target?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-buzz-others",
			shape: "polling",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 120_000);
});

// Local helpers used by delta rows that need a bare world handle.

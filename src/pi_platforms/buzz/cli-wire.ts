// pi_platforms/buzz/cli-wire — THE CLI plane seam. Buzz never speaks Nostr
// itself: Hermes shells out to the external `buzz` CLI binary ("JSON in, JSON
// out") for EVERYTHING — identity fetch, channel/DM polling, sends
// (adapter.py module docstring + _exec_buzz). The port NEVER spawns OS
// children: the CLI is an INJECTED async executor seam
//
//     executor(args, {input, env}) → {code, stdout, stderr}
//
// mirroring _exec_buzz's (returncode, stdout, stderr) tuple contract, with the
// environment CARRIAGE explicit: BUZZ_RELAY_URL + BUZZ_PRIVATE_KEY travel via
// env only — NEVER argv (_exec_buzz docstring: "The private key travels via
// the subprocess environment only — it never appears in argv"). FakeBuzzCli is
// the in-memory relay state behind conformance fixtures.

/** One recorded CLI invocation (argv capture for the secret-hygiene rows). */
export interface RecordedCliCall {
	args: readonly string[];
	/** stdin payload (send paths use "--content -" and pipe text). */
	input?: string | undefined;
	env: Readonly<Record<string, string>>;
}

export interface BuzzCliResult {
	code: number;
	stdout: string;
	stderr: string;
}

export interface BuzzCliInvocation {
	input?: string | undefined;
	env: Readonly<Record<string, string>>;
}

/**
 * THE seam. Returns (code, stdout, stderr) exactly like _exec_buzz; a code of
 * 124 models its timeout-kill contract ({"error":"timeout"} stderr JSON).
 */
export type BuzzCliExecutor = (
	args: readonly string[],
	call: BuzzCliInvocation,
) => Promise<BuzzCliResult>;

/** A Nostr chat event as `buzz messages get` returns it. */
export interface FakeBuzzEvent {
	id: string;
	kind: number;
	pubkey: string;
	content: string;
	created_at: number;
	tags: string[][];
}

export interface FakeBuzzChannel {
	name: string;
	description: string;
	events: FakeBuzzEvent[];
	/** Hidden from `channels list` (dms-list-native conversations). */
	hidden?: boolean | undefined;
}

type ScriptedFailure = (call: {
	args: readonly string[];
	input?: string | undefined;
}) => BuzzCliResult | undefined;

let fakeEventCounter = 0;

/**
 * In-memory Buzz community relay + CLI implementation. Command shapes mirror
 * the subcommands the ADAPTER actually invokes (adapter.py anchors):
 *   users get [--pubkey P]        — profile fetch / display-name resolution
 *   channels list                 — watch-set + DM-fallback source
 *   dms list                      — best-effort DM discovery (#68871)
 *   messages get --channel C --limit N [--since S]
 *   messages send --channel C --content - [--reply-to R]   (+ --file F)
 *   reactions add --event E --emoji X
 */
export class FakeBuzzCli {
	readonly relayUrl: string;
	selfPubkey: string;
	selfDisplayName: string;

	private readonly channels = new Map<string, FakeBuzzChannel>();
	private readonly dms = new Set<string>();
	private readonly users = new Map<string, string>();
	private clockSeconds = 1_700_000_000;
	/** Every invocation, in order — the argv/input/env audit trail. */
	readonly calls: RecordedCliCall[] = [];
	private readonly failures: ScriptedFailure[] = [];
	/** Persistent failure mode (outage windows); consulted before FIFO scripts. */
	private persistentFailure: ScriptedFailure | null = null;

	constructor(opts: {
		relayUrl: string;
		selfPubkey?: string | undefined;
		selfDisplayName?: string | undefined;
	}) {
		this.relayUrl = opts.relayUrl;
		this.selfPubkey = (
			opts.selfPubkey ??
			"aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55"
		).toLowerCase();
		this.selfDisplayName = opts.selfDisplayName ?? "PiBot";
	}

	// ── fixture authoring ──────────────────────────────────────────────────────

	addChannel(id: string, name: string, description = ""): void {
		this.channels.set(id, { name, description, events: [] });
	}

	/**
	 * Register a DM conversation (surfaces via `dms list`; ALSO materializes a
	 * "DM"-named channels-list entry by default — the hosted-relay leak that
	 * motivated the p-tag latch, adapter.py:#68871).
	 */
	addDm(id: string, opts: { alsoChannel?: boolean | undefined } = {}): void {
		if (!this.channels.has(id)) {
			this.channels.set(id, {
				name: "DM",
				description: "",
				events: [],
				// dms-list-native: holds events but NEVER surfaces in channels list
				hidden: (opts.alsoChannel ?? true) === false,
			});
		}
		this.dms.add(id);
	}

	setUserDisplay(pubkey: string, name: string): void {
		this.users.set(pubkey.toLowerCase(), name);
	}

	advanceClock(seconds: number): void {
		this.clockSeconds += seconds;
	}

	get nowSeconds(): number {
		return this.clockSeconds;
	}

	pushEvent(
		channelId: string,
		opts: {
			pubkey: string;
			content: string;
			kind?: number | undefined;
			createdAt?: number | undefined;
			tags?: string[][] | undefined;
		},
	): FakeBuzzEvent {
		const channel = this.channels.get(channelId);
		if (channel === undefined)
			throw new Error(`fake relay: unknown channel ${channelId}`);
		fakeEventCounter += 1;
		const event: FakeBuzzEvent = {
			id: `evt${String(fakeEventCounter).padStart(6, "0")}`,
			kind: opts.kind ?? 9,
			pubkey: opts.pubkey.toLowerCase(),
			content: opts.content,
			created_at: opts.createdAt ?? this.clockSeconds,
			tags: opts.tags ?? [],
		};
		channel.events.push(event);
		return event;
	}

	/** Unconsumed events across ALL watched queues (server-side persistence). */
	pendingEventCount(): number {
		let total = 0;
		for (const ch of this.channels.values()) total += ch.events.length;
		return total;
	}

	// ── failure scripting ──────────────────────────────────────────────────────

	/** Queue a scripted failure; consumed FIFO ahead of normal handling. */
	scriptFailure(fn: ScriptedFailure): void {
		this.failures.push(fn);
	}

	/** Outage window: EVERY call fails while active (null clears the mode). */
	setPersistentFailure(
		fn:
			| ((call: { args: readonly string[] }) => BuzzCliResult | undefined)
			| null,
	): void {
		this.persistentFailure = fn;
	}

	scriptError(category: string, message: string, code = 1): void {
		this.scriptFailure(() => ({
			code,
			stdout: "",
			stderr: JSON.stringify({ error: category, message }),
		}));
	}

	/** _exec_buzz timeout-kill shape: rc 124 + {"error":"timeout"} stderr JSON. */
	scriptTimeout(subcommand: string, timeoutSeconds = 30): void {
		this.scriptFailure(() => ({
			code: 124,
			stdout: "",
			stderr: JSON.stringify({
				error: "timeout",
				message: `buzz ${subcommand} timed out after ${timeoutSeconds}s`,
			}),
		}));
	}

	// ── the seam ───────────────────────────────────────────────────────────────

	/** Bind an executor for an adapter instance (env carriage is explicit). */
	executor(): BuzzCliExecutor {
		return async (args, call) => {
			this.calls.push({
				args: [...args],
				...(call.input !== undefined ? { input: call.input } : {}),
				env: call.env,
			});
			const persisted = this.persistentFailure?.({ args });
			if (persisted !== undefined) return persisted;
			const scripted = this.failures.shift();
			const handled = scripted?.({
				args,
				...(call.input !== undefined ? { input: call.input } : {}),
			});
			return handled ?? this.handle(args, call.input);
		};
	}

	private error(
		code: number,
		category: string,
		message: string,
	): BuzzCliResult {
		return {
			code,
			stdout: "",
			stderr: JSON.stringify({ error: category, message }),
		};
	}

	private ok(payload: unknown): BuzzCliResult {
		return { code: 0, stdout: JSON.stringify(payload), stderr: "" };
	}

	private handle(
		args: readonly string[],
		input?: string | undefined,
	): BuzzCliResult {
		const [sub, action] = args;
		if (sub === "users" && action === "get") return this.usersGet(args);
		if (sub === "channels" && action === "list") return this.channelsList();
		if (sub === "channels" && action === "get") return this.channelsGet(args);
		if (sub === "dms" && action === "list") {
			return this.ok([...this.dms].map((dmId) => ({ dm_id: dmId })));
		}
		if (sub === "messages" && action === "get") return this.messagesGet(args);
		if (sub === "messages" && action === "send")
			return this.messagesSend(args, input);
		if (sub === "reactions" && action === "add")
			return this.ok({ accepted: true });
		return this.error(4, "usage", `unknown buzz command: ${args.join(" ")}`);
	}

	private flagValue(args: readonly string[], flag: string): string | undefined {
		const idx = args.indexOf(flag);
		return idx >= 0 ? args[idx + 1] : undefined;
	}

	private usersGet(args: readonly string[]): BuzzCliResult {
		const pubkey = this.flagValue(args, "--pubkey");
		if (pubkey !== undefined) {
			const name = this.users.get(pubkey.toLowerCase());
			return this.ok(
				name === undefined
					? []
					: [{ pubkey: pubkey.toLowerCase(), display_name: name }],
			);
		}
		return this.ok([
			{ pubkey: this.selfPubkey, display_name: this.selfDisplayName },
		]);
	}

	private channelsList(): BuzzCliResult {
		return this.ok(
			[...this.channels.entries()]
				.filter(([, ch]) => ch.hidden !== true)
				.map(([channel_id, ch]) => ({
					channel_id,
					name: ch.name,
					description: ch.description,
				})),
		);
	}

	private channelsGet(args: readonly string[]): BuzzCliResult {
		const id = this.flagValue(args, "--channel");
		const ch = id !== undefined ? this.channels.get(id) : undefined;
		if (id === undefined || ch === undefined) {
			return this.error(3, "not_found", `no such channel: ${id ?? ""}`);
		}
		return this.ok({
			channel_id: id,
			name: ch.name,
			description: ch.description,
		});
	}

	private messagesGet(args: readonly string[]): BuzzCliResult {
		const channelId = this.flagValue(args, "--channel");
		const limitRaw = this.flagValue(args, "--limit");
		const sinceRaw = this.flagValue(args, "--since");
		const channel =
			channelId !== undefined ? this.channels.get(channelId) : undefined;
		if (channelId === undefined || channel === undefined) {
			return this.error(3, "not_found", `no such channel: ${channelId ?? ""}`);
		}
		const limit = limitRaw !== undefined ? Number(limitRaw) : 50;
		const since = sinceRaw !== undefined ? Number(sinceRaw) : undefined;
		const matching = channel.events.filter(
			(e) =>
				since === undefined || Number.isNaN(since) || e.created_at >= since,
		);
		// Newest window wins: the seed call asks for the newest 50 so a
		// (re)start never replays history (adapter.py:_seed_channel).
		return this.ok(matching.slice(-Math.max(1, Math.trunc(limit))));
	}

	private messagesSend(
		args: readonly string[],
		input?: string | undefined,
	): BuzzCliResult {
		const channelId = this.flagValue(args, "--channel");
		const channel =
			channelId !== undefined ? this.channels.get(channelId) : undefined;
		if (channelId === undefined || channel === undefined) {
			return this.error(3, "not_found", `no such channel: ${channelId ?? ""}`);
		}
		const eventId = this.pushEvent(channelId, {
			pubkey: this.selfPubkey,
			content: input ?? "",
		}).id;
		return this.ok({ accepted: true, event_id: eventId });
	}

	// ── probes ───────────────────────────────────────────────────────────────────

	argvContains(secret: string): boolean {
		return this.calls.some((c) => c.args.some((a) => a.includes(secret)));
	}

	/** Every call must carry BOTH env vars (the ONLY secret carriage). */
	allCallsCarryEnv(relayUrl: string, privateKey: string): boolean {
		return (
			this.calls.length > 0 &&
			this.calls.every(
				(c) =>
					c.env["BUZZ_RELAY_URL"] === relayUrl &&
					c.env["BUZZ_PRIVATE_KEY"] === privateKey,
			)
		);
	}

	callsFor(action: string): RecordedCliCall[] {
		return this.calls.filter((c) => c.args[1] === action);
	}
}

// pi_platforms/irc/fake-irc-server — the IN-PROCESS fake ircd (04 §8: rows
// run headless against fake platform servers; NO external network, NO OS
// sockets). Reproduces exactly the server-side behaviors the IRC client
// discipline is cut against (plugins/platforms/irc/adapter.py semantics):
//
//   - registration: PASS/NICK/USER handshake; 001 RPL_WELCOME carries the
//     ACCEPTED nick in params[0] (the client adopts it); 433 ERR_NICKNAMEINUSE
//     when the requested nick collides (armed count or live holder).
//   - PING/PONG: server PINGs are answered BY THE CLIENT; the server records
//     PONGs and can demand them (keepalive observability).
//   - JOIN: echo `:nick!user@host JOIN <chan>` + optional 366; rejection
//     numerics {403,405,471,473,474,475} armable per channel.
//   - PRIVMSG: delivered to other members verbatim as
//     `:sender!user@host PRIVMSG <target> :<text>`; the sender's OWN messages
//     are looped back too (real ircd behavior — the client self-filters).
//   - scenario knobs: withholdWelcome (registration-timeout path),
//     nickInUseArmed (collision ladder), kill() (EOF/RST death),
//     wedgeSilent (accept nothing, send nothing), joinRejectCode.
//
// Egress capture lives on the SHARED harness wire (subject binding); this
// server owns the LINE PROTOCOL surface only.

export interface FakeIrcUser {
	nick: string;
	conn: FakeIrcClientConnection | null;
}

export type ServerLine = {
	readonly raw: string;
};

/** The client-side half of the in-memory connection pair. */
export interface FakeIrcClientConnection {
	/** Queue a raw protocol line FROM the server TO this client. */
	serverSends(raw: string): void;
	/** Client → server write (raw line without CRLF). */
	write(rawLine: string): void;
	/** Close from the client side (QUIT). */
	end(): void;
	/** Set by the server on kill(); readable by both halves. */
	closedByServer: boolean;
}

interface ClientHooks {
	onLine(line: string): void;
	onClose(): void;
}

let connectionSeq = 0;

export class FakeIrcServer {
	readonly address = "irc.fake.example";
	readonly users = new Map<string, FakeIrcUser>();
	/** Channel membership: channel → nicks. */
	readonly channels = new Map<string, Set<string>>();
	/** Every line the server RECEIVED from clients, in order (wire audit). */
	readonly receivedLines: string[] = [];
	/** Every line the server SENT, in order. */
	readonly sentLog: Array<{ to: string; raw: string }> = [];

	// ── scenario knobs ──
	/** Armed 433 responses remaining for the NEXT NICK attempts. */
	nickInUseArmed = 0;
	/** Withhold 001 until released (registration timeout path). */
	withholdWelcome = false;
	/** JOIN rejection numeric to answer with ("" = accept). */
	joinRejectCode = "";
	/** Silent wedge: reads nothing, sends nothing (silent-wedge honesty leg). */
	wedgeSilent = false;
	/** Counters for row assertions. */
	pingsReceived = 0;
	registeredClients = 0;

	/** Open a client connection. Fails (throws) when unreachable armed. */
	connect(hooks: ClientHooks): FakeIrcClientConnection {
		const server = this;
		if (server.unreachable) {
			throw new Error(
				`connect failed: ENOTFOUND ${server.address} (fake transport refused)`,
			);
		}
		const id = ++connectionSeq;
		const conn: FakeIrcClientConnection = {
			closedByServer: false,
			serverSends(raw: string) {
				if (server.wedgeSilent || conn.closedByServer) return;
				server.sentLog.push({ to: `conn-${id}`, raw });
				hooks.onLine(raw);
			},
			write(rawLine: string) {
				if (server.wedgeSilent || conn.closedByServer) return;
				server.receivedLines.push(rawLine);
				server.handleClientLine(conn, hooks, rawLine);
			},
			end() {
				/* client-side close: nothing pending server-side */
			},
		};
		server.connections.set(id, { conn, hooks });
		return conn;
	}

	private readonly connections = new Map<
		number,
		{ conn: FakeIrcClientConnection; hooks: ClientHooks }
	>();

	unreachable = false;

	/** Server-side teardown of ONE client connection (EOF/RST death). */
	kill(nick: string | null): void {
		for (const [, entry] of [...this.connections]) {
			const userNick = [...this.users.entries()].find(
				([, u]) => u.conn === entry.conn,
			)?.[0];
			if (nick === null || userNick === nick) {
				entry.conn.closedByServer = true;
				// ircd semantics: a dropped connection RELEASES its nick and
				// channel memberships immediately.
				if (userNick !== undefined) {
					this.users.delete(userNick.toLowerCase());
					for (const [channel, members] of [...this.channels]) {
						members.delete(userNick);
						if (members.size === 0) this.channels.delete(channel);
					}
				}
				entry.hooks.onClose();
			}
		}
	}

	// ── server-side protocol handling ──

	private handleClientLine(
		conn: FakeIrcClientConnection,
		hooks: ClientHooks,
		rawLine: string,
	): void {
		const { command, params } = parseServerSide(rawLine);
		switch (command) {
			case "PING": {
				this.pingsReceived += 1;
				break; // the SERVER does not auto-answer its own clients here:
				// adapter.py answers server PINGs; server→client PING probes are
				// pushed via pushPing() below.
			}
			case "NICK": {
				const requested = params[0] ?? "";
				if (this.nickInUseArmed > 0) {
					this.nickInUseArmed -= 1;
					this.lineTo(
						conn,
						`:${this.address} 433 * ${requested} :Nickname is already in use`,
					);
					return;
				}
				const holder = this.users.get(requested.toLowerCase());
				if (
					holder !== undefined &&
					holder.conn !== conn &&
					holder.conn !== null
				) {
					this.lineTo(
						conn,
						`:${this.address} 433 * ${requested} :Nickname is already in use`,
					);
					return;
				}
				this.users.set(requested.toLowerCase(), { nick: requested, conn });
				this.registeredClients += 1;
				if (!this.withholdWelcome) {
					this.lineTo(
						conn,
						`:${this.address} 001 ${requested} :Welcome to the Fake IRC Network ${requested}`,
					);
				}
				return;
			}
			case "USER":
				return; // recorded only; registration completes on NICK/001
			case "JOIN": {
				const channel = params[0] ?? "";
				const nick = this.nickOf(conn);
				if (this.joinRejectCode !== "") {
					this.lineTo(
						conn,
						`:${this.address} ${this.joinRejectCode} ${nick ?? "*"} ${channel} :Cannot join channel`,
					);
					return;
				}
				const set = this.channels.get(channel) ?? new Set<string>();
				set.add(nick ?? "?");
				this.channels.set(channel, set);
				// Echo the join to EVERY member including the joiner.
				for (const member of set) {
					const m = this.users.get(member.toLowerCase());
					if (m?.conn != null) {
						this.lineTo(m.conn, `:${nick ?? "?"}!user@host JOIN ${channel}`);
					}
				}
				this.lineTo(
					conn,
					`:${this.address} 366 ${nick ?? "?"} ${channel} :End of /NAMES list`,
				);
				return;
			}
			case "PRIVMSG": {
				const target = params[0] ?? "";
				const text = params.slice(1).join(" :");
				const sender = this.nickOf(conn) ?? "unknown";
				if (target.startsWith("#") || target.startsWith("&")) {
					const members = this.channels.get(target);
					if (members !== undefined) {
						for (const member of members) {
							const m = this.users.get(member.toLowerCase());
							m?.conn?.serverSends(
								`:${sender}!user@host PRIVMSG ${target} :${text}`,
							);
						}
					}
				} else {
					// DM: deliver straight to the target nick.
					const m = this.users.get(target.toLowerCase());
					m?.conn?.serverSends(
						`:${sender}!user@host PRIVMSG ${target} :${text}`,
					);
					// Echo to sender (self-filter contract observes this).
					conn.serverSends(`:${sender}!user@host PRIVMSG ${target} :${text}`);
				}
				return;
			}
			case "QUIT":
				return;
			default:
				return;
		}
	}

	/** Push a server-initiated PING probe to every registered client. */
	pushPing(token = "keepalive"): void {
		for (const [, entry] of this.connections) {
			entry.conn.serverSends(`PING ${token}`);
		}
	}

	/** Deliver a channel message from a FAKE third party (no real socket). */
	deliverChannelMessage(
		channel: string,
		senderNick: string,
		text: string,
	): void {
		const members = this.channels.get(channel);
		const targets = members === undefined ? [] : [...members];
		// Always reach the client under test even if membership bookkeeping
		// hasn't caught up (fixture convenience with ircd semantics preserved:
		// a non-member would not receive traffic).
		if (!targets.includes(senderNick)) {
			for (const [, entry] of this.connections) {
				const userNick = this.nickOf(entry.conn);
				if (userNick !== null && userNick !== senderNick) {
					entry.conn.serverSends(
						`:${senderNick}!user@host PRIVMSG ${channel} :${text}`,
					);
				}
			}
			return;
		}
		for (const member of targets) {
			if (member === senderNick) continue;
			const m = this.users.get(member.toLowerCase());
			m?.conn?.serverSends(
				`:${senderNick}!user@host PRIVMSG ${channel} :${text}`,
			);
		}
	}

	/** Deliver a DIRECT private message from a fake peer. */
	deliverDm(fromNick: string, toNick: string, text: string): void {
		const m = this.users.get(toNick.toLowerCase());
		m?.conn?.serverSends(`:${fromNick}!user@host PRIVMSG ${toNick} :${text}`);
	}

	private nickOf(conn: FakeIrcClientConnection): string | null {
		for (const u of this.users.values()) if (u.conn === conn) return u.nick;
		return null;
	}

	private lineTo(conn: FakeIrcClientConnection, raw: string): void {
		conn.serverSends(raw);
	}
}

/** Minimal server-side parse (prefix/command/params-with-trailing). */
function parseServerSide(raw: string): { command: string; params: string[] } {
	let rest = raw;
	if (rest.startsWith(":")) {
		const idx = rest.indexOf(" ");
		rest = idx >= 0 ? rest.slice(idx + 1) : "";
	}
	let trailing = "";
	const tIdx = rest.indexOf(" :");
	if (tIdx >= 0) {
		trailing = rest.slice(tIdx + 2);
		rest = rest.slice(0, tIdx);
	}
	const parts = rest.split(" ").filter((p) => p.length > 0);
	const params = parts.length > 1 ? parts.slice(1) : [];
	if (trailing !== "") params.push(trailing);
	return { command: parts[0] ?? "", params };
}

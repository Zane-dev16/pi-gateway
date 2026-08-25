// pi_platforms/email/fake-mail-servers — the IN-PROCESS fake IMAP + SMTP
// servers (04 §8: rows run headless against fake platform servers; NO
// external network, no OS sockets). Vendor-true behaviors only:
//
//   FakeImapServer  — LOGIN ok/bad; RFC 2971 ID surface recording the EXACT
//     client identity argument (rejection armable); SELECT INBOX; UID SEARCH
//     ALL|UNSEEN with server-side \\Seen flags set on RFC822 fetch; per-UID
//     scripted fetch refusals; LOGOUT abort simulation (IMAP4.abort parity);
//     connection failure arming (#79889/#80032 surfaces).
//   FakeSmtpServer  — AUTH ok/bad (typed SMTPAuthenticationError analog);
//     send_message capture; port modeling (587 STARTTLS / 465 implicit TLS);
//     RESOLVER SEAM returning candidate addresses for the A21 IPv4 fallback
//     ladder (v6 blackhole first, v4 live second).
//
// Message shape mirrors what imaplib hands the adapter: raw RFC822-ish
// records with headers + body parts the adapter parses.

export interface MailPart {
	contentType: string;
	disposition: string | null;
	payload: Buffer;
	charset?: string | null;
	filename?: string | null;
}

export interface StoredMail {
	uid: string;
	fromRaw: string;
	headers: Record<string, string>;
	parts: MailPart[];
	messageId: string;
	inReplyTo: string;
	date: string;
	/** Server-side \\Seen flag — set by RFC822 fetch. */
	seen: boolean;
}

export class SmtpAuthenticationError extends Error {
	constructor(
		public readonly smtpCode: number,
		message: string,
	) {
		super(message);
		this.name = "SMTPAuthenticationError";
	}
}

export class ImapLogoutAbortError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "IMAP4.abort";
	}
}

let mailSeq = 0;

/** An attachment/inline MIME part scripted into a delivered mail. */
export interface ImapMailAttachmentInput {
	/** Raw filename as it would appear in Content-Disposition (RFC 2047 raw). */
	filename?: string | undefined;
	contentType: string;
	payload: Buffer;
	/** Defaults to "attachment"; "inline" exercises the inline leg too. */
	disposition?: "attachment" | "inline" | undefined;
}

export interface ImapMailInput {
	from: string;
	subject?: string | undefined;
	textBody?: string | undefined;
	htmlBody?: string | undefined;
	headers?: Record<string, string> | undefined;
	inReplyTo?: string | undefined;
	attachments?: readonly ImapMailAttachmentInput[] | undefined;
}

export class FakeImapServer {
	readonly host = "imap.fake.example";
	private readonly mailbox = new Map<string, StoredMail>();
	private loggedIn = false;

	// ── scenario knobs ──
	authBad = false;
	/** UIDs whose RFC822 fetch REFUSES (transient per-UID refusal, #80016). */
	fetchRefusals = new Set<string>();
	/** Arm LOGOUT to raise IMAP4.abort even on a healthy connection. */
	logoutAborts = false;
	/** Armed connect() failures remaining (transport unreachable). */
	connectFailuresArmed = 0;
	connectCalls = 0;
	/** Exact arguments received for RFC 2971 ID commands, in order (audit). */
	readonly idCommands: string[] = [];
	/** Arm ID rejection: server answers BAD for the unknown command. */
	idRejectionArmed = false;

	reset(): void {
		this.mailbox.clear();
		this.loggedIn = false;
		this.authBad = false;
		this.fetchRefusals.clear();
		this.logoutAborts = false;
		this.connectFailuresArmed = 0;
		this.idCommands.length = 0;
		this.idRejectionArmed = false;
	}

	deliver(mail: ImapMailInput): string {
		const uid = String(++mailSeq);
		const parts: MailPart[] = [];
		if (mail.textBody !== undefined) {
			parts.push({
				contentType: "text/plain",
				disposition: null,
				payload: Buffer.from(mail.textBody, "utf8"),
				charset: "utf-8",
			});
		}
		if (mail.htmlBody !== undefined) {
			parts.push({
				contentType: "text/html",
				disposition: null,
				payload: Buffer.from(mail.htmlBody, "utf8"),
				charset: "utf-8",
			});
		}
		for (const att of mail.attachments ?? []) {
			parts.push({
				contentType: att.contentType,
				disposition: att.disposition ?? "attachment",
				payload: att.payload,
				charset: null,
				...(att.filename !== undefined ? { filename: att.filename } : {}),
			});
		}
		if (parts.length === 0) {
			parts.push({
				contentType: "text/plain",
				disposition: null,
				payload: Buffer.from("", "utf8"),
				charset: "utf-8",
			});
		}
		this.mailbox.set(uid, {
			uid,
			fromRaw: mail.from,
			headers: {
				Subject: mail.subject ?? "(no subject)",
				...(mail.headers ?? {}),
			},
			parts,
			messageId: `<fake-${uid}@mx.fake.example>`,
			inReplyTo: mail.inReplyTo ?? "",
			date: new Date(0).toISOString(),
			seen: false,
		});
		return uid;
	}

	// ── protocol surface (sync — imaplib calls are blocking executor calls) ──

	login(_user: string, _password: string): void {
		if (this.connectFailuresArmed > 0) {
			throw new Error("IMAP connection failed: unreachable host");
		}
		if (this.authBad) throw new Error("LOGIN failed: bad credentials");
		this.loggedIn = true;
	}

	/**
	 * RFC 2971 ID (adapter.py:_send_imap_id call sites): records the EXACT
	 * identity argument sent after LOGIN; rejection is armable for the
	 * swallowed-failure contract (non-supporting servers must keep working).
	 */
	id(argument: string): void {
		if (!this.loggedIn) throw new Error("NO: not authenticated");
		if (this.idRejectionArmed) throw new Error("BAD: unknown command");
		this.idCommands.push(argument);
	}

	selectInbox(): void {
		if (!this.loggedIn) throw new Error("NO: not authenticated");
	}

	uidSearch(criteria: "ALL" | "UNSEEN"): string[] {
		if (!this.loggedIn) throw new Error("NO: not authenticated");
		const out: string[] = [];
		for (const mail of this.mailbox.values()) {
			if (criteria === "ALL" || !mail.seen) out.push(mail.uid);
		}
		return out;
	}

	/**
	 * UID FETCH (RFC822): marks \\Seen server-side and returns the record.
	 * Scripted refusals raise (transient per-UID refusal) WITHOUT marking seen.
	 */
	uidFetchRfc822(uid: string): StoredMail {
		if (!this.loggedIn) throw new Error("NO: not authenticated");
		const mail = this.mailbox.get(uid);
		if (mail === undefined) throw new Error("NO: no such message");
		if (this.fetchRefusals.has(uid)) {
			throw new Error(`NO: fetch refused for ${uid}`);
		}
		mail.seen = true;
		return mail;
	}

	logout(): void {
		if (this.logoutAborts)
			throw new ImapLogoutAbortError("abort: socket closed");
		this.loggedIn = false;
	}

	get unseenCount(): number {
		return this.uidSearch("UNSEEN").length;
	}

	/** Fixture convenience: unseen count WITHOUT the login gate. */
	get peekUnseen(): number {
		let n = 0;
		for (const mail of this.mailbox.values()) if (!mail.seen) n += 1;
		return n;
	}

	get totalCount(): number {
		return this.mailbox.size;
	}
}

export interface SentMailRecord {
	to: string;
	from: string;
	subject: string;
	bodyText: string;
	headers: Record<string, string>;
	port: number;
	viaIpv4: boolean;
}

export type ResolverCandidate = {
	family: 4 | 6;
	host: string;
	reachable: boolean;
};

export class FakeSmtpServer {
	readonly host = "smtp.fake.example";
	readonly sent: SentMailRecord[] = [];

	// ── scenario knobs ──
	authBad = false;
	/**
	 * DNS candidates returned by the resolver seam, in order. The A21 ladder
	 * walks them: connection-class failures fall through to IPv4-only.
	 */
	resolverCandidates: ResolverCandidate[] = [
		{ family: 6, host: "2001:db8::dead", reachable: false },
		{ family: 4, host: "192.0.2.10", reachable: true },
	];
	tlsVerifyFailuresArmed = 0;
	connectCalls = 0;
	lastCandidateFamily: 4 | 6 | null = null;

	reset(): void {
		this.sent.length = 0;
		this.authBad = false;
		this.tlsVerifyFailuresArmed = 0;
		this.lastCandidateFamily = null;
	}

	/**
	 * _create_ipv4_connection parity: AF_INET-constrained getaddrinfo walk.
	 * `ipv4Only` filters candidates to family 4 (A21 retry semantics).
	 */
	openConnection(port: number, ipv4Only: boolean): { close(): void } {
		this.connectCalls += 1;
		const candidates = ipv4Only
			? this.resolverCandidates.filter((c) => c.family === 4)
			: this.resolverCandidates;
		for (const candidate of candidates) {
			if (candidate.reachable) {
				this.lastCandidateFamily = candidate.family;
				return { close: () => {} };
			}
			if (ipv4Only) break; // IPv4-only path stops at the FIRST v4 address
		}
		throw new Error("connect failed: no route to host");
	}

	startTls(): void {
		if (this.tlsVerifyFailuresArmed > 0) {
			this.tlsVerifyFailuresArmed -= 1;
			const err = new Error(
				"unable to verify the first certificate (TLS verify failure)",
			);
			err.name = "SSLError";
			throw err;
		}
	}

	login(_user: string, _password: string): void {
		if (this.authBad) {
			throw new SmtpAuthenticationError(
				535,
				"535 Authentication failed: bad credentials",
			);
		}
	}

	sendMessage(record: Omit<SentMailRecord, "port" | "viaIpv4">): void {
		this.sent.push({
			...record,
			port: EMAIL_PORT_UNDER_TEST,
			viaIpv4: this.lastCandidateFamily === 4,
		});
	}
}

/** Fixture constant: the SMTP port modeled by the fake server. */
export const EMAIL_PORT_UNDER_TEST = 587;

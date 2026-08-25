// pi_platforms/teams/teams-fixture — the REAL-engine fixture for the Teams
// shape-delta rows (WaCloudFixture/MSGraphFixture pattern): the actual
// TeamsAdapter over FakeBotFrameworkServer with an injected clock,
// mkdtemp-isolated media cache, Bot Framework activity/card wire builders, and
// scripted STS/threaded-reply failures. NO stubbed return values — rows drive
// the real ingress pipeline, TTL dedupe, card-action authz, and outbound REST
// shapes.

import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { TeamsAdapter } from "./teams-adapter.js";
import {
	FakeBotFrameworkServer,
	type RecordedActivity,
} from "./bot-framework-wire.js";
import {
	FIXTURE_CLIENT_ID,
	FIXTURE_CLIENT_SECRET,
	FIXTURE_TENANT_ID,
} from "./fixture-secrets.js";

export const TEAMS_FIXTURE_CLIENT_ID = FIXTURE_CLIENT_ID;

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it — the TTL-dedupe boundary tests mutate THIS.
 */
export class FixtureClock {
	constructor(private nowValue: number = 1_700_000_000_000) {}
	get nowMs(): number {
		return this.nowValue;
	}
	advance(ms: number): void {
		this.nowValue += ms;
	}
}

export interface TeamsFixtureOptions {
	scalarMaxUnits?: number | undefined;
	allowedUsers?: readonly string[] | undefined;
	allowAllUsers?: boolean | undefined;
	serviceUrl?: string | undefined;
	withSecret?: boolean | undefined;
}

const DEFAULT_CONVERSATION = {
	id: "19:meeting123@thread.tacv2",
	conversation_type: "channel",
	tenant_id: FIXTURE_TENANT_ID,
};

export class TeamsFixture {
	readonly bf = new FakeBotFrameworkServer();
	readonly adapter: TeamsAdapter;
	readonly clock = new FixtureClock();
	readonly mediaDir: string;

	private readonly tempRoot: string;

	constructor(opts: TeamsFixtureOptions = {}) {
		this.tempRoot = mkdtempSync(join(tmpdir(), "teams-fix-"));
		this.mediaDir = join(this.tempRoot, "media");
		mkdirSync(this.mediaDir, { recursive: true });
		this.adapter = new TeamsAdapter({
			transport: this.bf,
			nowMs: () => this.clock.nowMs,
			mediaCacheDir: this.mediaDir,
			...(opts.scalarMaxUnits !== undefined
				? { scalarMaxUnits: opts.scalarMaxUnits }
				: {}),
			...(opts.allowedUsers !== undefined
				? { allowedUsers: opts.allowedUsers }
				: {}),
			...(opts.allowAllUsers !== undefined
				? { allowAllUsers: opts.allowAllUsers }
				: {}),
			...(opts.serviceUrl !== undefined ? { serviceUrl: opts.serviceUrl } : {}),
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				if (name === "TEAMS_CLIENT_ID") return FIXTURE_CLIENT_ID;
				if (name === "TEAMS_CLIENT_SECRET") return FIXTURE_CLIENT_SECRET;
				if (name === "TEAMS_TENANT_ID") return FIXTURE_TENANT_ID;
				return undefined;
			},
		});
		this.adapter.attachStandardGuard();
	}

	advance(ms: number): void {
		this.clock.advance(ms);
	}

	dispose(): void {
		rmSync(this.tempRoot, { recursive: true, force: true });
	}

	// ── transport-level requests ───────────────────────────────────────────────

	async postActivityBody(body: string | Buffer): Promise<{
		status: number;
		json: Record<string, unknown> | undefined;
	}> {
		const raw = Buffer.isBuffer(body) ? body : Buffer.from(body, "utf8");
		const resp = await this.adapter.handleActivityPost({ rawBody: raw });
		return { status: resp.status, json: resp.json };
	}

	// ── Bot Framework activity builders ────────────────────────────────────────

	messageActivity(extras: {
		id?: string;
		fromId?: string;
		fromName?: string;
		aadObjectId?: string;
		conversationType?: string;
		conversationId?: string;
		text?: string;
		attachments?: Array<Record<string, unknown>>;
		botAuthored?: boolean;
	}): Record<string, unknown> {
		return {
			type: "message",
			...(extras.id !== undefined ? { id: extras.id } : {}),
			text: extras.text ?? "",
			from: {
				id:
					extras.botAuthored === true
						? FIXTURE_CLIENT_ID
						: (extras.fromId ?? "user-8"),
				...(extras.aadObjectId !== undefined
					? { aad_object_id: extras.aadObjectId }
					: {}),
				...(extras.fromName !== undefined ? { name: extras.fromName } : {}),
			},
			conversation: {
				id: extras.conversationId ?? DEFAULT_CONVERSATION.id,
				conversation_type:
					extras.conversationType ?? DEFAULT_CONVERSATION.conversation_type,
				tenant_id: DEFAULT_CONVERSATION.tenant_id,
			},
			...(extras.attachments !== undefined
				? { attachments: extras.attachments }
				: {}),
		};
	}

	async postMessageActivity(activity: Record<string, unknown>) {
		return this.postActivityBody(JSON.stringify(activity));
	}

	// ── observation helpers ──

	textSends(): readonly RecordedActivity[] {
		return this.bf.textSendsOf();
	}
}

export function makeTeamsFixture(opts?: TeamsFixtureOptions): TeamsFixture {
	return new TeamsFixture(opts);
}

// pi_platforms/feishu/comment-rules — A12 3-tier Drive-comment access rules
// (feishu_comment_rules.py ported symbol-for-symbol).
//
// Tier model (:43–:60 dataclasses): per-document rules keyed
// `{file_type}:{file_token}` / `wiki:{node_token}` / `*`, layered
// exact → wiki-exact → wildcard → top-level → code defaults, each FIELD
// (enabled/policy/allow_from) independently inherited from the highest layer
// that defines it (None = inherit). Policies ∈ {allowlist, pairing}.
//
// Posture: missing/corrupt rules file ⇒ DEFAULT config (enabled=true,
// policy="pairing", empty allow_from) ⇒ deny-by-default unless the user is
// paired or explicitly allowlisted (:72/:136 — fail-closed WITHIN pairing).
//
// Hot reload: stat() on EVERY access; re-read only when mtime changed
// (_MtimeCache :72) — no watchers, no timers. Deleted file ⇒ defaults.

import {
	existsSync,
	mkdirSync,
	readFileSync,
	statSync,
	writeFileSync,
	renameSync,
} from "node:fs";
import { dirname } from "node:path";

export type CommentPolicy = "allowlist" | "pairing";
export const VALID_POLICIES: readonly CommentPolicy[] = [
	"allowlist",
	"pairing",
];

export interface CommentDocumentRule {
	enabled?: boolean | undefined;
	policy?: string | undefined;
	allow_from?: readonly string[] | undefined;
}

export interface CommentsConfigFile {
	enabled?: boolean | undefined;
	policy?: string | undefined;
	allow_from?: readonly string[] | undefined;
	documents?: Record<string, CommentDocumentRule> | undefined;
}

/** Fully-resolved rule (:60 ResolvedCommentRule). */
export interface ResolvedCommentRule {
	enabled: boolean;
	policy: CommentPolicy;
	allowFrom: ReadonlySet<string>;
	matchSource: `exact:${string}` | "wildcard" | "top" | "default";
}

export const DEFAULT_COMMENTS_CONFIG: Readonly<ResolvedCommentRule> =
	Object.freeze({
		enabled: true,
		policy: "pairing" as CommentPolicy,
		allowFrom: new Set<string>(),
		matchSource: "default" as const,
	});

function coercePolicy(raw: unknown): CommentPolicy | undefined {
	return VALID_POLICIES.includes(raw as CommentPolicy)
		? (raw as CommentPolicy)
		: undefined;
}

/**
 * Tier resolution (:170 resolve_rule). Key construction: exact
 * `f"{file_type}:{file_token}"`; wiki-exact `wiki:{wiki_token}` occupies the
 * SAME precedence slot as exact when no exact key matched; wildcard "*".
 */
export function resolveRule(
	cfg: CommentsConfigFile,
	fileType: string,
	fileToken: string,
	wikiToken = "",
): ResolvedCommentRule {
	const docs = cfg.documents ?? {};
	const layers: Array<{
		rule: CommentDocumentRule;
		source: string;
		rank: number;
	}> = [];
	const exactKey = `${fileType}:${fileToken}`;
	if (docs[exactKey] !== undefined)
		layers.push({
			rule: docs[exactKey] as CommentDocumentRule,
			source: `exact:${exactKey}`,
			rank: 0,
		});
	else if (wikiToken && docs[`wiki:${wikiToken}`] !== undefined)
		layers.push({
			rule: docs[`wiki:${wikiToken}`] as CommentDocumentRule,
			source: `exact:wiki:${wikiToken}`,
			rank: 0,
		});
	if (docs["*"] !== undefined)
		layers.push({
			rule: docs["*"] as CommentDocumentRule,
			source: "wildcard",
			rank: 1,
		});

	let enabled = cfg.enabled ?? DEFAULT_COMMENTS_CONFIG.enabled;
	let policy: CommentPolicy =
		coercePolicy(cfg.policy) ?? DEFAULT_COMMENTS_CONFIG.policy;
	let allowFrom = new Set<string>(cfg.allow_from ?? []);
	let matchSource: ResolvedCommentRule["matchSource"] = "top";

	for (const layer of layers) {
		const r = layer.rule;
		if (r.enabled !== undefined) enabled = r.enabled === true;
		const p = coercePolicy(r.policy);
		if (p !== undefined) policy = p;
		if (r.allow_from !== undefined) allowFrom = new Set(r.allow_from);
		if (layer.rank < 2)
			matchSource = layer.source.startsWith("wildcard")
				? "wildcard"
				: (`exact:${layer.source.slice(6)}` as `exact:${string}`);
	}
	if (layers.length === 0) matchSource = "top";

	return { enabled, policy, allowFrom, matchSource };
}

/**
 * has_wiki_keys (:165): any document rule key starts with "wiki:" — gates
 * the reverse-lookup re-resolution in comment ingress.
 */
export function hasWikiKeys(cfg: CommentsConfigFile): boolean {
	return Object.keys(cfg.documents ?? {}).some((k) => k.startsWith("wiki:"));
}

/**
 * The gate (:285 is_user_allowed): allowlist membership ALWAYS passes;
 * pairing policy consults the approved store; allowlist without membership
 * DENIES.
 */
export function isUserAllowed(
	rule: ResolvedCommentRule,
	userOpenId: string,
	pairingApproved: ReadonlySet<string>,
): boolean {
	if (rule.allowFrom.has(userOpenId)) return true;
	if (rule.policy === "pairing") return pairingApproved.has(userOpenId);
	return false;
}

/** mtime-keyed cache (:72 _MtimeCache): stat on every access, parse on change. */
export class MtimeCache<T> {
	private mtimeMs = -1;
	private data: T | null = null;
	constructor(
		private readonly path: string,
		private readonly parse: (raw: unknown) => T,
		private readonly fallback: () => T,
	) {}
	load(): T {
		let mtime = -1;
		try {
			mtime = statSync(this.path).mtimeMs;
		} catch {
			this.mtimeMs = 0;
			this.data = null; // file deleted ⇒ defaults immediately
			return this.fallback();
		}
		if (mtime !== this.mtimeMs || this.data === null) {
			this.mtimeMs = mtime;
			try {
				this.data = this.parse(JSON.parse(readFileSync(this.path, "utf8")));
			} catch {
				this.data = this.fallback(); // corrupt/unreadable ⇒ warn-equivalent default
			}
		}
		return this.data as T;
	}

	/** Explicit cache reset — savePairing invalidates so the next access re-reads
	 * even when the rename lands within the same mtime tick (:243 parity). */
	invalidate(): void {
		this.mtimeMs = -1;
		this.data = null;
	}
}

function parseCommentsConfig(raw: unknown): CommentsConfigFile {
	if (raw === null || typeof raw !== "object") return {};
	const rawObj = raw as Record<string, unknown>;
	const documents: Record<string, CommentDocumentRule> = {};
	const rawDocs = rawObj["documents"];
	if (rawDocs !== null && typeof rawDocs === "object") {
		for (const [key, val] of Object.entries(
			rawDocs as Record<string, unknown>,
		)) {
			if (val !== null && typeof val === "object")
				documents[key] = val as CommentDocumentRule;
		}
	}
	return {
		enabled:
			rawObj["enabled"] === undefined ? undefined : rawObj["enabled"] === true,
		policy:
			rawObj["policy"] === undefined ? undefined : String(rawObj["policy"]),
		allow_from: Array.isArray(rawObj["allow_from"])
			? (rawObj["allow_from"] as unknown[]).map(String)
			: undefined,
		documents,
	};
}

interface PairingFile {
	approved?: Record<string, unknown> | string[] | null;
}

/**
 * The rules + pairing stores over a scoped home dir (mkdtemp-isolated in
 * tests). Hermes paths: get_hermes_home()/feishu_comment_rules.json +
 * feishu_comment_pairing.json (:32–33); the port takes the directory via
 * deps — never process env.
 */
export class FeishuCommentRulesStore {
	static readonly RULES_FILE = "feishu_comment_rules.json";
	static readonly PAIRING_FILE = "feishu_comment_pairing.json";

	private readonly rulesCache: MtimeCache<CommentsConfigFile>;
	private readonly pairingCache: MtimeCache<PairingFile>;
	private readonly pairingPath: string;

	constructor(private readonly homeDir: string) {
		this.rulesCache = new MtimeCache<CommentsConfigFile>(
			`${homeDir}/${FeishuCommentRulesStore.RULES_FILE}`,
			parseCommentsConfig,
			() => ({}),
		);
		this.pairingPath = `${homeDir}/${FeishuCommentRulesStore.PAIRING_FILE}`;
		this.pairingCache = new MtimeCache<PairingFile>(
			this.pairingPath,
			(raw) =>
				raw !== null && typeof raw === "object" ? (raw as PairingFile) : {},
			() => ({}),
		);
	}

	loadConfig(): CommentsConfigFile {
		return this.rulesCache.load();
	}

	/** Approved open_id set (:224 _load_pairing_approved): dict keys OR list items. */
	loadPairingApproved(): Set<string> {
		const file = this.pairingCache.load();
		const approved = file.approved ?? {};
		if (Array.isArray(approved)) return new Set(approved.map(String));
		return new Set(Object.keys(approved));
	}

	/** Atomic tmp+replace write with explicit cache invalidation (:235). */
	private savePairing(file: PairingFile): void {
		mkdirSync(dirname(this.pairingPath), { recursive: true });
		const tmp = `${this.pairingPath}.tmp`;
		writeFileSync(tmp, JSON.stringify(file), "utf8");
		renameSync(tmp, this.pairingPath);
		// Force the next access to re-read (cache invalidation parity :243).
		this.pairingCache.invalidate();
	}

	pairingAdd(openId: string): boolean {
		const file = this.pairingCache.load();
		const approved = (
			file.approved && !Array.isArray(file.approved)
				? { ...(file.approved as Record<string, unknown>) }
				: {}
		) as Record<string, unknown>;
		if (openId in approved) return false;
		approved[openId] = { approved_at: Date.now() / 1000 };
		this.savePairing({ approved });
		return true;
	}

	pairingRemove(openId: string): boolean {
		const file = this.pairingCache.load();
		const approved = (
			file.approved && !Array.isArray(file.approved)
				? { ...(file.approved as Record<string, unknown>) }
				: {}
		) as Record<string, unknown>;
		if (!(openId in approved)) return false;
		delete approved[openId];
		this.savePairing({ approved });
		return true;
	}

	rulesFileExists(): boolean {
		return existsSync(`${this.homeDir}/${FeishuCommentRulesStore.RULES_FILE}`);
	}
}

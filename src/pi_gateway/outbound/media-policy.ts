// pi_gateway/outbound/media-policy.ts — outbound media path validation and
// the config→env policy bridge (03-message-routing.md §9.2 "Path validation
// ladder" + "Policy bridge").
//
// Ladder (parity gateway/platforms/base.py:validate_media_delivery_path):
//   strip quote wrappers → expanduser → absolute-or-reject → Docker
//   container-path translation → symlink resolution → accept when under a
//   managed cache root / operator allowlist → else DEFAULT mode accepts any
//   existing regular file off the denylist; STRICT mode additionally requires
//   production inside the recency window for files outside trusted roots.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py:validate_media_delivery_path     → validateMediaDeliveryPath
//   base.py:_media_delivery_allowed_roots                       → collectAllowedRoots
//   base.py:_profile_cache_roots                                 → profileCacheRoots
//   base.py:_path_under_denied_prefix        → pathUnderDeniedPrefix
//   base.py:_file_is_recently_produced       → fileIsRecentlyProduced
//   base.py:filter_media_delivery_paths      → filterMediaDeliveryPaths
//   media_policy.py:apply_media_policy_env   → applyMediaPolicyEnv

import { homedir } from "node:os";
import { lstatSync, readdirSync, realpathSync, statSync } from "node:fs";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { resolvePiHome } from "../../pi_home.js";

/** Env var names — kept byte-identical to the Hermes bridge (proposed DEC text: renaming to PI_MEDIA_* is cosmetic-only; behavior must not drift). */
export const MEDIA_DELIVERY_STRICT_ENV = "HERMES_MEDIA_DELIVERY_STRICT";
export const MEDIA_DELIVERY_ALLOW_DIRS_ENV = "HERMES_MEDIA_ALLOW_DIRS";
export const MEDIA_DELIVERY_TRUST_RECENT_ENV =
	"HERMES_MEDIA_TRUST_RECENT_FILES";
export const MEDIA_DELIVERY_TRUST_RECENT_SECONDS_ENV =
	"HERMES_MEDIA_TRUST_RECENT_SECONDS";
export const DOCKER_VOLUMES_ENV = "TERMINAL_DOCKER_VOLUMES";

/** Default recency window (seconds) for trusting freshly-produced files in strict mode. */
export const MEDIA_DELIVERY_TRUST_RECENT_DEFAULT_SECONDS = 600;

/** System-path denylist prefixes (base.py:_MEDIA_DELIVERY_DENIED_PREFIXES). */
const SYSTEM_DENIED_PREFIXES = [
	"/etc",
	"/proc",
	"/sys",
	"/dev",
	"/root",
	"/boot",
	"/var/log",
	"/var/lib",
	"/var/run",
];

/** Credential/config directories denied under $HOME (base.py:_MEDIA_DELIVERY_DENIED_HOME_SUBPATHS). */
const HOME_DENIED_SUBPATHS = [
	".ssh",
	".aws",
	".gnupg",
	".kube",
	".docker",
	".config",
	".azure",
	".gcloud",
	"Library/Keychains",
];

/** Per-file credential stores at the pi-home root (base.py:_ROOT_CREDENTIAL_FILES). */
const ROOT_CREDENTIAL_FILES = [
	".env",
	"auth.json",
	"auth.lock",
	"credentials",
	"config.yaml",
	".anthropic_oauth.json",
	"google_token.json",
	"google_oauth_pending.json",
	join("auth", "google_oauth.json"),
	"webhook_subscriptions.json",
	join("cache", "bws_cache.json"),
	join("cache", "bws_cache.enc.json"),
];

/** Directory trees whose every child is credential material (base.py:_ROOT_CREDENTIAL_DIRS). */
const ROOT_CREDENTIAL_DIRS = ["pairing", "mcp-tokens"];

/** Canonical cache subdirectories holding deliverable artifacts. */
const CACHE_SUBDIRS = ["images", "audio", "videos", "documents", "screenshots"];

export interface PathValidationEnv {
	/** Environment variables (defaults to process.env at call time). */
	env?: NodeJS.ProcessEnv;
	/** The user's home directory (defaults to os.homedir()). */
	home?: string;
	/** The pi home root — cache roots + credential stores live under it (defaults to resolvePiHome()). */
	piHome?: string;
	/** Clock for the strict-mode recency window (INJECTED in tests). */
	nowMs?: number;
}

function envOf(deps: PathValidationEnv): NodeJS.ProcessEnv {
	return deps.env ?? process.env;
}

function homeOf(deps: PathValidationEnv): string {
	if (deps.home !== undefined) return deps.home;
	try {
		return homedir();
	} catch {
		return "";
	}
}

/**
 * Pi-parity root: pi_home has a single root (context override → PI_HOME → ~/.pi);
 * Hermes distinguishes active-profile HERMES_HOME from the shared default root.
 * Under-determined ⇒ smallest consistent shape: ONE root plays both roles
 * (proposed DEC text recorded in the phase report).
 */
function piHomeOf(deps: PathValidationEnv): string {
	if (deps.piHome !== undefined) return deps.piHome;
	try {
		return resolvePiHome();
	} catch {
		return "";
	}
}

/** Strip quote wrappers + edge punctuation exactly like the validator's head. */
export function stripPathWrappers(candidate: string): string {
	let c = String(candidate ?? "").trim();
	if (
		c.length >= 2 &&
		c[0] === c[c.length - 1] &&
		(c[0] === "`" || c[0] === '"' || c[0] === "'")
	) {
		c = c.slice(1, -1).trim();
	}
	return c.replace(/^[`"']+/, "").replace(/[`"',.;:)}\]]+$/, "");
}

/** `~`-expansion parity of os.path.expanduser; throws on embedded NUL like Python's ValueError. */
export function expandUser(path: string, home?: string): string {
	if (path === "~") return home ?? homedir();
	if (path.startsWith("~/") || path.startsWith("~\\")) {
		const h = home ?? homedir();
		if (!h) throw new Error("expanduser: no home");
		return `${h}${path.slice(1)}`;
	}
	return path;
}

function statIsRegularFile(path: string): boolean {
	try {
		return statSync(path).isFile();
	} catch {
		return false;
	}
}

function isDir(p: string): boolean {
	try {
		return statSync(p).isDirectory();
	} catch {
		return false;
	}
}

/**
 * Per-profile canonical cache roots (base.py:_profile_cache_roots):
 * <root>/profiles/<name>/cache/<subdir> for every EXISTING profile directory,
 * enumerated dynamically at check time so profiles created after startup are
 * covered, and so a resolved profile path is allowlisted BEFORE the /root-
 * style system denylist is consulted (#31733).
 */
function profileCacheRoots(piHome: string): string[] {
	const roots: string[] = [];
	const profilesDir = join(piHome, "profiles");
	let entries: string[];
	try {
		entries = readdirSync(profilesDir);
	} catch {
		return roots;
	}
	for (const name of entries) {
		const profileDir = join(profilesDir, name);
		if (!isDir(profileDir)) continue;
		for (const sub of CACHE_SUBDIRS) {
			roots.push(join(profileDir, "cache", sub));
		}
	}
	return roots;
}

/**
 * Managed cache roots + operator allowlist (base.py:_media_delivery_allowed_roots):
 * legacy `*_cache` dirs AND canonical `cache/<subdir>` layout under the pi home,
 * per-profile `profiles/<name>/cache/<subdir>` roots,
 * plus HERMES_MEDIA_ALLOW_DIRS entries (os.pathsep- or comma-separated).
 */
export function collectAllowedRoots(deps: PathValidationEnv): string[] {
	const piHome = piHomeOf(deps);
	const env = envOf(deps);
	const roots: string[] = [];
	if (piHome) {
		for (const d of [
			"image_cache",
			"audio_cache",
			"video_cache",
			"document_cache",
			"browser_screenshots",
		]) {
			roots.push(join(piHome, d));
		}
		for (const sub of CACHE_SUBDIRS) {
			roots.push(join(piHome, "cache", sub));
		}
		roots.push(...profileCacheRoots(piHome));
	}
	const raw = env[MEDIA_DELIVERY_ALLOW_DIRS_ENV] ?? "";
	for (const chunk of raw.split(sep === "\\" ? ";" : ":")) {
		for (const rawRoot of chunk.split(",")) {
			const expanded = rawRoot.trim();
			if (!expanded) continue;
			let resolvedRoot = expanded;
			try {
				resolvedRoot = expandUser(expanded, homeOf(deps));
			} catch {
				continue;
			}
			if (isAbsolute(resolvedRoot)) roots.push(resolvedRoot);
		}
	}
	return roots;
}

/** Absolute denylist paths (system prefixes + $HOME credential dirs + pi-root credential stores). */
export function collectDeniedPaths(deps: PathValidationEnv): string[] {
	const denied = [...SYSTEM_DENIED_PREFIXES];
	const home = homeOf(deps);
	if (home) {
		for (const sub of HOME_DENIED_SUBPATHS) denied.push(join(home, sub));
	}
	const piHome = piHomeOf(deps);
	if (piHome) {
		for (const rel of [...ROOT_CREDENTIAL_FILES, ...ROOT_CREDENTIAL_DIRS]) {
			denied.push(join(piHome, rel));
		}
	}
	return denied;
}

function isWithin(path: string, root: string): boolean {
	if (!root || !isAbsolute(root)) return false;
	const p = resolve(path);
	const r = resolve(root);
	if (p === r) return true;
	const rel = relative(r, p);
	return rel !== "" && !rel.startsWith("..") && !isAbsolute(rel);
}

/**
 * True when `resolved` lives under a deny-listed path. One narrow exception:
 * when a denied prefix IS the running user's own home, the home itself is not
 * treated as denied (`/root` blocks OTHER users' homes on non-root gateways;
 * on a root-run gateway the operator's own deliverables live under it).
 * Credential sub-directories keep their own more-specific entries.
 */
export function pathUnderDeniedPrefix(
	resolved: string,
	deps: PathValidationEnv = {},
): boolean {
	const home = homeOf(deps);
	let homeResolved: string | null = null;
	if (home) {
		try {
			homeResolved = realpathSync(home);
		} catch {
			homeResolved = null;
		}
	}
	for (const denied of collectDeniedPaths(deps)) {
		let resolvedDenied = denied;
		try {
			resolvedDenied = expandUser(denied, home);
		} catch {
			continue;
		}
		if (!(isWithin(resolved, resolvedDenied) || resolved === resolvedDenied))
			continue;
		if (homeResolved !== null && resolve(resolvedDenied) === homeResolved)
			continue;
		return true;
	}
	return false;
}

/** Recency window seconds; 0 disables recency-based trust entirely. */
export function mediaDeliveryRecencySeconds(
	deps: PathValidationEnv = {},
): number {
	const env = envOf(deps);
	const raw = (env[MEDIA_DELIVERY_TRUST_RECENT_ENV] ?? "1")
		.trim()
		.toLowerCase();
	if (["0", "false", "no", "off", ""].includes(raw)) return 0;
	const custom = (env[MEDIA_DELIVERY_TRUST_RECENT_SECONDS_ENV] ?? "").trim();
	if (custom) {
		const seconds = Number.parseFloat(custom);
		if (!Number.isNaN(seconds)) return Math.max(0, seconds);
	}
	return MEDIA_DELIVERY_TRUST_RECENT_DEFAULT_SECONDS;
}

/** Strict mode flag: require allowlist/recency match instead of default-denylist acceptance. */
export function mediaDeliveryStrictMode(deps: PathValidationEnv = {}): boolean {
	const raw = (envOf(deps)[MEDIA_DELIVERY_STRICT_ENV] ?? "0")
		.trim()
		.toLowerCase();
	return ["1", "true", "yes", "on"].includes(raw);
}

function fileIsRecentlyProduced(
	resolved: string,
	windowSeconds: number,
	deps: PathValidationEnv,
): boolean {
	if (windowSeconds <= 0) return false;
	try {
		const mtimeMs = statSync(resolved).mtimeMs;
		const now = deps.nowMs ?? Date.now();
		return now - mtimeMs <= windowSeconds * 1000;
	} catch {
		return false;
	}
}

interface DockerMount {
	host: string;
	container: string;
}

/** Parse TERMINAL_DOCKER_VOLUMES (JSON list of "host:container[:mode]") (base.py:_parse_docker_volume_mounts). */
export function parseDockerVolumeMounts(
	env: NodeJS.ProcessEnv = process.env,
): DockerMount[] {
	const raw = (env[DOCKER_VOLUMES_ENV] ?? "").trim();
	if (!raw) return [];
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		return [];
	}
	if (!Array.isArray(parsed)) return [];
	const mounts: DockerMount[] = [];
	for (const entry of parsed) {
		if (typeof entry !== "string") continue;
		const spec = entry.trim();
		if (!spec) continue;
		const sepIdx = spec.indexOf(":/");
		if (sepIdx <= 0) continue;
		const hostRaw = spec.slice(0, sepIdx);
		const containerRaw = spec.slice(sepIdx + 1).split(":", 1)[0] as string;
		if (!containerRaw.startsWith("/")) continue;
		let hostExpanded = hostRaw;
		try {
			hostExpanded = expandUser(hostRaw);
		} catch {
			continue;
		}
		if (
			!(
				hostExpanded.startsWith("/") ||
				(hostExpanded.length > 1 && hostExpanded[1] === ":")
			)
		)
			continue;
		mounts.push({ host: resolve(hostExpanded), container: containerRaw });
	}
	return mounts;
}

/**
 * Translate a container-absolute path to its host path via longest-prefix
 * match over configured volume mounts (base.py:_translate_docker_container_media_path).
 * The synthetic /workspace and /root sandbox mounts are Desktop-backend state
 * that does not exist in this runtime — under-determined ⇒ omitted (recorded
 * as proposed DEC text in the phase report).
 */
export function translateDockerContainerMediaPath(
	candidate: string,
	env: NodeJS.ProcessEnv = process.env,
): string | null {
	if (!isAbsolute(candidate)) return null;
	let best: DockerMount | null = null;
	for (const m of parseDockerVolumeMounts(env)) {
		if (
			candidate === m.container ||
			candidate.startsWith(
				m.container.endsWith("/") ? m.container : `${m.container}/`,
			)
		) {
			if (best === null || m.container.length > best.container.length) best = m;
		}
	}
	if (best === null) return null;
	const suffix = candidate.slice(best.container.length);
	return resolve(`${best.host}${suffix}`);
}

/**
 * Return a safe absolute file path for native media delivery, else null.
 *
 * DEFAULT mode accepts any existing regular file off the credential/system
 * denylist (inbound/outbound symmetry: platforms hand the agent any file, so
 * the agent hands back any file that isn't a credential). STRICT mode
 * requires managed-cache/allowlist membership or recency-window production.
 * Symlinks are resolved before any containment/denylist check.
 */
export function validateMediaDeliveryPath(
	path: string,
	deps: PathValidationEnv = {},
): string | null {
	if (!path) return null;

	const candidate = stripPathWrappers(path);
	if (!candidate) return null;

	let expanded: string;
	try {
		expanded = expandUser(candidate, homeOf(deps));
	} catch {
		// Crafted ~\x00-style path skips itself rather than aborting the batch.
		return null;
	}
	if (!isAbsolute(expanded)) return null;

	// Docker agents emit container paths; translate before host-side checks.
	let resolved: string | null = translateDockerContainerMediaPath(
		expanded,
		envOf(deps),
	);
	if (resolved === null) {
		try {
			resolved = realpathSync(expanded); // strict=True analogue: throws when missing
		} catch {
			return null;
		}
	}

	if (!statIsRegularFile(resolved)) return null;

	// Cache/operator allowlist wins FIRST — unconditionally trusted regardless
	// of mode, even when a root sits under a denied prefix (operator's choice).
	for (const root of collectAllowedRoots(deps)) {
		let resolvedRoot = root;
		try {
			resolvedRoot = realpathSync(root);
		} catch {
			resolvedRoot = resolve(root);
		}
		if (isWithin(resolved, resolvedRoot)) return resolved;
	}

	if (!mediaDeliveryStrictMode(deps)) {
		if (pathUnderDeniedPrefix(resolved, deps)) return null;
		return resolved;
	}

	// Strict mode: recency-window trust for freshly produced files; system
	// paths and credential locations stay blocked even when "recent".
	const window = mediaDeliveryRecencySeconds(deps);
	if (window > 0 && !pathUnderDeniedPrefix(resolved, deps)) {
		if (fileIsRecentlyProduced(resolved, window, deps)) return resolved;
	}
	return null;
}

export interface MediaFileInput {
	path: string;
	isVoice: boolean;
}

/**
 * Drop unsafe MEDIA paths and normalize accepted paths
 * (base.py:filter_media_delivery_paths). One failed validation never cancels
 * siblings — the rest of the batch delivers.
 */
export function filterMediaDeliveryPaths<T extends MediaFileInput>(
	mediaFiles: readonly T[] | null | undefined,
	deps: PathValidationEnv = {},
): T[] {
	const safe: T[] = [];
	for (const entry of mediaFiles ?? []) {
		const safePath = validateMediaDeliveryPath(String(entry.path), deps);
		if (safePath) safe.push({ ...entry, path: safePath });
	}
	return safe;
}

export interface MediaPolicyConfig {
	gateway?: {
		strict?: boolean;
		media_delivery_allow_dirs?: string | readonly string[];
		trust_recent_files?: boolean;
	} | null;
}

/**
 * Bridge gateway media-policy settings into the environment
 * (media_policy.py:apply_media_policy_env). IDEMPOTENT and ENV-WINS: a
 * variable already present is never overwritten so operator shell exports keep
 * precedence. NEVER RAISES — a bridge failure falls back to validator defaults.
 */
export function applyMediaPolicyEnv(
	config: MediaPolicyConfig | null | undefined,
): void {
	try {
		const gatewayCfg = config?.gateway;
		if (gatewayCfg == null || typeof gatewayCfg !== "object") return;

		const strict = gatewayCfg.strict;
		if (strict != null && !process.env[MEDIA_DELIVERY_STRICT_ENV]) {
			process.env[MEDIA_DELIVERY_STRICT_ENV] = strict ? "1" : "0";
		}

		const allowDirs = gatewayCfg.media_delivery_allow_dirs;
		if (allowDirs && !process.env[MEDIA_DELIVERY_ALLOW_DIRS_ENV]) {
			let allowStr = "";
			if (typeof allowDirs === "string") allowStr = allowDirs;
			else
				allowStr = [...allowDirs]
					.filter(Boolean)
					.join(sep === "\\" ? ";" : ":");
			if (allowStr) process.env[MEDIA_DELIVERY_ALLOW_DIRS_ENV] = allowStr;
		}

		const trustRecent = gatewayCfg.trust_recent_files;
		if (trustRecent != null && !process.env[MEDIA_DELIVERY_TRUST_RECENT_ENV]) {
			process.env[MEDIA_DELIVERY_TRUST_RECENT_ENV] = trustRecent ? "1" : "0";
		}
	} catch {
		// Policy bridge must never break delivery.
	}
}

/** lstat-based helper exposed for tests/diagnostics: does `p` exist as a symlink? */
export function isSymlink(p: string): boolean {
	try {
		return lstatSync(p).isSymbolicLink();
	} catch {
		return false;
	}
}

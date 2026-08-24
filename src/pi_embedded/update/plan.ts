// plan.ts — update-plan stage: read-only inventory + deployment-kind
// classification. Runs BEFORE the refusal gates and refuses nothing itself
// (08 §5: "--plan is deployment-kind aware and runs BEFORE the refusal gates:
// on an image-managed install the plan itself reports 'not updatable in
// place' plus the right mechanism — strictly more useful than bare refusal").
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/config.py:detect_install_method
//       → classifyDeploymentKind — code-scoped `.install_method` stamp is
//         AUTHORITATIVE (each install tree carries its own truthful marker;
//         a home-scoped slot poisons shared homes), then `.git` presence,
//         then package-manager fallback, else unknown.
//   hermes_cli/update_inventory.py:UpdatePlan/RuntimeRecord/collect_runtime_inventory
//       → UpdatePlan / RuntimeRecord / buildUpdatePlan
//   hermes_cli/update_cmd.py:_read_project_version
//       → readTreeVersion — on-disk version file, never installed metadata;
//         after a pull only the tree reflects what was just pulled.
//   gateway/status.py:looks_like_gateway_command_line consumers
//       → live-runtime discovery uses the CANONICAL matcher (proc-matchers.ts),
//         never argv substrings (08 §9).

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

/** Deployment kinds for pi-gateway installs (08 §5 table). */
export type DeploymentKind = "git" | "zip-package" | "npm-class" | "unknown";

/**
 * Code-scoped install stamp (parity of `<install tree>/.install_method`).
 * The stamp names EVERY kind this module returns; an unrecognized stamp is
 * ignored (falls through to structural detection) rather than trusted.
 */
export const INSTALL_METHOD_STAMP = ".install_method";

const SUPPORTED_STAMP_METHODS: ReadonlySet<string> = new Set([
	"git",
	"zip-package",
	"npm-class",
	"unknown",
]);

export interface DeploymentClassification {
	kind: DeploymentKind;
	updatableInPlace: boolean;
	/** Human-readable referral for non-in-place kinds (08 §5). */
	updateMechanism: string;
	detail: string;
}

const GIT_CLASSIFICATION: DeploymentClassification = {
	kind: "git",
	updatableInPlace: true,
	updateMechanism: "pi update (in-place git pull)",
	detail: ".git checkout — the only in-place updatable kind (08 §5)",
};

function externalKind(
	kind: Exclude<DeploymentKind, "git">,
	mechanism: string,
	detail: string,
): DeploymentClassification {
	return {
		kind,
		updatableInPlace: false,
		updateMechanism: mechanism,
		detail,
	};
}

/**
 * Classify how pi-gateway was installed at `root`, from REAL tree state.
 *
 * Resolution order (parity of detect_install_method):
 *  1. code-scoped stamp `<root>/.install_method` — authoritative;
 *  2. `.git` presence (directory OR file — git worktrees use a .git FILE) → git;
 *  3. `package.json` presence without .git → npm-class;
 *  4. fallback → unknown (not updatable in place).
 *
 * A zip-package install self-identifies ONLY via the stamp baked by the
 * packaging step — there is no reliable structural signal for it.
 */
export function classifyDeploymentKind(root: string): DeploymentClassification {
	// 1. Code-scoped stamp — immune to shared-home poisoning (#34397 class).
	try {
		const stamped = readFileSync(join(root, INSTALL_METHOD_STAMP), "utf8")
			.trim()
			.toLowerCase();
		if (stamped === "zip-package") {
			return externalKind(
				"zip-package",
				"reinstall from a fresh package archive (in-place updates unsupported)",
				`${INSTALL_METHOD_STAMP} stamp names zip-package`,
			);
		}
		if (stamped === "npm-class") {
			return externalKind(
				"npm-class",
				"reinstall via the package manager (`npm install -g @earendil-works/pi-gateway` class)",
				`${INSTALL_METHOD_STAMP} stamp names npm-class`,
			);
		}
		if (stamped === "git") {
			return {
				...GIT_CLASSIFICATION,
				detail: `${INSTALL_METHOD_STAMP} stamp names git`,
			};
		}
		if (SUPPORTED_STAMP_METHODS.has(stamped)) {
			// "unknown" stamp — fall through to structural detection.
		}
	} catch {
		// No readable stamp — structural detection below.
	}

	// 2. .git presence (worktree-safe: .git may be a file).
	try {
		if (existsSync(join(root, ".git"))) return GIT_CLASSIFICATION;
	} catch {
		/* stat failure falls through */
	}

	// 3. Package-manager install without a checkout.
	try {
		if (existsSync(join(root, "package.json"))) {
			return externalKind(
				"npm-class",
				"reinstall via the package manager (`npm install -g @earendil-works/pi-gateway` class)",
				"package.json present without .git",
			);
		}
	} catch {
		/* stat failure falls through */
	}

	return externalKind(
		"unknown",
		"manual reinstall (install method could not be determined)",
		"no install stamp, no .git, no package.json",
	);
}

/** One running/expected runtime on this machine (08 §5 UpdatePlan schema). */
export interface RuntimeRecord {
	/** gateway | dashboard | serve */
	kind: string;
	/** "default", ... */
	profile: string;
	/** Live PID when known. */
	pid: number | null;
	/** systemd | launchd | desktop | manual */
	supervisor: string;
	/** Stamped running-code sha. */
	codeSha: string | null;
	codeVersion: string | null;
	/** Human-readable restart mechanism. */
	restartVia: string;
	detail: Record<string, unknown>;
}

/** Verified plan schema (08 §5). */
export interface UpdatePlan {
	installMethod: DeploymentKind;
	updatableInPlace: boolean;
	updateMechanism: string;
	/** Checkout HEAD pre-pull (expected_sha). */
	expectedSha: string | null;
	/** On-disk version pre-pull (expected_version). */
	expectedVersion: string | null;
	profiles: string[];
	runtimes: RuntimeRecord[];
	classificationDetail: string;
}

export interface GitIdentityProbe {
	/** `git rev-parse HEAD` at root, or null on any failure. */
	headSha(root: string): string | null;
}

/** Production git identity probe (spawn-free: reads .git/HEAD + ref file). */
export const headShaViaFiles: GitIdentityProbe["headSha"] = (
	root: string,
): string | null => {
	try {
		const head = readFileSync(join(root, ".git", "HEAD"), "utf8").trim();
		const match = /^ref: (.+)$/.exec(head);
		if (!match) return /^[0-9a-f]{40,64}$/.test(head) ? head : null;
		return (
			readFileSync(join(root, ".git", match[1] as string), "utf8").trim() ||
			null
		);
	} catch {
		return null;
	}
};

/**
 * Read the version field from the checkout's on-disk manifest — parity of
 * `_read_project_version`: after a pull, installed metadata still describes
 * the OLD version; the tree file is the only source reflecting what was just
 * pulled. Returns null on any failure (cosmetic, must never break an update).
 */
export function readTreeVersion(root: string): string | null {
	try {
		const parsed: unknown = JSON.parse(
			readFileSync(join(root, "package.json"), "utf8"),
		);
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			!Array.isArray(parsed)
		) {
			const version = (parsed as Record<string, unknown>)["version"];
			return typeof version === "string" ? version : null;
		}
		return null;
	} catch {
		return null;
	}
}

/** Inputs the plan stage needs beyond the tree itself. */
export interface PlanStageInputs {
	/** Install tree root (the checkout the updater would mutate). */
	treeRoot: string;
	/** Profile names this host serves (fleet scope). */
	profiles: readonly string[];
	/** Live runtimes discovered via canonical process matchers (08 §9). */
	runtimes?: readonly RuntimeRecord[];
	gitProbe?: GitIdentityProbe;
}

/**
 * Build the read-only update plan. Records into the receipt later; refuses
 * nothing yet — the refusal gate lives in the pipeline (08 §8 stage order).
 */
export function buildUpdatePlan(inputs: PlanStageInputs): UpdatePlan {
	const classification = classifyDeploymentKind(inputs.treeRoot);
	const probe: GitIdentityProbe = inputs.gitProbe ?? {
		headSha: headShaViaFiles,
	};
	const expectedSha =
		classification.kind === "git" ? probe.headSha(inputs.treeRoot) : null;
	return {
		installMethod: classification.kind,
		updatableInPlace: classification.updatableInPlace,
		updateMechanism: classification.updateMechanism,
		expectedSha,
		expectedVersion: readTreeVersion(inputs.treeRoot),
		profiles: [...inputs.profiles],
		runtimes: inputs.runtimes ? [...inputs.runtimes] : [],
		classificationDetail: classification.detail,
	};
}

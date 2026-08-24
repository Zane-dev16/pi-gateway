// run.ts — the ONLY place the update pipeline spawns child commands.
//
// Every update child (git, package managers) runs hangup-safe (DEC-042(b)):
// on POSIX the spawn argv is wrapped with `trap '' HUP; exec "$@"` so the
// final program inherits true SIG_IGN across exec, and callers classify
// failures against the LOGICAL argv — never the wrapper. Hermes anchor:
// hermes_cli/update_cmd.py:_cmd_update_impl runs git/pip under the same
// inherited-SIG_IGN guarantee via signal.signal(SIGHUP, SIG_IGN) in-process.

import { spawnSync } from "node:child_process";
import { wrapHangupSafe } from "./hangup.js";

export interface CommandResult {
	/** Exit status, or null when the child was killed by a signal / failed to spawn. */
	status: number | null;
	/** Signal that terminated the child, if any. */
	signal: NodeJS.Signals | null;
	stdout: string;
	stderr: string;
	/** spawn-level failure text (ENOENT etc.), null on a normal run. */
	spawnError: string | null;
	/** The logical argv this result corresponds to (classification input). */
	logicalArgv: readonly string[];
}

export type UpdateCommandRunner = (
	logicalArgv: readonly string[],
	cwd: string,
) => CommandResult;

export const nodeUpdateCommandRunner: UpdateCommandRunner = (
	logicalArgv,
	cwd,
) => {
	const wrapped = wrapHangupSafe(logicalArgv);
	try {
		const result = spawnSync(
			wrapped.spawnArgv[0] as string,
			[...wrapped.spawnArgv.slice(1)],
			{
				cwd,
				encoding: "utf8",
				maxBuffer: 64 * 1024 * 1024,
			},
		);
		return {
			status: result.status,
			signal: result.signal ?? null,
			stdout: typeof result.stdout === "string" ? result.stdout : "",
			stderr: typeof result.stderr === "string" ? result.stderr : "",
			spawnError: result.error ? result.error.message : null,
			logicalArgv,
		};
	} catch (error) {
		return {
			status: null,
			signal: null,
			stdout: "",
			stderr: "",
			spawnError: error instanceof Error ? error.message : String(error),
			logicalArgv,
		};
	}
};

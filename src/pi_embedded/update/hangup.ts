// hangup.ts — update-window hangup protection (08 §8 closing note, DEC-013 /
// DEC-042). Hermes installs SIGHUP→SIG_IGN around the whole update run so an
// accidental terminal loss cannot kill the updater or its pip/git children
// (POSIX preserves SIG_IGN across exec).
//
// Node has no sigaction surface, so the binding property is realized as two
// cooperating mechanisms (DEC-042):
//   (a) installHangupProtection() — a no-op SIGHUP listener absorbs hangups
//       inside THIS process for exactly the window between install/restore;
//   (b) hangupSafeArgvPrefix() — every update child command is spawned
//       through a POSIX `trap '' HUP; exec "$@"` wrapper, which sets TRUE
//       SIG_IGN inside the child before exec — inherited across exec, the
//       property Hermes relies on (`hermes_cli/main.py:cmd_update` wrap,
//       gateway/run.py::shutdown_signal_handler context).
//
// Failure classification always operates on the LOGICAL argv (what the caller
// asked to run), never the sh wrapper argv — preserving
// hermes_cli/update_cmd.py:_called_process_error_is_git semantics verbatim.
//
// SIGINT/SIGTERM are left alone as legitimate cancellation (08 §8).

/** No-op handler: absorbs SIGHUP without changing any other semantics. */
function absorbHangup(): void {
	/* intentional no-op — the disposition IS the protection */
}

export interface HangupGuard {
	/** Remove the absorption listener; default disposition applies again. */
	restore(): void;
}

/**
 * Install hangup protection for the update window. Idempotent per listener
 * identity: Node's EventEmitter dedupes identical function references, so
 * nested installs stay balanced under paired restore calls.
 */
export function installHangupProtection(): HangupGuard {
	if (process.platform === "win32") {
		// No SIGHUP on Windows — nothing to protect; guard is a no-op.
		return { restore: () => {} };
	}
	process.on("SIGHUP", absorbHangup);
	let restored = false;
	return {
		restore(): void {
			if (restored) return;
			restored = true;
			process.removeListener("SIGHUP", absorbHangup);
		},
	};
}

/**
 * POSIX prefix that turns any argv into a hangup-immune exec chain:
 * `sh -c "trap '' HUP; exec \"$@\"" sh <logical argv...>`. The trap sets
 * SIG_IGN (not a handler), which POSIX preserves across exec — so the final
 * program inherits true signal-ignore. Empty on Windows (nothing to wrap).
 */
export const HANGUP_SAFE_ARGV_PREFIX: readonly string[] =
	process.platform === "win32"
		? []
		: ["sh", "-c", "trap '' HUP; exec \"$@\"", "sh"];

export interface WrappedCommand {
	/** The argv to actually spawn (wrapper-prefixed on POSIX). */
	spawnArgv: readonly string[];
	/** What the caller asked to run — THE classification input. */
	logicalArgv: readonly string[];
}

export function wrapHangupSafe(logicalArgv: readonly string[]): WrappedCommand {
	if (HANGUP_SAFE_ARGV_PREFIX.length === 0) {
		return { spawnArgv: logicalArgv, logicalArgv };
	}
	return {
		spawnArgv: [...HANGUP_SAFE_ARGV_PREFIX, ...logicalArgv],
		logicalArgv,
	};
}

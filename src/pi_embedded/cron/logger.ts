// pi_embedded/cron/logger.ts — minimal logging seam for the ticker.
//
// Shape mirrors the lifecycle Logger structurally (pi_embedded cannot import
// runner internals); production default writes loud stderr lines so a
// degraded cron stage is never silent (01 §3.1).

export interface CronLogger {
	info(message: string, meta?: Record<string, unknown>): void;
	warn(message: string, meta?: Record<string, unknown>): void;
	error(message: string, meta?: Record<string, unknown>): void;
}

/** Loud stderr logger — the degrade-loudly default. */
export function stderrLogger(prefix = "[pi-gateway:cron]"): CronLogger {
	const line = (
		level: string,
		message: string,
		meta?: Record<string, unknown>,
	) => {
		let suffix = "";
		if (meta !== undefined) {
			try {
				suffix =
					" " +
					Object.entries(meta)
						.map(
							([k, v]) =>
								`${k}=${typeof v === "string" ? v : JSON.stringify(v)}`,
						)
						.join(" ");
			} catch {
				suffix = "";
			}
		}
		try {
			process.stderr.write(`${prefix} ${level} ${message}${suffix}\n`);
		} catch {
			/* stderr unavailable */
		}
	};
	return {
		info: (m, meta) => line("INFO", m, meta),
		warn: (m, meta) => line("WARN", m, meta),
		error: (m, meta) => line("ERROR", m, meta),
	};
}

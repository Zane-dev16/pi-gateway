// pi_gateway/setup/machine — guided setup state machine (DEC-081).
//
// Data shape first: the flow is a state machine over a per-platform var
// declaration table, not scattered conditionals. States are implicit in the
// driver's sequence (platform → prompt* → validate → confirm → write → done)
// with cancellation and validation failure as terminal arms. The table is
// SetupPlatformSpec (required + optional vars); the driver never imports a
// platform manifest so layering stays downward-only — entrypoints adapt
// PluginManifest rows into this shape.
//
// Secrets discipline: values flow only through `values` and the injected
// writer. `notify`/`log` receive platform names, var NAMES, and counts —
// never values. Validators MUST NOT echo values in their error strings;
// the telegram wiring returns generic transport messages for this reason.

export interface SetupVarSpec {
	readonly name: string;
	readonly description?: string | undefined;
	readonly password?: boolean | undefined;
	readonly optional?: boolean | undefined;
}

export interface SetupPlatformSpec {
	readonly name: string;
	readonly required: readonly SetupVarSpec[];
	readonly optional?: readonly SetupVarSpec[] | undefined;
}

/** TUI/chat-agnostic prompts. Implementations render via TUI dialogs. */
export interface SetupIO {
	selectPlatform(platforms: readonly string[]): Promise<string | undefined>;
	inputVar(platform: string, spec: SetupVarSpec): Promise<string | undefined>;
	confirmSave(platform: string, varNames: readonly string[]): Promise<boolean>;
	notify(message: string): void;
}

export type SetupValidation =
	| { readonly ok: true }
	| { readonly ok: false; readonly error: string };

/** Live check BEFORE any write (getMe and per-platform equivalents). */
export type SetupValidator = (
	platform: string,
	values: ReadonlyMap<string, string>,
) => Promise<SetupValidation>;

/** Profile .env write (0600, merge-preserving). */
export type SetupWriter = (
	home: string,
	values: ReadonlyMap<string, string>,
) => Promise<void> | void;

export interface SetupRequest {
	readonly home: string;
	readonly platforms: readonly SetupPlatformSpec[];
	readonly io: SetupIO;
	readonly validate: SetupValidator;
	readonly write: SetupWriter;
	readonly log?: ((message: string) => void) | undefined;
}

export type SetupResult =
	| { readonly ok: true; readonly platform: string; readonly vars: string[] }
	| { readonly ok: false; readonly error: string; readonly platform?: string }
	| {
			readonly ok: false;
			readonly cancelled: true;
			readonly platform?: string;
	  };

/** Required vars first, then optional — the single prompt order. */
export function allSetupVars(spec: SetupPlatformSpec): readonly SetupVarSpec[] {
	return [...spec.required, ...(spec.optional ?? [])];
}

function emit(log: SetupRequest["log"], io: SetupIO, message: string): void {
	try {
		log?.(message);
	} catch {
		/* logging never breaks setup */
	}
	try {
		io.notify(message);
	} catch {
		/* notify is best-effort */
	}
}

export async function runSetupFlow(req: SetupRequest): Promise<SetupResult> {
	if (req.platforms.length === 0) {
		return { ok: false, error: "no platforms available for setup" };
	}
	let spec: SetupPlatformSpec | undefined;
	if (req.platforms.length === 1) {
		spec = req.platforms[0] as SetupPlatformSpec | undefined;
		if (spec === undefined) {
			return { ok: false, error: "no platforms available for setup" };
		}
	} else {
		const names = req.platforms.map((p) => p.name);
		let picked: string | undefined;
		try {
			picked = await req.io.selectPlatform(names);
		} catch (err) {
			return {
				ok: false,
				error: `platform select failed: ${(err as Error)?.message ?? String(err)}`,
			};
		}
		if (picked === undefined) return { ok: false, cancelled: true };
		spec = req.platforms.find((p) => p.name === picked);
		if (spec === undefined) {
			return { ok: false, error: `unknown platform ${JSON.stringify(picked)}` };
		}
	}
	const platform = spec.name;
	emit(
		req.log,
		req.io,
		`setup ${platform}: prompting ${allSetupVars(spec).length} vars`,
	);

	const values = new Map<string, string>();
	for (const v of allSetupVars(spec)) {
		let answer: string | undefined;
		try {
			answer = await req.io.inputVar(platform, v);
		} catch (err) {
			return {
				ok: false,
				platform,
				error: `prompt for ${v.name} failed: ${(err as Error)?.message ?? String(err)}`,
			};
		}
		if (answer === undefined) return { ok: false, cancelled: true, platform };
		const trimmed = answer.trim();
		if (trimmed === "") {
			if (v.optional === true) continue;
			return { ok: false, platform, error: `${v.name} is required` };
		}
		values.set(v.name, trimmed);
	}

	emit(req.log, req.io, `setup ${platform}: validating ${values.size} vars`);
	let validation: SetupValidation;
	try {
		validation = await req.validate(platform, values);
	} catch (err) {
		return {
			ok: false,
			platform,
			error: `validation failed: ${(err as Error)?.message ?? String(err)}`,
		};
	}
	if (!validation.ok) {
		emit(req.log, req.io, `setup ${platform}: validation failed`);
		return { ok: false, platform, error: validation.error };
	}

	const names = [...values.keys()];
	let confirmed: boolean;
	try {
		confirmed = await req.io.confirmSave(platform, names);
	} catch (err) {
		return {
			ok: false,
			platform,
			error: `confirm failed: ${(err as Error)?.message ?? String(err)}`,
		};
	}
	if (!confirmed) return { ok: false, cancelled: true, platform };

	emit(req.log, req.io, `setup ${platform}: writing ${names.length} vars`);
	try {
		await req.write(req.home, values);
	} catch (err) {
		return {
			ok: false,
			platform,
			error: `write failed: ${(err as Error)?.message ?? String(err)}`,
		};
	}
	emit(req.log, req.io, `setup ${platform}: saved ${names.length} vars`);
	return { ok: true, platform, vars: names };
}

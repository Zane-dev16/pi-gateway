// pi_embedded/cron/delivery.ts — cron result delivery: wrap default,
// isolation default, mirror carve-out.
//
// Hermes anchors (READ-ONLY reference):
//   cron/scheduler.py:_deliver_result wrap block (~2896–2912)
//     → wrapCronResponse / WrapOptions
//   cron/scheduler.py:_cron_mirror_delivery_enabled → resolveMirrorEnabled
//   cron/scheduler.py mirror append via gateway/mirror.py:mirror_to_session
//                                             → MirrorAppender seam
//   cron/scheduler.py origin scoping (_target_matches_origin) → matchesOrigin
//
// Binding invariants (07 §5.2 delivery-isolation + mirror rows):
// - Deliveries originate in the cron job's OWN session; the wrapped payload
//   goes to platform targets and NOWHERE ELSE.
// - Header/footer wrap is ON by default (`Cronjob Response: <name>` frame);
//   `cron.wrap_response: false` opts out (clean output).
// - Mirroring into a conversation transcript exists ONLY as OPT-IN:
//   per-job `attach_to_session` wins, else global `cron.mirror_delivery`,
//   default OFF — and the mirrored copy is the CLEANED output (no header/
//   footer), appended as an assistant turn at a turn boundary so it stays
//   alternation- and cache-safe.
// - Mirror scope is the job's ORIGIN conversation only; fan-out targets that
//   are not the origin are never mirrored.

import type { CronDeliveryTarget } from "./store.js";

export interface CronWrapConfig {
	/** `cron.wrap_response` — default TRUE. */
	wrapResponse?: boolean;
}

/**
 * The exact Hermes frame: header + rule + blank line + content + footer with
 * the management hint. Byte-parity matters — users regex on this frame.
 */
export function wrapCronResponse(
	taskName: string,
	jobId: string,
	content: string,
): string {
	return (
		`Cronjob Response: ${taskName}\n` +
		`(job_id: ${jobId})\n` +
		`-------------\n\n` +
		`${content}\n\n` +
		`To stop or manage this job, send me a new message (e.g. "stop reminder ${taskName}").`
	);
}

/** Apply the wrap config to raw agent output. */
export function applyWrap(
	jobName: string,
	jobId: string,
	content: string,
	config?: CronWrapConfig,
): string {
	const wrap = config?.wrapResponse ?? true;
	return wrap ? wrapCronResponse(jobName, jobId, content) : content;
}

/**
 * CLEANED output for the optional transcript mirror: no cron header/footer,
 * trimmed (parity of `(mirror_text or "").strip()`).
 */
export function cleanedCronOutput(content: string): string {
	return content.trim();
}

export interface MirrorConfig {
	/** Global `cron.mirror_delivery` — default FALSE (isolation default). */
	mirrorDelivery?: boolean;
}

/**
 * Precedence (first decisive value wins, parity of _cron_mirror_delivery_enabled):
 *   1. per-job attach_to_session when it is a boolean;
 *   2. global cron.mirrorDelivery;
 *   3. false.
 */
export function resolveMirrorEnabled(
	job: { attachToSession?: boolean },
	config?: MirrorConfig,
): boolean {
	if (typeof job.attachToSession === "boolean") return job.attachToSession;
	return config?.mirrorDelivery ?? false;
}

/** Structural job fields delivery needs (avoids a store import cycle). */
export interface CronJobLike {
	id: string;
	name: string;
	/** Per-job mirror opt-in (07 §5.2 attach_to_session override). */
	attachToSession?: boolean;
}

/** True when the target IS the job's origin conversation. */
export function matchesOrigin(
	origin: CronDeliveryTarget | null | undefined,
	target: CronDeliveryTarget,
): boolean {
	if (origin === null || origin === undefined) return false;
	if (origin.platform !== target.platform) return false;
	if (origin.chatId !== target.chatId) return false;
	return (origin.threadId ?? null) === (target.threadId ?? null);
}

/** Transport seam: one platform target. Returns a delivery error or null. */
export interface DeliverySink {
	deliver(target: CronDeliveryTarget, content: string): Promise<string | null>;
}

/**
 * Transcript-append seam for the mirror carve-out (gateway/mirror.py::
 * mirror_to_session analogue): appends an ASSISTANT turn to the session's
 * durable transcript at a turn boundary. Production adapter binds
 * pi_state's appendMessage(role='assistant').
 */
export interface MirrorAppender {
	appendAssistantTurn(sessionId: string, text: string): Promise<boolean>;
}

export interface DeliverCronResultInput {
	job: CronJobLike;
	/** Authoritative final text from the agent turn. */
	outputText: string;
	targets: readonly CronDeliveryTarget[];
	sink: DeliverySink;
	wrap?: CronWrapConfig;
	mirror?: MirrorConfig;
	/** The conversation the job was created in; mirror scope root. */
	origin?: CronDeliveryTarget | null;
	/** Session id of the ORIGIN conversation's gateway session. */
	originSessionId?: string | null;
	appender?: MirrorAppender;
}

export interface DeliverCronResultReport {
	deliveryErrors: string[];
	/** True when the opt-in mirror actually appended the cleaned copy. */
	mirrored: boolean;
}

/**
 * Deliver one finished run. Default posture: wrapped output → targets only;
 * NO transcript mutation anywhere (isolation). Opt-in mirror: additionally
 * append the CLEANED output to the origin session transcript, only for
 * targets that match the origin, only when an appender + originSessionId
 * exist (a missing piece degrades to NOT mirroring — never to misdelivery).
 */
export async function deliverCronResult(
	input: DeliverCronResultInput,
): Promise<DeliverCronResultReport> {
	const deliveryContent = applyWrap(
		input.job.name,
		input.job.id,
		input.outputText,
		input.wrap,
	);
	const deliveryErrors: string[] = [];

	const mirrorEnabled = resolveMirrorEnabled(input.job, input.mirror);
	const canMirror =
		mirrorEnabled &&
		input.appender !== undefined &&
		input.originSessionId !== null &&
		input.originSessionId !== undefined &&
		input.origin !== null &&
		input.origin !== undefined;

	let mirrored = false;
	for (const target of input.targets) {
		const error = await input.sink.deliver(target, deliveryContent);
		if (error !== null && error !== undefined && error !== "") {
			deliveryErrors.push(error);
			continue;
		}
		if (canMirror && !mirrored && matchesOrigin(input.origin ?? null, target)) {
			const ok = await input.appender!.appendAssistantTurn(
				input.originSessionId!,
				cleanedCronOutput(input.outputText),
			);
			mirrored = ok;
		}
	}
	return { deliveryErrors, mirrored };
}

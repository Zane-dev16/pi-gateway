// TEST INFRASTRUCTURE — manual clock + fake sender for behavior contracts.
// No wall-clock reads anywhere: time moves only when a test advances it.

import type { GatewayClock } from "../clock.js";
import type { DeliveryOutcome, DeliveryRequest } from "../sender.js";

export class ManualClock implements GatewayClock {
	private currentSeconds: number;
	readonly sleepRequestsMs: number[] = [];

	constructor(startSeconds = 1_000_000) {
		this.currentSeconds = startSeconds;
	}

	nowSeconds(): number {
		return this.currentSeconds;
	}

	async sleepMs(_ms: number): Promise<void> {
		this.sleepRequestsMs.push(_ms);
		// Yield a MACROtask: a purely-microtask sleep would let a free-running
		// scheduler loop starve the event loop (stop() could never run).
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 0);
		});
	}

	advance(seconds: number): void {
		this.currentSeconds += seconds;
	}

	set(seconds: number): void {
		this.currentSeconds = seconds;
	}
}

/** Deterministic sender: scripted outcomes + call journal. */
export class ScriptedSender {
	readonly calls: DeliveryRequest[] = [];
	private script: Array<(req: DeliveryRequest) => DeliveryOutcome> = [];
	private defaultOutcome: (req: DeliveryRequest) => DeliveryOutcome = () => ({
		ok: true,
	});

	/** Queue outcomes in order: "ok", "fail", "throw", or a custom fn. */
	queue(
		...steps: Array<"ok" | "fail" | ((req: DeliveryRequest) => DeliveryOutcome)>
	): this {
		this.script.push(
			...steps.map((s) =>
				typeof s === "function"
					? s
					: s === "ok"
						? (): DeliveryOutcome => ({ ok: true })
						: (): DeliveryOutcome => ({ ok: false, error: "scripted failure" }),
			),
		);
		return this;
	}

	alwaysFail(error = "scripted failure"): this {
		this.defaultOutcome = () => ({ ok: false, error });
		return this;
	}

	async send(req: DeliveryRequest): Promise<DeliveryOutcome> {
		this.calls.push(req);
		const next = this.script.shift();
		if (next) return next(req);
		return this.defaultOutcome(req);
	}

	get callCount(): number {
		return this.calls.length;
	}

	/** The instance as a DeliverySender (stable binding for seams). */
	bind(): (req: DeliveryRequest) => Promise<DeliveryOutcome> {
		return (req) => this.send(req);
	}
}

// pi_platforms/conformance/wire — the IN-PROCESS fake platform server
// (04 §8: rows run headless against fake platform servers; NO external
// network). Pure in-memory transport with SCRIPTED behaviors and full wire
// capture — the egress-capture half of the fixture harness.

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";

export interface WireSendOp {
	op: "send" | "edit" | "draft" | "seal" | "rich";
	chatId: string;
	content: string;
	metadata: Metadata;
	messageId?: string | undefined;
	draftId?: number | undefined;
	final?: boolean | undefined;
	/** Server wall-clock receipt order (monotonic). */
	seq: number;
}

export type WireBehavior =
	| { kind: "ok" }
	| { kind: "fail"; error: string; retryable?: boolean; retryAfter?: number }
	| { kind: "timeout" };

/**
 * Scripted fake platform endpoint. Behaviors are consumed FIFO per op kind;
 * an exhausted script defaults to ok.
 */
export class FakePlatformWire {
	readonly ops: WireSendOp[] = [];
	private scripts = new Map<string, WireBehavior[]>();
	private seqCounter = 0;

	/** Program the next N behaviors for an op kind ("send", "edit", …). */
	script(opKind: WireSendOp["op"], ...behaviors: WireBehavior[]): void {
		const queue = this.scripts.get(opKind) ?? [];
		queue.push(...behaviors);
		this.scripts.set(opKind, queue);
	}

	/** Whether an explicit behavior script exists for an op kind.
	 * (Consumed entries still count — a drained script means the endpoint
	 * was deliberately programmed.) */
	hasScript(opKind: WireSendOp["op"]): boolean {
		return (this.scripts.get(opKind)?.length ?? 0) > 0;
	}

	reset(): void {
		this.ops.length = 0;
		this.scripts.clear();
	}

	sendsOf(chatId?: string): WireSendOp[] {
		return this.ops.filter(
			(o) => o.op === "send" && (chatId === undefined || o.chatId === chatId),
		);
	}

	editsOf(chatId?: string): WireSendOp[] {
		return this.ops.filter(
			(o) => o.op === "edit" && (chatId === undefined || o.chatId === chatId),
		);
	}

	draftsOf(chatId?: string): WireSendOp[] {
		return this.ops.filter(
			(o) => o.op === "draft" && (chatId === undefined || o.chatId === chatId),
		);
	}

	private next(opKind: WireSendOp["op"]): WireBehavior {
		const queue = this.scripts.get(opKind);
		if (queue === undefined || queue.length === 0) return { kind: "ok" };
		return queue.shift() as WireBehavior;
	}

	private record(op: Omit<WireSendOp, "seq">): {
		result: SendResult;
		behavior: WireBehavior;
	} {
		const behavior = this.next(op.op);
		this.seqCounter += 1;
		const messageId = `wire-${this.seqCounter}`;
		this.ops.push({ ...op, seq: this.seqCounter });
		return { result: this.resultFor(behavior, messageId), behavior };
	}

	private resultFor(behavior: WireBehavior, messageId: string): SendResult {
		switch (behavior.kind) {
			case "ok":
				return { success: true, messageId };
			case "fail":
				return {
					success: false,
					error: behavior.error,
					retryable: behavior.retryable,
					retryAfter: behavior.retryAfter,
				};
			case "timeout":
				return { success: false, error: "request timed out", retryable: false };
		}
	}

	async transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const { result } = this.record({ op: "send", chatId, content, metadata });
		return result;
	}

	async transmitEdit(
		chatId: string,
		_messageId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const { result } = this.record({ op: "edit", chatId, content, metadata });
		return result;
	}

	async transmitDraft(
		chatId: string,
		draftId: number,
		content: string,
		final: boolean,
		metadata: Metadata,
	): Promise<SendResult> {
		const { result } = this.record({
			op: final ? "seal" : "draft",
			chatId,
			draftId,
			final,
			content,
			metadata,
		});
		return result;
	}

	async transmitRich(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const { result } = this.record({ op: "rich", chatId, content, metadata });
		return result;
	}
}

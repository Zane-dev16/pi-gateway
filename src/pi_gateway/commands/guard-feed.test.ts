// Guard L2 integration: the guards' busy machinery fed FROM this registry —
// byte-stable catch-all, per-command reject overrides honored through the
// SAME resolver the runner uses (resolveBusyDispatch over our projected
// rows). Tests import the sibling guards module deliberately: this is the
// wiring proof (DEC-005), not a layering pattern for src code.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "../guards/events.js";
import {
	RunnerBusyGuard,
	type RunnerBusyOptions,
	catchAllBusyRejectText,
	resolveBusyDispatch,
	buildCommandLookup,
} from "../guards/index.js";
import type { CommandDef } from "./command-def.js";
import { toGuardRows } from "./busy-resolver.js";
import { CommandRegistry } from "./registry.js";

const row = (name: string, extra: Partial<CommandDef>): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Session",
	...extra,
});

const REGISTRY = new CommandRegistry([
	row("new", {
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "new",
	}),
	row("stop", { busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" }),
	row("queue", {
		aliases: ["q"],
		argsHint: "<prompt>",
		busyPolicy: "dispatch",
		busyHandler: "queue",
	}),
	row("model", { busyPolicy: "reject", busyHandler: "model" }),
	row("title", {}),
]);

function guardFromRegistry(): RunnerBusyGuard {
	const options: RunnerBusyOptions = {
		registry: toGuardRows(REGISTRY.rows()),
		slots: new Map<string, IncomingEvent>(),
	};
	return new RunnerBusyGuard(options);
}

const textEvent = (text: string): IncomingEvent => ({
	messageType: "text",
	text,
});

describe("guard L2 machinery fed from THE registry (DEC-005)", () => {
	it("unknown '/foo' resolves to null mid-run → caller queues it as TEXT", async () => {
		const guard = guardFromRegistry();
		await expect(
			guard.dispatchBusySlashCommand("foo", textEvent("/foo"), "k"),
		).resolves.toBeNull();
		expect(guard.shouldBypassActiveSession("/foo")).toBe(false);
	});

	it("catch-all reject is BYTE-STABLE through the shared resolver (/title)", async () => {
		const guard = guardFromRegistry();
		await expect(
			guard.dispatchBusySlashCommand("title", textEvent("/title x"), "k"),
		).resolves.toBe(catchAllBusyRejectText("title"));
		await expect(
			guard.dispatchBusySlashCommand("title", textEvent("/title x"), "k"),
		).resolves.toBe(
			"⏳ Agent is running — `/title` can't run mid-turn. Wait for the current response or `/stop` first.",
		);
	});

	it("per-command override (/model) honored via busy_handler through the SAME resolver", () => {
		const dispatch = resolveBusyDispatch(
			buildCommandLookup(toGuardRows(REGISTRY.rows())),
			"/model",
		);
		expect(dispatch?.kind).toBe("reject");
		expect(dispatch?.rejectText).toBe(
			"Agent is running — wait or /stop first, then switch models.",
		);
		expect(dispatch?.handlerKey).toBe("model");
	});

	it("alias routing matches ('reset' → /new interrupt class) on registry rows alone", () => {
		const guard = guardFromRegistry();
		expect(guard.isInterruptThenDispatch("reset")).toBe(true);
		expect(guard.resolve("q")?.name).toBe("queue");
	});
});

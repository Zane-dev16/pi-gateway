// §1.6 injection contract: inject-first ordering, deferred-dirty-until-
// boundary, --now immediate opt-in, non-mutating no-op, unknown → text.

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import {
	type CacheInvalidationHooks,
	DeferredInvalidationBuffer,
	runSlashInjection,
} from "./inject.js";
import { CommandRegistry } from "./registry.js";

const row = (name: string, extra: Partial<CommandDef>): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Tools & Skills",
	...extra,
});

const REGISTRY = new CommandRegistry([
	row("skills", { argsHint: "<install|list> [id]" }),
	row("toolset", {}),
	row("help", {}),
]);

const resolve = (name: string | null | undefined) => REGISTRY.resolve(name);

function recorder() {
	const events: string[] = [];
	return {
		events,
		inject: (m: { text: string }) => events.push(`inject:${m.text}`),
	};
}

describe("runSlashInjection — 07 §1.6 cache-awareness", () => {
	it("mutating execution DEFERS by default: mark queues but is NOT applied until boundary drain", () => {
		const rec = recorder();
		const buffer = new DeferredInvalidationBuffer();
		const result = runSlashInjection(resolve, "/skills install pdf", {
			inject: rec.inject,
			mutatesPromptState: true,
			pendingDeferred: buffer,
		});

		expect(result.kind).toBe("injected");
		if (result.kind !== "injected") return;
		expect(result.message).toEqual({
			role: "user",
			origin: "slash-execution",
			command: "skills",
			text: "/skills install pdf",
		});
		// Injected FIRST; the dirty mark only QUEUED afterwards.
		expect(rec.events).toEqual(["inject:/skills install pdf"]);
		expect(buffer.size).toBe(1);
		expect(result.invalidation).toBe("deferred");

		// Turn/session boundary: the drain applies the marks in order.
		const drained = buffer.drain();
		expect(drained).toEqual([
			{
				kind: "deferred",
				command: "skills",
				injectedText: "/skills install pdf",
				reason: "prompt-state-mutation",
			},
		]);
		expect(buffer.size).toBe(0);
	});

	it("--now opts into IMMEDIATE invalidation and the flag never reaches the injected text", () => {
		const rec = recorder();
		const nowSignals: string[] = [];
		const buffer = new DeferredInvalidationBuffer();
		const hooks: CacheInvalidationHooks = {
			invalidateNow: (s) => nowSignals.push(s.command),
		};
		const result = runSlashInjection(resolve, "/skills install pdf --now", {
			inject: rec.inject,
			hooks,
			mutatesPromptState: true,
			pendingDeferred: buffer,
		});
		expect(result.kind).toBe("injected");
		if (result.kind !== "injected") return;
		expect(result.message.text).toBe("/skills install pdf");
		expect(result.argsWithoutFlags).toBe("install pdf");
		expect(result.nowRequested).toBe(true);
		expect(result.invalidation).toBe("immediate");
		expect(nowSignals).toEqual(["skills"]);
		expect(buffer.size).toBe(0); // nothing deferred behind --now
		// Ordering: injection strictly before invalidation.
		expect(rec.events[0]).toBe("inject:/skills install pdf");
	});

	it("--now WITHOUT an immediate sink degrades to the deferred queue (never silently dropped)", () => {
		const buffer = new DeferredInvalidationBuffer();
		const result = runSlashInjection(resolve, "/toolset add web --now", {
			inject: () => {},
			mutatesPromptState: true,
			pendingDeferred: buffer,
		});
		expect(result.kind).toBe("injected");
		if (result.kind === "injected")
			expect(result.invalidation).toBe("deferred");
		expect(buffer.size).toBe(1);
	});

	it("hooks.deferInvalidation intercepts the queue when provided", () => {
		const seen: string[] = [];
		const result = runSlashInjection(resolve, "/toolset remove web", {
			inject: () => {},
			hooks: { deferInvalidation: (s) => seen.push(s.command) },
			mutatesPromptState: true,
		});
		expect(seen).toEqual(["toolset"]);
		if (result.kind === "injected")
			expect(result.invalidation).toBe("deferred");
	});

	it("non-mutating executions never invalidate anything", () => {
		const buffer = new DeferredInvalidationBuffer();
		let invalidated = false;
		const result = runSlashInjection(resolve, "/help", {
			inject: () => {},
			hooks: {
				deferInvalidation: () => {
					invalidated = true;
				},
				invalidateNow: () => {
					invalidated = true;
				},
			},
			pendingDeferred: buffer,
		});
		expect(invalidated).toBe(false);
		expect(buffer.size).toBe(0);
		if (result.kind === "injected") expect(result.invalidation).toBe("none");
	});
});

describe("text fallbacks preserve original bytes (07 §2)", () => {
	it("unknown command falls back to the raw line as text", () => {
		const result = runSlashInjection(resolve, "/frobnicate everything", {
			inject: () => {},
		});
		expect(result).toEqual({
			kind: "text-fallback",
			reason: "unknown-command",
			text: "/frobnicate everything",
			invalidation: "none",
		});
	});

	it("plain text falls back without consulting the registry", () => {
		const result = runSlashInjection(resolve, "just words", {
			inject: () => {},
		});
		expect(result).toEqual({
			kind: "text-fallback",
			reason: "no-slash",
			text: "just words",
			invalidation: "none",
		});
	});
});

describe("deferred buffer discipline", () => {
	it("drain returns marks in arrival order across multiple mutating executions", () => {
		const buffer = new DeferredInvalidationBuffer();
		const inject = () => {};
		runSlashInjection(resolve, "/skills install a", {
			inject,
			mutatesPromptState: true,
			pendingDeferred: buffer,
		});
		runSlashInjection(resolve, "/skills remove b", {
			inject,
			mutatesPromptState: true,
			pendingDeferred: buffer,
		});
		expect(buffer.drain().map((s) => s.injectedText)).toEqual([
			"/skills install a",
			"/skills remove b",
		]);
		expect(buffer.drain()).toEqual([]);
	});
});

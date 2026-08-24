// §6 Kanban contracts — the TENANT is a SOFT namespace within a board.
//
// Collision-handling table (tenant.ts prefix policy). Soft means tenants
// never gate visibility or dispatch; the policy only guarantees that derived
// keys (workspace paths, memory keys) of different tenants cannot collide.

import { describe, expect, it } from "vitest";

import {
	KANBAN_TENANT_ENV,
	namespacedKey,
	normalizeTenant,
	workerTenantEnv,
} from "./tenant.js";

describe("tenant soft-namespace collision table", () => {
	const cases: Array<{
		name: string;
		a: string | null;
		b: string | null;
		base: string;
		expectSameNamespace: boolean;
	}> = [
		{
			name: "same tenant ⇒ shared namespace (by design)",
			a: "acme",
			b: "acme",
			base: "task-1",
			expectSameNamespace: true,
		},
		{
			name: "different tenants ⇒ disjoint namespaces",
			a: "acme",
			b: "globex",
			base: "task-1",
			expectSameNamespace: false,
		},
		{
			name: "tenant vs absent ⇒ disjoint (default namespace is its own)",
			a: "acme",
			b: null,
			base: "task-1",
			expectSameNamespace: false,
		},
		{
			name: "both absent ⇒ same default namespace",
			a: null,
			b: null,
			base: "task-1",
			expectSameNamespace: true,
		},
		{
			name: "case differences normalize into ONE namespace (no silent fork)",
			a: "Acme",
			b: "acme",
			base: "task-1",
			expectSameNamespace: true,
		},
		{
			name: "whitespace differences normalize into ONE namespace",
			a: " acme ",
			b: "acme",
			base: "task-1",
			expectSameNamespace: true,
		},
		{
			name: "empty-string ≡ absent (schema-default empty means no tenant)",
			a: "",
			b: null,
			base: "task-1",
			expectSameNamespace: true,
		},
	];

	for (const c of cases) {
		it(c.name, () => {
			const keyA = namespacedKey(c.a, c.base);
			const keyB = namespacedKey(c.b, c.base);
			expect(keyA === keyB).toBe(c.expectSameNamespace);
		});
	}

	it("prefix shape is '<tenant>/' — a single path-segment boundary", () => {
		expect(namespacedKey("acme", "ws-42")).toBe("acme/ws-42");
		expect(namespacedKey(null, "ws-42")).toBe("ws-42");
	});

	it("distinct bases under one tenant never collide with another tenant's identical base", () => {
		// The actual collision the policy exists to kill: two tenants both
		// derive a workspace/memory key from base "scratch".
		expect(namespacedKey("acme", "scratch")).not.toBe(
			namespacedKey("globex", "scratch"),
		);
	});
});

describe("tenant label normalization + worker env", () => {
	it("normalizes to lowercase/trimmed or null", () => {
		expect(normalizeTenant("  ACME ")).toBe("acme");
		expect(normalizeTenant("")).toBeNull();
		expect(normalizeTenant(null)).toBeNull();
		expect(normalizeTenant(undefined)).toBeNull();
	});

	it("worker env carries the tenant ONLY when present (parity _default_spawn)", () => {
		expect(workerTenantEnv("acme")).toEqual({ [KANBAN_TENANT_ENV]: "acme" });
		expect(workerTenantEnv(null)).toEqual({});
	});
});

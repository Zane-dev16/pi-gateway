// tenant.ts — the SOFT namespace within a board (07 §6).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/kanban_db.py:tasks.tenant column   → KanbanCard.tenant
//   hermes_cli/kanban_db.py:_default_spawn:
//     env["HERMES_TENANT"] = task.tenant          → workerTenantEnv
//
// Soft means: a tenant NEVER gates board visibility or dispatch eligibility.
// Cards from every tenant on the board flow through the same reclaim →
// promote → claim pipeline; the tenant only shapes derived keys (workspace
// paths, memory keys, log dirs) so same-named artifacts of different tenants
// cannot collide. Two tenants on one board are expected steady state, not an
// isolation failure.
//
// Prefix policy (collision table, see tenant.test.ts): any derived key that
// would otherwise be tenant-ambiguous is prefixed `<tenant>/` when a tenant
// is present and left bare when absent. Normalization mirrors the board-slug
// normalizer (lowercase + trim) so "Acme" and "acme" resolve to ONE namespace
// instead of silently forking it; empty/whitespace ≡ absent. Same tenant ⇒
// same namespace by design (shared workspace is the point); different tenants
// ⇒ disjoint namespaces; tenant vs absent ⇒ disjoint.

/** The worker-facing tenant env var (parity HERMES_TENANT). */
export const KANBAN_TENANT_ENV = "HERMES_TENANT";

/**
 * Normalize a tenant label: trim + lowercase; null for empty/whitespace.
 * Deliberately lenient (unlike board slugs): a tenant is a soft label, never
 * a security boundary, so odd characters degrade to a usable prefix rather
 * than raising.
 */
export function normalizeTenant(
	tenant: string | null | undefined,
): string | null {
	if (tenant === null || tenant === undefined) return null;
	const s = String(tenant).trim().toLowerCase();
	return s === "" ? null : s;
}

/**
 * Derive a namespaced key for `base` under `tenant`.
 *
 *   tenant=null      → base            (default namespace)
 *   tenant="acme"    → "acme/base"
 *
 * The separator is "/" so workspace-path derivation stays a single path
 * segment boundary (07 §6 "workspace-path … isolation").
 */
export function namespacedKey(
	tenant: string | null | undefined,
	base: string,
): string {
	const t = normalizeTenant(tenant);
	return t === null ? base : `${t}/${base}`;
}

/**
 * Worker env injection for the tenant label (parity _default_spawn). Absent
 * tenant injects nothing — Hermes only sets the var when task.tenant truthy.
 */
export function workerTenantEnv(
	tenant: string | null | undefined,
): Record<string, string> {
	const t = normalizeTenant(tenant);
	return t === null ? {} : { [KANBAN_TENANT_ENV]: t };
}

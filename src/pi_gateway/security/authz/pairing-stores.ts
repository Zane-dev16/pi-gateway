// authz/pairing-stores — per-profile pairing-store selection (multiplex
// isolation, 06 §2.4 "Multiplex isolation" row).
//
// Port of gateway/authz_mixin.py::_pairing_store_for: in a multiplexing
// gateway each profile owns its own pairing whitelist so isolation is
// preserved. A source with no profile (single-profile gateway, or a path that
// hasn't stamped profile yet) or an UNREGISTERED profile falls back to the
// global default store so existing behavior is preserved.
//
// Isolation contract (06 §10 "Pairing multiplex isolation"): profile A's
// approval NEVER admits profile B's sender. The fail direction here is
// fallback-to-global for unknown profiles — the same fail-open shape as the
// authz adapter-registry refusal would be if it consulted the default store
// first; selection therefore consults ONLY the requested profile's store when
// one is registered, and the global store only when nothing else exists.

import type Database from "better-sqlite3";

import { PairingStore, type PairingStoreOptions } from "./pairing.js";

/**
 * Registry of per-profile PairingStores.
 *
 * Hermes splits stores per profile HOME (separate JSON dirs under
 * `<root>/profiles/<name>`); Pi Gateway keeps ONE SUBSTRATE PER PROFILE
 * (pi_state/store.ts: "One SQLite substrate per profile") — so profile
 * isolation is structural: each registered profile contributes ITS OWN
 * state.db connection, exactly mirroring the per-profile HERMES_HOME layout.
 * The default/global store rides the default profile's db.
 */
export class PairingStores {
	private readonly global: PairingStore;
	private readonly perProfile = new Map<string, PairingStore>();

	constructor(globalDb: Database.Database, opts: PairingStoreOptions = {}) {
		this.global = new PairingStore(globalDb, opts);
	}

	/** The global default store (unstamped/unregistered sources). */
	default(): PairingStore {
		return this.global;
	}

	/**
	 * Register the store for a named profile over THAT profile's own db
	 * (idempotent: re-registration with the same name is a no-op — first
	 * registration wins, matching adapter-registry refusal semantics).
	 */
	forProfile(
		name: string,
		profileDb: Database.Database,
		opts: PairingStoreOptions = {},
	): PairingStore {
		const existing = this.perProfile.get(name);
		if (existing !== undefined) return existing;
		const created = new PairingStore(profileDb, opts);
		this.perProfile.set(name, created);
		return created;
	}

	hasProfile(name: string): boolean {
		return this.perProfile.has(name);
	}

	/**
	 * Selection parity of _pairing_store_for(source): the source's registered
	 * profile store wins; anything else falls back to the global default.
	 */
	select(profile: string | null | undefined): PairingStore {
		const name = (profile ?? "").trim();
		if (name !== "" && this.perProfile.has(name)) {
			return this.perProfile.get(name) as PairingStore;
		}
		return this.global;
	}
}

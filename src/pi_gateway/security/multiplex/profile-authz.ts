// multiplex/profile-authz — per-profile pairing + authz store instances and
// the profile-aware adapter-view router (06 §4 build item 2 + the §4
// adapter-lookup refusal).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/authz_mixin.py::_pairing_store_for → ProfileAuthzIsolation.pairingStoreFor
//     ("in a multiplexing gateway each profile owns its own pairing whitelist")
//   gateway/authz_mixin.py::_authorization_adapter → ProfileAdapterViews.resolve
//     (§4: "a stamped secondary profile with no registry entry must NOT fall
//      back to the default profile's adapter — replies would leave through
//      the wrong bot")
//
// Store instances consume the authz module's factory types directly
// (PairingStores/PairingStore — one SUBSTRATE PER PROFILE, pi_state/store.ts),
// so profiles share NO mutable state: each registered profile contributes its
// own connection, mirroring Hermes' per-profile HERMES_HOME layout. The
// DEFAULT/global store rides the default profile's db.
//
// Deliberate asymmetry (both directions pinned by tests):
//   · PAIRING stores follow _pairing_store_for exactly — a stamped but
//     UNREGISTERED profile falls back to the GLOBAL default store (existing
//     behavior preserved; see authz/pairing-stores.ts);
//   · ADAPTER views REFUSE that fallback (06 §4 _authorization_adapter): a
//     stamped profile with no registry entry yields undefined — never the
//     default profile's view.

import type Database from "better-sqlite3";

import {
	PairingStores,
	type AdapterAuthzView,
	type PairingStore,
	type PairingStoreOptions,
} from "../authz/index.js";
import type { AuthzSource } from "../authz/index.js";

/** Opens a profile's OWN state.db connection (injected — sync by contract). */
export type OpenProfileDb = () => Database.Database;

export interface ProfileAuthzIsolationOptions {
	/**
	 * The default/global profile's already-open connection (its state.db).
	 * Unstamped sources and unregistered profiles resolve here.
	 */
	globalDb: Database.Database;
	/**
	 * Opens a secondary profile's own connection on first registration.
	 * Called at most once per profile (first registration wins). Production
	 * wiring opens `<home>/state.db` through pi_state; tests inject
	 * better-sqlite3 handles directly.
	 */
	openProfileDb?: OpenProfileDb | undefined;
	/**
	 * Per-profile PairingStore tuning (clock/randomness/mirror injection).
	 * Evaluated ONCE at registration time; static object or per-profile fn.
	 */
	pairingOptions?:
		| PairingStoreOptions
		| ((profile: string) => PairingStoreOptions)
		| undefined;
}

function optionsFor(
	opts: ProfileAuthzIsolationOptions,
	profile: string,
): PairingStoreOptions {
	if (typeof opts.pairingOptions === "function")
		return opts.pairingOptions(profile);
	return opts.pairingOptions ?? {};
}

/**
 * Owns ONE PairingStores registry over per-profile connections. Lazy,
 * idempotent per profile (first registration wins — matching the adapter
 * registry refusal semantics), and closes every connection it opened.
 */
export class ProfileAuthzIsolation {
	private readonly stores: PairingStores;
	private readonly homes = new Map<string, string>();
	private readonly opened: Database.Database[] = [];

	constructor(private readonly opts: ProfileAuthzIsolationOptions) {
		this.stores = new PairingStores(opts.globalDb);
	}

	/**
	 * Register `profile` over ITS OWN db (lazily opened via the injected
	 * opener). Idempotent: re-registration returns the existing store without
	 * opening anything. Requires an opener when the profile is new.
	 */
	registerProfile(profile: string, home?: string): PairingStore {
		const name = profile.trim();
		if (this.stores.hasProfile(name)) return this.stores.select(name);
		if (this.opts.openProfileDb === undefined) {
			throw new Error(
				`ProfileAuthzIsolation: no openProfileDb configured — cannot open a ` +
					`substrate for profile ${JSON.stringify(name)}.`,
			);
		}
		const db = this.opts.openProfileDb();
		this.opened.push(db);
		if (home !== undefined && home.trim() !== "") this.homes.set(name, home);
		return this.stores.forProfile(name, db, optionsFor(this.opts, name));
	}

	hasProfile(profile: string): boolean {
		return this.stores.hasProfile(profile.trim());
	}

	homeOf(profile: string): string | undefined {
		return this.homes.get(profile.trim());
	}

	/**
	 * Selection parity of `_pairing_store_for(source)`: the source's stamped,
	 * REGISTERED profile store wins; unstamped or unregistered sources fall
	 * back to the global default store. Feed straight into decision-chain
	 * deps.pairingStoreFor.
	 */
	pairingStoreFor(source: Pick<AuthzSource, "profile">): PairingStore {
		return this.stores.select(source.profile ?? null);
	}

	/** The default (global/default-profile) store itself. */
	defaultStore(): PairingStore {
		return this.stores.default();
	}

	/** Close every connection THIS registry opened (never the injected global). */
	close(): void {
		for (const db of this.opened) {
			try {
				db.close();
			} catch {
				/* already closed — idempotent teardown */
			}
		}
		this.opened.length = 0;
	}
}

/**
 * Per-profile ADAPTER registries (06 §4 isolation checklist row) with the
 * Fail-closed lookup rule: a stamped secondary profile resolves ONLY its own
 * registrations — never the default profile's entry (`_authorization_adapter`
 * refusal; a fallback would egress through the wrong bot). Views are plain
 * AdapterAuthzView records consumed by the decision chain's deps.adapterView.
 */
export class ProfileAdapterViews {
	private readonly defaultViews = new Map<string, AdapterAuthzView>();
	private readonly perProfile = new Map<
		string,
		Map<string, AdapterAuthzView>
	>();

	/** Register the DEFAULT profile's view for a platform (last wins). */
	registerDefault(platform: string, view: AdapterAuthzView): void {
		this.defaultViews.set(platform.trim(), view);
	}

	/** Register a view under a SPECIFIC profile (own map — no default mixing). */
	registerProfile(
		profile: string,
		platform: string,
		view: AdapterAuthzView,
	): void {
		const name = profile.trim();
		let views = this.perProfile.get(name);
		if (views === undefined) {
			views = new Map<string, AdapterAuthzView>();
			this.perProfile.set(name, views);
		}
		views.set(platform.trim(), view);
	}

	hasProfile(profile: string): boolean {
		return this.perProfile.has(profile.trim());
	}

	/**
	 * Resolve the LIVE adapter view for (platform, stamped profile).
	 *   unstamped            → the default profile's view (single-profile path)
	 *   stamped + registered → THAT profile's own view (missing platform ⇒
	 *                          undefined — still no cross-profile borrow)
	 *   stamped + unregistered → undefined ALWAYS (the §4 refusal)
	 */
	resolve(
		platform: string,
		profile: string | null | undefined,
	): AdapterAuthzView | undefined {
		const p = String(platform ?? "").trim();
		const name = (profile ?? "").trim();
		if (name === "") return this.defaultViews.get(p);
		return this.perProfile.get(name)?.get(p);
	}
}

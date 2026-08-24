// service-entry.ts — the STRUCTURAL shape pi_embedded services expose for
// optional-stage registration (DEC-040).
//
// Layering (01 §5.3): pi_embedded may NOT import runner internals
// (pi_gateway/lifecycle/**), so this module MIRRORS — by value shape, never
// by import — the GatewayLifecycle `ServiceEntry` / `ServiceStartOutcome`
// contract. TypeScript structural typing makes a conforming object
// registrable at the composition root without either layer importing the
// other. Any change on the lifecycle side must be reflected HERE and in the
// stage-entry tests that pin each service's mapping.

/** Mirrors GatewayLifecycle.ServiceHandle structurally (name + optional stop). */
export interface EmbeddedServiceHandle {
	name: string;
	stop?: () => Promise<void>;
}

/**
 * Mirrors GatewayLifecycle.ServiceStartOutcome structurally (DEC-040):
 *   ok=true                          ⇒ started (handle optional)
 *   ok=false + degraded=true         ⇒ loud per-service DEGRADE, startup continues
 *   ok=false + degraded absent/false ⇒ disabled/skipped — loud, NOT a failure
 */
export interface EmbeddedServiceOutcome {
	ok: boolean;
	degraded?: boolean;
	reason?: string;
	handle?: EmbeddedServiceHandle;
}

/**
 * Mirrors GatewayLifecycle.ServiceEntry structurally (DEC-040). The zero-arg
 * start() remains assignable to the lifecycle's start(ctx): extra engine
 * parameters are ignored by structural function compatibility.
 */
export interface EmbeddedServiceEntry {
	name: string;
	start(): EmbeddedServiceOutcome | Promise<EmbeddedServiceOutcome>;
}

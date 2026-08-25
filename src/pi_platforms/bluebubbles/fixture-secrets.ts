// pi_platforms/bluebubbles/fixture-secrets — fixture-only shared secrets.
// NEVER import production config from here; mirrors msgraph-webhook's
// FIXTURE_CLIENT_STATE pattern.

/** The BlueBubbles server password every fixture uses (token-gate carrier). */
export const FIXTURE_BB_PASSWORD = 'bluebubbles-fixture-password';

/** The BlueBubbles server URL every fixture registers against. */
export const FIXTURE_BB_SERVER_URL = 'http://localhost:1234';

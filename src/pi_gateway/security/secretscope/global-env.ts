// secretscope/global-env — the genuinely-global env carve-out (06 §3.1).
//
// Port of agent/secret_scope.py:_GLOBAL_ENV_EXACT / _GLOBAL_ENV_PREFIXES.
// These name deployment-level variables that ALWAYS read process env: runtime
// knobs, OS vars, api-server LISTENER settings, relay ROUTING
// stamps. Membership is exact-name OR prefix.
//
// Credentials are deliberately EXCLUDED even when prefixed (Hermes anchor:
// agent/secret_scope.py:_GLOBAL_ENV_EXACT — API_SERVER_KEY, GATEWAY_RELAY_SECRET,
// GATEWAY_RELAY_ID, GATEWAY_RELAY_DELIVERY_KEY and IDP_* stay profile-scoped
// with the fail-closed multiplex guard). Keep this list tight: when in doubt,
// a value is a profile secret (06 §3.1).
//
// Naming note: Hermes' HERMES_* deployment knobs translate to PI_* for Pi
// Gateway (parity of hermes_constants.py:get_hermes_home → pi_home.ts PI_HOME);
// OS-level names are identical.

export const GLOBAL_ENV_EXACT: ReadonlySet<string> = new Set([
	// Gateway runtime / deployment (HERMES_* → PI_*)
	"PI_HOME",
	"PI_PROFILE",
	"PI_GATEWAY_LOCK_DIR",
	"PI_MAX_ITERATIONS",
	"PI_MAX_TOKENS",
	"PI_API_TIMEOUT",
	"PI_REDACT_SECRETS",
	"_PI_GATEWAY",
	// OS / interpreter
	"PATH",
	"HOME",
	"USER",
	"LANG",
	"LC_ALL",
	"TZ",
	"PWD",
	"SHELL",
	"TMPDIR",
	"VIRTUAL_ENV",
	"NODE_PATH",
	"SSL_CERT_FILE",
	"NODE_EXTRA_CA_CERTS",
	// API-server LISTENER settings — deployment config (Docker compose
	// `environment:` block, systemd Environment=), not profile secrets. The
	// scoped runner reload (#64674) must keep seeing them or container
	// deployments silently lose the api_server platform (#69379). NOTE:
	// API_SERVER_KEY is deliberately NOT here — it IS a credential and stays
	// profile-scoped.
	"API_SERVER_ENABLED",
	"API_SERVER_HOST",
	"API_SERVER_PORT",
	"API_SERVER_CORS_ORIGINS",
	// Relay-connector ROUTING stamps — deployment config injected into the
	// container/process env by managed deploys. Every reader must resolve the
	// SAME value; a scope-dependent split leaves the adapter registered but
	// the platform absent from config. GATEWAY_RELAY_SECRET /
	// GATEWAY_RELAY_ID / GATEWAY_RELAY_DELIVERY_KEY are auth material and
	// deliberately NOT here — they stay profile-scoped.
	"GATEWAY_RELAY_URL",
	"GATEWAY_RELAY_ENDPOINT",
	"GATEWAY_RELAY_ALLOW_DIRECT_PLATFORMS",
	"GATEWAY_RELAY_PLATFORMS",
	"GATEWAY_RELAY_BOT_IDS",
	"GATEWAY_RELAY_ROUTE_KEYS",
	"GATEWAY_RELAY_INSTANCE_ID",
	"GATEWAY_RELAY_WAKE_URL",
	"GATEWAY_RELAY_DISPLAY_NAME",
]);

export const GLOBAL_ENV_PREFIXES: readonly string[] = [
	"PI_TELEGRAM_", // tuning knobs (batch delays, fallback toggles) — NOT the token
	"TERMINAL_", // terminal/sandbox backend settings
];

/** True for genuinely process-global (non-profile-secret) env var names. */
export function isGlobalEnv(name: string): boolean {
	if (GLOBAL_ENV_EXACT.has(name)) return true;
	return GLOBAL_ENV_PREFIXES.some((p) => name.startsWith(p));
}

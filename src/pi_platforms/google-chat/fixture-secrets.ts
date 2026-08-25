// pi_platforms/google-chat/fixture-secrets — fixture-only shared secrets.
// NEVER import production config from here; mirrors msgraph-webhook's
// FIXTURE_CLIENT_STATE pattern.

export const FIXTURE_HTTP_EVENTS_AUDIENCE = "https://gchat-fixture.example/evt";
export const FIXTURE_SA_EMAIL =
	"chat-relay@gchat-fixture.iam.gserviceaccount.com";

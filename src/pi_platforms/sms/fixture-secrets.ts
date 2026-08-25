// pi_platforms/sms/fixture-secrets — fixture-only Twilio credentials. NEVER
// import production config from here; mirrors msgraph-webhook's
// FIXTURE_CLIENT_STATE pattern.

export const FIXTURE_ACCOUNT_SID = "AC7f3e2b1c9d8a4f5b6e7d8c9a0b1c2d3";
export const FIXTURE_AUTH_TOKEN = "sms-fixture-auth-token-0123456789abcdef";
export const FIXTURE_FROM_NUMBER = "+15551234567";

/** Public webhook URL Twilio signs against (SMS_WEBHOOK_URL fixture value). */
export const FIXTURE_WEBHOOK_URL =
	"https://sms-fixture.example/webhooks/twilio";

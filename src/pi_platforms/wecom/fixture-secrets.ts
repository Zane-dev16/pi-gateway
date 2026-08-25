// pi_platforms/wecom/fixture-secrets — fixture-only shared secrets.
// NEVER import production config from here; mirrors msgraph-webhook's
// FIXTURE_CLIENT_STATE pattern. The AES keys are valid 43-char base64
// (decodes to 32 bytes ⇒ AES-256, the WeCom BizMsgCrypt shape).

export const FIXTURE_WECOM_TOKEN_A = "wecom-fixture-token-alpha";
export const FIXTURE_WECOM_AES_KEY_A =
	"wecomfixtureaesaeskeyaaaaaaaaaaaaaaaaaaaaaa";
export const FIXTURE_CORP_ID_A = "corp-alpha";

export const FIXTURE_WECOM_TOKEN_B = "wecom-fixture-token-beta";
export const FIXTURE_WECOM_AES_KEY_B =
	"wecomfixtureaeskeybbbbbbbbbbbbbbbbbbbbbbbbb";
export const FIXTURE_CORP_ID_B = "corp-beta";

// pi_gateway/setup/platforms — the one platform table setup owns (DEC-081
// CLI wave). Data shape first: every platform is one row (credential vars,
// allowlist vars, instructions, live-check eligibility) instead of
// per-platform conditionals scattered across prompts. Status derives only
// from var presence: all required present means configured, some means
// partially configured, none means not configured. No fake validators.

import {
	PLATFORM_ALLOW_ALL_ENV,
	PLATFORM_ALLOWED_USERS_ENV,
} from "../security/authz/platform-tables.js";

export interface PlatformVarSpec {
	readonly name: string;
	readonly description?: string | undefined;
	readonly password?: boolean | undefined;
}

export type LiveCheckKind = "telegram" | "discord" | "manual";

export interface PlatformSpec {
	readonly name: string;
	readonly instructions: string;
	readonly required: readonly PlatformVarSpec[];
	readonly allowlistVar?: string | undefined;
	readonly homeChannelVar?: string | undefined;
	readonly live: LiveCheckKind;
}

function secret(
	name: string,
	description: string,
): PlatformVarSpec {
	return { name, description, password: true };
}

function plain(name: string, description: string): PlatformVarSpec {
	return { name, description };
}

/**
 * Registry rows. Required-var names are transcribed from each platform's
 * manifest requiresEnv (telegram/slack/signal/matrix/mattermost/sms/email/
 * whatsapp-cloud/irc/line/feishu/qqbot/wecom/weixin/yuanbao) or the
 * platform's documented token var (discord DISCORD_BOT_TOKEN). Telegram and
 * discord rows carry live checks; every other row is manual vars with status
 * only — never a claimed validation.
 */
export const PLATFORM_REGISTRY: readonly PlatformSpec[] = [
	{
		name: "telegram",
		instructions:
			"Create a bot with @BotFather on Telegram and paste the bot token it issues.",
		required: [secret("TELEGRAM_BOT_TOKEN", "Bot token issued by BotFather")],
		allowlistVar: "TELEGRAM_ALLOWED_USERS",
		homeChannelVar: "TELEGRAM_HOME_CHANNEL",
		live: "telegram",
	},
	{
		name: "discord",
		instructions:
			"Create an application in the Discord developer portal, add a bot user, enable the message-content intent, and paste the bot token.",
		required: [secret("DISCORD_BOT_TOKEN", "Bot token from the developer portal")],
		allowlistVar: "DISCORD_ALLOWED_USERS",
		homeChannelVar: "DISCORD_HOME_CHANNEL",
		live: "discord",
	},
	{
		name: "slack",
		instructions:
			"Create a Slack app, install it to the workspace, and paste both tokens. See the vars file for scopes.",
		required: [
			secret("SLACK_BOT_TOKEN", "Bot token (xoxb-…)"),
			secret("SLACK_APP_TOKEN", "App token (xapp-…)"),
		],
		allowlistVar: "SLACK_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "signal",
		instructions:
			"Run signal-cli locally and note the account plus the HTTP endpoint it serves. See the vars file.",
		required: [
			plain("SIGNAL_ACCOUNT", "Phone number registered with signal-cli"),
			plain("SIGNAL_HTTP_URL", "signal-cli HTTP endpoint URL"),
		],
		allowlistVar: "SIGNAL_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "matrix",
		instructions:
			"Create a Matrix user for the bot, issue an access token, and note the homeserver URL. See the vars file.",
		required: [
			plain("MATRIX_HOMESERVER", "Homeserver URL"),
			secret("MATRIX_ACCESS_TOKEN", "Bot access token"),
		],
		allowlistVar: "MATRIX_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "mattermost",
		instructions:
			"Create a bot account on the Mattermost server and paste its token plus the server URL. See the vars file.",
		required: [
			plain("MATTERMOST_URL", "Server URL"),
			secret("MATTERMOST_TOKEN", "Bot token"),
		],
		allowlistVar: "MATTERMOST_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "sms",
		instructions:
			"Note the Twilio account SID, auth token, and sending number from the Twilio console. See the vars file.",
		required: [
			plain("TWILIO_ACCOUNT_SID", "Twilio account SID"),
			secret("TWILIO_AUTH_TOKEN", "Twilio auth token"),
		],
		allowlistVar: "SMS_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "email",
		instructions:
			"Note the mailbox address plus an app password for it. See the vars file.",
		required: [
			plain("EMAIL_ADDRESS", "Mailbox address"),
			secret("EMAIL_PASSWORD", "App password"),
		],
		allowlistVar: "EMAIL_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "whatsapp_cloud",
		instructions:
			"Note the phone-number ID and a Catalog access token from the Meta developer dashboard. See the vars file.",
		required: [
			plain("WHATSAPP_CLOUD_PHONE_NUMBER_ID", "Phone-number ID"),
			secret("WHATSAPP_CLOUD_ACCESS_TOKEN", "Access token"),
		],
		allowlistVar: "WHATSAPP_CLOUD_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "irc",
		instructions:
			"Note the IRC server and the channel the bot should join. See the vars file.",
		required: [
			plain("IRC_SERVER", "IRC server hostname"),
			plain("IRC_CHANNEL", "Channel to join"),
		],
		allowlistVar: undefined,
		live: "manual",
	},
	{
		name: "line",
		instructions:
			"Create a channel in the LINE developers console and paste the channel access token plus the channel secret.",
		required: [
			secret("LINE_CHANNEL_ACCESS_TOKEN", "Channel access token"),
			secret("LINE_CHANNEL_SECRET", "Channel secret"),
		],
		allowlistVar: undefined,
		live: "manual",
	},
	{
		name: "feishu",
		instructions:
			"Create a Feishu/Lark app and paste the app id plus the app secret. See the vars file for domain and mode.",
		required: [
			plain("FEISHU_APP_ID", "App id"),
			secret("FEISHU_APP_SECRET", "App secret"),
		],
		allowlistVar: "FEISHU_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "qqbot",
		instructions:
			"Create a QQ bot in the open platform and paste the app id plus the client secret. See the vars file.",
		required: [
			plain("QQ_APP_ID", "App id"),
			secret("QQ_CLIENT_SECRET", "Client secret"),
		],
		allowlistVar: "QQ_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "wecom",
		instructions:
			"Note the corp id, callback token, and encoding AES key from the WeCom admin console. See the vars file.",
		required: [
			plain("WECOM_CALLBACK_CORP_ID", "Corp id"),
			secret("WECOM_CALLBACK_TOKEN", "Callback token"),
			secret("WECOM_CALLBACK_ENCODING_AES_KEY", "Encoding AES key"),
		],
		allowlistVar: "WECOM_CALLBACK_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "weixin",
		instructions:
			"Note the official-account token and account id from the Weixin admin console. See the vars file.",
		required: [
			secret("WEIXIN_TOKEN", "Official-account token"),
			plain("WEIXIN_ACCOUNT_ID", "Account id"),
		],
		allowlistVar: "WEIXIN_ALLOWED_USERS",
		live: "manual",
	},
	{
		name: "yuanbao",
		instructions:
			"Create a Yuanbao app and paste the app id plus the app secret. See the vars file.",
		required: [
			plain("YUANBAO_APP_ID", "App id"),
			secret("YUANBAO_APP_SECRET", "App secret"),
		],
		allowlistVar: "YUANBAO_ALLOWED_USERS",
		live: "manual",
	},
];

export type PlatformStatus = "configured" | "partially configured" | "not configured";

export interface PlatformStatusReport {
	readonly spec: PlatformSpec;
	readonly status: PlatformStatus;
	readonly missing: readonly string[];
}

/** Process env wins for the reading process; the file stays the durable default. */
export type VarReader = (name: string) => string | undefined;

function isPresent(raw: string | undefined): boolean {
	return raw !== undefined && raw.trim() !== "";
}

export function statusOf(
	spec: PlatformSpec,
	read: VarReader,
): PlatformStatusReport {
	const missing = spec.required
		.map((v) => v.name)
		.filter((name) => !isPresent(read(name)));
	const status: PlatformStatus =
		missing.length === 0
			? "configured"
			: missing.length === spec.required.length
				? "not configured"
				: "partially configured";
	return { spec, status, missing };
}

export const PLATFORMS_ENV_VAR = "PI_GATEWAY_PLATFORMS";

export interface FilteredPlatforms {
	readonly specs: readonly PlatformSpec[];
	readonly warnings: readonly string[];
}

/**
 * Comma allowlist over the picker, --list, and --status. It filters rows, it
 * never enables credentials. Unknown names warn and are ignored. Empty means
 * every platform.
 */
export function filterPlatforms(raw: string | undefined): FilteredPlatforms {
	const text = (raw ?? "").trim();
	if (text === "") return { specs: PLATFORM_REGISTRY, warnings: [] };
	const wanted = text
		.split(",")
		.map((part) => part.trim().toLowerCase())
		.filter((part) => part !== "");
	const byName = new Map(PLATFORM_REGISTRY.map((spec) => [spec.name, spec]));
	const specs: PlatformSpec[] = [];
	const warnings: string[] = [];
	for (const name of wanted) {
		const spec = byName.get(name);
		if (spec === undefined) {
			warnings.push(`unknown platform ${JSON.stringify(name)} — ignored`);
			continue;
		}
		if (!specs.includes(spec)) specs.push(spec);
	}
	return { specs, warnings };
}

/** Explicit operator opt-out flag var for a platform, if the matrix defines one. */
export function allowAllVarFor(platform: string): string | undefined {
	return PLATFORM_ALLOWED_USERS_ENV[platform] !== undefined
		? PLATFORM_ALLOW_ALL_ENV[platform]
		: undefined;
}

/** Telegram format gate: digits, a colon, then at least 30 token chars. */
export const TELEGRAM_TOKEN_FORMAT = /^[0-9]+:[A-Za-z0-9_-]{30,}$/;

export function telegramTokenFormatOk(token: string): boolean {
	return TELEGRAM_TOKEN_FORMAT.test(token.trim());
}

// pi_platforms/kit — the shared platform-adapter kit (04-platform-adapters.md
// §1/§2/§4/§6/§9/§10; DEC-016, DEC-017; Q17). Public surface for Phase-3
// reference adapters and the conformance suite. This layer sits ABOVE
// pi_gateway/pi_agent_core/pi_state (01 §5.3) and imports downward only.

export {
	CALLBACK_DATA_MAX_BYTES,
	CallbackDataOverflowError,
	EXEC_APPROVAL_CHOICES,
	SLASH_CONFIRM_CHOICES,
	FAMILY_PREFIXES,
	buildChoicePickerCallback,
	buildClarifyCallback,
	buildExecApprovalCallback,
	buildModelCommitCallback,
	buildModelGroupNavCallback,
	buildModelMemberCallback,
	buildModelPageNavCallback,
	buildModelProviderCallback,
	buildModelProviderGroupCallback,
	buildSlashConfirmCallback,
	buildWhatsappApprovalCallback,
	parseCallbackData,
} from "./callback-grammar.js";

export type {
	ParsedCallback,
	WhatsappApprovalChoice,
	ExecApprovalChoice,
	SlashConfirmChoice,
} from "./callback-grammar.js";

export {
	ActionHandlerRegistry,
	APPROVAL_ACTION_IDS,
	CLARIFY_CHOICE_ACTION_RE,
	CLARIFY_OTHER_ACTION_ID,
	MAX_BLOCKS,
	MAX_SECTION_TEXT_CHARS,
	SLACK_ACK_WINDOW_MS,
	approvalActionId,
	assembleInteractiveMessage,
	clarifyChoiceActionId,
	parseClarifyChoiceActionId,
	renderBlocks,
	slashConfirmActionId,
} from "./block-kit.js";
export type {
	ActionConstraint,
	InteractiveMessage,
	KitBlock,
} from "./block-kit.js";

export {
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
} from "./callback-router.js";
export type {
	CallbackAnswer,
	CallbackTapContext,
	ClickAuthorizer,
	CreateRouterOptions,
	PopOutcome,
	RouterStores,
} from "./callback-router.js";

export {
	capabilityFlag,
	governingTier,
	DEFAULT_CAPABILITIES,
} from "./capabilities.js";
export type {
	CapabilityManifest,
	RateBudget,
	RateOp,
	RateTier,
} from "./capabilities.js";

export {
	chunkWithFenceCarry,
	snapOffSurrogateTrail,
	stripChunkScaffolding,
	INDICATOR_RESERVE,
} from "./chunking.js";
export type { ChunkPlan, ScaffoldRecord } from "./chunking.js";

export {
	codePointLen,
	lenFnForUnit,
	resolveChatLengthPolicy,
	utf16Len,
	DEFAULT_MAX_MESSAGE_LENGTH,
} from "./length-policy.js";
export type { ChatLengthPolicy, LengthUnit } from "./length-policy.js";

export {
	AdapterDisabledError,
	AdapterLifecycleState,
	TokenLockConflictError,
	describeReason,
} from "./lifecycle-state.js";
export type {
	AdapterRunState,
	AdapterStatusSnapshot,
	DisableReason,
} from "./lifecycle-state.js";

export {
	FormattingLadder,
	classifyRichFailure,
	stripMarkdownMarkup,
} from "./formatting-ladder.js";
export type {
	FormattingTransport,
	LadderOutcome,
	LadderTier,
	RichErrorClass,
} from "./formatting-ladder.js";

export {
	TokenLockManagerSeam,
	type AcquiredTokenLock,
	type LockAcquisition,
	type LockHolderInfo,
	type TokenLockManagerOptions,
} from "./token-lock.js";

export {
	PluginContext,
	resolveEnablement,
} from "./registration.js";
export type {
	EnvVarSpec,
	PlatformEnablement,
	PlatformFactory,
	PluginManifest,
	RegisteredPlatform,
	ScopedSecretReader,
	TransportShape,
} from "./registration.js";

export {
	secureCompare,
	validateTrustBoundaryManifest,
	verifyHmacSignature,
} from "./trust.js";
export type {
	BackpressureWindow,
	SignatureScheme,
	TrustBoundaryManifest,
} from "./trust.js";

export {
	BasePlatformAdapter,
	MAX_MESSAGE_LENGTH_DEFAULT,
} from "./base-adapter.js";
export type { BaseAdapterDeps } from "./base-adapter.js";

export {
	DELIVERY_FAILED_NOTICE,
	PLAIN_TEXT_FALLBACK_CAP,
	PLAIN_TEXT_FALLBACK_PREFIX,
	classifySendError,
	errorBlob,
	extractRetryAfterSeconds,
	plainTextFallbackBody,
	sendWithRetry,
} from "./send-retry.js";
export type {
	RetryLadderOptions,
	SendErrorClass,
} from "./send-retry.js";

export {
	REDACTED_PLACEHOLDER,
	SecretRedactor,
	createRedactingLogger,
	MIN_REDACTABLE_LENGTH,
} from "./log-redaction.js";

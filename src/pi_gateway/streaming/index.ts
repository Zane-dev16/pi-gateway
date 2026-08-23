// pi_gateway/streaming — production stream consumer + typed stream events
// (04-platform-adapters.md §5; DEC-006). Public surface for the runner
// (pi_agent_core drives it) and Phase-3 platform adapters (implement the seam).

// Typed stream-event vocabulary (§5.3) — frozen factories + union.
export {
	commentary,
	gatewayNotice,
	longToolHint,
	messageChunk,
	messageStop,
	toolCallChunk,
	toolCallFinished,
	type Commentary,
	type GatewayNotice,
	type LongToolHint,
	type MessageChunk,
	type MessageStop,
	type StreamEvent,
	type ToolCallChunk,
	type ToolCallFinished,
} from "./stream-events.js";

// The adapter STREAM SEAM (Phase-3 adapters implement these; no concrete
// platform imports here).
export {
	INTERIM_SEND_MARKER,
	REPLY_TO_METADATA_KEY,
	defaultFormatToolEvent,
	defaultRenderMessageEvent,
	type ConsumerSink,
	type DraftFrameArgs,
	type EditOptions,
	type Metadata,
	type SendResult,
	type StreamEgressAdapter,
	type StreamLogger,
	type StreamRenderAdapter,
	type ToolProgressMode,
} from "./adapter-seam.js";

// Per-chat capability discovery with per-chat latch (DEC-006 method probes).
export {
	StreamingCapabilities,
	type CapabilityProbeSource,
} from "./capability.js";

// THE single-audited seal chokepoint (both doors, one code path; §5.1).
export {
	EgressChokepoint,
	turnKey,
	type ChokepointAuditEntry,
	type DoorAdmission,
	type DoorName,
	type DoorTransport,
	type DraftAdmissionVerdict,
	type EgressAction,
} from "./egress-door.js";

// Production GatewayStreamConsumer (§5.2).
export {
	GatewayStreamConsumer,
	type PrefixViolation,
	type StreamConsumerConfig,
} from "./gateway-stream-consumer.js";

// Typed-event dispatcher (§5.3 contract clauses).
export {
	GatewayEventDispatcher,
	type DispatcherOptions,
} from "./dispatcher.js";

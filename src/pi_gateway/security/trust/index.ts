// trust — HTTP-ingress trust boundary ENGINE (06 §8; DEC-017).
//
// Adapters declare their boundaries AS DATA and configure THIS engine with
// it: signature scheme registry (constant-time compares, replay seen-set),
// script confinement (relative_to), pre-parse body caps, msgraph CIDR
// gating, api_server opt-in session headers, artifact TTL/size/MIME caps.
// pi_gateway cannot import pi_platforms (01 §5.3 layering), so the engine
// is self-contained over node builtins and is downward-importable by every
// adapter (kit/trust composes the identical node primitive).

export {
	compareAfterCompute,
	constantTimeEqual,
} from "./constant-time.js";
export {
	BoundedSeenSet,
	DEFAULT_MAX_SEEN_ENTRIES,
	SEEN_SET_TTL_MS,
} from "./replay-seen-set.js";
export {
	SIGNATURE_SCHEMES,
	createSignatureValidator,
	validateSignature,
	type HeaderMap,
	type SignatureAdmissionInput,
	type SignatureSchemeData,
	type SignatureSchemeId,
	type SignatureVerdict,
	type SignatureValidationCall,
} from "./signature-schemes.js";
export {
	expandUser,
	expandVars,
	resolveScriptPath,
	type ConfinementFs,
	type ScriptConfinementConfig,
	type ScriptPathResolution,
} from "./confinement.js";
export {
	API_SERVER_BODY_CAP_BYTES,
	MSGRAPH_BODY_CAP_BYTES,
	WEBHOOK_BODY_CAP_BYTES,
	readBodyWithinCap,
	type BodyCapDeps,
} from "./body-cap.js";
export {
	allowlistRequiredButMissing,
	ipInNetworks,
	isLoopbackBindHost,
	parseCidr,
	parseCidrAllowlist,
	parseIpAddress,
	sourceIpAllowed,
	type ParsedNetwork,
	type PeerRequest,
} from "./cidr.js";
export {
	MAX_SESSION_HEADER_LEN,
	SESSION_ID_HEADER,
	SESSION_KEY_HEADER,
	extractOptInSessionHeaders,
	type SessionHeadersVerdict,
} from "./session-headers.js";
export {
	DEFAULT_ARTIFACT_TTL_MS,
	DEFAULT_ALLOWED_MIME_TYPES,
	DEFAULT_MAX_ARTIFACT_BYTES,
	ArtifactChecksumMismatch,
	ArtifactError,
	ArtifactExpired,
	ArtifactMimeRejected,
	ArtifactNotFound,
	ArtifactScopeMismatch,
	ArtifactStore,
	ArtifactTooLarge,
	ArtifactTraversal,
	NodeArtifactFs,
	artifactScopeKey,
	boundedFilename,
	normalizeContentType,
} from "./artifacts.js";

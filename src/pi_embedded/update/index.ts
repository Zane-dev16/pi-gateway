// index.ts — public surface of the embedded update subsystem (Phase 5).
//
// Spec: /root/pi-gateway/08-operations.md §5–§10. Transactional pipeline:
// plan → snapshot → apply → restart-per-kind → verify → receipt.

export type { GatewayClock } from "./clock.js";
export { systemClock } from "./clock.js";

export {
	INSTALL_METHOD_STAMP,
	classifyDeploymentKind,
	buildUpdatePlan,
	readTreeVersion,
	headShaViaFiles,
	type DeploymentKind,
	type DeploymentClassification,
	type UpdatePlan,
	type RuntimeRecord,
	type PlanStageInputs,
	type GitIdentityProbe,
} from "./plan.js";

export {
	PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE,
	PRE_UPDATE_KEEP,
	SNAPSHOTS_DIRNAME,
	PRE_UPDATE_LABEL,
	QUICK_STATE_FILES,
	snapshotProfileHome,
	createPreUpdateSnapshotsAllProfiles,
	pruneSnapshotDirs,
	safeCopyDb,
	type SnapshotSkip,
	type ProfileSnapshotResult,
	type SnapshotStageResult,
} from "./snapshot.js";

export {
	DEFAULT_PULL_TARGET,
	isGitCommand,
	isDependencyInstallCommand,
	classifyFailure,
	shouldZipFallbackOnUpdateError,
	zipOverlayBlockReason,
	zipOverlay,
	graftBuiltArtifacts,
	applyStage,
	isStagingArtifactStatusLine,
	OVERLAY_PRESERVE_SET,
	BUILT_ARTIFACT_DIRS,
	type UpdateCommandFailure,
	type FailureClass,
	type PullTarget,
	type ZipOverlayOptions,
	type ZipOverlayOutcome,
	type ApplyStageOutcome,
} from "./apply.js";

export {
	nodeUpdateCommandRunner,
	type CommandResult,
	type UpdateCommandRunner,
} from "./run.js";

export {
	MIN_DRAIN_TIMEOUT_MS,
	restartFleet,
	restartPhaseFailureIsIncomplete,
	type SupervisorKind,
	type RestartUnit,
	type UnitRestartTrace,
	type RestartStageResult,
	type RestartPorts,
} from "./restart.js";

export {
	DEFAULT_SETTLE_WINDOW_MS,
	collectFleetVersions,
	fleetHasStaleGateway,
	formatFleetVersionMatrix,
	verifyStage,
	type FleetEntry,
	type FleetIdentityState,
	type FleetStatusSource,
	type FleetProbePorts,
	type VerifyStageResult,
} from "./verify.js";

export {
	installHangupProtection,
	wrapHangupSafe,
	HANGUP_SAFE_ARGV_PREFIX,
	type HangupGuard,
} from "./hangup.js";

export {
	UPDATE_RECEIPTS_DIRNAME,
	LATEST_POINTER_FILENAME,
	RECEIPTS_KEEP_DEFAULT,
	UpdateReceiptWriter,
	exitCodeForOutcome,
	pruneReceipts,
	readLatestPointer,
	receiptsDirFor,
	type UpdateOutcome,
	type ReceiptStep,
	type ReceiptSkip,
	type UpdateReceiptPayload,
} from "./receipt.js";

export {
	tokenizeCommandLine,
	gatewayCommandSubcommand,
	looksLikeGatewayCommandLine,
	looksLikeGatewayRuntimeCommandLine,
	holderValueFlags,
	piHolderSubcommand,
	TOP_LEVEL_OPTION_SPECS,
	readProcCmdline,
	listProcIdentities,
	discoverLiveGateways,
	type OptionSpec,
	type ProcessIdentity,
} from "./proc-matchers.js";

export {
	runUpdatePipeline,
	defaultReadStatus,
	type ProfileHome,
	type StatusView,
	type UpdatePipelineOptions,
	type UpdatePipelineResult,
} from "./pipeline.js";

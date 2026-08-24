// pi_embedded/handoff — the DEC-008 CLI→gateway handoff watcher.
//
// Layer: pi_embedded (rank 4). Imports downward only (pi_state, and the
// shared session-key/event vocabulary of pi_gateway); NEVER lifecycle/run
// internals. Started BY the gateway driver as an optional embedded-watchers
// stage service (01 §3.1 stage 8): degrade loudly, never block siblings.
//
// Public surface:
//   clock       — GatewayClock seam (injected time; contracts drive it)
//   queue       — HandoffQueue (pending-row protocol over sessions columns)
//   binder      — RoutingBinder (switch_session re-bind over gateway_routing)
//   pipeline    — HandoffPipeline (claim→…→synthetic-turn step sequence)
//   dispatcher  — GuardQuiesceDispatcher (L1-ingress composition reference)
//   watcher     — HandoffWatcher (2s poll + 5s startup delay loop)
//   cli-client  — HandoffCliClient (request + 60s/0.5s poll-block)

export * from "./clock.js";
export * from "./queue.js";
export * from "./binder.js";
export * from "./pipeline.js";
export * from "./dispatcher.js";
export * from "./watcher.js";
export * from "./cli-client.js";
export * from "./stage-entry.js";

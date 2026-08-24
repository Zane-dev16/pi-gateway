// pi_platforms/discord/clock — injected-clock type seams for the Discord
// transport (workspace hard rule: timing behavior is proven against INJECTED
// clocks; the deterministic ManualClock implementation is the transport-
// family's shared one — roadmap Phase-6 heuristic 2, ports inherit the family).

export type NowFn = () => number;
export type SleepFn = (ms: number) => Promise<void>;

export { ManualClock } from "../persistent-ws/manual-clock.js";

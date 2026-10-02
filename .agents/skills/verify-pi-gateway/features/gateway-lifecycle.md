# Gateway lifecycle

Gateway lifecycle lets a user start the gateway on a home directory, tell
whether it is running, and stop it cleanly, with a second start against the
same live home refused at the duplicate guard instead of double-polling.

## Sub-features

- `lifecycle-run` starts the gateway and reaches `gateway READY`.
- `lifecycle-doctor` reports HEALTHY for the running home.
- `lifecycle-double-run` refuses a second launch on the same live home.
- `lifecycle-stop` drains to `planned_stop` on SIGTERM.

## How to get to it (user POV)

- Start `pi --mode rpc` with `PI_GATEWAY_AUTO_START=1` and `PI_HOME=<dir>`.
- Read `gateway_state.json` in the home for `running` vs `stopped`.
- Start a second `pi --mode rpc` on the same live home (refused).
- SIGTERM the gateway pid (drains and stops).

## Driving it with control-pi-gateway

Preconditions:

- `HOME_DIR=/tmp/pi-verify-$RUN_ID` does not exist yet.
- No other gateway holds `$HOME_DIR` (a live one triggers the refusal path).

- **Run.** Start the gateway. Run `control-pi-gateway launch --home "$HOME_DIR"`. The output prints `HOME_DIR`, `GATEWAY_PID`, and `LOG`, and the log contains `gateway READY home=… platforms=[none]`.
- **Doctor healthy.** Check readiness. Run `control-pi-gateway doctor --home "$HOME_DIR"`. Output ends with `doctor: HEALTHY …` and `state=running` with a live pid.
- **Double run refused.** Launch again on the same live home. Run `control-pi-gateway launch --home "$HOME_DIR"` (second home var or same dir). It exits nonzero; its log names the live pid (`another gateway instance is already running (pid …)`). The first instance keeps serving (`doctor` still HEALTHY).
- **Stop.** Stop what this run started. Run `kill -TERM <GATEWAY_PID>` (the pid launch printed). Within ~10s `gateway_state.json` reads `\"gateway_state\": \"stopped\", \"exit_reason\": \"planned_stop\"` and the pid is gone.
- **Proof.** Capture the run evidence BEFORE cleanup. Run `grep -E "gateway READY|platforms=" "$HOME_DIR.launch.log" > artifacts/gateway-lifecycle/launch.txt; control-pi-gateway db --home "$HOME_DIR" -- "SELECT name FROM sqlite_master WHERE type='table'" > artifacts/gateway-lifecycle/db.txt; cat "$HOME_DIR/gateway_state.json" > artifacts/gateway-lifecycle/final-state.txt` after the stop. The artifacts show the READY line, the table list including `sessions`, `messages`, `delivery_obligations`, and the drained final state.
- **Cleanup.** Run `control-pi-gateway cleanup --home "$HOME_DIR"`, then confirm the three artifact files still exist. The home and its sibling launch log are gone; the artifacts remain.

## Gotchas

- `pi --mode rpc` needs its stdin held open (`tail -f /dev/null`); the helper does this. A bare `< /dev/null` redirect EOFs and the process exits on its own later — a `stopped` state then is a harness artifact, not a gateway defect.
- The launch log is a sibling (`$HOME_DIR.launch.log`), not inside the home; the DB proof must still be captured before cleanup because `state.db` lives inside the removed home. Retain the proof artifacts.
- `bin/pi-gateway status` does not see extension-launched gateways (it reads `gateway.pid`, which only standalone `run` writes). Assert on `doctor` and `gateway_state.json`, not on bin status.
- The refusal names the live pid; assert that line, not any particular exit code of the wrapping `pi` process.

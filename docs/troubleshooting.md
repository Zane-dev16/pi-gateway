# Troubleshooting

Symptom → cause → fix for the most common Pi Gateway failure modes. Check
`gateway_state.json` and `logs/errors.log` under your `PI_HOME` first; almost
every failure logs a reason code. For lifecycle mechanics behind these fixes,
see [docs/operations.md](operations.md).

## Gateway won't start

| Symptom                                      | Cause and fix                                                              |
| -------------------------------------------- | -------------------------------------------------------------------------- |
| "duplicate instance" / PID file in use       | Another gateway owns this `PI_HOME`. Stop it, or run `pi gateway run --replace` (takeover handshake, spec 01 §3.2) |
| Startup fails after takeover attempt         | The takeover couldn't signal the old process (permission denied). Clear the condition manually; the marker is always cleaned up before failing |
| Config error at boot                         | Config is validated before anything else; fix the reported key and restart. There is no partial-boot on invalid config |
| Adapter never appears                        | Look for the loud `adapter_disabled` line: a manifest-required secret (e.g. `TELEGRAM_BOT_TOKEN`) is missing. Provide it and restart (spec 04 §4.2) |
| Fatal connect error naming a holder profile/PID | Token lock held: another profile or process is connected with the same credential (spec 06 §5). Stop the other gateway or `--replace` it |

## Turns and streaming

| Symptom | Cause and fix |
| --------------------------------------------- | -------------------------------------------------------------------- |
| Turn never starts; logs show `turn_lease` waits | Another process holds the turn lease for that conversation. The lease expires after TTL 300s and a provably dead holder is reclaimed immediately (spec 02 §5, DEC-004) |
| "lease lost mid-turn" | First-class condition, not a crash: the turn aborts cleanly and the next message re-runs. If it repeats, check for two gateways on one home |
| Draft edits stop mid-response but no final arrives | Check `errors.log` for seal/delivery reasons. A failed seal still delivers the final as a plain send (DEC-006) |
| Messages from a group are ignored | Authorization is deny-by-default: set `{P}_GROUP_ALLOWED_CHATS` or the sender allowlist; see [docs/configuration.md](configuration.md) |
| Own messages / other bots ignored | `{P}_ALLOW_BOTS` defaults to none; set `mentions` or `all` if intended (spec 06 §2.5) |

## Delivery

| Symptom                                     | Cause and fix                                                           |
| ------------------------------------------- | ----------------------------------------------------------------------- |
| "delivery failed, please resend"            | The obligation ladder exhausted its caps (3 attempts / 24h stale), so the message was not delivered. Resend (DEC-053/054) |
| Sends fail with flood errors                | `retry_after` from the platform is authoritative and honored; persistent floods mean rate-tier limits. Slow the workload |
| Timeout-classified send failures loop       | They don't: timeouts are never retried inside platform ladders (DEC-046). Check the vendor's status instead |

## Configuration

| Symptom                                     | Cause and fix                                                             |
| ------------------------------------------- | ------------------------------------------------------------------------- |
| Config change has no effect                 | No live reload in v0.1 (DEC-013). Restart with `pi gateway run --replace` |
| Denials in `errors.log` with reason codes   | Cross-check the reason code against the authz knobs in [docs/configuration.md](configuration.md); denials are never silent (spec 06 §2) |
| Pairing code rejected / locked out          | 5 failed approvals lock for 3600s; a fresh success resets counters (spec 06 §2.4) |
| Another profile's allowlist "leaking"       | Should be impossible: scoped reads fail closed and are grep-gated (`npm run check:secrets`, DEC-003/009). Report it as a security bug; see [SUPPORT.md](../SUPPORT.md) |

## Storage and search

| Symptom                                     | Cause and fix                                                              |
| ------------------------------------------- | -------------------------------------------------------------------------- |
| `SQLITE_BUSY` in logs                       | Normal contention is retried with patience. Repeated BUSY under heavy load points to a second process on the same `state.db` |
| Boot replays `pending_messages/*.json`      | A previous shutdown flushed un-persisted messages to disk; they're re-inserted on boot and the files removed. Structurally invalid files are preserved, never deleted (spec 08 §1.3) |

## Process health

| Symptom                                     | Cause and fix                                                               |
| ------------------------------------------- | ---------------------------------------------------------------------------- |
| Exit code 75, logs mention loop liveness    | The event loop froze; the watchdog dumped all thread stacks and exited for a supervisor restart (spec 08 §1.3). Bring the stack dump + `logs/gateway-shutdown-watchdog.log` |
| Exit code 1 after "shutdown watchdog"       | The drain wedged past its budget; stack dump is in the watchdog log          |
| `gateway-shutdown-diag.log` exists          | An unexpected external signal killed the gateway; the forensics probe recorded who/what (spec 08 §1.3) |

## Where to look

```
<PI_HOME>/
├── gateway_state.json        # runtime status: state, platforms, code_sha
├── state/gateway.heartbeat   # fresh mtime = event loop alive
├── logs/errors.log           # reason codes (start here)
├── logs/agent.log            # full activity
├── logs/gateway.log          # gateway-scoped records
└── pending_messages/         # shutdown-flush recovery files
```

Diagnostics live outside the gateway process: `gateway_state.json`, the
heartbeat file, and `/status` are all read-only snapshots. A monitoring check
must never poke adapters directly (spec 08 §4).

If none of this helps, gather the files above (redact secrets) and open an
issue per [SUPPORT.md](../SUPPORT.md).

## See also

- [docs/operations.md](operations.md): signals, backstops
- [docs/configuration.md](configuration.md): policies and allowlists
- [docs/installation.md](installation.md): layout and requirements
- [README.md](../README.md): project hub

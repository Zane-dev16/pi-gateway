# Platforms

Pi Gateway ships 31 platform surfaces. Every adapter implements one of three
transport shapes, and every adapter, whether a reference adapter or a census
port, passes the same executable conformance suite before merge (spec 04 §8,
DEC-002).

## Transport shapes

| Shape | Ingress mechanics | Reference adapter |
| -------------------- | ---------------------------------- | --------------------- |
| Polling | SDK long-poll (e.g. Bot API `getUpdates`) with generations, heartbeats, held-inbound redispatch | `src/pi_platforms/polling/` |
| Persistent WebSocket | Always-on ws/stream with replay, keepalive, Retry-After capture (DEC-044) | `src/pi_platforms/persistent-ws/` |
| Webhook | Stateless HTTP ingress with HMAC signature verification and explicit trust boundaries (DEC-017) | `src/pi_platforms/webhook/` |

The three reference adapters are the seam proof: the remaining census ports
inherit their fixtures and obligations. Polling adapters must preserve
server-side update queues across reconnects and evict zombie sessions on 409
conflict. Webhook adapters use constant-time signature compares, a ±300s
replay window, and bounded body and concurrency limits.

## Supported surfaces

Counted from each adapter's manifest (`transportShape`).

### Polling (7 = 6 surfaces + 1 reference)

| Surface             | Notes                                          |
| ------------------- | ---------------------------------------------- |
| `polling`           | generic reference adapter (Bot API long-poll)  |
| `telegram`          | Bot API long-poll; first census port (DEC-024) |
| `whatsapp-personal` | Personal WhatsApp bridge                       |
| `matrix`            | Matrix homesync                                |
| `weixin`            | WeChat personal bridge                         |
| `email`             | IMAP/SMTP polling                              |
| `buzz`              | Buzz polling bridge                            |

### Persistent WebSocket (13 = 12 surfaces + 1 reference)

| Surface             | Notes                                  |
| ------------------- | -------------------------------------- |
| `persistent-ws`     | generic reference adapter              |
| `discord`           | Gateway websocket, slash callbacks     |
| `slack`             | Socket Mode                            |
| `mattermost`        | ws + REST backfill replay (DEC-050)    |
| `signal`            | signal-cli ws                          |
| `simplex`           | SimpleX                                |
| `feishu`            | Feishu/Lark ws + card ingress          |
| `irc`               | IRC with formatting/budget modules     |
| `qqbot`             | QQ bot ws                              |
| `ntfy`              | ntfy subscribe (ws family)             |
| `photon`            | Photon                                 |
| `homeassistant`     | Home Assistant conversation API        |
| `yuanbao`           | Yuanbao bridge                         |

### Webhook (11 = 10 surfaces + 1 reference)

| Surface           | Notes                                         |
| ----------------- | --------------------------------------------- |
| `webhook`         | generic reference adapter                     |
| `whatsapp-cloud`  | Cloud API, bounded backpressure window        |
| `google-chat`     | Google Chat push                              |
| `teams`           | Microsoft Teams                               |
| `msgraph-webhook` | Microsoft Graph subscriptions                 |
| `bluebubbles`     | iMessage via BlueBubbles                      |
| `sms`             | SMS webhook gateway                           |
| `raft`            | Raft bridge (never spawned headless, DEC-062) |
| `a2a`             | Agent-to-agent ingress (DEC-064)              |
| `line`            | LINE Messaging API callbacks                  |
| `wecom`           | WeCom (WeChat Work) callbacks                 |

> The manifest under `src/pi_platforms/<name>/manifest.ts` is the source of
> truth for each surface's shape, secrets, and capabilities; these tables
> summarize it. `dingtalk` is an explicit, documented exclusion from the
> census (DEC-043) and ships no adapter.

## What every surface gets

Once an adapter passes conformance, platform mechanics are shared, not
per-platform: two-guard routing and the two-layer turn lease (DEC-004/005),
the streaming draft contract where the platform supports edits (DEC-006),
chunking and send-retry ladders (`retry_after` is server-authoritative;
timeout-classified failures are never retried, DEC-046), the command
registry and busy policies (spec 07 §1), the delivery-obligations ledger
(DEC-053/054), pairing and allowlist authorization (spec 06 §2), and
token-lock protection for unique credentials (spec 06 §5).

## Configuring a surface

1. Provide the secrets the manifest declares in `requiresEnv`. A missing
   required secret disables the adapter loudly, and the log names the
   missing key.
2. Set the authorization knobs you want (`{P}_ALLOWED_USERS`,
   `{P}_GROUP_ALLOWED_CHATS`, policies). See
   [docs/configuration.md](configuration.md).
3. Restart (`pi gateway run --replace` takes over a running instance) and
   check `/status` for the adapter's connection state.

Per-surface specifics (token endpoints, signature setup, media caching) are
documented in the adapter module and its manifest description.

## Adding your own

See [docs/adding-a-platform.md](adding-a-platform.md): inherit a
reference fixture, declare manifest data, pass the conformance gate.

## See also

[docs/configuration.md](configuration.md) covers secrets and policies,
[docs/architecture.md](architecture.md) describes the shared mechanics, and
[docs/troubleshooting.md](troubleshooting.md) collects adapter diagnostics.
The project hub is [README.md](../README.md).

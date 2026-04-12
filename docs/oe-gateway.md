# FIX Gateway

The FIX gateway is the front door for clients that speak FIX 4.2. It
terminates FIX sessions, translates orders into the internal Melin
protocol, and translates execution reports back to FIX. One gateway
process can serve many concurrent FIX clients on a single thread,
backed by an `io_uring` event loop.

This document describes the gateway's externally visible behavior:
session lifecycle, sequence number handling, gap recovery, and the
operational guarantees an exchange operator can rely on.

## Session model

Each TCP connection from a FIX client is one independent FIX session.
Sessions are **stateless across connections**: when a client
disconnects, the gateway forgets everything about its sequence numbers
and stored messages. A reconnecting client starts a fresh session at
`MsgSeqNum = 1` with an empty outbound store, and must re-Logon.

This is a deliberate operational choice. It avoids cross-connection
state that would otherwise need to survive process restarts (and
potentially be replicated), and it makes recovery semantics simple:
"reconnect" always means "clean slate". Clients that need durable
sequence continuity across connections must reconcile out of band
(typically via order ID).

## Logon

The first message on a new connection must be a `Logon (35=A)` with
`MsgSeqNum = 1`. The gateway validates:

- `BeginString` is `FIX.4.2`
- `TargetCompID` matches the gateway's configured `target_comp_id`
- `SenderCompID` resolves to a configured session entry
- `MsgSeqNum` is exactly `1`

Any failure terminates the connection with a `Logout (35=5)` carrying
a human-readable reason in tag 58 (Text). Clients should treat the
reason as diagnostic and not attempt automated recovery without
operator review.

After a successful Logon the gateway opens its own authenticated
connection to the Melin server on behalf of the session, and replies
to the client with a `Logon` ack once the upstream is ready.

## Sequence numbers and gap recovery

The gateway implements the standard FIX 4.2 §4.6/§4.7 gap recovery
flow on both directions.

### Inbound gaps (peer is ahead of us)

If the gateway receives a message with `MsgSeqNum` higher than
expected, it sends a `ResendRequest (35=2)` covering
`[expected, received]` and tracks the high-water mark of the gap. It
continues to process incoming messages while waiting for the gap to be
filled. The gap is closed either by the peer's replayed messages or by
a `SequenceReset (35=4)` advancing the inbound sequence past the
high-water mark.

### Outbound gaps (we are ahead of the peer)

If a peer sends a `ResendRequest`, the gateway replays its outbound
store for the requested range:

- **Application messages** (e.g. `ExecutionReport`) are re-sent verbatim
  with their original `MsgSeqNum` plus `PossDupFlag=Y` and
  `OrigSendingTime`.
- **Administrative messages** (`Heartbeat`, `TestRequest`, `Logon`,
  `Logout`, `ResendRequest`, `SequenceReset`) are never replayed.
  Consecutive runs of admin messages collapse into a single
  `SequenceReset-GapFill (35=4, GapFillFlag=Y)` telling the peer to
  skip them.

### Outbound store bounds

The gateway retains every outbound message in a per-session store so
it can answer a future `ResendRequest`. To cap per-session memory, the
store is bounded at **10,000 messages** (~2.5 MB at typical message
sizes). When the store is full, the oldest entry is evicted on each
new push.

If a peer requests resend of a sequence that has been evicted, the
gateway answers with a `SequenceReset-GapFill` covering the missing
range. This is explicitly permitted by FIX 4.2 §4.7 and tells the peer
that the messages are no longer available; the peer must treat the
range as gap-filled and continue. **Operators should size session
flows so that legitimate gap recovery never spans more than 10k
messages**; clients that fall behind by more than that must reconcile
out of band.

## Heartbeats and TestRequest

After Logon the gateway sends `Heartbeat (35=0)` messages whenever it
has sent nothing for the configured `HeartBtInt`. If it has received
nothing for `HeartBtInt`, it sends a `TestRequest (35=1)` to probe
the client. If the `TestRequest` is not answered within another
`HeartBtInt`, the gateway disconnects the session.

## Translation to Melin

`NewOrderSingle (35=D)`, `OrderCancelRequest (35=F)`, and
`OrderCancelReplaceRequest (35=G)` are translated to the corresponding
internal Melin requests. Symbols are mapped via the gateway's symbol
table; prices and quantities are converted from FIX decimal strings to
internal tick units using each symbol's configured `tick_size_inverse`
and `lot_size_inverse`. Conversions use checked integer arithmetic and
reject malformed, negative, overflowing, or out-of-precision values
without forwarding them to the engine.

Execution reports from Melin are translated back into
`ExecutionReport (35=8)` or `OrderCancelReject (35=9)` as appropriate
and queued for the FIX client.

## Metrics

When `metrics_addr` is set in the config, the gateway exposes a
Prometheus-compatible `/metrics` endpoint on that address. Counters
are collected on the io_uring hot path with relaxed atomics (no lock
contention) and read on demand by the scrape thread.

Exposed series:

| Metric | Type | Meaning |
|---|---|---|
| `fix_gateway_sessions_accepted_total` | counter | Cumulative client sessions accepted |
| `fix_gateway_sessions_active` | gauge | Currently active sessions |
| `fix_gateway_messages_received_total` | counter | Complete FIX frames received (including parse failures) |
| `fix_gateway_messages_sent_total` | counter | FIX frames written to clients (including resend replays) |
| `fix_gateway_parse_errors_total` | counter | Inbound frames that failed to parse |
| `fix_gateway_resend_requests_sent_total` | counter | ResendRequests sent in response to inbound gaps |
| `fix_gateway_resend_requests_received_total` | counter | ResendRequests received from peers |
| `fix_gateway_store_evictions_total` | counter | Outbound store entries evicted at the per-session cap |
| `fix_gateway_rate_limit_hits_total` | counter | Inbound messages dropped by per-session rate limit |

If `metrics_addr` is omitted the counters are still maintained but
no scrape endpoint is exposed.

## Configuration

Sessions, symbols, and per-session limits are loaded from a TOML file
at startup. The gateway validates the file at load time and refuses to
start if any of the following hold:

- Duplicate `sender_comp_id` across sessions
- Duplicate `fix_symbol` across symbols
- `tick_size_inverse` or `lot_size_inverse` is `0` for any symbol
- `server_addr` is not IPv4

See [admin-guide.md](admin-guide.md) for the full configuration
schema.

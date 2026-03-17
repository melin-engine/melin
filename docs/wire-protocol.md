# Wire Protocol Specification

Binary wire protocol for client-server communication over TCP or Unix domain sockets. Manual serialization (no serde) for zero allocation, predictable layout, and no format stability concerns across dependency versions.

All multi-byte integers are **little-endian**. No CRC on the wire -- TCP handles integrity.

## Frame Format

Every message is wrapped in a length-prefixed frame:

```
+----------------+----------+-----------+
| length (4B LE) | tag (1B) | payload   |
+----------------+----------+-----------+
```

| Field   | Type | Size    | Description                                    |
|---------|------|---------|------------------------------------------------|
| length  | u32  | 4 bytes | Byte count of tag + payload (excludes itself)  |
| tag     | u8   | 1 byte  | Message type discriminant                      |
| payload | ...  | 0..N    | Variant-specific fields                        |

**Maximum frame payload size**: 1024 bytes (1 KiB). Frames exceeding this limit are rejected and the connection is closed.

The `BlockingFrameReader` reads the 4-byte length prefix, validates it against the 1024-byte limit, then reads exactly that many bytes. The `BlockingFrameWriter` prepends the 4-byte LE length prefix before the payload.

---

## Type Reference

These types appear throughout the field layouts below:

| Type       | Wire size | Encoding                              |
|------------|-----------|---------------------------------------|
| Symbol     | 4 bytes   | u32 LE instrument identifier          |
| OrderId    | 8 bytes   | u64 LE                                |
| AccountId  | 4 bytes   | u32 LE                                |
| CurrencyId | 4 bytes   | u32 LE                                |
| Price      | 8 bytes   | u64 LE (NonZeroU64, must not be zero) |
| Quantity   | 8 bytes   | u64 LE (NonZeroU64, must not be zero) |
| Side       | 1 byte    | 0 = Buy, 1 = Sell                     |
| TimeInForce| 1 byte    | 0 = GTC, 1 = IOC, 2 = FOK            |
| SelfTradePrevention | 1 byte | 0 = Allow, 1 = CancelNewest, 2 = CancelOldest, 3 = CancelBoth |

### Order Encoding

Orders are encoded inline within `SubmitOrder` requests. The layout is variable-length because the order type fields differ:

```
id(8) + account(4) + side(1) + order_type_tag(1) + order_type_fields(0..16) + tif(1) + quantity(8) + stp(1)
```

| Offset | Field           | Size   | Notes                                    |
|--------|-----------------|--------|------------------------------------------|
| 0      | id              | 8      | OrderId, u64 LE                          |
| 8      | account         | 4      | AccountId, u32 LE                        |
| 12     | side            | 1      | 0=Buy, 1=Sell                            |
| 13     | order_type_tag  | 1      | See below                                |
| 14     | order_type_data | 0..16  | Variable, depends on order_type_tag      |
| ...    | time_in_force   | 1      | 0=GTC, 1=IOC, 2=FOK                     |
| ...    | quantity        | 8      | u64 LE (NonZeroU64)                      |
| ...    | stp             | 1      | Self-trade prevention mode               |

**Order type tags and fields**:

| Tag | Type      | Extra fields                                   | Extra size |
|-----|-----------|------------------------------------------------|------------|
| 0   | Market    | (none)                                         | 0 bytes    |
| 1   | Limit     | price (u64 LE)                                 | 8 bytes    |
| 2   | Stop      | trigger_price (u64 LE)                         | 8 bytes    |
| 3   | StopLimit | trigger_price (u64 LE) + limit_price (u64 LE)  | 16 bytes   |

Total order size: 24 bytes (Market) to 40 bytes (StopLimit).

---

## Request Messages (Client to Server)

| Tag | Name              | Permission     | Payload size         |
|-----|-------------------|----------------|----------------------|
| 1   | SubmitOrder       | Admin, Trader  | 4 + 24..40 (variable)|
| 2   | CancelOrder       | Admin, Trader  | 12                   |
| 3   | Heartbeat         | Any            | 0                    |
| 4   | CancelAll         | Admin, Trader  | 4                    |
| 5   | ChallengeResponse | Any (pre-auth) | 96                   |
| 6   | AddInstrument     | Admin          | 12                   |
| 7   | Deposit           | Admin          | 16                   |
| 8   | SetRiskLimits     | Admin          | 5..21 (variable)     |
| 9   | SetCircuitBreaker | Admin          | 5..21 (variable)     |
| 10  | CancelReplace     | Admin, Trader  | 28                   |
| 30  | QueryStats        | Admin          | 0                    |
| 31  | SetFeeSchedule    | Admin          | 8                    |

Payload sizes above exclude the 1-byte tag. The frame length = 1 (tag) + payload size.

### Tag 1: SubmitOrder

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |
| 4      | order  | 24..40   |

The order is encoded inline per the Order Encoding section above.

### Tag 2: CancelOrder

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | symbol   | 4 (u32)  |
| 4      | order_id | 8 (u64)  |

### Tag 3: Heartbeat

No payload. Tag-only message. Resets the server's idle timeout for this connection.

### Tag 4: CancelAll

| Offset | Field   | Size     |
|--------|---------|----------|
| 0      | account | 4 (u32)  |

Kill switch: cancels all resting orders and pending stops for the given account across all instruments.

### Tag 5: ChallengeResponse

| Offset | Field      | Size |
|--------|------------|------|
| 0      | signature  | 64   |
| 64     | public_key | 32   |

Ed25519 signature (64 bytes) over the server-provided 32-byte nonce, followed by the client's Ed25519 public key (32 bytes). Total payload: 96 bytes.

### Tag 6: AddInstrument

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |
| 4      | base   | 4 (u32)  |
| 8      | quote  | 4 (u32)  |

Registers a new instrument with its base and quote currency identifiers.

### Tag 7: Deposit

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | account  | 4 (u32)  |
| 4      | currency | 4 (u32)  |
| 8      | amount   | 8 (u64)  |

Credits funds to an account.

### Tag 8: SetRiskLimits

| Offset | Field                | Size     | Notes                           |
|--------|----------------------|----------|---------------------------------|
| 0      | symbol               | 4 (u32)  |                                 |
| 4      | flags                | 1        | Bitmask (see below)             |
| 5      | max_order_qty        | 0 or 8   | Present if flags bit 0 is set   |
| 5 or 13| max_order_notional   | 0 or 8   | Present if flags bit 1 is set   |

Flags byte:
- Bit 0: has `max_order_qty` (u64 LE, NonZeroU64)
- Bit 1: has `max_order_notional` (u64 LE)

Omitted fields clear the corresponding limit.

### Tag 9: SetCircuitBreaker

| Offset | Field            | Size     | Notes                           |
|--------|------------------|----------|---------------------------------|
| 0      | symbol           | 4 (u32)  |                                 |
| 4      | flags            | 1        | Bitmask (see below)             |
| 5      | price_band_lower | 0 or 8   | Present if flags bit 0 is set   |
| 5 or 13| price_band_upper | 0 or 8   | Present if flags bit 1 is set   |

Flags byte:
- Bit 0: has `price_band_lower` (u64 LE, NonZeroU64)
- Bit 1: has `price_band_upper` (u64 LE, NonZeroU64)
- Bit 2: `halted` (1 = trading halted, 0 = not halted)

### Tag 10: CancelReplace

| Offset | Field        | Size     |
|--------|--------------|----------|
| 0      | symbol       | 4 (u32)  |
| 4      | order_id     | 8 (u64)  |
| 12     | new_price    | 8 (u64)  |
| 20     | new_quantity | 8 (u64)  |

Atomically amends a resting limit order's price and quantity. Both `new_price` and `new_quantity` must be NonZeroU64. If the amendment fails, the original order remains intact.

### Tag 30: QueryStats

No payload. Tag-only message. Requests a server stats snapshot. Response is a StatsHeader followed by BatchEnd.

### Tag 31: SetFeeSchedule

| Offset | Field          | Size     |
|--------|----------------|----------|
| 0      | symbol         | 4 (u32)  |
| 4      | maker_fee_bps  | 2 (u16)  |
| 6      | taker_fee_bps  | 2 (u16)  |

Fee values are in basis points (1 bps = 0.01%).

---

## Response Messages (Server to Client)

| Tag | Name              | Payload size |
|-----|-------------------|--------------|
| 11  | Placed            | 25           |
| 12  | Fill              | 56           |
| 13  | Cancelled         | 20           |
| 14  | Triggered         | 16           |
| 15  | Rejected          | 13           |
| 16  | EngineError       | 0            |
| 17  | BatchEnd          | 0            |
| 18  | ServerReady       | 0            |
| 19  | Heartbeat         | 0            |
| 20  | Challenge         | 32           |
| 21  | AuthFailed        | 0            |
| 22  | Replaced          | 41           |
| 23  | StatsHeader       | 24           |

### Tag 11: Placed

Confirms a limit order was placed on the book (resting).

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | order_id | 8 (u64)  |
| 8      | side     | 1        |
| 9      | price    | 8 (u64)  |
| 17     | quantity | 8 (u64)  |

### Tag 12: Fill

Reports a trade execution between a maker and taker.

| Offset | Field          | Size     |
|--------|----------------|----------|
| 0      | maker_order_id | 8 (u64)  |
| 8      | taker_order_id | 8 (u64)  |
| 16     | maker_account  | 4 (u32)  |
| 20     | taker_account  | 4 (u32)  |
| 24     | price          | 8 (u64)  |
| 32     | quantity       | 8 (u64)  |
| 40     | maker_fee      | 8 (u64)  |
| 48     | taker_fee      | 8 (u64)  |

### Tag 13: Cancelled

Confirms an order was cancelled.

| Offset | Field              | Size     |
|--------|--------------------|----------|
| 0      | order_id           | 8 (u64)  |
| 8      | account            | 4 (u32)  |
| 12     | remaining_quantity | 8 (u64)  |

### Tag 14: Triggered

Reports that a stop order was triggered (converted to a market/limit order).

| Offset | Field         | Size     |
|--------|---------------|----------|
| 0      | order_id      | 8 (u64)  |
| 8      | trigger_price | 8 (u64)  |

### Tag 15: Rejected

Reports that an order was rejected by the matching engine.

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | order_id | 8 (u64)  |
| 8      | account  | 4 (u32)  |
| 12     | reason   | 1        |

**Reject reason codes**:

| Code | Reason                |
|------|-----------------------|
| 0    | NoLiquidity           |
| 1    | FOKCannotFill         |
| 2    | InsufficientBalance   |
| 3    | UnknownAccount        |
| 4    | UnknownSymbol         |
| 5    | SelfTradePrevented    |
| 6    | DuplicateOrderId      |
| 7    | ExceedsMaxOrderQty    |
| 8    | ExceedsMaxNotional    |
| 9    | TradingHalted         |
| 10   | OutsidePriceBand      |
| 11   | UnknownOrder          |
| 12   | PriceWouldCross       |

### Tag 16: EngineError

No payload. The matching engine encountered an internal error processing the request.

### Tag 17: BatchEnd

No payload. Signals the end of a response batch for a single request. See "BatchEnd Semantics" below.

### Tag 18: ServerReady

No payload. Sent after successful authentication to indicate the client may begin sending requests.

### Tag 19: Heartbeat (response)

No payload. Server-initiated keepalive sent to idle connections.

### Tag 20: Challenge

| Offset | Field | Size |
|--------|-------|------|
| 0      | nonce | 32   |

Sent immediately after connection acceptance. Contains a 32-byte random nonce that the client must sign with its Ed25519 private key.

### Tag 21: AuthFailed

No payload. Authentication failed (invalid signature, unknown key, or other auth error). The server drops the connection after sending this.

### Tag 22: Replaced

Confirms a cancel-replace amendment succeeded.

| Offset | Field         | Size     |
|--------|---------------|----------|
| 0      | order_id      | 8 (u64)  |
| 8      | side          | 1        |
| 9      | old_price     | 8 (u64)  |
| 17     | new_price     | 8 (u64)  |
| 25     | old_remaining | 8 (u64)  |
| 33     | new_remaining | 8 (u64)  |

### Tag 23: StatsHeader

Server stats snapshot, sent in response to `QueryStats`.

| Offset | Field              | Size     |
|--------|--------------------|----------|
| 0      | active_connections | 8 (u64)  |
| 8      | events_processed   | 8 (u64)  |
| 16     | journal_sequence   | 8 (u64)  |

---

## Authentication Handshake

Every connection must complete an Ed25519 challenge-response handshake before sending any trading or admin requests. The handshake runs on the accept thread (cold path), not the matching engine hot path.

### Flow

```
Client                              Server
  |                                    |
  |  <--- TCP/UDS connect --->         |
  |                                    |
  |    Challenge (tag=20, 32B nonce)   |
  |  <---------------------------------|  Server generates 32 random bytes
  |                                    |
  |  ChallengeResponse (tag=5)        |
  |  sig(64B) + pubkey(32B)            |
  |  --------------------------------->|  Client signs nonce with Ed25519 key
  |                                    |
  |         [verify signature]         |
  |         [lookup pubkey in          |
  |          authorized_keys]          |
  |                                    |
  |    ServerReady (tag=18)            |
  |  <---------------------------------|  Auth succeeded, normal operation begins
  |                                    |
  |  --- OR ---                        |
  |                                    |
  |    AuthFailed (tag=21)             |
  |  <---------------------------------|  Auth failed, connection dropped
```

### Timeout

The server sets a **5-second read timeout** on the socket during the auth handshake. If the client does not send a `ChallengeResponse` within 5 seconds, the connection is closed. The timeout is cleared after successful authentication.

### Auth frame reading

During the handshake, the server reads the `ChallengeResponse` frame using raw `read_exact` (not `BufReader`) to avoid over-reading bytes that belong to the first post-auth request. Those bytes would be lost when the fd moves to the epoll/io_uring reader pool.

The maximum accepted auth frame size is 256 bytes. The expected `ChallengeResponse` frame is 97 bytes (1 tag + 64 signature + 32 public key).

### Post-auth behavior

After authentication, any `ChallengeResponse` messages are silently ignored by the reader thread.

---

## Permission Model

Permission levels are assigned per public key in the `authorized_keys` file and checked on the reader thread (zero cost on the hot path).

### Permission levels

| Level    | Trading | Admin | Heartbeat |
|----------|---------|-------|-----------|
| Admin    | Yes     | Yes   | Yes       |
| Trader   | Yes     | No    | Yes       |
| ReadOnly | No      | No    | Yes       |

**Trading operations** (require `Admin` or `Trader`):
- SubmitOrder, CancelOrder, CancelAll, CancelReplace

**Administrative operations** (require `Admin`):
- AddInstrument, Deposit, SetRiskLimits, SetCircuitBreaker, SetFeeSchedule, QueryStats

**Universal operations** (any permission level):
- Heartbeat

Permission checking uses `Request::requires_admin()` for admin gating and `Permission::can_trade()` for trading gating. Requests that fail the permission check are dropped on the reader thread and never reach the matching engine.

### Authorized keys file format

```
# <permission> <base64-public-key> <optional-comment>
admin AAAA...base64...= ops-team
trader BBBB...base64...= market-maker-1
readonly DDDD...base64...= monitoring
```

Lines starting with `#` and empty lines are ignored. Public keys are 32-byte Ed25519 keys encoded in standard base64. If a key appears multiple times, the last entry wins.

---

## Heartbeat and Keepalive

### Client-to-server heartbeat (tag 3)

A tag-only request with no payload. The server's reader thread handles it inline: any received data (including heartbeats) resets the connection's `last_activity` timestamp. Heartbeat requests do **not** enter the disruptor pipeline -- they are filtered out on the reader thread.

### Server-to-client heartbeat (tag 19)

The response stage sends heartbeat responses to connections that have been idle for the configured interval. Default: **10 seconds** (`--heartbeat-interval-secs`).

### Connection timeout

If no data is received from a client within the configured window, the connection is closed. Default: **30 seconds** (`--connection-timeout-secs`). Set to 0 to disable. The timeout is checked approximately once per second via a coarse scan to avoid overhead during high throughput.

Clients should send heartbeat requests at an interval shorter than the connection timeout to prevent disconnection during idle periods.

---

## BatchEnd Semantics

A single request can produce multiple response messages. For example, a `SubmitOrder` that crosses multiple resting orders produces:

1. One or more **Fill** reports (one per price level matched)
2. Possibly a **Placed** report (if the order partially fills and the remainder rests)
3. Possibly a **Rejected** report (if the order is rejected)
4. A **BatchEnd** to signal completion

The **BatchEnd** (tag 17) message tells the client that all reports for the preceding request have been sent. This allows pipelined clients to correlate responses with requests: after sending N requests, the client reads responses until it receives N BatchEnd markers.

For requests that produce a single response (e.g., `CancelOrder` produces one `Cancelled` or `Rejected`), BatchEnd still follows to maintain the uniform protocol.

---

## Byte-Level Encoding Examples

### Example 1: Heartbeat Request

The simplest possible message -- tag only, no payload.

```
Frame (5 bytes total):
  [01 00 00 00]   length = 1 (LE u32)
  [03]            tag = 3 (Heartbeat)
```

### Example 2: CancelOrder Request

Cancel order ID 42 on symbol 1.

```
Frame (17 bytes total):
  [0D 00 00 00]   length = 13 (LE u32)
  [02]            tag = 2 (CancelOrder)
  [01 00 00 00]   symbol = 1 (LE u32)
  [2A 00 00 00    order_id = 42 (LE u64)
   00 00 00 00]
```

### Example 3: SubmitOrder Request (Limit Buy)

Submit a GTC limit buy: order ID 100, account 42, price 5000, quantity 10, STP=CancelNewest, on symbol 1.

```
Frame (34 bytes total):
  [1E 00 00 00]   length = 30 (LE u32: 1 tag + 4 symbol + 25 order)
  [01]            tag = 1 (SubmitOrder)
  [01 00 00 00]   symbol = 1 (LE u32)
  [64 00 00 00    order.id = 100 (LE u64)
   00 00 00 00]
  [2A 00 00 00]   order.account = 42 (LE u32)
  [00]            order.side = 0 (Buy)
  [01]            order.order_type_tag = 1 (Limit)
  [88 13 00 00    order.price = 5000 (LE u64)
   00 00 00 00]
  [00]            order.time_in_force = 0 (GTC)
  [0A 00 00 00    order.quantity = 10 (LE u64)
   00 00 00 00]
  [01]            order.stp = 1 (CancelNewest)
```

### Example 4: BatchEnd Response

```
Frame (5 bytes total):
  [01 00 00 00]   length = 1 (LE u32)
  [11]            tag = 17 (BatchEnd)
```

### Example 5: Challenge Response (server to client)

```
Frame (37 bytes total):
  [21 00 00 00]   length = 33 (LE u32: 1 tag + 32 nonce)
  [14]            tag = 20 (Challenge)
  [xx xx ... xx]  nonce (32 random bytes)
```

---

## Error Handling

- **Truncated frames**: If a frame's payload is shorter than expected for its tag, the codec returns `ProtocolError::Truncated`.
- **Unknown tags**: Unrecognized tag bytes produce `ProtocolError::UnknownTag(tag)`.
- **Invalid fields**: Zero values in NonZeroU64 fields (prices, quantities) produce `ProtocolError::InvalidField`.
- **Oversized frames**: Frames with length > 1024 bytes are rejected at the framing layer and the connection is closed.

---

## Source Files

- `crates/protocol/src/codec.rs` -- encode/decode functions, tag constants, field layouts
- `crates/protocol/src/message.rs` -- `Request` and `ResponseKind` enum definitions
- `crates/protocol/src/blocking.rs` -- `BlockingFrameReader`/`BlockingFrameWriter`, length-prefixed framing
- `crates/protocol/src/auth.rs` -- `Permission` enum, `AuthorizedKeys` file loader
- `crates/server/src/server.rs` -- authentication handshake implementation (`authenticate_connection`)
- `crates/engine/src/le.rs` -- shared little-endian helpers and enum encoding (Side, TimeInForce, SelfTradeProtection)

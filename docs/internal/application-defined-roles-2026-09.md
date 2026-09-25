# Application-defined client roles (plan)

Status: **proposed, not started** (2026-09). Implements the roadmap item
"Application-defined client roles" ([roadmap.md](roadmap.md)); read that
entry for the problem statement. This document records what the code
actually looks like against that entry, the design decisions, and the
order of work.

The one-line goal: **the runtime owns the roles it acts on, and the
application owns every other one.** `operator` and `replication` stay the
runtime's; every other token in `authorized_keys` names a role the
application declares as its own type, and the decoder matches on that
type.

## What the code shows

Findings that confirm, sharpen or correct the roadmap entry.

- **The runtime acts on two roles, in four places.** The admin endpoint
  requires `Operator` (`admin.rs`). The replication handshake requires
  `Replication` (`replication/auth.rs`), and so does the raft crate's
  own peer handshake (`melin-raft`'s `auth.rs`), which gets the same
  keys table. The client listener refuses `Replication`
  (`client_auth.rs`, shared by the kernel TCP and DPDK handshakes).
  Nothing else in the runtime reads a role. It stores one per connection
  (the reader's connection registration, the DPDK `AuthState`) and hands
  it to the decoder on every request (`client_frames.rs`).
- **`operator` is also an application role.** The exchange takes
  `AddInstrument`, risk limits and the rest over the client listener,
  gated on `is_operator()`. Making `operator` admin-only would move
  those requests off the client protocol. It stays a role the decoder
  sees (decision 2).
- **The decoder can already never see `Replication`.** The client
  listener refuses such a key during the handshake, yet the type still
  has the variant, so every decoder's `match` carries an arm for a case
  that cannot happen. The roadmap entry lists `Replication` among what
  the decoder receives; this plan removes it from that type.
- **Roles never reach the journal.** `ApplyCtx` carries `key_hash`, not
  the role, and no journal or replication frame encodes one. Changing
  the role type changes no on-disk or on-wire format.
- **The keys table leaves the runtime in one place.** `EventPublisherFn`
  hands `Arc<AuthorizedKeys>` to the application's event publisher. The
  exchange's publisher uses it to admit subscribers by the client
  listener's rule (`verify_subscriber`) and discards the role it gets
  back. It needs the runtime's refusal of `replication`, not the
  application's role.
- **The table is loaded in four places,** one per transport and node
  mode (kernel TCP and DPDK, replica and primary start), each reading
  `config.authorized_keys` in its own `AuthorizedKeys::load`. Each is a
  point where the decoder's role type has to meet the file.
- **Every in-repo decoder uses the same test.** Echo, counter and notary
  all refuse writes from `ReadOnly` and admit every other role; their
  tests and quick-start lines write `trader` keys. The runtime's own
  test decoder (`reader.rs`) ignores the role.
- **Exchange Core follows `main` by git dependency.** When this merges,
  its build breaks until its migration lands (see "Downstream impact").

## Design decisions

### 1. The application declares its roles as a type

A new trait in `melin_app::auth`:

```rust
pub trait Role: Copy + Eq + Debug + Send + Sync + 'static {
    /// Every role, each with the token that names it in
    /// `authorized_keys`.
    const ROLES: &'static [(&'static str, Self)];
}
```

`RequestDecoder` gains `type Role: Role`, and `decode` receives
`ClientRole<Self::Role>` (decision 2). The exchange declares
`trader`, `custodian` and `readonly`, so its key files keep working
unchanged. A payments system declares `payer` and `auditor`.

One table pairing each token with its value, not a `token()` method
beside a list of values: the two could not then disagree.

The table is validated whenever a keys file is parsed, before anything
binds a port. Parsing refuses:

- a token the runtime owns (`operator`, `replication`);
- a token listed twice, and a value listed twice (one token per role;
  aliases can be allowed later without breaking anything, but not
  withdrawn once allowed);
- an empty token, one containing whitespace, or one starting with `#`,
  none of which the file format can express;
- a table longer than a `RoleId` can index (decision 3).

Parsing an empty file validates the table as well, so an application
pins its own table with a one-line test
(`AuthorizedKeys::parse::<MyRole>("")`). The same check at compile
time would need string comparison in `const` evaluation of a trait's
constant, and it would fail as a post-monomorphization error that
points into `melin-app`. Checked at startup, the node still refuses to
serve, and the message names the bad token.

An unknown token in the file names every valid one in its error,
runtime tokens first: `unknown role 'tradr' (expected operator,
replication, trader, custodian, readonly)`.

`melin_app::auth::NoRoles`, an empty role type in the manner of
`NoQuery`, is for a keys table that needs no application roles: the
runtime's own tests, and tools that only authenticate replicas (the
exchange's `replication-bench`). It is not a default access model: an
application on `NoRoles` admits operator keys only.

### 2. The decoder sees `ClientRole<R>`: `Operator` or `App(R)`

```rust
pub enum ClientRole<R> {
    Operator,
    App(R),
}
```

No `Replication` variant. The client listener refuses those keys during
the handshake, so the type says so, and no decoder writes an arm for
it. `is_operator()` stays. `can_trade()` and `can_manage_funds()` leave
the runtime; the exchange defines what its roles may do on its own
type.

`Permission` is renamed `ClientRole`, and `decode`'s `permission`
parameter `role`. The value now says who the connection is, not what it
may do: deciding that is the decoder's job, and a refusal is still
`Decoded::PermissionDenied`. The keys file, the operator docs and the
new `Role`, `RoleId` and `KeyRole` all speak of roles, and
`Permission<ExchangeRole>` would read as a permission holding a role.
The rename adds no call site to the migration, since every decoder's
signature and every named variant change in step 1 anyway; done later,
it would be a second breaking change for every application.

### 3. Typed at the edge, erased inside the runtime

The runtime never names the application's role type. Inside it a role is
an index into the application's table:

- **`RoleId(u8)`**, the index of a role in `R::ROLES`. `u8` because a
  connection's role sits beside other per-connection state on the reader
  and in the DPDK connection table, and no access model needs more than
  a few hundred roles. Its field is private to `melin-app`; only
  parsing a keys file makes one, so every `RoleId` indexes a real
  table.
- **`KeyRole`**, what the keys table maps a key to: `Replication`, or
  `Client(ClientRole<RoleId>)`. `may_connect_as_client()` and
  `is_replication()` move here from `Permission`, since that is where
  `Replication` now lives. The admin endpoint, the replication
  handshake, `melin-raft`'s peer handshake, the client listener and the
  exchange's subscriber handshake all decide on `KeyRole` and never
  need the application's type.
- **`AuthorizedKeys` stays one non-generic type.** `parse::<R>` and
  `load::<R>` are generic; the table they build is not. It keeps the
  tokens so logs and errors name a key's role by its token rather than
  an index, and it records `TypeId::of::<R>()`.
- **The runtime holds an erased decoder.** A sealed, object-safe
  `ErasedDecoder<E>` in `melin_app::decoder`, blanket-implemented for
  every `D: RequestDecoder<Event = E>`, takes `ClientRole<RoleId>`,
  turns `App(id)` back into `D::Role` through `D::Role::ROLES`, and
  calls the typed `decode`. It also loads the keys table for its role
  type, so the runtime gets the table from the decoder and the two are
  paired where they are built. The runtime's `RequestDecoderArc<A>`
  becomes `Arc<dyn ErasedDecoder<A::Event>>`, and `run`,
  `run_with_listener` and the DPDK entry take `impl RequestDecoder`
  exactly as today.

The conversion runs once per request on the reader thread (the DPDK
poll loop on that transport): one bounds-checked load from a static
table. It never runs on the business-logic thread.

Where the runtime pairs a table with a decoder, it checks the table's
recorded `TypeId` against the decoder's role type, and refuses to start
on a mismatch. With the table loaded through the decoder this cannot
fire today. It keeps a future path that passes a table in from
elsewhere from ever turning an index into the wrong role at decode
time, which would grant one role another's rights in silence.

The four load sites become one helper that loads through the decoder and
logs the count, so there is one place where the file meets the role
type.

Rejected: threading `R` through the runtime as a type parameter
(`AuthorizedKeys<R>`, a generic reader, DPDK transport, admin endpoint,
replication handshake and raft driver). It type-checks the same things
at the application boundary, but it puts a parameter on every runtime
component that touches the keys table, none of which looks at an
application role, and it changes
`EventPublisherFn`'s signature for a publisher that only needs
`may_connect_as_client`.

### 4. The examples speak their own vocabulary

Each example declares its own role type, and the counter becomes the
reference for how:

- counter and echo: `writer` and `reader`;
- notary: `submitter` and `auditor`, the domain's own words.

No shared read/write role type in `melin-app`: it would be the
fixed-set option the roadmap entry rejects, arriving by the back door
as the thing every new application copies. The declaration is a few
lines. A `roles!` macro can come later if it proves tedious; it is not
part of this item.

## Order of work

One commit per step, each reviewable on its own.

1. **Role model and erased seam** (`melin-app`, `server-runtime`,
   `melin-raft`, the three examples, the doc-tested
   `building-an-application.md`): the
   `Role` trait with table validation, `NoRoles`, `RoleId`, `KeyRole`,
   `ClientRole<R>` replacing `Permission`, `AuthorizedKeys::parse::<R>` /
   `load::<R>` recording the role type, `RequestDecoder::Role`, and
   `ErasedDecoder` with its blanket impl. The runtime moves to
   `KeyRole` in the admin endpoint, the replication handshake,
   `melin-raft`'s peer handshake and the client listener (tests that
   build a table for replication keys only parse it with `NoRoles`),
   stores `ClientRole<RoleId>` per connection, loads
   the table through the decoder in one helper with the `TypeId` check,
   and logs roles by token. The examples declare a role type that keeps
   today's tokens (`trader`, `readonly`, and the rest they use), so
   this step is mechanical for them and no key file changes. One commit
   because the new `decode` signature breaks every decoder at once.

   Tests, in `melin-app` unless noted:
   - table validation: each refusal in decision 1, including on an
     empty file;
   - parsing: runtime tokens take precedence, app tokens map to their
     values, the unknown-token error names every valid token;
   - a round trip for every role of a test role type: listed in a file,
     parsed, looked up, and passed through the erased decoder to a
     recording decoder, which must receive exactly that role, and
     `Operator` as `Operator`;
   - the `TypeId` mismatch refused (the runtime's startup helper);
   - the existing handshake tests (`client_auth.rs`, `admin.rs`,
     `replication/auth.rs`, `melin-raft`'s `auth.rs`, `server.rs`) on
     `KeyRole`, with a
     replication key still refused on the client listener.
2. **The examples adopt their own vocabulary** (decision 4): new role
   types, their decoders, unit tests and `round_trip.rs` key files, and
   the `echo "trader $PUB me"` quick-start lines in `echo` and `notary`.
   A reviewer can see here what an application author writes.
3. **Docs:** `building-an-application.md`'s roles section rewritten
   around declaring a role type (the runtime's two roles, what the
   decoder receives, how the table is validated); the quick-start line
   there; `melin-client`'s `authorized_keys_line` doc, which lists the
   runtime's roles; CHANGELOG under Unreleased (`Permission` renamed
   `ClientRole` with its variants replaced, and `decode`'s parameter,
   under **Changed**; `can_trade` and `can_manage_funds` under
   **Removed**; `may_connect_as_client` moving to `KeyRole` and the new
   types under **Added**); the S6 note in
   [application-api-review-2026-09.md](application-api-review-2026-09.md);
   remove the roadmap entry.

No journal format or replication protocol bump: roles are never
journaled or sent between nodes.

## Downstream impact (Exchange Core)

Changed, in one commit on its side, ready to land as soon as this merges
(its git dependency on `main` breaks until then):

- `ExchangeRole { Trader, Custodian, ReadOnly }` with the tokens
  `trader`, `custodian`, `readonly`: existing key files load unchanged.
- `RequestDecoder` declares `type Role = ExchangeRole`;
  `check_permission` matches on `ClientRole<ExchangeRole>`, with
  `can_trade` and `can_manage_funds` moving onto `ExchangeRole` or into
  the match. Its tests move from `Permission::Trader` to
  `ClientRole::App(ExchangeRole::Trader)`.
- `event_publisher.rs`: `verify_subscriber` decides on `KeyRole`, and
  `SubscriberAuthError::RoleRefused` carries one.
- `replication-bench` parses its keys with `NoRoles`.
- `message.rs`'s docs on `requires_operator` and fund management name
  the new types.

## Not part of this item

- **Several roles per key.** Separation of duties stays one key, one
  role. An application that wants a key to hold several capabilities
  can model that inside its own role type.
- **Scoping a key to accounts or other resources.** Operators of trading
  venues will ask for a key restricted to certain accounts. Nothing here
  blocks it: a later version can let the role parser read further fields
  on the key's line. It is a separate design.
- **Role aliases**, for renaming a role without editing key files.
  Refused for now (decision 1); allowing them later breaks nothing.
- **A `roles!` macro.** See decision 4.
- **Reloading the keys file without a restart.** Unchanged: the table is
  loaded once at startup, as today.

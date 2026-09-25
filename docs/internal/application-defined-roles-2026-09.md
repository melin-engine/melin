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
  has the variant, so a decoder that matches exhaustively must write an
  arm for a case that cannot happen. The roadmap entry lists
  `Replication` among what the decoder receives; this plan removes it
  from that type.
- **Roles never reach the journal or the wire.** `ApplyCtx` carries
  `key_hash`, not the role, and no journal, replication or client frame
  encodes one. Changing the role type changes no on-disk or on-wire
  format, and `melin-client`, which is published, gets no API change:
  only the doc comment of `authorized_keys_line`, which lists the
  runtime's roles.
- **The keys table leaves the runtime in one place.** `EventPublisherFn`
  hands `Arc<AuthorizedKeys>` to the application's event publisher. The
  exchange's publisher uses it to admit subscribers by the client
  listener's rule (`verify_subscriber`) and discards the role it gets
  back. It needs the runtime's refusal of `replication`, not the
  application's role.
- **The table is loaded in four places,** one per transport and node
  mode (kernel TCP and DPDK, replica and primary start), each reading
  `config.authorized_keys` in its own `AuthorizedKeys::load`, all before
  journal recovery. Each is a point where the decoder's role type has
  to meet the file.
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

The order of the table is free. A role travels inside the runtime as its
index in the table (decision 3), but that index is never persisted or
sent: not journaled, not in a snapshot, not on any wire. Adding a role
in the middle of the table, or reordering it, between two builds changes
nothing a node reads back.

`melin_app::auth::validate_roles::<R>()` checks the table, and every
parse of a keys file calls it first, so a node refuses a bad table
before it binds a port. It refuses:

- a token the runtime owns (`operator`, `replication`);
- a token listed twice, and a value listed twice (one token per role);
- a token outside `[a-z][a-z0-9_-]*`: lowercase ASCII letters, digits,
  `-` and `_`, starting with a letter. That also rules out the empty
  token, whitespace and a leading `#`, which the file format cannot
  express;
- a table longer than a `RoleId` can index (decision 3).

A token the runtime claims in a later release collides with any
application that already declared it. That is a breaking change for
those applications, and it surfaces as this refusal at startup, naming
the token, never as a key silently granted the runtime's role. No
namespace is reserved in advance: a prefix every application token
must carry would be paid on every line of every keys file for a
collision that may never happen.

Matching a token in the file is exact and case-sensitive. The likeliest
slip, `Trader` in one place and `trader` in the other, cannot come from
the type, whose tokens are lowercase by the rule above; from the file,
it fails the load with an error that lists `trader`. Every token in use
today fits the charset. As with
aliases (refused too: one token per role), each of these refusals can
be relaxed later without breaking anything, and none can be tightened
once relaxed.

An application pins its own table with a one-line test,
`validate_roles::<MyRole>()`. The same check at compile time would need
string comparison in `const` evaluation of a trait's constant, and it
would fail as a post-monomorphization error that points into
`melin-app`. Checked at startup, the node still refuses to serve, and
the message names the bad token.

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
it. `is_operator()` stays.

Exhaustive on purpose: not `#[non_exhaustive]`. That attribute would
force every decoder outside `melin-app` to write a wildcard arm, and a
wildcard in an access check is a default grant: a role the runtime
adds later would inherit whatever the wildcard allows, silently. As an
exhaustive enum, a new runtime role is a compile error in every
decoder, which is what it should be. `can_trade()` and `can_manage_funds()` leave
the runtime; the exchange defines what its roles may do on its own
type.

`Permission` is renamed `ClientRole`, and `decode`'s `permission`
parameter `role`. The value now says who the connection is, not what it
may do: deciding that is the decoder's job, and a refusal is still
`Decoded::PermissionDenied`. The keys file, the operator docs and the
new `Role`, `RoleId` and `KeyRole` all speak of roles, and
`Permission<ExchangeRole>` would read as a permission holding a role.
The rename adds no call site to the migration, since every decoder's
signature and every named variant change in step 2 anyway; done later,
it would be a second breaking change for every application.

### 3. Typed at the edge, erased inside the runtime

The runtime never names the application's role type. Inside it a role is
an index into the application's table:

- **`RoleId(u8)`**, the index of a role in `R::ROLES`. `u8` because a
  connection's role sits beside other per-connection state on the reader
  and in the DPDK connection table, and no access model needs more than
  256 roles. Its field is private to `melin-app`; only parsing a keys
  file makes one, so every `RoleId` indexes a real table. Never
  persisted or sent (decision 1).
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
  calls the typed `decode`. It also reports its role type's `TypeId`.
  The runtime's `RequestDecoderArc<A>` becomes
  `Arc<dyn ErasedDecoder<A::Event>>`, and `run` and `run_with_listener`
  take `impl RequestDecoder` exactly as today.

The conversion runs once per request on the reader thread (the DPDK
poll loop on that transport): one bounds-checked load from a static
table. It never runs on the business-logic thread. The call into the
decoder stays one indirect call per request, as today: the blanket
impl calls the typed `decode` statically. A failed bounds
check can only be a bug, never client input, and the reader thread must
not panic on it: the erased decoder logs an `error!` naming the index
and the table's length, and answers `PermissionDenied` without calling
the typed `decode`. It fails closed. The `TypeId` check below makes it
unreachable, which is why it gets a stated behaviour rather than an
index expression that would panic.

**The keys table is loaded where the role type is still in scope.**
`run` and `run_with_listener`, the only public entry points (the DPDK
path is reached through `run`), load the table with
`AuthorizedKeys::load::<D::Role>` before they erase the decoder, and
pass the `Arc<AuthorizedKeys>` down the internal chain (`run_tcp`,
`run_impl`, `run_dpdk`, `run_dpdk_impl`) beside it. The four load sites
go. The decoder stays a decoder: reading a file is not its job.

Where the table and the erased decoder arrive together as separate
parameters (`run_tcp` and `run_dpdk`), the runtime checks the table's
recorded `TypeId` against the decoder's and refuses to start on a
mismatch. Both come from the same `D` in the same entry function, so
the pairing is correct by construction today. The check sits at the
seam a later refactor could break, where it keeps a table from another
role type from ever turning an index into the wrong role at decode time,
which would grant one role another's rights in silence.

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

1. **`KeyRole`, and `Replication` out of the decoder's type**
   (`melin-app`, `server-runtime`, `melin-raft`): the keys table maps a
   key to `KeyRole { Replication, Client(Permission) }`, and
   `Permission` loses its `Replication` variant (decision 2 without the
   rename, which comes next). `may_connect_as_client()` and
   `is_replication()` move to `KeyRole`. The admin endpoint, the
   replication handshake, `melin-raft`'s peer handshake and the client
   listener decide on `KeyRole`, and the handshake logs name a key's
   role by its token. The tokens and the decoder signature are
   unchanged, so no decoder and no key file in this repo changes. It
   already breaks Exchange Core, whose subscriber check calls
   `may_connect_as_client()` on a `Permission` and whose tests name
   `Permission::Replication`: the branch merges as a whole, and the
   exchange migrates once, after the merge, not step by step. This commit is
   the handshake diff on its own, and it survives the next one, which
   only changes what `KeyRole::Client` holds.

   Tests: the existing handshake tests (`client_auth.rs`, `admin.rs`,
   `replication/auth.rs`, `melin-raft`'s `auth.rs`, `server.rs`) on
   `KeyRole`, a replication key still refused on the client listener,
   and a log-by-token check on the refusal message.
2. **Role model and erased seam** (`melin-app`, `server-runtime`,
   `melin-raft`, the three examples, the doc-tested
   `building-an-application.md`): the `Role` trait, `validate_roles`,
   `NoRoles`, `RoleId`, `ClientRole<R>` replacing `Permission` (so
   `KeyRole::Client(Permission)` becomes
   `KeyRole::Client(ClientRole<RoleId>)`), `AuthorizedKeys::parse::<R>`
   / `load::<R>` recording the role type, `RequestDecoder::Role`, and
   `ErasedDecoder` with its blanket impl and fail-closed conversion.
   `run` and `run_with_listener` load the table and pass it down; the
   four load sites go; `run_tcp` and `run_dpdk` check the `TypeId`. The
   runtime stores `ClientRole<RoleId>` per connection. Tests that build
   a table for replication keys only parse it with `NoRoles`. The
   examples declare a role type that keeps today's tokens (`trader`,
   `readonly`, and the rest they use), so this step is mechanical for
   them and no key file changes. One commit because the new `decode`
   signature breaks every decoder at once.

   Tests, in `melin-app` unless noted:
   - `validate_roles`: each refusal in decision 1 (a runtime token, a
     duplicate token, a duplicate value, each charset violation, an
     oversized table), and a valid table accepted;
   - parsing: runtime tokens parse to their `KeyRole` whatever the app
     table holds, app tokens map to their values, matching is
     case-sensitive (`Trader` refused where `trader` is listed), and the
     unknown-token error names every valid token;
   - a round trip for every role of a test role type: listed in a file,
     parsed, looked up, and passed through the erased decoder to a
     recording decoder, which must receive exactly that role, and
     `Operator` as `Operator`;
   - an out-of-range `RoleId` through the erased decoder answers
     `PermissionDenied` without calling `decode` (the test builds the
     id through a table of a larger role type, the one way to reach it,
     and calls the erased decoder directly: at that layer there is no
     `TypeId` check, which lives in the runtime's entry chain);
   - a `NoRoles` table: `operator` and `replication` parse, any other
     token is refused, and its error lists only the runtime's two;
   - the `TypeId` mismatch refused at startup (`server-runtime`).
3. **The examples adopt their own vocabulary** (decision 4): new role
   types, their decoders, unit tests and `round_trip.rs` key files, and
   the `echo "trader $PUB me"` quick-start lines in `echo`, `notary` and
   `building-an-application.md` (its quick-start runs echo, so the line
   moves with echo's tokens rather than waiting for step 4). A reviewer can see
   here what an application author writes.
4. **Docs:** `building-an-application.md`'s roles section rewritten
   around declaring a role type (the runtime's two roles, what the
   decoder receives, how the table is validated); `melin-client`'s `authorized_keys_line` doc, which lists the
   runtime's roles; CHANGELOG under Unreleased (`Permission` renamed
   `ClientRole` with its variants replaced, and `decode`'s parameter,
   under **Changed**; `can_trade` and `can_manage_funds` under
   **Removed**; `may_connect_as_client` moving to `KeyRole` and the new
   types under **Added**); the S6 note in
   [application-api-review-2026-09.md](application-api-review-2026-09.md);
   remove the roadmap entry.

No journal format, replication protocol or client protocol bump: roles
are never journaled or sent over any wire.

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
  venues will ask for a key restricted to certain accounts. It is a
  separate design, and this plan leaves it room, though not where it
  might look: everything after the key on a line is a free-text comment
  today, so new trailing fields would change the meaning of existing
  files. The room is in the role field. The charset rule (decision 1)
  keeps `:`, `=` and `,` out of every token, so options attached to the
  role, in the manner of OpenSSH's `authorized_keys` options
  (`trader:accounts=17,42`), can never be mistaken for a token someone
  already declared.
- **A typed role lookup outside the decoder.** The exchange's subscriber
  handshake needs only `KeyRole`. An application that later wants to
  gate its own listener on its roles needs `ClientRole<R>` there too: a
  lookup generic over `R` that checks the table's `TypeId`, as the
  runtime does. Additive, so it waits for a user.
- **Role aliases**, for renaming a role without editing key files.
  Refused for now (decision 1); allowing them later breaks nothing.
- **A `roles!` macro.** See decision 4.
- **Reloading the keys file without a restart.** Unchanged: the table is
  loaded once at startup, as today.

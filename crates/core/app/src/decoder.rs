//! Wire-side request decoder seam.
//!
//! The server runtime (accept loop, frame reader, DPDK transport)
//! consumes incoming frames from the network and needs to turn them
//! into application events to publish to the pipeline. The decoding
//! itself — telling the application's messages apart, mapping
//! per-variant fields, enforcing per-connection role policy — is
//! application-shaped: a trading server decodes order submissions, a
//! payments server decodes transfers, a logistics server decodes
//! shipment events. This trait is the seam that lets the runtime
//! delegate that decoding to the application without ever naming the
//! concrete wire enum.
//!
//! The frame around the request is not the application's. Every client
//! frame is `[length: u32 LE][tag: u8][body]`, and the tag is the
//! protocol's: the runtime reads it, handles or drops the protocol's own
//! frames, and hands the decoder the body of an application frame alone.
//! The body's layout is the application's from its first byte, with no
//! value reserved.
//!
//! The runtime calls
//! [`RequestDecoder::decode`](crate::decoder::RequestDecoder::decode) once
//! per such frame; the [`Decoded`](crate::decoder::Decoded) return value
//! encodes exactly the four outcomes the runtime acts on (drop, publish,
//! reject with reason, log decode error).

use tracing::error;

use crate::AppEvent;
use crate::auth::{AuthorizedKeys, ClientRole, Role, RoleId};

/// Decode an authenticated client request into an application event the
/// runtime can publish to the pipeline.
///
/// Stateless on the connection (the runtime carries connection-level
/// state — the client's role, `key_hash` — and feeds the relevant piece
/// in per call). Implementors are typically zero-sized types.
pub trait RequestDecoder: Send + Sync {
    /// Application event type produced on a successful decode. The
    /// runtime wraps this in a transport-level envelope (e.g.
    /// `JournalEvent::App`) before publishing.
    type Event: AppEvent;

    /// The application's client roles, named by tokens in the
    /// `authorized_keys` file. [`NoRoles`](crate::auth::NoRoles) for an
    /// application that admits operator keys only.
    type Role: Role;

    /// Decode one request. `body` is the application frame's body, up to
    /// the end of the frame, and may be empty. `role` is the role of the
    /// key the connection authenticated with: the runtime's operator, or
    /// one of the application's own.
    fn decode(&self, body: &[u8], role: ClientRole<Self::Role>) -> Decoded<Self::Event>;
}

/// Outcome of a single [`RequestDecoder::decode`] call. The runtime
/// branches on this and never needs to know the underlying wire enum.
pub enum Decoded<E: AppEvent> {
    /// Drop the request silently. For application messages that are not
    /// events — a subscription request arriving on a connection that
    /// only submits events, say. The protocol's own frames never reach the
    /// decoder.
    Filter,
    /// Request OK and authorized. The runtime publishes the event. Whether
    /// it needs a timestamp is derived by the runtime from
    /// [`AppEvent::is_query`] — query events bypass the journal and skip
    /// the wall-clock stamp.
    Permitted(E),
    /// The connection's role may not make this request. The static string
    /// is logged at debug level on the reader thread; the runtime drops
    /// the request.
    PermissionDenied(&'static str),
    /// Decode failure (unknown message, malformed body, invalid field). The
    /// runtime logs at debug level and drops the request; the connection
    /// is not closed (a misbehaving client drops itself on the next read
    /// timeout).
    DecodeError(&'static str),
}

mod sealed {
    /// Keeps [`ErasedDecoder`](super::ErasedDecoder) implemented by its
    /// blanket impl alone.
    pub trait Sealed {}
}

impl<D: RequestDecoder> sealed::Sealed for D {}

/// A [`RequestDecoder`] as the runtime holds it: with its role type
/// erased, so nothing in the runtime names the application's roles.
///
/// Implemented for every `RequestDecoder`, and by nothing else. An
/// application implements `RequestDecoder` and never calls this.
pub trait ErasedDecoder<E: AppEvent>: sealed::Sealed + Send + Sync {
    /// Turn `role` back into the decoder's own role type and decode `body`
    /// as [`RequestDecoder::decode`] does.
    ///
    /// An application role index outside the decoder's table can only be
    /// a bug (a keys table parsed for another role type), never client
    /// input. It is logged as an error and the request is refused without
    /// reaching the decoder: failing closed, where indexing would panic the
    /// reader thread. The runtime's pairing check ([`Self::matches_keys`])
    /// keeps it unreachable.
    fn decode_erased(&self, body: &[u8], role: ClientRole<RoleId>) -> Decoded<E>;

    /// Whether `keys` was parsed for this decoder's role type, so the
    /// indices it hands out name this decoder's roles.
    fn matches_keys(&self, keys: &AuthorizedKeys) -> bool;
}

impl<D: RequestDecoder> ErasedDecoder<D::Event> for D {
    #[inline]
    fn decode_erased(&self, body: &[u8], role: ClientRole<RoleId>) -> Decoded<D::Event> {
        let role = match role {
            ClientRole::Operator => ClientRole::Operator,
            ClientRole::App(id) => match D::Role::ROLES.get(id.index()) {
                Some(&(_, role)) => ClientRole::App(role),
                None => {
                    error!(
                        index = id.index(),
                        roles = D::Role::ROLES.len(),
                        "role index outside the decoder's role table; request refused"
                    );
                    return Decoded::PermissionDenied("role index outside the role table");
                }
            },
        };
        self.decode(body, role)
    }

    fn matches_keys(&self, keys: &AuthorizedKeys) -> bool {
        keys.is_for::<D::Role>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::CodecError;
    use crate::auth::tests::DeskRole;
    use crate::auth::{KeyRole, NoRoles};

    /// Minimal event: decoding produces this for every request.
    #[derive(Debug, Clone, Copy)]
    struct Unit;

    impl AppEvent for Unit {
        const MAX_ENCODED_SIZE: usize = 0;

        fn encoded_size(&self) -> usize {
            0
        }
        fn encode(&self, _buf: &mut [u8]) -> usize {
            0
        }
        fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
            Ok(Unit)
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    /// Records the role of every request it decodes, and permits it.
    struct Recording<R: Role> {
        seen: Mutex<Vec<ClientRole<R>>>,
    }

    impl<R: Role> Recording<R> {
        fn new() -> Self {
            Recording {
                seen: Mutex::new(Vec::new()),
            }
        }
        fn seen(&self) -> Vec<ClientRole<R>> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl<R: Role> RequestDecoder for Recording<R> {
        type Event = Unit;
        type Role = R;

        fn decode(&self, _body: &[u8], role: ClientRole<R>) -> Decoded<Unit> {
            self.seen.lock().unwrap().push(role);
            Decoded::Permitted(Unit)
        }
    }

    /// The client role a one-line keys file for `R` gives `token`.
    fn client_role_of<R: Role>(token: &str) -> ClientRole<RoleId> {
        let key = [0x11u8; 32];
        let listed = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key);
        let keys = AuthorizedKeys::parse::<R>(&format!("{token} {listed} test\n")).unwrap();
        keys.lookup(&key)
            .and_then(KeyRole::client)
            .expect("a client role")
    }

    /// Every application role, listed in a file, parsed, looked up and
    /// passed through the erased decoder, reaches the typed decoder as
    /// exactly that role; the operator as the operator.
    #[test]
    fn every_role_reaches_the_decoder_as_itself() {
        let decoder = Recording::<DeskRole>::new();
        let mut expected = vec![ClientRole::Operator];
        decoder.decode_erased(b"", client_role_of::<DeskRole>("operator"));
        for &(token, role) in DeskRole::ROLES {
            decoder.decode_erased(b"", client_role_of::<DeskRole>(token));
            expected.push(ClientRole::App(role));
        }
        assert_eq!(decoder.seen(), expected);
    }

    /// An index past the decoder's table fails closed, without reaching
    /// the decoder. The only way to get one is a table parsed for a larger
    /// role type, handed straight to the erased decoder: at this layer
    /// there is no pairing check, which lives in the runtime's entry
    /// chain.
    #[test]
    fn an_index_outside_the_role_table_is_refused_without_decoding() {
        let past_the_end = client_role_of::<DeskRole>("readonly");
        let decoder = Recording::<NoRoles>::new();
        assert!(matches!(
            decoder.decode_erased(b"", past_the_end),
            Decoded::PermissionDenied(_)
        ));
        assert!(decoder.seen().is_empty());
    }

    #[test]
    fn a_decoder_matches_only_keys_parsed_for_its_role_type() {
        let decoder = Recording::<DeskRole>::new();
        assert!(decoder.matches_keys(&AuthorizedKeys::parse::<DeskRole>("").unwrap()));
        assert!(!decoder.matches_keys(&AuthorizedKeys::parse::<NoRoles>("").unwrap()));
    }
}

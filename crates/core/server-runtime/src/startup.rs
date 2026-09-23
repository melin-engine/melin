//! Application events the runtime journals on a node's own behalf.

/// Events the runtime journals as a node takes up the primary role,
/// before it serves its first client.
///
/// They go through the journal like any client event, and that is the
/// point: replicas receive them in the stream, and every replay — from
/// genesis, from a snapshot, on a promoted replica — applies them again,
/// in order. Nothing the application's state depends on bypasses the
/// journal, so recovery can only reproduce the state the node had.
///
/// `Vec` rather than an iterator: both sets are built once at startup
/// from operator configuration and consumed once, off any hot path.
pub struct StartupEvents<E> {
    /// The first events of the history: what every node's state starts
    /// from (reference data, initial balances, ...). Journaled once, by the node that
    /// creates the journal as primary. A node that recovers an existing
    /// journal, or follows a primary, already has them in its history.
    pub genesis: Vec<E>,
    /// Journaled every time a node becomes primary — after `genesis` on a
    /// new journal, on recovering an existing journal as primary, and
    /// right after the epoch bump on promotion.
    ///
    /// Operator configuration the application enforces (rate limits,
    /// caps) belongs here. The values in force are then always those of
    /// the node serving clients, recorded in the history they govern:
    /// replicas apply the primary's values, not their own, and replay
    /// makes the decisions the values made the first time. A node's own
    /// values take effect only once it is primary. Re-applying a value
    /// already in force should leave the state unchanged.
    pub on_primary: Vec<E>,
}

impl<E> StartupEvents<E> {
    /// No startup events: an application whose history starts empty and
    /// which takes no operator configuration.
    pub fn none() -> Self {
        Self {
            genesis: Vec::new(),
            on_primary: Vec::new(),
        }
    }
}

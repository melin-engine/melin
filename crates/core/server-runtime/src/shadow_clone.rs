//! The shadow copy of the application, as every node builds it.

use std::io;

use melin_app::Application;

/// The error a pipeline build fails with when its shadow stage is wired
/// but [`shadow_clone`] took no copy: one wording for the primaries and
/// the replica.
pub(crate) const NO_SHADOW_CLONE: &str = "shadow application must be Some when shadow is enabled";

/// The shadow copy of `app` when the node runs a shadow stage
/// (`enable_shadow`), sized as the node sizes its live instance:
/// [`Application::clone_via_snapshot`], then [`Application::prefault`] with
/// the node's `sizing`. `None` without a shadow stage, as a clone is a
/// snapshot round trip and a prefault, wasted on a node that never uses it.
///
/// A clone is a restore, which sizes collections for the entries it
/// holds, while the live instance was prefaulted for the node's workload.
/// A compliant application's decisions do not depend on capacity
/// (`prefault`'s contract), but one that emits reports in a hash map's
/// iteration order would otherwise see its shadow disagree with its live
/// instance for a capacity difference the node itself made. Every shadow
/// is taken through here: the kernel-TCP and DPDK primaries and the
/// replica.
pub(crate) fn shadow_clone<A: Application>(
    app: &A,
    sizing: &A::Sizing,
    enable_shadow: bool,
) -> io::Result<Option<A>> {
    if !enable_shadow {
        return Ok(None);
    }
    let mut shadow = app.clone_via_snapshot()?;
    shadow.prefault(sizing);
    Ok(Some(shadow))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::hash::{BuildHasherDefault, Hasher};
    use std::io::{Read, Write};

    use melin_app::{AppEvent, ApplyCtx, CodecError, RejectReason};

    use super::*;

    /// Insert one key.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Key(u64);

    impl AppEvent for Key {
        const MAX_ENCODED_SIZE: usize = 8;

        fn encoded_size(&self) -> usize {
            8
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[..8].copy_from_slice(&self.0.to_le_bytes());
            8
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            let bytes = buf.get(..8).ok_or(CodecError::Truncated)?;
            Ok(Key(u64::from_le_bytes(bytes.try_into().expect("8 bytes"))))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    /// Hashes a `u64` key to itself, so a key's bucket is its low bits
    /// and the test's orders follow from the bucket count alone, not from
    /// a std hash algorithm that may change between releases.
    #[derive(Default)]
    struct IdentityHasher(u64);

    impl Hasher for IdentityHasher {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write(&mut self, _bytes: &[u8]) {
            unreachable!("only u64 keys are hashed");
        }
        fn write_u64(&mut self, n: u64) {
            self.0 = n;
        }
    }

    /// An application that breaks `prefault`'s contract the way the shadow
    /// prefault guards against: every apply reports the keys it holds in
    /// its map's iteration order, which follows the map's capacity.
    #[derive(Default)]
    struct CapacityOrdered {
        /// A fixed, identity hasher, so that two maps of one capacity
        /// holding the same keys iterate alike, and only capacity tells
        /// them apart.
        keys: HashMap<u64, (), BuildHasherDefault<IdentityHasher>>,
    }

    impl Application for CapacityOrdered {
        type Event = Key;
        type Report = u64;
        type QueryResponse = u64;
        /// The key count to reserve for.
        type Sizing = usize;
        const APP_VERSION: u16 = 1;

        fn apply(&mut self, event: Key, _ctx: &ApplyCtx, out: &mut Vec<u64>) {
            self.keys.insert(event.0, ());
            out.extend(self.keys.keys().copied());
        }
        fn build_reject(event: &Key, _reason: RejectReason) -> u64 {
            event.0
        }
        fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
            w.write_all(&(self.keys.len() as u64).to_le_bytes())?;
            for key in self.keys.keys() {
                w.write_all(&key.to_le_bytes())?;
            }
            Ok(())
        }
        /// Sized for the entries restored, as a restore is.
        fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
            let mut word = [0u8; 8];
            r.read_exact(&mut word)?;
            let len = u64::from_le_bytes(word) as usize;
            let mut keys = HashMap::with_capacity_and_hasher(len, Default::default());
            for _ in 0..len {
                r.read_exact(&mut word)?;
                keys.insert(u64::from_le_bytes(word), ());
            }
            Ok(CapacityOrdered { keys })
        }
        fn prefault(&mut self, sizing: &usize) {
            self.keys.reserve(sizing.saturating_sub(self.keys.len()));
        }
    }

    const SIZING: usize = 1024;

    fn reports_after(app: &mut CapacityOrdered, key: u64) -> Vec<u64> {
        let mut out = Vec::new();
        app.apply(
            Key(key),
            &ApplyCtx {
                now_ns: 0,
                key_hash: 0,
            },
            &mut out,
        );
        out
    }

    /// Held before the probe: each in a bucket of its own at any bucket
    /// count, so their order does not depend on the order they went in.
    const HELD: std::ops::RangeInclusive<u64> = 1..=8;
    /// Wraps onto bucket 0 of a table sized for the held keys alone, ahead
    /// of all of them, and lands after them in one sized for `SIZING`.
    const PROBE: u64 = 16;

    /// The prefault is what aligns the shadow with the live instance: a
    /// bare clone of a prefaulted instance reports in another order, the
    /// shadow clone in the same one.
    #[test]
    fn the_shadow_is_sized_as_the_live_instance() {
        let mut live = CapacityOrdered::default();
        live.prefault(&SIZING);
        for key in HELD {
            reports_after(&mut live, key);
        }
        let mut bare = live.clone_via_snapshot().expect("clone");
        let mut shadow = shadow_clone(&live, &SIZING, true)
            .expect("shadow clone")
            .expect("a shadow stage takes a copy");

        let expected = reports_after(&mut live, PROBE);
        assert_eq!(
            expected,
            HELD.chain([PROBE]).collect::<Vec<_>>(),
            "the prefaulted instance reports in key order"
        );
        assert_ne!(
            reports_after(&mut bare, PROBE),
            expected,
            "the test app must show the capacity difference a bare clone has"
        );
        assert_eq!(reports_after(&mut shadow, PROBE), expected);
    }

    /// Without a shadow stage no copy is taken.
    #[test]
    fn no_shadow_stage_takes_no_copy() {
        let live = CapacityOrdered::default();
        assert!(
            shadow_clone(&live, &SIZING, false)
                .expect("no clone to fail")
                .is_none()
        );
    }
}

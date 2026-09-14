//! A test application whose outcome depends on configuration it does not
//! journal — the shape that exposes a recovery path replaying entries
//! under different limits than they were first applied under.

use std::io::{self, Read, Write};
use std::path::Path;

use melin_app::app_factory::AppFactory;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};
use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};
use melin_transport_core::cursors::WireSeq;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Add(pub(crate) u64);

impl AppEvent for Add {
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
        Ok(Add(u64::from_le_bytes(bytes.try_into().expect("8 bytes"))))
    }
    fn is_query(&self) -> bool {
        false
    }
}

/// Sums the values it applies, dropping any above `cap`. `cap` is
/// operator policy: set by [`CapFactory`], absent from the snapshot —
/// a restored app is uncapped, like an empty one.
#[derive(Debug, PartialEq)]
pub(crate) struct CappedSum {
    pub(crate) sum: u64,
    pub(crate) cap: u64,
}

impl Application for CappedSum {
    type Event = Add;
    type Report = ();
    type QueryResponse = ();
    const APP_VERSION: u16 = 1;

    fn apply(&mut self, event: Add, _ctx: &ApplyCtx, _out: &mut Vec<()>) -> Option<()> {
        if event.0 <= self.cap {
            self.sum += event.0;
        }
        None
    }
    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<()>) {}
    fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
        true
    }
    fn build_reject(_event: &Add, _reason: RejectReason) {}
    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.sum.to_le_bytes())
    }
    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut sum = [0u8; 8];
        r.read_exact(&mut sum)?;
        Ok(CappedSum {
            sum: u64::from_le_bytes(sum),
            cap: u64::MAX,
        })
    }
}

/// Builds uncapped [`CappedSum`]s; the policy it applies is the cap.
pub(crate) struct CapFactory(pub(crate) u64);

impl AppFactory for CapFactory {
    type App = CappedSum;

    fn empty(&self) -> CappedSum {
        CappedSum {
            sum: 0,
            cap: u64::MAX,
        }
    }
    fn prefault(&self, _app: &mut CappedSum) {}
    fn apply_operator_policy(&self, app: &mut CappedSum) {
        app.cap = self.0;
    }
}

/// The cap every fixture below is recorded under.
pub(crate) const CAP: u64 = 10;

/// The recorded history: one value above [`CAP`] between two below it.
/// Applied under the cap it sums to [`CAPPED_SUM`]; replayed uncapped it
/// sums to 62.
pub(crate) const HISTORY: [u64; 3] = [5, 50, 7];

/// What [`HISTORY`] sums to under [`CAP`].
pub(crate) const CAPPED_SUM: u64 = 12;

/// Journal [`HISTORY`] at `journal`, and snapshot its state after the
/// first entry at `journal.with_extension("snapshot")` — so recovery from
/// the snapshot has to replay a tail holding the over-cap value.
pub(crate) fn record_history(journal: &Path) {
    let mut writer = BufferedWriter::<Add>::create(journal).expect("create journal");
    let mut snapshot_chain = [0u8; 32];
    for (i, value) in HISTORY.into_iter().enumerate() {
        writer
            .append(&JournalEvent::App(Add(value)))
            .expect("append");
        if i == 0 {
            // `None` without `hash-chain`: nothing to tie the snapshot to.
            snapshot_chain = writer.chain_hash().unwrap_or([0u8; 32]);
        }
    }
    drop(writer);
    melin_transport_core::snapshot::save::<CappedSum>(
        &CappedSum {
            sum: HISTORY[0],
            cap: CAP,
        },
        WireSeq::new(1),
        snapshot_chain,
        0,
        &journal.with_extension("snapshot"),
    )
    .expect("save snapshot");
}

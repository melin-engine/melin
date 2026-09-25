//! The sequencer clock: time is assigned the way sequence is, strictly
//! increasing by construction.
//!
//! Every event the primary journals is stamped by one
//! [`SequencerClock`](crate::clock::SequencerClock), owned by the
//! [`StampingProducer`](crate::clock::StampingProducer) that wraps the
//! input ring's single producer. Client writes, clock ticks, startup
//! events and the epoch bump all publish through it, and it is the only
//! place a journaled timestamp is chosen: each stamp is
//! `max(reading, last + 1)`, so no two events share an instant and none
//! goes back in time, whatever the wall clock does. A replica's pipeline
//! keeps the raw producer: its slots carry the primary's stamps.
//!
//! ## Reading the wall clock
//!
//! The clock is read once per batch
//! ([`read_clock`](crate::clock::StampingProducer::read_clock)), and each
//! event in the batch costs one compare: at peak request rates a clock
//! read per event shows in the ingress thread's profile. A reading has
//! first passed the jump guard.
//!
//! ## The jump guard
//!
//! Strict time makes a forward jump of the wall clock permanent: every
//! later stamp stays at least as far ahead, and nothing that has seen the
//! future time can be wound back. So a reading more than the jump limit
//! ahead of both where the wall clock should be and the last stamp is
//! refused. "Where it should be" comes from `CLOCK_BOOTTIME`, which never
//! jumps (and, unlike `CLOCK_MONOTONIC`, keeps counting while a VM is
//! suspended, so a resume is not taken for a jump): the last accepted wall
//! reading plus the boot time elapsed since. While refused, the clock
//! issues that expected time, so time keeps flowing at the real rate, and
//! it follows the wall clock again as soon as a reading comes back within
//! the limit. A step back is accepted; the `last + 1` rule then holds
//! stamps until the wall clock catches up, and a correction of the step
//! is accepted too, since it takes journaled time no further than the
//! last stamp plus the limit.
//!
//! The guard protects a running node only. The first reading has nothing
//! to compare against and is taken as it is, so a restart accepts a wall
//! clock the guard was refusing. So does the operator, with `CLOCK-ACCEPT`
//! on the admin endpoint ([`ClockControl::request_accept`](crate::clock::ClockControl::request_accept)),
//! which the clock honours at its next read.
//!
//! ## Held time, and what the operator sees
//!
//! Time is never wound back, so when the wall clock is behind the last
//! stamp (a step back, a failover onto a slower clock) the clock holds
//! stamps one nanosecond apart until the wall clock catches up, and the
//! application's time stands still. The clock says so: a `warn!` when it
//! is seeded more than [`LEAD_WARNING`](crate::clock::LEAD_WARNING) behind
//! the journal's floor, the same `warn!` once each time a running clock
//! crosses that lead (re-armed below half of it), and the signed offset
//! it publishes on every read through its
//! [`ClockControl`](crate::clock::ClockControl), which the health endpoint
//! serves beside the count of refused jumps. Seeding also asks the kernel
//! whether the wall clock is synchronized and warns if not; it only warns,
//! since time daemons do not all report their state to the kernel.
//!
//! ## Ticks
//!
//! The ingress thread (io_uring reader or DPDK poll loop) also generates
//! the application's clock ticks, between reads of client traffic, rather
//! than a separate thread doing it: the input ring stays single-producer
//! and the tick goes through the same clock as everything else.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use melin_app::{AppEvent, SequencerTime};
use melin_journal::JournalEvent;
use melin_pipeline::ring::{self, Full};
use tracing::{info, warn};

use crate::pipeline::InputSlot;
use crate::trace::mono_trace_ns;

/// Default for how far ahead of its expected time a wall-clock reading
/// may land before the clock refuses it. Just above the largest step a
/// time daemon makes on a running node (ntpd steps past 128 ms; chrony
/// steps only at startup), while capping the freeze a wrongly accepted
/// jump can cause once the wall clock is corrected.
pub const DEFAULT_JUMP_LIMIT: Duration = Duration::from_secs(5);

/// How far the clock's time may lead the wall clock before it warns that
/// time is held. A constant, not a setting: in steady state the lead is a
/// batch's worth of nanoseconds, and after a failover the skew between
/// two disciplined clocks, so this much means the clocks have a problem.
/// Low enough to catch a one-second leap-second step. A finer line
/// belongs on the offset gauge.
pub const LEAD_WARNING: Duration = Duration::from_millis(100);

/// [`LEAD_WARNING`] in signed nanoseconds, the offset's unit.
const LEAD_WARNING_NS: i64 = 100_000_000;
/// The running warning re-arms once the lead falls back under this, so a
/// lead hovering at the threshold does not warn on every batch.
const LEAD_REARM_NS: i64 = LEAD_WARNING_NS / 2;

/// Where the clock reads time from. A type parameter rather than a trait
/// object, so the system clocks cost no indirect call on the ingress path
/// while tests drive the clock by hand.
pub trait TimeSource {
    /// Wall-clock time, nanoseconds since the Unix epoch.
    fn wall_ns(&self) -> u64;
    /// Nanoseconds on a clock that never jumps and counts time spent
    /// suspended (`CLOCK_BOOTTIME`). Only differences are meaningful.
    fn boot_ns(&self) -> u64;
    /// Whether the wall clock is disciplined, as far as the source can
    /// tell. Asked once, when the clock is seeded.
    fn sync_state(&self) -> SyncState {
        SyncState::Unknown
    }
}

/// The kernel's view of whether the wall clock is synchronized. Advisory:
/// a daemon that disciplines the clock without reporting to the kernel
/// (phc2sys, for one) leaves it reading unsynchronized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    Synchronized,
    Unsynchronized,
    /// The source cannot tell (a test clock, or the query failed).
    Unknown,
}

/// The system's clocks: `CLOCK_REALTIME` and `CLOCK_BOOTTIME`, both vDSO
/// reads on Linux.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClocks;

impl TimeSource for SystemClocks {
    #[inline]
    fn wall_ns(&self) -> u64 {
        melin_app::unix_epoch_nanos()
    }

    #[inline]
    fn boot_ns(&self) -> u64 {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid, writable timespec for the duration of
        // the call.
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
        // Cannot fail with a valid clock id and pointer; a zero reading
        // instead would freeze the guard's expected time and refuse every
        // reading after the limit, so an impossible failure stops here.
        assert_eq!(rc, 0, "clock_gettime(CLOCK_BOOTTIME) failed");
        // Both fields are non-negative for CLOCK_BOOTTIME; u64 nanoseconds
        // since boot outlast any uptime.
        (ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
    }

    /// Asks `adjtimex` without changing anything (`modes` zero, which
    /// needs no privilege).
    fn sync_state(&self) -> SyncState {
        // SAFETY: `timex` is plain old data; all zeroes is a valid value,
        // and zero `modes` makes the call a read-only query.
        let mut tx: libc::timex = unsafe { std::mem::zeroed() };
        // SAFETY: `tx` is a valid, writable timex for the duration of the
        // call.
        let state = unsafe { libc::adjtimex(&mut tx) };
        if state < 0 {
            // A sandbox may refuse the call. The check is advisory, so the
            // clock seeds as usual.
            SyncState::Unknown
        } else if state == libc::TIME_ERROR || tx.status & libc::STA_UNSYNC != 0 {
            SyncState::Unsynchronized
        } else {
            SyncState::Synchronized
        }
    }
}

/// The sequencer clock's operator surface, shared with the admin and
/// health endpoints: the offset and refusal count the clock publishes on
/// every read, and the operator's request to accept the wall clock
/// (`CLOCK-ACCEPT`), which the clock takes up on its next read.
///
/// Created once per process, before the admin endpoint, so the endpoint a
/// replica starts with already holds the handle its clock attaches to on
/// promotion. Plain atomics, each meaningful on its own, so a reader never
/// coordinates with the ingress thread.
#[derive(Debug, Default)]
pub struct ClockControl {
    /// Whether a clock has attached: the node is a primary. Until then
    /// there is nothing to accept.
    attached: AtomicBool,
    accept_requested: AtomicBool,
    /// The time the clock issues next minus the wall clock, at its last
    /// read. Signed, since a refused jump leaves the clock behind; `i64`
    /// nanoseconds span 292 years either way.
    offset_ns: AtomicI64,
    /// Forward jumps refused, one per episode. A count that cannot wrap.
    jumps_refused: AtomicU64,
}

impl ClockControl {
    pub fn new() -> Self {
        Self::default()
    }

    /// An attached control already reporting `offset_ns` and
    /// `jumps_refused`, for the health endpoint's tests.
    #[cfg(test)]
    pub(crate) fn reporting(offset_ns: i64, jumps_refused: u64) -> Self {
        Self {
            attached: AtomicBool::new(true),
            accept_requested: AtomicBool::new(false),
            offset_ns: AtomicI64::new(offset_ns),
            jumps_refused: AtomicU64::new(jumps_refused),
        }
    }

    /// Whether a sequencer clock runs on this node (it is a primary).
    pub fn is_attached(&self) -> bool {
        self.attached.load(Ordering::Relaxed)
    }

    /// Ask the clock to accept the wall clock at its next read: the next
    /// batch of writes, or the next tick. Returns `false`, requesting
    /// nothing, when no clock is attached.
    pub fn request_accept(&self) -> bool {
        if !self.is_attached() {
            return false;
        }
        self.accept_requested.store(true, Ordering::Relaxed);
        true
    }

    /// The clock's time minus the wall clock at its last read, in
    /// nanoseconds: positive while stamps are held ahead of the wall
    /// clock, negative while a refused jump leaves the clock behind.
    pub fn offset_ns(&self) -> i64 {
        self.offset_ns.load(Ordering::Relaxed)
    }

    /// Forward jumps the clock has refused since the process started.
    pub fn jumps_refused(&self) -> u64 {
        self.jumps_refused.load(Ordering::Relaxed)
    }
}

/// `a - b`, saturated to `i64`: real times never come near the bounds,
/// but a nonsense reading must not wrap the gauge.
fn signed_diff(a: u64, b: u64) -> i64 {
    // In range after the clamp, so the cast cannot truncate.
    (i128::from(a) - i128::from(b)).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// A wall-clock reading that has passed the jump guard, taken once per
/// batch and stamped onto each event in it by [`SequencerClock::stamp`].
/// Only the clock makes one, so a batch cannot be stamped without a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockReading(u64);

impl ClockReading {
    /// The reading in nanoseconds since the Unix epoch.
    pub fn ns(self) -> u64 {
        self.0
    }
}

/// The last accepted wall-clock reading and the boot-clock reading taken
/// with it: the guard's measure of where the wall clock should be now.
#[derive(Debug, Clone, Copy)]
struct Reference {
    wall_ns: u64,
    boot_ns: u64,
}

/// Issues strictly increasing [`SequencerTime`]s from a guarded wall
/// clock. See the module docs.
#[derive(Debug)]
pub struct SequencerClock<S = SystemClocks> {
    source: S,
    /// The last stamp issued, or the floor the clock was seeded at; the
    /// next is strictly greater. Its u64 nanoseconds run out in 2554.
    last: SequencerTime,
    /// See [`DEFAULT_JUMP_LIMIT`]. Nanoseconds, to compare against
    /// readings without a conversion per batch.
    jump_limit_ns: u64,
    /// `None` until the first reading, which is accepted as it is.
    reference: Option<Reference>,
    /// Readings are being refused. Warns once per episode, not per batch.
    refusing: bool,
    /// The lead warning has fired and not yet re-armed.
    lead_warned: bool,
    control: Arc<ClockControl>,
}

impl<S: TimeSource> SequencerClock<S> {
    /// A clock seeded at the journal's time floor: its first stamp is
    /// strictly later than `floor`, the stamp of the last entry the
    /// journal holds, whatever the wall clock reads. That is what carries
    /// strictly increasing time across a restart, a snapshot boot and a
    /// promotion.
    ///
    /// Attaches to `control`, and warns if the floor leads the wall clock
    /// by more than [`LEAD_WARNING`] or the kernel reports the wall clock
    /// unsynchronized (see the module docs).
    pub fn new(
        source: S,
        jump_limit: Duration,
        floor: SequencerTime,
        control: Arc<ClockControl>,
    ) -> Self {
        let wall_ns = source.wall_ns();
        // No overflow before the year 2554 (see `last`).
        let lead_ns = signed_diff(wall_ns.max(floor.as_ns() + 1), wall_ns);
        let lead_warned = lead_ns > LEAD_WARNING_NS;
        if lead_warned {
            warn!(
                lead_ms = lead_ns / 1_000_000,
                "the journal's last stamp leads the wall clock; time is held, \
                 advancing one nanosecond per event, until the wall clock catches up"
            );
        }
        if source.sync_state() == SyncState::Unsynchronized {
            warn!(
                "the kernel reports the wall clock unsynchronized; expected if the time \
                 daemon does not report to the kernel (phc2sys), otherwise check it"
            );
        }
        control.offset_ns.store(lead_ns, Ordering::Relaxed);
        control.attached.store(true, Ordering::Relaxed);
        Self {
            source,
            last: floor,
            // A limit past u64 nanoseconds (584 years) never refuses.
            jump_limit_ns: u64::try_from(jump_limit.as_nanos()).unwrap_or(u64::MAX),
            reference: None,
            refusing: false,
            lead_warned,
            control,
        }
    }

    /// Read the wall clock through the jump guard, taking up an operator's
    /// accept first, and publish the offset the reading leaves.
    pub fn read(&mut self) -> ClockReading {
        let wall_ns = self.source.wall_ns();
        let boot_ns = self.source.boot_ns();
        // A load before the swap, so the batch path only reads a line the
        // admin endpoint rarely writes.
        if self.control.accept_requested.load(Ordering::Relaxed)
            && self.control.accept_requested.swap(false, Ordering::Relaxed)
        {
            self.accept(wall_ns, boot_ns);
        }
        let reading = self.guard(wall_ns, boot_ns);
        // No overflow before the year 2554 (see `last`).
        self.observe_offset(signed_diff(reading.max(self.last.as_ns() + 1), wall_ns));
        ClockReading(reading)
    }

    /// The next stamp: `reading`, or one nanosecond past the last stamp
    /// if the reading is not ahead of it.
    #[inline]
    pub fn stamp(&mut self, reading: ClockReading) -> SequencerTime {
        // No overflow before the year 2554 (see `last`).
        self.last = SequencerTime::from_ns(reading.0.max(self.last.as_ns() + 1));
        self.last
    }

    /// Read the clock and issue one stamp.
    pub fn now(&mut self) -> SequencerTime {
        let reading = self.read();
        self.stamp(reading)
    }

    /// Issue one stamp from a time the caller supplies instead of a
    /// reading, under the same rule. For the journaled test helpers,
    /// which drive time by hand.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn stamp_at(&mut self, now_ns: u64) -> SequencerTime {
        self.stamp(ClockReading(now_ns))
    }

    /// The last stamp issued, or the floor the clock was seeded at.
    pub fn last(&self) -> SequencerTime {
        self.last
    }

    fn guard(&mut self, wall_ns: u64, boot_ns: u64) -> u64 {
        let Some(reference) = self.reference else {
            self.reference = Some(Reference { wall_ns, boot_ns });
            return wall_ns;
        };
        let expected_ns = reference
            .wall_ns
            .saturating_add(boot_ns.saturating_sub(reference.boot_ns));
        // Judged against the journal as well as the projection: after a
        // step back the projection trails the last stamp, and the reading
        // that corrects the step lands well ahead of the projection but
        // moves journaled time forward no further than any other reading.
        let ahead_ns = wall_ns.saturating_sub(expected_ns.max(self.last.as_ns()));
        if ahead_ns > self.jump_limit_ns {
            if !self.refusing {
                self.refusing = true;
                self.control.jumps_refused.fetch_add(1, Ordering::Relaxed);
                warn!(
                    jump_ms = ahead_ns / 1_000_000,
                    limit_ms = self.jump_limit_ns / 1_000_000,
                    "wall clock jumped forward past the jump limit; issuing time \
                     from the boot clock until it comes back within the limit, \
                     or CLOCK-ACCEPT on the admin endpoint accepts it"
                );
            }
            // Keep the reference: the expected time advances at the boot
            // clock's rate, and the next reading is judged against it.
            return expected_ns;
        }
        if self.refusing {
            self.refusing = false;
            info!("wall clock back within the jump limit; following it again");
        }
        self.reference = Some(Reference { wall_ns, boot_ns });
        wall_ns
    }

    /// The operator's `CLOCK-ACCEPT`: make this reading the reference, so
    /// the guard follows it and judges later readings from it.
    fn accept(&mut self, wall_ns: u64, boot_ns: u64) {
        info!(
            refusing = self.refusing,
            offset_ms = self.control.offset_ns() / 1_000_000,
            "operator accepted the wall clock (CLOCK-ACCEPT)"
        );
        self.refusing = false;
        self.reference = Some(Reference { wall_ns, boot_ns });
    }

    /// Publish `offset_ns`, the time the clock issues next minus the wall
    /// clock, and warn once each time its lead crosses [`LEAD_WARNING`].
    fn observe_offset(&mut self, offset_ns: i64) {
        self.control.offset_ns.store(offset_ns, Ordering::Relaxed);
        if self.lead_warned {
            if offset_ns < LEAD_REARM_NS {
                self.lead_warned = false;
                info!("sequencer time no longer held ahead of the wall clock");
            }
        } else if offset_ns > LEAD_WARNING_NS {
            self.lead_warned = true;
            warn!(
                lead_ms = offset_ns / 1_000_000,
                "the wall clock is behind the last stamp; time is held, \
                 advancing one nanosecond per event, until the wall clock catches up"
            );
        }
    }
}

/// The primary's input-ring producer, stamping every event it publishes
/// through its [`SequencerClock`]. See the module docs.
///
/// Queries are published unstamped (the zero time) and consume no stamp:
/// they are never journaled, and nothing reads their time.
pub struct StampingProducer<E: AppEvent, S = SystemClocks> {
    producer: ring::Producer<InputSlot<E>>,
    clock: SequencerClock<S>,
}

impl<E: AppEvent, S: TimeSource> StampingProducer<E, S> {
    pub fn new(producer: ring::Producer<InputSlot<E>>, clock: SequencerClock<S>) -> Self {
        Self { producer, clock }
    }

    /// Read the wall clock for the next batch of client events.
    pub fn read_clock(&mut self) -> ClockReading {
        self.clock.read()
    }

    /// Start a batch of in-place publishes stamped from `reading`. See
    /// [`ring::Producer::batch`] for the commit and rollback rules; a
    /// stamp issued to a rolled-back slot is simply never used.
    pub fn batch(&mut self, reading: ClockReading) -> StampingBatch<'_, E, S> {
        StampingBatch {
            batch: self.producer.batch(),
            clock: &mut self.clock,
            reading,
        }
    }

    /// Publish an event the node originates itself (startup events, the
    /// epoch bump), waiting for space: no client connection, key hash 0.
    /// Reads the clock for it, since such events come one at a time.
    ///
    /// `sequence: 0` because the journal stage is the authoritative
    /// sequence allocator on the primary (see `InputSlot::sequence`). A
    /// query, which an application's startup events could hold, is left
    /// unstamped as in a batch.
    pub fn publish_internal(&mut self, event: JournalEvent<E>) -> u64 {
        let timestamp = if event.is_query() {
            SequencerTime::default()
        } else {
            self.clock.now()
        };
        self.producer.publish(internal_slot(event, timestamp))
    }

    /// Publish a clock tick, or drop it on a full ring. A dropped tick
    /// costs a stamp, never a slot, and the next one carries a later time,
    /// so a full ring delays time-driven work by one cadence at worst
    /// rather than blocking ingress.
    pub fn try_publish_tick(&mut self) -> Result<u64, Full> {
        let now = self.clock.now();
        self.producer
            .try_publish(internal_slot(JournalEvent::Tick, now))
    }

    /// The last stamp issued, or the floor the clock was seeded at.
    pub fn last_stamp(&self) -> SequencerTime {
        self.clock.last()
    }
}

fn internal_slot<E: AppEvent>(event: JournalEvent<E>, timestamp: SequencerTime) -> InputSlot<E> {
    InputSlot {
        connection_id: 0,
        key_hash: 0,
        sequence: 0,
        timestamp,
        event,
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    }
}

/// A batch of in-place publishes, each stamped as it is written, so
/// stamps follow ring order by construction. See
/// [`StampingProducer::batch`].
pub struct StampingBatch<'a, E: AppEvent, S> {
    batch: ring::Batch<'a, InputSlot<E>>,
    clock: &'a mut SequencerClock<S>,
    reading: ClockReading,
}

impl<E: AppEvent, S: TimeSource> StampingBatch<'_, E, S> {
    /// Fill the next slot with `fill`, then stamp it. Any timestamp `fill`
    /// writes is overwritten: the clock is the only stamping site. Returns
    /// `Err(Full)` without calling `fill` or issuing a stamp when the ring
    /// has no room.
    #[inline]
    pub fn try_push_with(&mut self, fill: impl FnOnce(&mut InputSlot<E>)) -> Result<u64, Full> {
        let clock = &mut *self.clock;
        let reading = self.reading;
        self.batch.try_push_with(|slot| {
            fill(slot);
            slot.timestamp = if slot.event.is_query() {
                SequencerTime::default()
            } else {
                clock.stamp(reading)
            };
        })
    }

    /// Entries written into the batch so far.
    pub fn len(&self) -> u64 {
        self.batch.len()
    }

    /// True when no entries have been written yet.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Ring sequence the next entry takes.
    pub fn next_sequence(&self) -> u64 {
        self.batch.next_sequence()
    }

    /// Publish the batch. See [`ring::Batch::commit`].
    pub fn commit(self) {
        self.batch.commit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_pipeline::wait::WaitStrategy;
    use std::cell::Cell;
    use std::rc::Rc;

    const LIMIT: Duration = Duration::from_secs(5);
    const LIMIT_NS: u64 = 5_000_000_000;
    const T0: u64 = 1_700_000_000_000_000_000;

    /// Clocks the test sets by hand. `Rc<Cell>` so the test keeps a
    /// handle while the clock owns the source.
    #[derive(Clone, Default)]
    struct ManualClocks {
        wall: Rc<Cell<u64>>,
        boot: Rc<Cell<u64>>,
    }

    impl ManualClocks {
        fn at(wall_ns: u64) -> Self {
            let clocks = Self::default();
            clocks.wall.set(wall_ns);
            clocks
        }

        /// Real time passes: both clocks advance together.
        fn advance(&self, ns: u64) {
            self.wall.set(self.wall.get() + ns);
            self.boot.set(self.boot.get() + ns);
        }

        /// The wall clock is set to `wall_ns`; the boot clock does not
        /// move.
        fn step_wall(&self, wall_ns: u64) {
            self.wall.set(wall_ns);
        }
    }

    impl TimeSource for ManualClocks {
        fn wall_ns(&self) -> u64 {
            self.wall.get()
        }
        fn boot_ns(&self) -> u64 {
            self.boot.get()
        }
    }

    fn clock_at(wall_ns: u64) -> (ManualClocks, SequencerClock<ManualClocks>) {
        let clocks = ManualClocks::at(wall_ns);
        (clocks.clone(), seeded(clocks, SequencerTime::default()))
    }

    fn seeded(clocks: ManualClocks, floor: SequencerTime) -> SequencerClock<ManualClocks> {
        SequencerClock::new(clocks, LIMIT, floor, Arc::new(ClockControl::new()))
    }

    /// Read the clock and issue one stamp, in nanoseconds.
    fn now_ns(clock: &mut SequencerClock<ManualClocks>) -> u64 {
        clock.now().as_ns()
    }

    #[test]
    fn the_first_reading_is_taken_as_it_is() {
        let (_, mut clock) = clock_at(T0);
        assert_eq!(clock.last(), SequencerTime::default());
        assert_eq!(now_ns(&mut clock), T0);
    }

    /// Seeded at the journal's floor, the clock's first stamp is past it
    /// even with the wall clock behind: a restart, a snapshot boot or a
    /// promotion onto a slower clock keeps time strictly increasing.
    #[test]
    fn the_first_stamp_is_past_the_floor_the_clock_was_seeded_at() {
        let floor = SequencerTime::from_ns(T0 + 1_000_000);
        let mut behind = seeded(ManualClocks::at(T0), floor);
        assert_eq!(behind.now().as_ns(), T0 + 1_000_001, "held past the floor");

        let mut ahead = seeded(ManualClocks::at(T0 + 2_000_000), floor);
        assert_eq!(
            ahead.now().as_ns(),
            T0 + 2_000_000,
            "the wall clock, past it"
        );
    }

    #[test]
    fn stamps_from_one_reading_strictly_increase() {
        let (_, mut clock) = clock_at(T0);
        let reading = clock.read();
        let stamps: Vec<u64> = (0..4).map(|_| clock.stamp(reading).as_ns()).collect();
        assert_eq!(stamps, [T0, T0 + 1, T0 + 2, T0 + 3]);
    }

    #[test]
    fn a_later_reading_is_followed() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.advance(1_000);
        assert_eq!(now_ns(&mut clock), T0 + 1_000);
    }

    #[test]
    fn a_step_back_holds_stamps_until_the_wall_clock_catches_up() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 - 1_000_000);
        assert_eq!(now_ns(&mut clock), T0 + 1, "held one past the last stamp");
        assert_eq!(now_ns(&mut clock), T0 + 2);
        // Time runs on from the stepped-back wall clock: once it passes
        // the last stamp it is followed.
        clocks.advance(2_000_000);
        assert_eq!(now_ns(&mut clock), T0 + 1_000_000);
    }

    /// The operator's natural fix for a step back is to set the wall
    /// clock right again. That reading lies far ahead of where the
    /// stepped-back clock should be, but no further ahead of the journal
    /// than any other reading, so it is followed, not refused.
    #[test]
    fn a_correction_of_a_step_back_is_followed() {
        const HOUR_NS: u64 = 3_600_000_000_000;
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 - HOUR_NS);
        assert_eq!(now_ns(&mut clock), T0 + 1, "held while the clock is behind");
        clocks.advance(1_000);
        clocks.step_wall(T0 + 1_000);
        assert_eq!(now_ns(&mut clock), T0 + 1_000, "the correction is followed");
    }

    /// Judging against the journal as well as the projection widens
    /// nothing: after a step back, a runaway reading is still more than
    /// the limit past both.
    #[test]
    fn a_runaway_jump_after_a_step_back_is_still_refused() {
        const HOUR_NS: u64 = 3_600_000_000_000;
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 - HOUR_NS);
        clock.now();
        clocks.step_wall(T0 + 2 + LIMIT_NS + 1);
        assert_eq!(
            now_ns(&mut clock),
            T0 + 2,
            "refused: held past the last stamp"
        );
    }

    #[test]
    fn a_forward_jump_within_the_limit_is_followed() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 + LIMIT_NS);
        assert_eq!(now_ns(&mut clock), T0 + LIMIT_NS);
    }

    #[test]
    fn a_forward_jump_past_the_limit_is_refused_and_time_keeps_flowing() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 + 3_600_000_000_000);
        assert_eq!(now_ns(&mut clock), T0 + 1, "no boot time elapsed: held");
        clocks.advance(1_000);
        assert_eq!(
            now_ns(&mut clock),
            T0 + 1_000,
            "issues the expected time, advancing at the boot clock's rate"
        );
        clocks.advance(1_000);
        assert_eq!(now_ns(&mut clock), T0 + 2_000, "still refused");
    }

    #[test]
    fn a_refused_clock_follows_the_wall_clock_once_it_is_back_within_the_limit() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 + 3_600_000_000_000);
        clocks.advance(1_000);
        assert_eq!(now_ns(&mut clock), T0 + 1_000);
        // The operator corrects the wall clock to a little ahead of the
        // expected time, within the limit.
        clocks.step_wall(T0 + 1_000 + LIMIT_NS);
        assert_eq!(now_ns(&mut clock), T0 + 1_000 + LIMIT_NS);
    }

    #[test]
    fn a_suspend_is_not_a_jump() {
        // CLOCK_BOOTTIME counts the suspension, so the wall clock's leap
        // on resume is the expected one.
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.advance(3_600_000_000_000);
        assert_eq!(now_ns(&mut clock), T0 + 3_600_000_000_000);
    }

    #[test]
    fn stamp_at_follows_the_same_rule() {
        let (_, mut clock) = clock_at(T0);
        let ahead = T0 + 60_000_000_000;
        assert_eq!(clock.stamp_at(ahead).as_ns(), ahead);
        assert_eq!(clock.stamp_at(T0).as_ns(), ahead + 1);
    }

    const HOUR_NS: u64 = 3_600_000_000_000;
    const HOUR_NS_I: i64 = 3_600_000_000_000;

    /// Seeding attaches the clock and publishes the lead the journal's
    /// floor holds over the wall clock, warning past the threshold: a
    /// failover onto a slower clock is visible before the first write.
    #[test]
    fn seeding_publishes_the_floors_lead_and_attaches() {
        let control = Arc::new(ClockControl::new());
        assert!(!control.is_attached());
        let floor = SequencerTime::from_ns(T0 + 200_000_000);
        let clock = SequencerClock::new(ManualClocks::at(T0), LIMIT, floor, Arc::clone(&control));
        assert!(control.is_attached());
        assert_eq!(
            control.offset_ns(),
            200_000_001,
            "the first stamp minus the wall clock"
        );
        assert!(clock.lead_warned, "past the threshold at seeding");

        let (_, clock) = clock_at(T0);
        assert_eq!(
            clock.control.offset_ns(),
            0,
            "a floor behind the wall clock"
        );
        assert!(!clock.lead_warned);
    }

    /// A step back holds time: the offset turns positive by the step,
    /// warns once, and re-arms as the wall clock catches up.
    #[test]
    fn a_step_back_shows_as_a_positive_offset_until_the_wall_clock_catches_up() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 - HOUR_NS);
        clock.now();
        assert_eq!(clock.control.offset_ns(), HOUR_NS_I + 1);
        assert!(clock.lead_warned);
        clocks.advance(HOUR_NS);
        clock.now();
        assert_eq!(clock.control.offset_ns(), 2, "held two stamps past T0");
        assert!(!clock.lead_warned, "re-armed");
    }

    /// The warning fires once per crossing: a lead that hovers between
    /// half the threshold and the threshold neither re-warns nor re-arms.
    #[test]
    fn the_lead_warning_rearms_only_below_half_the_threshold() {
        let (_, mut clock) = clock_at(T0);
        let mut warned = |offset_ns: i64| {
            clock.observe_offset(offset_ns);
            clock.lead_warned
        };
        assert!(!warned(LEAD_WARNING_NS), "at the threshold: not past it");
        assert!(warned(LEAD_WARNING_NS + 1));
        assert!(warned(LEAD_REARM_NS), "not under half");
        assert!(warned(LEAD_WARNING_NS + 1), "still the same crossing");
        assert!(!warned(LEAD_REARM_NS - 1), "re-armed");
        assert!(warned(LEAD_WARNING_NS + 1), "a new crossing");
    }

    /// A refused jump leaves the clock behind the wall clock, which the
    /// offset shows as negative, and counts once per episode.
    #[test]
    fn a_refused_jump_shows_as_a_negative_offset_and_counts_once_per_episode() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 + HOUR_NS);
        clock.now();
        assert_eq!(clock.control.offset_ns(), -(HOUR_NS_I - 1));
        clock.now();
        assert_eq!(
            clock.control.jumps_refused(),
            1,
            "one episode, however many reads"
        );

        // Back within the limit ends the episode; the next jump is another.
        clocks.step_wall(clock.last().as_ns());
        clock.now();
        assert_eq!(clock.control.offset_ns(), 1);
        clocks.step_wall(clock.last().as_ns() + HOUR_NS);
        clock.now();
        assert_eq!(clock.control.jumps_refused(), 2);
    }

    /// `CLOCK-ACCEPT` makes the next read follow the wall clock the guard
    /// was refusing, and later readings are judged from it.
    #[test]
    fn an_accepted_jump_is_followed_from_the_next_read() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        clocks.step_wall(T0 + HOUR_NS);
        clocks.advance(1_000);
        assert_eq!(now_ns(&mut clock), T0 + 1_000, "refused");

        assert!(clock.control.request_accept());
        assert_eq!(now_ns(&mut clock), T0 + HOUR_NS + 1_000, "accepted");
        assert_eq!(clock.control.offset_ns(), 0, "in step with the wall clock");
        clocks.advance(1_000);
        assert_eq!(
            now_ns(&mut clock),
            T0 + HOUR_NS + 2_000,
            "followed from there"
        );
        assert_eq!(
            clock.control.jumps_refused(),
            1,
            "an accept is not a refusal"
        );
    }

    /// An accept with nothing refused changes nothing, and is consumed:
    /// it does not wait to wave through a later jump.
    #[test]
    fn an_accept_is_consumed_by_the_next_read() {
        let (clocks, mut clock) = clock_at(T0);
        clock.now();
        assert!(clock.control.request_accept());
        clocks.advance(1_000);
        assert_eq!(now_ns(&mut clock), T0 + 1_000);
        clocks.step_wall(T0 + HOUR_NS);
        assert_eq!(now_ns(&mut clock), T0 + 1_001, "the later jump is refused");
    }

    /// No clock, nothing to accept: the admin endpoint tells a replica's
    /// operator so rather than latching a request for a later promotion.
    #[test]
    fn an_accept_needs_an_attached_clock() {
        let control = ClockControl::new();
        assert!(!control.request_accept());
        assert!(!control.accept_requested.load(Ordering::Relaxed));
    }

    type Slot = InputSlot<TestEvent>;

    fn producer_at(
        wall_ns: u64,
        capacity: usize,
    ) -> (
        StampingProducer<TestEvent, ManualClocks>,
        ring::Consumer<Slot>,
    ) {
        let (producer, mut consumers) = DisruptorBuilder::<Slot>::new(capacity)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let clock = seeded(ManualClocks::at(wall_ns), SequencerTime::default());
        (
            StampingProducer::new(producer, clock),
            consumers.pop().unwrap(),
        )
    }

    fn drain(consumer: &mut ring::Consumer<Slot>) -> Vec<Slot> {
        std::iter::from_fn(|| consumer.try_consume().map(|(_, slot)| slot)).collect()
    }

    fn stamps(slots: &[Slot]) -> Vec<u64> {
        slots.iter().map(|s| s.timestamp.as_ns()).collect()
    }

    /// The frame decoder writes everything but the time; the batch stamps
    /// each write in ring order, past any timestamp the fill left, and
    /// leaves queries unstamped without spending a stamp on them.
    #[test]
    fn a_batch_stamps_writes_in_order_and_leaves_queries_unstamped() {
        let (mut producer, mut consumer) = producer_at(T0, 16);
        let reading = producer.read_clock();
        let mut batch = producer.batch(reading);
        for event in [
            TestEvent::Add(1),
            TestEvent::Query,
            TestEvent::Add(2),
            TestEvent::Add(3),
        ] {
            batch
                .try_push_with(|slot| {
                    slot.event = JournalEvent::App(event);
                    slot.timestamp = SequencerTime::from_ns(42);
                })
                .unwrap();
        }
        batch.commit();

        assert_eq!(stamps(&drain(&mut consumer)), [T0, 0, T0 + 1, T0 + 2]);
        assert_eq!(producer.last_stamp().as_ns(), T0 + 2);
    }

    #[test]
    fn a_push_refused_by_a_full_ring_spends_no_stamp() {
        let (mut producer, mut consumer) = producer_at(T0, 2);
        let reading = producer.read_clock();
        let mut batch = producer.batch(reading);
        for _ in 0..2 {
            batch
                .try_push_with(|slot| slot.event = JournalEvent::App(TestEvent::Add(1)))
                .unwrap();
        }
        assert!(
            batch
                .try_push_with(|_| panic!("a full ring must not call fill"))
                .is_err()
        );
        batch.commit();
        assert_eq!(producer.last_stamp().as_ns(), T0 + 1);
        assert_eq!(drain(&mut consumer).len(), 2);
    }

    #[test]
    fn stamps_stay_strict_across_batches_ticks_and_internal_events() {
        let (mut producer, mut consumer) = producer_at(T0, 16);
        producer.publish_internal(JournalEvent::EpochBump { epoch: 2 });
        producer.try_publish_tick().unwrap();
        let reading = producer.read_clock();
        let mut batch = producer.batch(reading);
        batch
            .try_push_with(|slot| slot.event = JournalEvent::App(TestEvent::Add(1)))
            .unwrap();
        batch.commit();
        producer.publish_internal(JournalEvent::App(TestEvent::Add(2)));

        let slots = drain(&mut consumer);
        assert_eq!(stamps(&slots), [T0, T0 + 1, T0 + 2, T0 + 3]);
        assert!(
            matches!(slots[1].event, JournalEvent::Tick),
            "the tick sits in ring order, its time in its stamp"
        );
    }

    #[test]
    fn a_tick_on_a_full_ring_is_dropped() {
        let (mut producer, mut consumer) = producer_at(T0, 2);
        producer.try_publish_tick().unwrap();
        producer.try_publish_tick().unwrap();
        assert!(producer.try_publish_tick().is_err());
        assert_eq!(drain(&mut consumer).len(), 2);
        // The dropped tick spent a stamp, never a slot: the next one is
        // still strictly later.
        producer.try_publish_tick().unwrap();
        assert_eq!(stamps(&drain(&mut consumer)), [T0 + 3]);
    }
}

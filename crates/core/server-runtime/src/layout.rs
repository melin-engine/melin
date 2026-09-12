//! Where each pipeline thread runs and how it waits there: the `--cores`
//! layout.
//!
//! A layout is one [`Placement`] per pipeline thread — a core and a
//! [`WaitStrategy`] — parsed from `--cores`, resolved against the
//! transport, and checked before any thread is spawned. `--cores` is the
//! only place a wait policy is stated, so there is one spelling of every
//! layout and nothing for a second flag to override. The check exists
//! because the two halves of a placement
//! constrain each other across threads: a busy-spinning thread needs its
//! core to itself, and two threads that share a core must both yield, or
//! the one waiting sits queued behind the one spinning for a full
//! scheduler slice on every hand-off. A node refuses to start with a
//! layout that breaks that rule rather than running slowly and
//! unpredictably.
//!
//! `--cores` names each thread (`journal-seq=1,matching=2,…`) rather than
//! listing cores by position. A positional list can only ever grow at its
//! end and can never lose an entry: dropping one re-reads every later core
//! as its neighbour's, and the result usually still parses. A named entry
//! says which thread it places, so a misspelt, repeated or missing name is
//! refused, a retired thread is simply an unknown one, and a thread added
//! later is one more name rather than one more position.

use melin_app::AppEvent;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::pipeline::{JournalStage, StageWaits};

/// The layout a node runs when `--cores` is not given.
pub(crate) const DEFAULT_CORES: &str = "journal-seq=1,matching=2,response=3,reader=4,\
     event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11";

/// How many threads a layout places. `usize` because it is an array
/// length: every per-thread list here is a fixed array of this size.
const THREAD_COUNT: usize = 10;

/// The threads every node runs, whatever its flags, and that the operator
/// docs say need a core to themselves: a request passes through each of
/// them on its way to an acknowledgement. Named as in `--cores`; a subset
/// of the list in [`PipelineCores::named_mut`], which the tests hold it to.
const MANDATORY_THREADS: [&str; 5] = [
    "journal-seq",
    "matching",
    "response",
    "reader",
    "journal-disk",
];

/// Where one pipeline thread runs and how it waits there.
///
/// The two are one value because they constrain each other: a thread
/// that busy-spins needs a core to itself, and a thread with no core of
/// its own (`core == 0`, the unpinned sentinel) can only ever yield — a
/// spinner the scheduler is free to place is the co-location bug with
/// the victim chosen at random. [`PipelineCores::validate`] enforces the
/// cross-thread half of that rule; the constructors here enforce the
/// per-thread half.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// Logical CPU to pin to. `0` = unpinned (OS scheduled).
    pub core: usize,
    /// How the thread waits when it has nothing to do.
    pub wait: WaitStrategy,
}

impl Placement {
    /// Pinned to `core` and busy-spinning: the thread owns that core.
    /// A `core` of `0` cannot spin — see the type docs — so this yields
    /// the unpinned placement instead.
    pub const fn spinning(core: usize) -> Self {
        if core == 0 {
            Self::unpinned()
        } else {
            Self {
                core,
                wait: WaitStrategy::BusySpin,
            }
        }
    }

    /// Pinned to `core`, spinning briefly then yielding: the thread may
    /// share that core with other yielding threads.
    pub const fn yielding(core: usize) -> Self {
        Self {
            core,
            wait: WaitStrategy::SpinThenYield,
        }
    }

    /// No core of its own: the OS scheduler places it within the CPU set
    /// the process was started with — and therefore it yields.
    pub const fn unpinned() -> Self {
        Self::yielding(0)
    }

    /// Whether this thread is pinned to a core (`core != 0`).
    pub fn is_pinned(&self) -> bool {
        self.core != 0
    }
}

/// What the `reader` entry of `--cores` pins on a given transport. The
/// two threads wait differently, and one of them cannot take a policy
/// at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderThread {
    /// The io_uring reader (kernel TCP). It blocks in the kernel between
    /// completions, so its entry's policy governs only the input ring it
    /// produces into.
    IoUring,
    /// The DPDK NIC poll thread. It polls the device unconditionally and
    /// never idles, so it busy-spins whatever its entry says; a `y` on
    /// the reader is refused rather than silently ignored.
    DpdkPoll,
}

/// Core assignments for pipeline threads, each with its wait policy.
///
/// All fields are always stored; event-publisher is only used when
/// `--event-bind` is set, and shadow only when `--snapshot-interval-ms` > 0.
/// repl-handler-0/1 are spawned on replica connect, not at startup.
/// A `core` of 0 = unpinned (OS scheduled) for any field.
///
/// The thread that accepts replica connections (`repl-accept`) has no
/// field: it accepts, reaps finished handlers and sleeps, and it must stay
/// unpinned because the handlers it spawns inherit its placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineCores {
    /// The journal's sequencing thread: orders, encodes and hash-chains
    /// events, and feeds the replicas and the disk thread.
    pub journal_seq: Placement,
    pub matching: Placement,
    pub response: Placement,
    /// io_uring reader thread (TCP) or DPDK poll thread — see
    /// [`ReaderThread`] for how the two differ.
    pub reader: Placement,
    pub event_publisher: Placement,
    pub shadow: Placement,
    /// Replication handler thread 0.
    pub repl_handler_0: Placement,
    /// Replication handler thread 1.
    pub repl_handler_1: Placement,
    /// The journal segment preparer (background staging of the next
    /// segment).
    ///
    /// Its wait policy is always yielding: the preparer blocks in file
    /// I/O and sleeps between attempts, it never polls a ring. The parser
    /// only produces a yielding preparer, and the layout check and the
    /// rendering read it as yielding whatever this field holds, so a
    /// busy-spin value set in code is ignored rather than refused. It is
    /// carried here so the layout check knows it is harmless to share a
    /// core with.
    pub journal_prep: Placement,
    /// The journal disk thread — the half that writes, syncs, and
    /// publishes durability.
    ///
    /// Unlike the preparer this is a hot-path thread: it polls for
    /// batches, and the durability cursors every ack gates on are
    /// published from it. Place it on the same CCD as `journal-seq` — the
    /// hand-off bounces a cache line between the two on every batch,
    /// and a cross-CCD transfer costs ~100 ns of that.
    pub journal_disk: Placement,
}

impl PipelineCores {
    /// Every thread unpinned, and therefore yielding: `--cores none`.
    pub fn unpinned() -> Self {
        let unpinned = Placement::unpinned();
        Self {
            journal_seq: unpinned,
            matching: unpinned,
            response: unpinned,
            reader: unpinned,
            event_publisher: unpinned,
            shadow: unpinned,
            repl_handler_0: unpinned,
            repl_handler_1: unpinned,
            journal_prep: unpinned,
            journal_disk: unpinned,
        }
    }

    /// Every thread with its `--cores` name, in the order the layout is
    /// rendered — the one list the parser, the layout check, the rendering
    /// and [`all_yielding`](Self::all_yielding) go through. The
    /// destructuring binds every field, so a thread added to the struct
    /// does not build until it is named here too.
    fn named_mut(&mut self) -> [(&'static str, &mut Placement); THREAD_COUNT] {
        let Self {
            journal_seq,
            matching,
            response,
            reader,
            event_publisher,
            shadow,
            repl_handler_0,
            repl_handler_1,
            journal_prep,
            journal_disk,
        } = self;
        [
            ("journal-seq", journal_seq),
            ("matching", matching),
            ("response", response),
            ("reader", reader),
            ("event-publisher", event_publisher),
            ("shadow", shadow),
            ("repl-handler-0", repl_handler_0),
            ("repl-handler-1", repl_handler_1),
            ("journal-prep", journal_prep),
            ("journal-disk", journal_disk),
        ]
    }

    /// [`named_mut`](Self::named_mut) by value, with the preparer read as
    /// yielding whatever its field holds. It blocks in I/O and never waits
    /// in a loop, so a busy-spin policy on it describes nothing; the
    /// layout check and the rendering both come through here, so neither
    /// refuses a layout nor logs one over a policy the thread cannot have.
    fn named(&self) -> [(&'static str, Placement); THREAD_COUNT] {
        let mut layout = *self;
        layout.journal_prep = Placement::yielding(layout.journal_prep.core);
        layout
            .named_mut()
            .map(|(name, placement)| (name, *placement))
    }

    /// The per-stage policies the pipeline builders take, drawn from the
    /// threads that own each stage: the reader produces into the input
    /// ring, journal-seq feeds the write and replication rings, matching
    /// produces into the output ring.
    pub fn stage_waits(&self) -> StageWaits {
        StageWaits {
            ingress: self.reader.wait,
            journal: self.journal_seq.wait,
            matching: self.matching.wait,
        }
    }

    /// Hand the journal stage the placements of the two helper threads its
    /// `start` launches: the segment preparer and the disk thread.
    /// journal-seq's own placement is applied where that thread is
    /// spawned, like every other stage's; the helpers are launched by the
    /// stage, so their placement has to reach it before `start`. One
    /// function so the primary, the DPDK primary and the replica cannot
    /// drift apart on which fields they forward.
    pub fn place_journal_children<E: AppEvent>(&self, stage: &mut JournalStage<E>) {
        stage.set_preparer_core(self.journal_prep.core);
        stage.set_disk_core(self.journal_disk.core);
        stage.set_disk_wait(self.journal_disk.wait);
    }

    /// The same layout with every thread yielding: a `y` on every entry.
    /// For code that builds a layout for a shared machine — the test
    /// harnesses, an embedded bench — without spelling the list out.
    /// Not valid for a DPDK reader, which cannot yield; see
    /// [`resolve`](Self::resolve).
    pub fn all_yielding(mut self) -> Self {
        for (_, placement) in self.named_mut() {
            *placement = Placement::yielding(placement.core);
        }
        self
    }

    /// The mandatory threads with no core of their own, in `--cores`
    /// order: what the boot log warns about. A thread without a core runs
    /// wherever the scheduler puts it and shares that core with whatever
    /// else is there. On an auxiliary thread that is the documented trade
    /// for a small box; on one of these five it is paid on every request,
    /// so it should be a choice the operator made knowingly. A `Vec` only
    /// to carry the names into one log line; this runs once, at startup.
    pub fn unpinned_mandatory(&self) -> Vec<&'static str> {
        self.named()
            .into_iter()
            .filter(|(name, placement)| MANDATORY_THREADS.contains(name) && !placement.is_pinned())
            .map(|(name, _)| name)
            .collect()
    }

    /// Whether no thread at all has a core: the `--cores none` layout.
    pub fn pins_nothing(&self) -> bool {
        self.named()
            .iter()
            .all(|(_, placement)| !placement.is_pinned())
    }

    /// The layout the node actually runs: the reader entry checked
    /// against what the transport's reader thread can do, then the whole
    /// thing checked with [`validate`](Self::validate). Every spawn site
    /// reads the result, so no thread waits under a policy the operator
    /// did not ask for.
    ///
    /// On DPDK the reader is the NIC poll thread, which busy-polls
    /// whatever its entry says, so a `y` there is a contradiction and is
    /// refused. An unpinned DPDK reader is accepted as written: it polls
    /// flat out wherever the scheduler puts it, which is the one
    /// placement this module cannot make honest, and the operator docs
    /// say so.
    ///
    /// An `Err` is a configuration the node refuses to start with.
    pub fn resolve(self, reader: ReaderThread) -> Result<Self, String> {
        if reader == ReaderThread::DpdkPoll
            && self.reader.is_pinned()
            && self.reader.wait == WaitStrategy::SpinThenYield
        {
            return Err(format!(
                "--cores: the reader entry is `reader={core}y`, but on DPDK the reader is the \
                 NIC poll thread, which busy-polls whatever its entry says. Drop the suffix and \
                 give it core {core} to itself",
                core = self.reader.core
            ));
        }
        self.validate()?;
        Ok(self)
    }

    /// Refuse a layout in which threads would starve each other: two
    /// threads pinned to the same core where either one busy-spins, or
    /// a busy-spinner with no core at all.
    ///
    /// Checked over every thread, whether or not today's flags spawn it
    /// — a layout is meant to be right independent of which features are
    /// on, and the message names the threads and the core so the fix is
    /// obvious. Two yielding threads on one core is the supported way to
    /// run on a small box and passes. The parser and the [`Placement`]
    /// constructors never produce an unpinned spinner, but the fields are
    /// public, so the check does not rely on that.
    pub fn validate(&self) -> Result<(), String> {
        let named = self.named();
        for (i, (name_a, a)) in named.iter().enumerate() {
            if !a.is_pinned() {
                if a.wait == WaitStrategy::BusySpin {
                    return Err(format!(
                        "--cores: {name_a} is unpinned but busy-spins; a thread without a \
                         core of its own would spin wherever the scheduler puts it, \
                         starving whatever shares that core. Give it a core, or let it yield"
                    ));
                }
                continue;
            }
            for (name_b, b) in &named[i + 1..] {
                if a.core != b.core {
                    continue;
                }
                let spinner = match (a.wait, b.wait) {
                    (WaitStrategy::BusySpin, _) => name_a,
                    (_, WaitStrategy::BusySpin) => name_b,
                    _ => continue,
                };
                return Err(format!(
                    "--cores: {name_a} and {name_b} share core {} but {spinner} busy-spins; \
                     a busy-spinning thread needs the core to itself. Give one of them \
                     another core, or suffix both with `y` so they yield when idle",
                    a.core
                ));
            }
        }
        Ok(())
    }

    /// Compact pipeline layout that fits on `num_cpus` physical+logical
    /// cores while reserving one core for an external client (bench or
    /// reader). Used by the embedded bench so it doesn't HT-collide with
    /// the pipeline cores.
    ///
    /// On the default workstation `Default` layout the journal (core 1),
    /// matching (core 2), and shadow/repl_handler cores (7/8/9) are HT
    /// siblings of each other on an 8-core (16-thread) Ryzen — a layout
    /// designed for a 10-physical-core box. This packs everything into
    /// the lower physical cores and leaves core 10+ free.
    ///
    /// Returns the chosen layout and the recommended bench/client core.
    /// Errors if `num_cpus` is too small to host the pipeline.
    pub fn compact(num_cpus: usize) -> Result<(Self, usize), String> {
        // Need: journal, matching, response, event_publisher, shadow,
        // reader (one each) + 1 reserved for bench = 7 cores. Plus
        // core 0 for the OS / IRQs. So minimum 8 logical cores.
        if num_cpus < 8 {
            return Err(format!(
                "compact layout needs >= 8 logical cores; have {num_cpus}"
            ));
        }
        let cores = PipelineCores {
            journal_seq: Placement::spinning(1),
            matching: Placement::spinning(2),
            response: Placement::spinning(3),
            reader: Placement::spinning(4),
            event_publisher: Placement::spinning(5),
            shadow: Placement::spinning(6),
            // repl handlers not spawned in compact; unpinned.
            repl_handler_0: Placement::unpinned(),
            repl_handler_1: Placement::unpinned(),
            // Preparer unpinned in compact — the embedded bench doesn't
            // rotate at production cadence, and raising compact's core
            // minimum for it isn't worth a core.
            journal_prep: Placement::unpinned(),
            // The disk thread IS hot, but compact exists for boxes that
            // cannot spare a core per stage. Unpinned, it yields like
            // every thread without a core of its own: on a box with an
            // idle core to land on that costs one `sched_yield` per
            // poll, not a timeslice. Give it `journal-disk=` on any box
            // with the cores to spare.
            journal_disk: Placement::unpinned(),
        };
        Ok((cores, 7))
    }
}

/// Renders the layout in `--cores` syntax, suffixes included, so what
/// the node logs at boot can be pasted straight back onto a command
/// line. Every thread is named, unpinned ones as `=0`, so the log shows
/// the whole layout rather than only what was pinned; a layout with
/// nothing pinned renders as `none`.
impl std::fmt::Display for PipelineCores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.pins_nothing() {
            return f.write_str("none");
        }
        for (i, (name, placement)) in self.named().into_iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            match (name, placement.core, placement.wait) {
                (_, 0, _) => write!(f, "{name}=0")?,
                // The preparer takes no suffix — its entry is a core only.
                ("journal-prep", core, _) => write!(f, "{name}={core}")?,
                (_, core, WaitStrategy::BusySpin) => write!(f, "{name}={core}")?,
                (_, core, WaitStrategy::SpinThenYield) => write!(f, "{name}={core}y")?,
            }
        }
        Ok(())
    }
}

/// Parse the core of one `--cores` entry, the part after `=`: a core ID
/// with an optional wait suffix.
///
/// `7` and `7s` busy-spin, `7y` spins then yields, `0`/`0y` is unpinned
/// (and yields — see [`Placement`]). `0s` is refused: it asks for a
/// spinner with no core to spin on.
fn parse_placement(entry: &str) -> Result<Placement, String> {
    let (digits, suffix) = match entry.as_bytes().last() {
        Some(b'y') => (&entry[..entry.len() - 1], Some(WaitStrategy::SpinThenYield)),
        Some(b's') => (&entry[..entry.len() - 1], Some(WaitStrategy::BusySpin)),
        _ => (entry, None),
    };
    let core = digits
        .parse::<usize>()
        .map_err(|_| format!("invalid core ID `{entry}`"))?;
    match (core, suffix) {
        (0, Some(WaitStrategy::BusySpin)) => Err(format!(
            "invalid core entry `{entry}`: 0 means unpinned, and a thread without a core \
             of its own cannot busy-spin"
        )),
        (0, _) => Ok(Placement::unpinned()),
        (core, Some(WaitStrategy::SpinThenYield)) => Ok(Placement::yielding(core)),
        (core, _) => Ok(Placement::spinning(core)),
    }
}

/// The thread names `--cores` accepts, for error messages.
fn thread_names() -> String {
    PipelineCores::unpinned()
        .named()
        .map(|(name, _)| name)
        .join(", ")
}

/// Parse a `--cores` value into `PipelineCores`: comma-separated
/// `thread=core` entries in any order, each core with the suffix
/// described at [`parse_placement`]. Every thread must be named; `none`
/// leaves every thread unpinned.
pub(crate) fn parse_cores(s: &str) -> Result<PipelineCores, String> {
    let mut cores = PipelineCores::unpinned();
    if s.trim() == "none" {
        return Ok(cores);
    }
    // A value made only of bare cores is the positional list earlier
    // releases took. Refuse it as a whole and say what replaced it, rather
    // than failing on its first entry as if it were a typo.
    if s.split(',')
        .all(|entry| parse_placement(entry.trim()).is_ok())
    {
        return Err(format!(
            "expected named entries such as `journal-seq=1,matching=2`; bare cores are the \
             positional form earlier releases took. Name each thread; the threads are {}. \
             An all-`0` list is `none`, which unpins every thread",
            thread_names()
        ));
    }
    // Which threads the value has named so far, indexed like `named_mut`.
    // A fixed array rather than a set: the threads are a closed list known
    // at compile time.
    let mut seen = [false; THREAD_COUNT];
    let slots = cores.named_mut();
    for entry in s.split(',') {
        let entry = entry.trim();
        let Some((name, value)) = entry.split_once('=') else {
            return Err(format!(
                "invalid entry `{entry}`: expected thread=core, such as `journal-seq=1`"
            ));
        };
        let (name, value) = (name.trim(), value.trim());
        let Some(index) = slots.iter().position(|(slot, _)| *slot == name) else {
            return Err(format!(
                "unknown thread `{name}`; the threads are {}",
                thread_names()
            ));
        };
        if std::mem::replace(&mut seen[index], true) {
            return Err(format!("{name} is named twice"));
        }
        // The preparer never busy-waits (it blocks in I/O), so its entry is
        // a core and nothing else: a `y` is redundant and an `s` asks for
        // something the thread cannot do.
        let is_prep = name == "journal-prep";
        if is_prep && value.ends_with('s') {
            return Err(format!(
                "invalid journal-prep entry `{value}`: the preparer never busy-waits and \
                 takes no `s` suffix"
            ));
        }
        let placement = parse_placement(value).map_err(|e| format!("{name}: {e}"))?;
        *slots[index].1 = if is_prep {
            Placement::yielding(placement.core)
        } else {
            placement
        };
    }
    // Every thread must be named: one left out would run somewhere the
    // operator did not choose. A `Vec` only to join the names into the
    // message; this runs once, at startup.
    let missing: Vec<&str> = slots
        .iter()
        .zip(seen)
        .filter(|(_, was_named)| !was_named)
        .map(|((name, _), _)| *name)
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "missing {}: every thread must be named (`0` leaves one unpinned, and `none` \
             unpins them all)",
            missing.join(", ")
        ));
    }
    Ok(cores)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ServerConfig;
    use clap::Parser;

    /// The mixed layout the operator docs show: everything on the
    /// acknowledgement path (the four stages, the disk thread, the two
    /// replication handlers) spins on a core of its own; the event
    /// publisher, the shadow and the preparer share one core and yield.
    const MIXED: &str = "journal-seq=1,matching=2,response=3,reader=4,journal-disk=5,\
         repl-handler-0=6,repl-handler-1=7,event-publisher=8y,shadow=8y,journal-prep=8";

    /// `entries` completed with `=0` for each thread it does not name, so
    /// a test spells out only the threads it is about.
    fn complete(entries: &str) -> String {
        let mut value = entries.to_owned();
        for (name, _) in PipelineCores::unpinned().named() {
            let named = entries
                .split(',')
                .any(|entry| entry.split_once('=').is_some_and(|(n, _)| n.trim() == name));
            if !named {
                value.push_str(&format!(",{name}=0"));
            }
        }
        value
    }

    /// Every thread must be named: leaving one out is refused, with every
    /// missing thread named at once, rather than leaving it somewhere the
    /// operator did not choose. That includes journal-prep and
    /// journal-disk, which shorter positional lists could leave out. The
    /// set is spelled out here rather than derived from the parser's own
    /// list, so a thread that stops being required is a test failure.
    #[test]
    fn parse_cores_requires_every_thread() {
        const REQUIRED: [&str; 10] = [
            "journal-seq",
            "matching",
            "response",
            "reader",
            "event-publisher",
            "shadow",
            "repl-handler-0",
            "repl-handler-1",
            "journal-prep",
            "journal-disk",
        ];
        for missing in REQUIRED {
            let prefix = format!("{missing}=");
            let value = DEFAULT_CORES
                .split(',')
                .filter(|entry| !entry.starts_with(&prefix))
                .collect::<Vec<_>>()
                .join(",");
            let err = parse_cores(&value).expect_err(missing);
            assert!(err.starts_with(&format!("missing {missing}:")), "{err}");
        }

        let err = parse_cores("journal-seq=1,matching=2").expect_err("eight threads missing");
        assert!(
            err.starts_with(
                "missing response, reader, event-publisher, shadow, repl-handler-0, \
                 repl-handler-1, journal-prep, journal-disk:"
            ),
            "{err}"
        );
        assert!(err.contains("`none`"), "points at the shorthand: {err}");
    }

    /// Entries are named, so their order carries no meaning and whitespace
    /// around them is tolerated.
    #[test]
    fn parse_cores_reads_named_entries_in_any_order() {
        let forward =
            parse_cores(&complete("journal-seq=1,matching=2,journal-disk=3")).expect("parses");
        let reversed =
            parse_cores(&complete("journal-disk=3, matching=2 ,journal-seq = 1")).expect("parses");
        assert_eq!(forward, reversed);
        assert_eq!(forward.journal_seq, Placement::spinning(1));
        assert_eq!(forward.matching, Placement::spinning(2));
        assert_eq!(forward.journal_disk, Placement::spinning(3));

        let every = parse_cores(DEFAULT_CORES).expect("the default parses");
        assert_eq!(every.event_publisher, Placement::spinning(6));
        assert_eq!(every.journal_prep, Placement::yielding(10));
        assert_eq!(every.journal_disk, Placement::spinning(11));
    }

    /// `none` is the whole layout unpinned, the same as naming nothing
    /// pinned.
    #[test]
    fn parse_cores_none_unpins_every_thread() {
        assert_eq!(parse_cores("none"), Ok(PipelineCores::unpinned()));
        assert_eq!(
            parse_cores(&complete("journal-seq=0")),
            Ok(PipelineCores::unpinned())
        );
        assert!(
            parse_cores("none,journal-seq=1").is_err(),
            "none is the whole value, not an entry"
        );
        assert!(PipelineCores::unpinned().pins_nothing());
        assert!(!ServerConfig::default().cores.pins_nothing());
    }

    /// The reason the syntax is named. A positional value from an earlier
    /// release must not parse under any length: read by name it would
    /// mean nothing, and read by position with its retired fifth entry
    /// gone it would put every later thread on its neighbour's core
    /// without a word. The message has to say what replaced it, `none`
    /// included: the all-`0` list is the positional value a test harness
    /// or a container entrypoint is most likely to still carry.
    #[test]
    fn parse_cores_refuses_a_positional_list() {
        for positional in [
            "1,2,3,4,0,6,7,8,9",
            "1,2,3,4,5,6,7,8,9,10",
            "1,2,3,4,5,6,7,8,9,10,11",
            "1y,2y,3y,4y,0,6y,7y,8y,9y,10,11y",
            "0,0,0,0,0,0,0,0,0",
        ] {
            let err = parse_cores(positional).expect_err(positional);
            assert!(err.contains("named entries"), "{positional}: {err}");
            assert!(err.contains("journal-disk"), "lists the threads: {err}");
            assert!(err.contains("`none`"), "points at the shorthand: {err}");
        }
    }

    /// A name that places nothing, or places a thread twice, is refused:
    /// silently dropping either would leave a thread somewhere the
    /// operator did not put it. The retired replication-sender thread is
    /// just an unknown name.
    #[test]
    fn parse_cores_refuses_unknown_repeated_and_malformed_entries() {
        let err = parse_cores("journal-seq=1,jornal=2").expect_err("misspelt thread");
        assert!(err.contains("unknown thread `jornal`"), "{err}");
        let err = parse_cores("repl-sender=5").expect_err("retired thread");
        assert!(err.contains("unknown thread `repl-sender`"), "{err}");

        let err =
            parse_cores("journal-seq=1,matching=2,journal-seq=3").expect_err("repeated thread");
        assert!(err.contains("journal-seq is named twice"), "{err}");

        let err = parse_cores("journal-seq=x").expect_err("bad core");
        assert!(err.contains("journal-seq: invalid core ID `x`"), "{err}");
        let err = parse_cores("journal-seq=").expect_err("empty core");
        assert!(
            err.contains("invalid core ID ``"),
            "an empty core is shown as such, not as a message trailing off: {err}"
        );
        assert!(parse_cores("journal-seq=0s").is_err());
        assert!(parse_cores("journal").is_err());
        assert!(parse_cores("journal-seq=1,").is_err(), "trailing comma");
        assert!(parse_cores("=1").is_err());
        assert!(parse_cores("").is_err());
    }

    /// A bare core busy-spins (the production default, and what an entry
    /// without a suffix means), `y` yields, `s` spells the default out,
    /// and `0` is unpinned — which can only yield.
    #[test]
    fn parse_placement_reads_the_wait_suffix() {
        assert_eq!(parse_placement("7"), Ok(Placement::spinning(7)));
        assert_eq!(parse_placement("7s"), Ok(Placement::spinning(7)));
        assert_eq!(parse_placement("7y"), Ok(Placement::yielding(7)));
        assert_eq!(parse_placement("0"), Ok(Placement::unpinned()));
        assert_eq!(parse_placement("0y"), Ok(Placement::unpinned()));
        assert!(
            parse_placement("0s").is_err(),
            "a thread with no core of its own cannot busy-spin"
        );
        assert!(parse_placement("x").is_err());
        assert!(parse_placement("7z").is_err());
        assert!(parse_placement("").is_err());
    }

    /// The preparer blocks in I/O and never polls, so its entry is a
    /// core only: it always yields, and asking it to spin is refused
    /// rather than silently ignored.
    #[test]
    fn journal_prep_always_yields() {
        let cores = parse_cores(&complete("journal-prep=10")).expect("parses");
        assert_eq!(cores.journal_prep, Placement::yielding(10));
        let explicit = parse_cores(&complete("journal-prep=10y")).expect("parses");
        assert_eq!(explicit.journal_prep, Placement::yielding(10));
        let err = parse_cores(&complete("journal-prep=10s")).expect_err("must refuse");
        assert!(err.contains("journal-prep"), "{err}");
    }

    /// The layout the whole change exists to refuse: two threads on one
    /// core where either busy-spins. The message has to name both
    /// threads, the core, and which one spins, or the operator is left
    /// guessing which of ten entries to change.
    #[test]
    fn validate_refuses_a_busy_spinner_sharing_a_core() {
        // Two spinners.
        let err = parse_cores(&complete("journal-seq=1,matching=1"))
            .expect("parses")
            .validate()
            .expect_err("two spinners on core 1 must be refused");
        assert!(err.contains("journal"), "{err}");
        assert!(err.contains("matching"), "{err}");
        assert!(err.contains("core 1"), "{err}");

        // A spinner next to a yielder: the yielder would still starve.
        let err = parse_cores(&complete("event-publisher=7,shadow=7y"))
            .expect("parses")
            .validate()
            .expect_err("a spinner sharing with a yielder must be refused");
        assert!(err.contains("event-publisher"), "{err}");
        assert!(err.contains("shadow"), "{err}");
        assert!(err.contains("event-publisher busy-spins"), "{err}");

        // Which of the two spins does not matter.
        let err = parse_cores(&complete("event-publisher=6y,shadow=6"))
            .expect("parses")
            .validate()
            .expect_err("6y and 6 is still a spinner sharing a core");
        assert!(err.contains("shadow busy-spins"), "{err}");

        // The disk thread sharing the journal's core is the same mistake.
        let err = parse_cores(&complete("journal-seq=1,journal-disk=1"))
            .expect("parses")
            .validate()
            .expect_err("journal-disk on the journal's core must be refused");
        assert!(err.contains("journal-disk"), "{err}");
    }

    /// The supported ways to share: two yielders on one core, the
    /// preparer (which never spins) next to them, and unpinned threads,
    /// which are not on any core to collide on.
    #[test]
    fn validate_allows_yielders_to_share_a_core() {
        parse_cores(
            "journal-seq=1,matching=2,response=3,reader=4,event-publisher=7y,shadow=7y,\
             repl-handler-0=7y,repl-handler-1=7y,journal-prep=7,journal-disk=7y",
        )
        .expect("parses")
        .validate()
        .expect("yielders and the preparer may share core 7");
        parse_cores("none")
            .expect("parses")
            .validate()
            .expect("every thread unpinned is a valid (test) layout");
    }

    /// The fields are public, so a layout built in code (not parsed)
    /// can hold what the parser refuses: an unpinned busy-spinner. The
    /// check catches it rather than trusting the constructors.
    #[test]
    fn validate_refuses_an_unpinned_busy_spinner_built_by_hand() {
        let mut cores = ServerConfig::default().cores;
        cores.shadow = Placement {
            core: 0,
            wait: WaitStrategy::BusySpin,
        };
        let err = cores
            .validate()
            .expect_err("unpinned spinner must be refused");
        assert!(err.contains("shadow"), "{err}");
        assert!(err.contains("unpinned"), "{err}");
    }

    /// The preparer never waits in a loop, so a busy-spin policy set on it
    /// in code describes nothing. The layout check must not refuse it for
    /// sharing a core with yielding threads, or for spinning unpinned, and
    /// the layout the boot log shows must parse back to one the check
    /// reads the same way.
    #[test]
    fn a_spinning_preparer_built_by_hand_is_read_as_yielding() {
        let mut cores = parse_cores(&complete("event-publisher=8y,shadow=8y")).expect("parses");
        cores.journal_prep = Placement::spinning(8);
        cores
            .validate()
            .expect("the preparer never spins, so it may share core 8 with yielders");
        let reparsed = parse_cores(&cores.to_string()).expect("the rendering parses");
        assert_eq!(reparsed.journal_prep, Placement::yielding(8));
        assert_eq!(reparsed.named(), cores.named(), "log and check agree");

        cores.journal_prep = Placement {
            core: 0,
            wait: WaitStrategy::BusySpin,
        };
        cores
            .validate()
            .expect("nor is an unpinned preparer an unpinned spinner");
    }

    /// The layouts the runtime ships must pass their own check, on both
    /// transports.
    #[test]
    fn shipped_layouts_validate() {
        let default = ServerConfig::default().cores;
        for reader in [ReaderThread::IoUring, ReaderThread::DpdkPoll] {
            default.resolve(reader).expect("default layout");
        }
        let (compact, _) = PipelineCores::compact(16).expect("compact fits 16 cores");
        compact
            .resolve(ReaderThread::DpdkPoll)
            .expect("compact layout");
        assert_eq!(
            compact.journal_disk,
            Placement::unpinned(),
            "compact leaves the disk thread unpinned, hence yielding"
        );
    }

    /// The threads the boot log warns about when they have no core are
    /// the five every node runs, and only those: an unpinned auxiliary
    /// thread is a documented layout, an unpinned mandatory one a cost on
    /// every request. The five are spelled out here rather than read from
    /// the module, so a thread that silently stops counting is a test
    /// failure, and each must be a name the parser knows.
    #[test]
    fn unpinned_mandatory_names_the_hot_path_threads_without_a_core() {
        const MANDATORY: [&str; 5] = [
            "journal-seq",
            "matching",
            "response",
            "reader",
            "journal-disk",
        ];
        let known = PipelineCores::unpinned().named().map(|(name, _)| name);
        for name in MANDATORY {
            assert!(known.contains(&name), "{name} is not a --cores thread");
        }

        assert_eq!(PipelineCores::unpinned().unpinned_mandatory(), MANDATORY);
        assert!(
            ServerConfig::default()
                .cores
                .unpinned_mandatory()
                .is_empty()
        );

        // The auxiliary threads unpinned and the five pinned: nothing to
        // warn about.
        let hot_pinned = parse_cores(&complete(
            "journal-seq=1,matching=2,response=3,reader=4,journal-disk=5",
        ))
        .expect("parses");
        assert!(hot_pinned.unpinned_mandatory().is_empty());

        // One of the five unpinned among pinned auxiliaries: it is named,
        // and nothing else is.
        let disk_unpinned = parse_cores(
            "journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,\
             repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=0",
        )
        .expect("parses");
        assert_eq!(disk_unpinned.unpinned_mandatory(), ["journal-disk"]);

        // The compact layout's documented small-box choice counts too.
        let (compact, _) = PipelineCores::compact(16).expect("compact fits 16 cores");
        assert_eq!(compact.unpinned_mandatory(), ["journal-disk"]);
    }

    /// `all_yielding` is the programmatic "`y` on every entry": it turns
    /// a packed layout the check would refuse into one it accepts, and
    /// leaves the cores where they were.
    #[test]
    fn all_yielding_makes_a_packed_layout_legal() {
        let packed = parse_cores(
            "journal-seq=1,matching=1,response=1,reader=1,event-publisher=1,shadow=1,\
             repl-handler-0=1,repl-handler-1=1,journal-prep=0,journal-disk=0",
        )
        .expect("parses");
        assert!(
            packed.resolve(ReaderThread::IoUring).is_err(),
            "eight spinners on core 1 must be refused"
        );
        let cores = packed
            .all_yielding()
            .resolve(ReaderThread::IoUring)
            .expect("the same cores, all yielding, are legal");
        assert_eq!(cores.journal_seq, Placement::yielding(1));
        assert_eq!(cores.journal_disk, Placement::unpinned());
        assert_eq!(
            cores.to_string(),
            "journal-seq=1y,matching=1y,response=1y,reader=1y,event-publisher=1y,shadow=1y,\
             repl-handler-0=1y,repl-handler-1=1y,journal-prep=0,journal-disk=0",
            "every pinned entry carries the suffix"
        );
    }

    /// The DPDK poll thread busy-polls whatever its entry says, so a
    /// `y` on the reader is a contradiction and is refused there, while
    /// the io_uring reader accepts it. A spinning reader sharing a core
    /// is refused by the ordinary check. An unpinned reader is left as
    /// the operator wrote it.
    #[test]
    fn dpdk_reader_never_yields() {
        let explicit = parse_cores(&complete("reader=4y")).expect("parses");
        let err = explicit
            .resolve(ReaderThread::DpdkPoll)
            .expect_err("`4y` on the DPDK reader must be refused");
        assert!(err.contains("reader=4y"), "{err}");
        assert!(err.contains("poll"), "{err}");
        explicit
            .resolve(ReaderThread::IoUring)
            .expect("the io_uring reader may yield");

        let shared = parse_cores(&complete("reader=4,event-publisher=4y")).expect("parses");
        let err = shared
            .resolve(ReaderThread::DpdkPoll)
            .expect_err("a core shared with the poll thread is refused");
        assert!(err.contains("reader busy-spins"), "{err}");

        let unpinned =
            parse_cores(&complete("journal-seq=1,matching=2,response=3,reader=0")).expect("parses");
        let cores = unpinned
            .resolve(ReaderThread::DpdkPoll)
            .expect("an unpinned reader is accepted as written");
        assert_eq!(cores.reader, Placement::unpinned());
    }

    /// The disk thread's suffix has to reach the disk thread, and the
    /// preparer's core the preparer — through the one seam all three
    /// spawn paths share. Without this the hop from `journal_disk.wait`
    /// to the stage is correct by inspection only.
    #[test]
    fn place_journal_children_forwards_the_disk_and_preparer_placements() {
        use counter_server::CounterEvent;
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_transport_core::pipeline::InputSlot;
        use std::time::Duration;

        let dir = tempfile::tempdir().expect("tempdir");
        let writer = melin_journal::BufferedWriter::<CounterEvent>::create(&dir.path().join("j"))
            .expect("create journal");
        let (_producer, mut consumers) = DisruptorBuilder::<InputSlot<CounterEvent>>::new(4)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        // The sequencing thread spins; the disk thread must not inherit
        // that once the layout says otherwise.
        let mut stage = JournalStage::new(
            writer,
            consumers.pop().expect("one consumer"),
            Duration::ZERO,
            64,
            WaitStrategy::BusySpin,
        );
        assert_eq!(
            stage.disk_wait(),
            WaitStrategy::BusySpin,
            "before placement the disk thread inherits the sequencer's strategy"
        );

        let cores =
            parse_cores(&complete("journal-seq=1,journal-prep=8,journal-disk=5y")).expect("parses");
        cores.place_journal_children(&mut stage);
        assert_eq!(stage.disk_core(), 5);
        assert_eq!(stage.disk_wait(), WaitStrategy::SpinThenYield);
        assert_eq!(stage.preparer_core(), 8);
    }

    /// What the node logs at boot is the layout in `--cores` syntax, so
    /// an operator can copy it back verbatim — which holds only if the
    /// rendering round-trips through the parser, suffixes and all.
    #[test]
    fn layout_display_round_trips_through_parse_cores() {
        for input in [
            DEFAULT_CORES.to_owned(),
            MIXED.to_owned(),
            "none".to_owned(),
            complete("journal-seq=1y,matching=2y,response=3y,reader=4y,journal-disk=7y"),
        ] {
            let cores = parse_cores(&input).expect("parses");
            let rendered = cores.to_string();
            let reparsed = parse_cores(&rendered).expect("rendering parses");
            assert_eq!(reparsed, cores, "{input} -> {rendered}");
        }
        assert_eq!(PipelineCores::unpinned().to_string(), "none");
        assert_eq!(
            ServerConfig::default().cores.to_string(),
            DEFAULT_CORES,
            "the hand-written default renders as the CLI default"
        );
        assert_eq!(
            ServerConfig::try_parse_from(["melin-server"])
                .expect("parses")
                .cores,
            ServerConfig::default().cores,
            "a node started without --cores runs the hand-written default"
        );
    }

    /// The mixed layout through the CLI, resolved on both transports.
    #[test]
    fn mixed_layout_parses_and_resolves() {
        let config =
            ServerConfig::try_parse_from(["melin-server", "--cores", MIXED]).expect("parses");
        for reader in [ReaderThread::IoUring, ReaderThread::DpdkPoll] {
            let cores = config.resolved_cores(reader).expect("a legal mixed layout");
            assert_eq!(cores.journal_seq, Placement::spinning(1));
            assert_eq!(cores.reader, Placement::spinning(4));
            assert_eq!(cores.journal_disk, Placement::spinning(5));
            assert_eq!(cores.repl_handler_0, Placement::spinning(6));
            assert_eq!(cores.repl_handler_1, Placement::spinning(7));
            assert_eq!(cores.event_publisher, Placement::yielding(8));
            assert_eq!(cores.shadow, Placement::yielding(8));
            assert_eq!(cores.journal_prep, Placement::yielding(8));
            assert_eq!(
                cores.stage_waits(),
                StageWaits {
                    ingress: WaitStrategy::BusySpin,
                    journal: WaitStrategy::BusySpin,
                    matching: WaitStrategy::BusySpin,
                }
            );
        }
    }
}

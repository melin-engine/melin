//! Where each pipeline thread runs and how it waits there: the `--cores`
//! layout.
//!
//! A layout is one [`Placement`] per pipeline thread — a core and a
//! [`WaitStrategy`] — parsed from `--cores`, resolved against
//! `--yield-idle` and the transport, and checked before any thread is
//! spawned. The check exists because the two halves of a placement
//! constrain each other across threads: a busy-spinning thread needs its
//! core to itself, and two threads that share a core must both yield, or
//! the one waiting sits queued behind the one spinning for a full
//! scheduler slice on every hand-off. A node refuses to start with a
//! layout that breaks that rule rather than running slowly and
//! unpredictably.

use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::pipeline::StageWaits;

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

    /// No core of its own, left to the OS scheduler — and therefore
    /// yielding.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineCores {
    pub journal: Placement,
    pub matching: Placement,
    pub response: Placement,
    /// io_uring reader thread (TCP) or DPDK poll thread — see
    /// [`ReaderThread`] for how the two differ.
    pub reader: Placement,
    // There is no field for the fifth `--cores` entry. It once pinned the
    // replication accept thread (`repl-accept`), which does no work worth
    // a core — it accepts connections, reaps finished handlers, and
    // sleeps. `parse_cores` validates that position and discards it, so
    // existing `--cores` values keep every other entry's meaning; the
    // replica data path is pinned by `repl_handler_0`/`repl_handler_1`.
    pub event_publisher: Placement,
    pub shadow: Placement,
    /// Replication handler thread 0.
    pub repl_handler_0: Placement,
    /// Replication handler thread 1.
    pub repl_handler_1: Placement,
    /// The journal segment preparer (background staging of the next
    /// segment). Optional tenth entry of `--cores` — omitted (9-entry)
    /// values leave it unpinned so existing explicit configurations
    /// keep their exact behavior.
    ///
    /// Its wait policy is always yielding: the preparer blocks in file
    /// I/O and sleeps between attempts, it never polls a ring. It is
    /// carried here so the layout check knows it is harmless to share a
    /// core with.
    pub journal_prep: Placement,
    /// The journal disk thread — the half that writes, syncs, and
    /// publishes durability. Optional eleventh entry of `--cores`, same
    /// compatibility rule as `journal_prep`.
    ///
    /// Unlike the preparer this is a hot-path thread: it polls for
    /// batches, and the durability cursors every ack gates on are
    /// published from it. Place it on the same CCD as `journal` — the
    /// hand-off bounces a cache line between the two on every batch,
    /// and a cross-CCD transfer costs ~100 ns of that.
    pub journal_disk: Placement,
}

impl PipelineCores {
    /// Every thread with its name, in `--cores` order — the one list the
    /// layout check and its error messages iterate.
    fn named(&self) -> [(&'static str, Placement); 10] {
        [
            ("journal", self.journal),
            ("matching", self.matching),
            ("response", self.response),
            ("reader", self.reader),
            ("event-publisher", self.event_publisher),
            ("shadow", self.shadow),
            ("repl-handler-0", self.repl_handler_0),
            ("repl-handler-1", self.repl_handler_1),
            ("journal-prep", self.journal_prep),
            ("journal-disk", self.journal_disk),
        ]
    }

    /// The per-stage policies the pipeline builders take, drawn from the
    /// threads that own each stage: the reader produces into the input
    /// ring, the journal thread feeds the write and replication rings,
    /// matching produces into the output ring.
    pub fn stage_waits(&self) -> StageWaits {
        StageWaits {
            ingress: self.reader.wait,
            journal: self.journal.wait,
            matching: self.matching.wait,
        }
    }

    /// The same layout with every thread yielding — what `--yield-idle`
    /// means.
    pub fn all_yielding(self) -> Self {
        let y = |p: Placement| Placement::yielding(p.core);
        Self {
            journal: y(self.journal),
            matching: y(self.matching),
            response: y(self.response),
            reader: y(self.reader),
            event_publisher: y(self.event_publisher),
            shadow: y(self.shadow),
            repl_handler_0: y(self.repl_handler_0),
            repl_handler_1: y(self.repl_handler_1),
            journal_prep: y(self.journal_prep),
            journal_disk: y(self.journal_disk),
        }
    }

    /// The layout the node actually runs: `--yield-idle` folded in, the
    /// reader entry corrected for what the transport's reader thread can
    /// do, and the whole thing checked with [`validate`](Self::validate).
    /// The one place those inputs are combined — every spawn site reads
    /// the result, so no thread waits under a policy the operator did
    /// not ask for.
    ///
    /// On DPDK the reader is the NIC poll thread, which busy-polls
    /// whatever its entry says. An explicit `y` there is a contradiction
    /// and is refused. Under `--yield-idle` — shorthand for "every thread
    /// that can" — the entry is quietly kept spinning instead, and the
    /// check then refuses any core it shares. An unpinned DPDK reader
    /// stays unpinned: it polls flat out wherever the scheduler puts it,
    /// which is the one placement this module cannot make honest, and
    /// the operator docs say so.
    ///
    /// An `Err` is a configuration the node refuses to start with.
    pub fn resolve(self, yield_idle: bool, reader: ReaderThread) -> Result<Self, String> {
        let mut cores = if yield_idle {
            self.all_yielding()
        } else {
            self
        };
        if reader == ReaderThread::DpdkPoll && cores.reader.is_pinned() {
            if !yield_idle && cores.reader.wait == WaitStrategy::SpinThenYield {
                return Err(format!(
                    "--cores: the reader entry is `{core}y`, but on DPDK the reader is the NIC \
                     poll thread, which busy-polls whatever its entry says. Drop the suffix and \
                     give it core {core} to itself",
                    core = cores.reader.core
                ));
            }
            cores.reader = Placement::spinning(cores.reader.core);
        }
        cores.validate()?;
        Ok(cores)
    }

    /// Refuse a layout in which threads would starve each other: two
    /// threads pinned to the same core where either one busy-spins, or
    /// a busy-spinner with no core at all.
    ///
    /// Checked over every entry, whether or not today's flags spawn that
    /// thread — a layout is meant to be right independent of which
    /// features are on, and the message names the threads and the core
    /// so the fix is obvious. Two yielding threads on one core is the
    /// supported way to run on a small box and passes. The parser and
    /// the [`Placement`] constructors never produce an unpinned spinner,
    /// but the fields are public, so the check does not rely on that.
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
            journal: Placement::spinning(1),
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
            // poll, not a timeslice. Give it entry 11 on any box with
            // the cores to spare.
            journal_disk: Placement::unpinned(),
        };
        Ok((cores, 7))
    }
}

/// Renders the layout in `--cores` syntax, suffixes included, so what
/// the node logs at boot can be pasted straight back onto a command
/// line. The retired fifth position prints as `0`.
impl std::fmt::Display for PipelineCores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entry = |p: Placement| match (p.core, p.wait) {
            (0, _) => "0".to_string(),
            (core, WaitStrategy::BusySpin) => core.to_string(),
            (core, WaitStrategy::SpinThenYield) => format!("{core}y"),
        };
        write!(
            f,
            "{},{},{},{},0,{},{},{},{},{},{}",
            entry(self.journal),
            entry(self.matching),
            entry(self.response),
            entry(self.reader),
            entry(self.event_publisher),
            entry(self.shadow),
            entry(self.repl_handler_0),
            entry(self.repl_handler_1),
            // The preparer takes no suffix — its entry is a core only.
            self.journal_prep.core,
            entry(self.journal_disk),
        )
    }
}

/// Parse one `--cores` entry: a core ID with an optional wait suffix.
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
        .map_err(|_| format!("invalid core ID: {entry}"))?;
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

/// Parse "j,m,r,rd,rs,ep,sh,h0,h1[,jp[,jd]]" into `PipelineCores` for
/// pipeline core affinity. Each entry takes the suffix described at
/// [`parse_placement`].
pub(crate) fn parse_cores(s: &str) -> Result<PipelineCores, String> {
    let parts: Vec<&str> = s.split(',').collect();
    // 9 to 11 entries: journal-prep (tenth) and journal-disk (eleventh)
    // were added later, so every explicit operator configuration written
    // before they existed must keep parsing, with the missing entries
    // left unpinned. An operator upgrading into the journal split gets a
    // working server that has not silently taken an extra core — see the
    // `journal_disk` field docs for why they should then give it one.
    if !(9..=11).contains(&parts.len()) {
        return Err(format!(
            "expected 9 to 11 comma-separated core IDs (journal,matching,response,reader,unused,event-publisher,shadow,repl-handler-0,repl-handler-1[,journal-prep[,journal-disk]]), got {}",
            parts.len()
        ));
    }
    // The fifth entry is the retired replication-accept core: validated,
    // then dropped. Validating it still matters — a typo there would
    // otherwise pass silently, and an operator who shifted their list by
    // one should hear about it rather than have every later thread land
    // on the wrong core. A plain integer, not a placement: no thread
    // reads it, so a wait suffix there has nothing to describe.
    parts[4]
        .parse::<usize>()
        .map_err(|_| format!("invalid core ID: {}", parts[4]))?;
    // The preparer never busy-waits (it blocks in I/O), so its entry is
    // a core and nothing else: a `y` is redundant and an `s` asks for
    // something the thread cannot do.
    let journal_prep = match parts.get(9) {
        None => Placement::unpinned(),
        Some(entry) if entry.ends_with('s') => {
            return Err(format!(
                "invalid journal-prep entry `{entry}`: the preparer never busy-waits and \
                 takes no `s` suffix"
            ));
        }
        Some(entry) => Placement::yielding(parse_placement(entry)?.core),
    };
    Ok(PipelineCores {
        journal: parse_placement(parts[0])?,
        matching: parse_placement(parts[1])?,
        response: parse_placement(parts[2])?,
        reader: parse_placement(parts[3])?,
        event_publisher: parse_placement(parts[5])?,
        shadow: parse_placement(parts[6])?,
        repl_handler_0: parse_placement(parts[7])?,
        repl_handler_1: parse_placement(parts[8])?,
        journal_prep,
        journal_disk: parts
            .get(10)
            .map(|p| parse_placement(p))
            .transpose()?
            .unwrap_or(Placement::unpinned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ServerConfig;
    use clap::Parser;

    /// The last two `--cores` entries are optional and were added in
    /// that order (journal-prep, then journal-disk). Every explicit
    /// configuration written before either existed must keep parsing,
    /// with the missing entries unpinned — an operator upgrading into
    /// the journal split gets a working server rather than a rejected
    /// command line.
    #[test]
    fn parse_cores_accepts_nine_through_eleven_entries() {
        let nine = parse_cores("1,2,3,4,5,6,7,8,9").expect("9 entries must parse");
        assert_eq!(
            nine.journal_prep,
            Placement::unpinned(),
            "omitted journal-prep = unpinned"
        );
        assert_eq!(
            nine.journal_disk,
            Placement::unpinned(),
            "omitted journal-disk = unpinned"
        );
        assert_eq!(nine.repl_handler_1.core, 9);

        let ten = parse_cores("1,2,3,4,5,6,7,8,9,10").expect("10 entries must parse");
        assert_eq!(ten.journal_prep.core, 10);
        assert_eq!(
            ten.journal_disk,
            Placement::unpinned(),
            "omitted journal-disk = unpinned"
        );

        let eleven = parse_cores("1,2,3,4,5,6,7,8,9,10,11").expect("11 entries must parse");
        assert_eq!(eleven.journal_prep.core, 10);
        assert_eq!(eleven.journal_disk.core, 11);

        assert!(parse_cores("1,2,3").is_err());
        assert!(parse_cores("1,2,3,4,5,6,7,8,9,10,11,12").is_err());
    }

    /// The fifth entry once pinned the replication accept thread and now
    /// has no field. It must be *skipped*, not dropped: everything after
    /// it has to keep its position, or an existing `--cores` value would
    /// silently move every later thread onto the wrong core.
    #[test]
    fn parse_cores_skips_the_retired_fifth_entry_without_shifting() {
        // A distinctive value in position five that must not surface
        // anywhere in the result.
        let cores = parse_cores("1,2,3,4,99,6,7,8,9,10,11").expect("11 entries must parse");
        assert_eq!(cores.journal.core, 1);
        assert_eq!(cores.matching.core, 2);
        assert_eq!(cores.response.core, 3);
        assert_eq!(cores.reader.core, 4);
        assert_eq!(
            cores.event_publisher.core, 6,
            "sixth entry stays event-publisher"
        );
        assert_eq!(cores.shadow.core, 7);
        assert_eq!(cores.repl_handler_0.core, 8);
        assert_eq!(cores.repl_handler_1.core, 9);
        assert_eq!(cores.journal_prep.core, 10);
        assert_eq!(cores.journal_disk.core, 11);
    }

    /// Discarded is not unvalidated. A typo in the retired position still
    /// fails the boot, because the likeliest cause is a list shifted by
    /// one — which would otherwise mispin every thread after it. It is a
    /// plain integer, though: a wait suffix describes a thread, and no
    /// thread reads this position.
    #[test]
    fn parse_cores_still_rejects_a_malformed_fifth_entry() {
        assert!(parse_cores("1,2,3,4,x,6,7,8,9").is_err());
        assert!(parse_cores("1,2,3,4,5y,6,7,8,9").is_err());
    }

    /// A bare core busy-spins (the production default, and what every
    /// existing `--cores` value means), `y` yields, `s` spells the
    /// default out, and `0` is unpinned — which can only yield.
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
        let cores = parse_cores("1,2,3,4,5,6,7,8,9,10,11").expect("parses");
        assert_eq!(cores.journal_prep, Placement::yielding(10));
        let explicit = parse_cores("1,2,3,4,5,6,7,8,9,10y,11").expect("parses");
        assert_eq!(explicit.journal_prep, Placement::yielding(10));
        let err = parse_cores("1,2,3,4,5,6,7,8,9,10s,11").expect_err("must refuse");
        assert!(err.contains("journal-prep"), "{err}");
    }

    /// The layout the whole change exists to refuse: two threads on one
    /// core where either busy-spins. The message has to name both
    /// threads, the core, and which one spins, or the operator is left
    /// guessing which of eleven entries to change.
    #[test]
    fn validate_refuses_a_busy_spinner_sharing_a_core() {
        // Two spinners.
        let err = parse_cores("1,1,3,4,0,6,7,8,9")
            .expect("parses")
            .validate()
            .expect_err("two spinners on core 1 must be refused");
        assert!(err.contains("journal"), "{err}");
        assert!(err.contains("matching"), "{err}");
        assert!(err.contains("core 1"), "{err}");

        // A spinner next to a yielder: the yielder would still starve.
        let err = parse_cores("1,2,3,4,0,7,7y,8,9")
            .expect("parses")
            .validate()
            .expect_err("a spinner sharing with a yielder must be refused");
        assert!(err.contains("event-publisher"), "{err}");
        assert!(err.contains("shadow"), "{err}");
        assert!(err.contains("event-publisher busy-spins"), "{err}");

        // The order of the two entries does not matter.
        parse_cores("1,2,3,4,0,6,6y,0,0")
            .expect("parses")
            .validate()
            .expect_err("6 and 6y is still a spinner sharing a core");

        // The disk thread sharing the journal's core is the same mistake.
        let err = parse_cores("1,2,3,4,0,6,7,8,9,10,1")
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
        parse_cores("1,2,3,4,0,7y,7y,7y,7y,7,7y")
            .expect("parses")
            .validate()
            .expect("yielders and the preparer may share core 7");
        parse_cores("0,0,0,0,0,0,0,0,0")
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

    /// The layouts the runtime ships must pass their own check, on both
    /// transports.
    #[test]
    fn shipped_layouts_validate() {
        let default = ServerConfig::default().cores;
        for reader in [ReaderThread::IoUring, ReaderThread::DpdkPoll] {
            default.resolve(false, reader).expect("default layout");
            default
                .resolve(true, reader)
                .expect("default layout under --yield-idle");
        }
        let (compact, _) = PipelineCores::compact(16).expect("compact fits 16 cores");
        compact
            .resolve(false, ReaderThread::DpdkPoll)
            .expect("compact layout");
        assert_eq!(
            compact.journal_disk,
            Placement::unpinned(),
            "compact leaves the disk thread unpinned, hence yielding"
        );
    }

    /// `--yield-idle` is shorthand for a `y` on every entry, and it is
    /// applied before the check — so the packed layouts the test suite
    /// runs with keep working under the flag alone.
    #[test]
    fn resolve_folds_in_yield_idle_before_checking() {
        let packed = parse_cores("1,1,1,1,0,1,1,1,1").expect("parses");
        assert!(
            packed.resolve(false, ReaderThread::IoUring).is_err(),
            "nine spinners on core 1 must be refused"
        );
        let cores = packed
            .resolve(true, ReaderThread::IoUring)
            .expect("--yield-idle makes the packed layout legal");
        assert_eq!(cores.journal, Placement::yielding(1));
        assert_eq!(cores.journal_disk, Placement::unpinned());
        assert_eq!(cores, cores.all_yielding(), "every thread yields");
    }

    /// The DPDK poll thread busy-polls whatever its entry says, so the
    /// reader's resolved placement must say what the thread will do:
    /// an explicit `y` is a contradiction and is refused; under the
    /// `--yield-idle` shorthand the reader stays a spinner, and a core
    /// it shares is refused on that basis; an unpinned reader is left
    /// as the operator wrote it.
    #[test]
    fn dpdk_reader_never_yields() {
        let explicit = parse_cores("1,2,3,4y,0,6,7,8,9").expect("parses");
        let err = explicit
            .resolve(false, ReaderThread::DpdkPoll)
            .expect_err("`4y` on the DPDK reader must be refused");
        assert!(err.contains("reader"), "{err}");
        assert!(err.contains("poll"), "{err}");
        explicit
            .resolve(false, ReaderThread::IoUring)
            .expect("the io_uring reader may yield");

        let alone = parse_cores("1,2,3,4,0,6,7,8,9").expect("parses");
        let cores = alone
            .resolve(true, ReaderThread::DpdkPoll)
            .expect("--yield-idle with the reader on its own core");
        assert_eq!(
            cores.reader,
            Placement::spinning(4),
            "the shorthand leaves the poll thread spinning"
        );
        assert_eq!(cores.journal, Placement::yielding(1));

        let shared = parse_cores("1,2,3,4,0,4,7,8,9").expect("parses");
        let err = shared
            .resolve(true, ReaderThread::DpdkPoll)
            .expect_err("a core shared with the poll thread is refused even under --yield-idle");
        assert!(err.contains("reader busy-spins"), "{err}");
        shared
            .resolve(true, ReaderThread::IoUring)
            .expect("the same layout is fine when the reader can yield");

        let unpinned = parse_cores("1,2,3,0,0,6,7,8,9").expect("parses");
        let cores = unpinned
            .resolve(true, ReaderThread::DpdkPoll)
            .expect("an unpinned reader is accepted as written");
        assert_eq!(cores.reader, Placement::unpinned());
    }

    /// What the node logs at boot is the layout in `--cores` syntax, so
    /// an operator can copy it back verbatim — which holds only if the
    /// rendering round-trips through the parser, suffixes and all.
    #[test]
    fn layout_display_round_trips_through_parse_cores() {
        for input in [
            "1,2,3,4,0,6,7,8,9,10,11",
            "1,2,3,4,0,6y,6y,6y,6y,6,5",
            "0,0,0,0,0,0,0,0,0",
            "1y,2y,3y,4y,0,0,0,0,0,0,7y",
        ] {
            let cores = parse_cores(input).expect("parses");
            let rendered = cores.to_string();
            let reparsed = parse_cores(&rendered).expect("rendering parses");
            assert_eq!(reparsed, cores, "{input} -> {rendered}");
        }
        assert_eq!(
            ServerConfig::default().cores.to_string(),
            "1,2,3,4,0,6,7,8,9,10,11"
        );
    }

    /// A mixed layout through the CLI — the one the operator docs show:
    /// the hot stages spin on their own cores, everything else shares
    /// one core and yields.
    #[test]
    fn mixed_layout_parses_and_resolves() {
        let config =
            ServerConfig::try_parse_from(["melin-server", "--cores", "1,2,3,4,0,6y,6y,6y,6y,6,5"])
                .expect("parses");
        for reader in [ReaderThread::IoUring, ReaderThread::DpdkPoll] {
            let cores = config.resolved_cores(reader).expect("a legal mixed layout");
            assert_eq!(cores.journal, Placement::spinning(1));
            assert_eq!(cores.reader, Placement::spinning(4));
            assert_eq!(cores.journal_disk, Placement::spinning(5));
            assert_eq!(cores.shadow, Placement::yielding(6));
            assert_eq!(cores.journal_prep, Placement::yielding(6));
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

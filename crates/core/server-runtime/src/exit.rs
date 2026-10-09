//! The node's exit status, for an application's `main`.
//!
//! [`crate::server::run`] returns an error when the node stops on a
//! failure. One class of failure needs a different response from
//! whatever supervises the process, and so its own exit status: the
//! kernel failed a sync of the journal
//! ([`melin_journal::JournalError::WriteFailed`], whose documentation
//! lists exactly which syncs). After a failed write-back, Linux keeps the
//! data in the page cache, marks it clean and reports the error once, so
//! a node restarted in place, on the same host, reads data the device
//! never took as if it were durable. The node has to be failed over and
//! re-seeded, or the host rebooted first, never simply restarted.
//!
//! Everything else that stops a node exits with status 1, as a `main`
//! returning the error would: configuration and bind errors, a recovery
//! that refuses the journal it finds, a journal that cannot be read or
//! written (a refused buffered write leaves no unwritten pages behind;
//! see the variant's documentation), a pipeline thread that panicked.
//! Restarting after those cannot present unwritten data as written: a
//! refusal is decided from what is on the device and repeats on every
//! start, and a read error leaves nothing in the page cache that the
//! device does not hold.
//!
//! # Why the status survives a lost error
//!
//! The runtime keeps the journal's error as the `source` of what it
//! returns, but an error can still be flattened into a message on its way
//! out, or replaced by another failure seen at the same teardown, and a
//! walk of the `source` chain would then miss it. So [`exit_code`] also
//! reads a process-wide latch, [`melin_journal::write_failure_latched`],
//! set whenever the journal builds a write failure: the status is 74 if
//! the chain holds one **or** the latch is set, whatever the layers above
//! did to the error. The chain is what names the failure in the printed
//! message; the latch is what makes the status reliable. The latch also
//! holds a failed sync the node survived (a failed rotation rolls back
//! and runs on), so a later, unrelated stop exits 74 too; [`exit_code`]
//! then prints a second line saying a journal sync failed earlier, since
//! the error it prints does not.
//!
//! ```no_run
//! # fn run() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
//! fn main() -> std::process::ExitCode {
//!     // server::run::<App>(config, startup, sizing, decoder, encoder, None)
//!     melin_server_runtime::exit::exit_code(run())
//! }
//! ```

use std::error::Error;
use std::fmt;
use std::process::ExitCode;

use melin_journal::JournalError;

/// Exit status of a node stopped after a journal write failure: 74,
/// `EX_IOERR` in `sysexits.h`. It reports precisely the failure that is
/// not [`JournalError::Io`]: [`JournalError::WriteFailed`]. A supervisor
/// must not restart the node in place on this status (systemd:
/// `RestartPreventExitStatus=74`).
///
/// `u8` because that is what an exit status is (`ExitCode::from`).
pub const EXIT_JOURNAL_WRITE_FAILED: u8 = 74;

/// Whether `error`, or any error in its `source` chain, is a journal
/// write failure: the failure [`EXIT_JOURNAL_WRITE_FAILED`] reports.
///
/// Reads the chain only. [`exit_code`] also consults the process-wide
/// latch (see the module documentation), which still holds when a layer
/// above the journal flattened the error into a message.
pub fn is_journal_write_failure(error: &(dyn Error + 'static)) -> bool {
    let mut next = Some(error);
    while let Some(e) = next {
        if let Some(JournalError::WriteFailed(_)) = e.downcast_ref::<JournalError>() {
            return true;
        }
        next = e.source();
    }
    false
}

/// The exit status for what [`crate::server::run`] returned:
/// [`EXIT_JOURNAL_WRITE_FAILED`] for an error when the error's chain
/// holds a journal write failure or this process latched one (see the
/// module documentation), `FAILURE` (1) for any other error, `SUCCESS`
/// for a clean shutdown. Prints the error to stderr first, with its
/// `Display` form (a `main` returning the error would print its `Debug`
/// form instead). When the status comes from the latch alone, a second
/// line says so, since the printed error then does not name the write
/// failure.
pub fn exit_code(result: Result<(), Box<dyn Error>>) -> ExitCode {
    let latched = melin_journal::write_failure_latched();
    if let Err(error) = &result {
        eprintln!("Error: {error}");
        if let Some(note) = latch_note(&**error, latched) {
            eprintln!("{note}");
        }
    }
    status_for(&result, latched)
}

/// The line [`exit_code`] adds when the latch, not the error it prints,
/// is what makes the status 74: a journal sync failed earlier in this
/// process (a failed rotation, which the node survives, or an error
/// flattened on its way out), and that is what the supervisor must
/// act on.
fn latch_note(error: &(dyn Error + 'static), latched: bool) -> Option<&'static str> {
    (latched && !is_journal_write_failure(error)).then_some(
        "Note: a journal sync failed earlier in this process (see the log), so the exit \
         status is 74. Do not restart this node in place.",
    )
}

/// [`exit_code`]'s decision, with the latch passed in so it can be tested
/// without the process-wide state other tests in the binary may set.
pub(crate) fn status_for(result: &Result<(), Box<dyn Error>>, latched: bool) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if latched || is_journal_write_failure(&**error) => {
            ExitCode::from(EXIT_JOURNAL_WRITE_FAILED)
        }
        Err(_) => ExitCode::FAILURE,
    }
}

/// A journal stage that stopped with an error, with what the runtime was
/// doing when it saw it. Keeps the journal's error as its `source`
/// rather than flattening it into a message, so [`is_journal_write_failure`]
/// finds it however many layers wrap it on the way out.
#[derive(Debug)]
pub(crate) struct JournalStageFailed {
    context: String,
    source: JournalError,
}

impl JournalStageFailed {
    pub(crate) fn new(context: impl Into<String>, source: JournalError) -> Self {
        Self {
            context: context.into(),
            source,
        }
    }
}

impl fmt::Display for JournalStageFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.context, self.source)
    }
}

impl Error for JournalStageFailed {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// These tests decide the status through [`status_for`] with an explicit
/// latch, never through the process-wide one, which another test in this
/// binary may have set.
#[cfg(test)]
mod tests {
    use super::*;

    /// Built directly rather than through `JournalError::write_failed`,
    /// so these tests do not set the process-wide latch.
    fn write_failure() -> JournalError {
        JournalError::WriteFailed(std::io::Error::from_raw_os_error(libc::EIO))
    }

    #[test]
    fn a_write_failure_is_found_through_the_wrappers() {
        let direct: Box<dyn Error> = Box::new(write_failure());
        assert!(is_journal_write_failure(&*direct));

        let wrapped: Box<dyn Error> = Box::new(JournalStageFailed::new(
            "journal stage failed",
            write_failure(),
        ));
        assert!(is_journal_write_failure(&*wrapped));
        assert_eq!(
            status_for(&Err(wrapped), false),
            ExitCode::from(EXIT_JOURNAL_WRITE_FAILED)
        );
    }

    /// The failures that leave nothing unwritten behind exit as any other
    /// error does: a plain I/O error (reading, allocating, a refused
    /// buffered write), a refused recovery, a message.
    #[test]
    fn other_failures_exit_with_status_1() {
        for error in other_failures() {
            assert!(!is_journal_write_failure(&*error), "{error}");
            assert_eq!(status_for(&Err(error), false), ExitCode::FAILURE);
        }
        assert_eq!(status_for(&Ok(()), false), ExitCode::SUCCESS);
    }

    /// The latch decides the status when the error that reached `main`
    /// no longer carries the write failure (flattened into a message, or
    /// replaced by another failure on the way out), and a clean shutdown
    /// stays clean.
    #[test]
    fn a_latched_write_failure_survives_a_flattened_error() {
        let flattened: Box<dyn Error> = format!("pipeline failed: {}", write_failure()).into();
        assert!(!is_journal_write_failure(&*flattened));
        assert_eq!(
            status_for(&Err(flattened), true),
            ExitCode::from(EXIT_JOURNAL_WRITE_FAILED)
        );
        for error in other_failures() {
            assert_eq!(
                status_for(&Err(error), true),
                ExitCode::from(EXIT_JOURNAL_WRITE_FAILED)
            );
        }
        assert_eq!(status_for(&Ok(()), true), ExitCode::SUCCESS);
    }

    /// The extra line appears exactly when the latch decides the status
    /// and the printed error does not name the write failure itself.
    #[test]
    fn the_latch_note_explains_a_status_the_message_does_not() {
        for error in other_failures() {
            assert!(latch_note(&*error, true).is_some(), "{error}");
            assert!(latch_note(&*error, false).is_none(), "{error}");
        }
        let named: Box<dyn Error> = Box::new(JournalStageFailed::new(
            "journal stage failed",
            write_failure(),
        ));
        assert!(latch_note(&*named, true).is_none());
        assert!(latch_note(&*named, false).is_none());
    }

    fn other_failures() -> Vec<Box<dyn Error>> {
        vec![
            Box::new(JournalError::Io(std::io::Error::from_raw_os_error(
                libc::EIO,
            ))),
            Box::new(JournalStageFailed::new(
                "journal stage failed",
                JournalError::UnrecoverableTail {
                    path: "j.journal".into(),
                    offset: 4096,
                    last_sequence: None,
                    nonzero_at: 1 << 30,
                    cause: None,
                },
            )),
            "pipeline failure".into(),
        ]
    }
}

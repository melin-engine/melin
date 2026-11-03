//! The kernel boot stamp: which boot of its host this process runs in, and
//! when in that boot it started.
//!
//! Two stamps from one host boot order process starts: `CLOCK_BOOTTIME` is
//! monotonic for the life of the kernel, counts time suspended, and is never
//! stepped by NTP or an operator, so a process that started later reads a
//! later value. Two stamps from different boots do not compare at all, and
//! need not: the kernel that ran the earlier one is gone, so nothing it was
//! sending can still be in flight from it.
//!
//! Nothing here is persisted, on purpose. A persisted counter that a
//! restored disk or a deleted file rewinds would make every frame a node
//! sends look older than the ones its peers already hold; the kernel's own
//! boot identity cannot be rewound that way (a VM snapshot restore rewinds
//! the clock, the memory and the disk together, a hazard the node shares
//! with its journal, documented for operators).
//!
//! Read once at startup by the control plane, which stamps every frame it
//! sends with it (see `melin-raft`'s `TipSource`).

use std::io;
use std::path::Path;

/// Where Linux publishes the identifier of the current boot: a random UUID
/// drawn when the kernel starts, the same for every process until the next
/// boot.
pub const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Which host boot a process runs in and when in that boot it started.
///
/// `[u8; 16]` for the boot id: it is a UUID, compared for equality only, so
/// its raw 128 bits are the whole of it, and an array keeps it a fixed 16
/// bytes on any wire (postcard writes a `u128` as a varint of up to 19
/// bytes, for no arithmetic anyone does on it). `u64` nanoseconds for the
/// start: `CLOCK_BOOTTIME` counts from the boot, so a `u64` of nanoseconds
/// lasts centuries of uptime, and nanoseconds keep two starts a moment
/// apart distinct.
///
/// Built only by [`BootStamp::read`] outside tests, so a stamp in
/// production is always the kernel's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootStamp {
    boot_id: [u8; 16],
    started_ns: u64,
}

impl BootStamp {
    /// This process's stamp: the host's [`BOOT_ID_PATH`] and the current
    /// `CLOCK_BOOTTIME`. Call it once, at startup; the time it reads is
    /// what stands for the process start.
    ///
    /// Refuses, rather than falling back to anything weaker, when the boot
    /// id cannot be read or does not parse, or the clock cannot be read: a
    /// stamp that does not identify the boot would let a frame from an
    /// earlier process pass as newer. The error names the path and why.
    pub fn read() -> io::Result<Self> {
        read_boot_stamp(Path::new(BOOT_ID_PATH))
    }

    /// [`BootStamp::read`] with the boot id taken from `path`: the seam that
    /// lets a test make it unreadable or malformed.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn read_from(path: &Path) -> io::Result<Self> {
        read_boot_stamp(path)
    }

    /// The host boot's identifier, as 16 raw bytes.
    pub fn boot_id(&self) -> [u8; 16] {
        self.boot_id
    }

    /// `CLOCK_BOOTTIME` in nanoseconds when the stamp was read: the process
    /// start, for ordering this process against others of the same boot.
    pub fn started_ns(&self) -> u64 {
        self.started_ns
    }

    /// A stamp with the given parts, for tests that play several processes
    /// or boots.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_parts(boot_id: [u8; 16], started_ns: u64) -> Self {
        Self {
            boot_id,
            started_ns,
        }
    }
}

/// [`BootStamp::read`] with the boot id taken from `path`.
fn read_boot_stamp(path: &Path) -> io::Result<BootStamp> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot read the kernel boot id at {}: {e}", path.display()),
        )
    })?;
    let boot_id = parse_boot_id(&text).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} does not hold a kernel boot id (a UUID): {:?}",
                path.display(),
                text.trim_end()
            ),
        )
    })?;
    Ok(BootStamp {
        boot_id,
        started_ns: boottime_ns()?,
    })
}

/// The 16 bytes of a boot id as the kernel prints it: a UUID,
/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` in hex digits, then a newline.
/// `None` for anything else, so a file that is not a boot id is never
/// half-read into one.
fn parse_boot_id(text: &str) -> Option<[u8; 16]> {
    // `Option`, not a `Result`: a missing newline is accepted, not an error
    // being dropped.
    let uuid = text.strip_suffix('\n').unwrap_or(text).as_bytes();
    if uuid.len() != 36 {
        return None;
    }
    let mut id = [0u8; 16];
    let mut nibble = 0;
    for (i, &c) in uuid.iter().enumerate() {
        if matches!(i, 8 | 13 | 18 | 23) {
            if c != b'-' {
                return None;
            }
            continue;
        }
        // At most 4 bits, so the narrowing cannot lose any.
        let value = char::from(c).to_digit(16)? as u8;
        id[nibble / 2] |= if nibble % 2 == 0 { value << 4 } else { value };
        nibble += 1;
    }
    Some(id)
}

/// `CLOCK_BOOTTIME` now, in nanoseconds.
fn boottime_ns() -> io::Result<u64> {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable `timespec` for the duration of the
    // call, which writes nothing else.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut now) } != 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(
            e.kind(),
            format!("cannot read CLOCK_BOOTTIME: {e}"),
        ));
    }
    timespec_ns(now.tv_sec, now.tv_nsec).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "CLOCK_BOOTTIME out of range: {}s {}ns",
                now.tv_sec, now.tv_nsec
            ),
        )
    })
}

/// A `timespec`'s seconds and nanoseconds as one count of nanoseconds, or
/// `None` if either part is negative or the sum overflows a `u64`. The
/// kernel never reports a negative time since boot, nor one that long; a
/// reading that did is refused rather than wrapped.
fn timespec_ns(secs: libc::time_t, nanos: libc::c_long) -> Option<u64> {
    let secs = u64::try_from(secs).ok()?;
    let nanos = u64::try_from(nanos).ok()?;
    secs.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kernel_boot_id_parses_to_its_sixteen_bytes() {
        assert_eq!(
            parse_boot_id("cf9371ac-8286-463a-9edb-801d90de42f1\n"),
            Some([
                0xcf, 0x93, 0x71, 0xac, 0x82, 0x86, 0x46, 0x3a, 0x9e, 0xdb, 0x80, 0x1d, 0x90, 0xde,
                0x42, 0xf1
            ])
        );
        // Without the trailing newline too.
        let mut one = [0u8; 16];
        one[15] = 1;
        assert_eq!(
            parse_boot_id("00000000-0000-0000-0000-000000000001"),
            Some(one)
        );
    }

    #[test]
    fn anything_but_a_uuid_is_not_a_boot_id() {
        for bad in [
            "",
            "\n",
            "cf9371ac-8286-463a-9edb-801d90de42f",      // short
            "cf9371ac-8286-463a-9edb-801d90de42f1a",    // long
            "cf9371ac+8286-463a-9edb-801d90de42f1",     // a dash missing
            "cf9371ac-8286-463a-9edb-801d90de42g1",     // not hex
            "cf9371ac82864-63a-9edb-801d90de42f1",      // a dash misplaced
            "cf9371ac-8286-463a-9edb-801d90de42f1\n\n", // two newlines
            " cf9371ac-8286-463a-9edb-801d90de42f1",    // leading space
            "cf9371ac-8286-463a-9edb-801d90de42é",      // multi-byte char
        ] {
            assert_eq!(parse_boot_id(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_timespec_converts_to_nanoseconds_or_is_refused() {
        assert_eq!(timespec_ns(0, 0), Some(0));
        assert_eq!(timespec_ns(2, 5), Some(2_000_000_005));
        assert_eq!(timespec_ns(-1, 0), None);
        assert_eq!(timespec_ns(0, -1), None);
        // Seconds whose nanoseconds overflow a `u64`, and a sum that does.
        let max_secs = (u64::MAX / 1_000_000_000) as libc::time_t;
        assert_eq!(timespec_ns(max_secs + 1, 0), None);
        assert_eq!(timespec_ns(max_secs, 999_999_999), None);
        assert!(timespec_ns(max_secs, 0).is_some());
    }

    /// The tests that read files and the clock: skipped under Miri
    /// (`scripts/miri.sh`), which isolates the host.
    mod host {
        use super::*;

        /// Startup refuses when the boot id cannot be read, naming the path.
        #[test]
        fn an_unreadable_boot_id_refuses() {
            let dir = tempfile::tempdir().unwrap();
            let missing = dir.path().join("boot_id");
            let err = BootStamp::read_from(&missing).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::NotFound);
            let message = err.to_string();
            assert!(
                message.contains("cannot read the kernel boot id")
                    && message.contains(&missing.display().to_string()),
                "{message}"
            );
        }

        /// Startup refuses a boot id file that does not hold a UUID, naming
        /// the path and quoting what it holds.
        #[test]
        fn a_malformed_boot_id_refuses() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("boot_id");
            std::fs::write(&path, "not a boot id\n").unwrap();
            let err = BootStamp::read_from(&path).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            let message = err.to_string();
            assert!(
                message.contains("\"not a boot id\"")
                    && message.contains(&path.display().to_string()),
                "{message}"
            );
        }

        #[test]
        fn a_boot_id_file_is_read_with_the_boot_clock() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("boot_id");
            std::fs::write(&path, "00000000-0000-0000-0000-0000000000ff\n").unwrap();
            let stamp = BootStamp::read_from(&path).unwrap();
            let mut expected = [0u8; 16];
            expected[15] = 0xff;
            assert_eq!(stamp.boot_id(), expected);
            assert!(stamp.started_ns() > 0);
        }

        /// The host's own stamp: two reads in one boot share the boot id,
        /// and the later one is no earlier (the clock is monotonic; a coarse
        /// clock source may report the same value for two reads a moment
        /// apart).
        #[test]
        fn this_hosts_stamps_share_the_boot_and_move_forward() {
            let first = BootStamp::read().unwrap();
            let second = BootStamp::read().unwrap();
            assert_eq!(first.boot_id(), second.boot_id());
            assert!(second.started_ns() >= first.started_ns());
        }
    }
}

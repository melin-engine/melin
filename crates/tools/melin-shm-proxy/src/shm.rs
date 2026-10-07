//! The shared-memory link: one file, two byte rings, a state word.
//!
//! The proxy creates the file and is the only writer of the header. The
//! client process maps the same file and finds everything it needs in
//! that header: the capacities, where the rings start, and whether the
//! proxy is connected. The layout is the contract with the other side,
//! which is not Rust, so it is spelled out in offsets rather than left to
//! a struct:
//!
//! ```text
//! offset   size  field
//!      0      8  magic, the ASCII bytes "MELINSHM"
//!      8      4  layout version, 1
//!     16      8  to-wire ring capacity, bytes, a power of two
//!     24      8  from-wire ring capacity, bytes, a power of two
//!     32      4  state: 0 starting, 1 connected, 2 closed, 3 failed
//!     40      4  close requested: the client sets it to 1, the proxy exits
//!     64      8  to-wire tail    (written by the client:  bytes produced)
//!    128      8  to-wire head    (written by the proxy:   bytes consumed)
//!    192      8  from-wire tail  (written by the proxy:   bytes produced)
//!    256      8  from-wire head  (written by the client:  bytes consumed)
//!   4096      -  to-wire ring data, then from-wire ring data
//! ```
//!
//! "To wire" carries what the client sends; "from wire" what the server
//! answers. Each ring is a byte stream with two monotonic cursors: the
//! producer advances the tail after writing, the consumer advances the
//! head after reading, and the bytes between them are the ones in
//! flight. Every cursor has a cache line to itself, because the one thing
//! two busy-spinning cores must not do is write the same line. Cursors
//! and counters are native-endian: both sides are the one machine.
//!
//! Ordering is the usual pair. A producer writes the bytes, then the
//! tail with release; a consumer reads the tail with acquire, then the
//! bytes. The mirror holds for the head. On x86 that is a plain store
//! and a plain load with the compiler kept honest, which is the cost the
//! design is built around.

use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub const MAGIC: [u8; 8] = *b"MELINSHM";
pub const VERSION: u32 = 1;
pub const HEADER_LEN: usize = 4096;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_TO_WIRE_CAPACITY: usize = 16;
const OFF_FROM_WIRE_CAPACITY: usize = 24;
const OFF_STATE: usize = 32;
const OFF_CLOSE_REQUESTED: usize = 40;
const OFF_TO_WIRE_TAIL: usize = 64;
const OFF_TO_WIRE_HEAD: usize = 128;
const OFF_FROM_WIRE_TAIL: usize = 192;
const OFF_FROM_WIRE_HEAD: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum State {
    /// The file exists; the proxy is still connecting.
    Starting = 0,
    /// The connection is up and the rings are live.
    Connected = 1,
    /// The connection is gone. Bytes left in the from-wire ring are
    /// still the server's last words and may be read.
    Closed = 2,
    /// The proxy could not connect. Its own output says why.
    Failed = 3,
}

/// The mapped file, owned by the proxy. Unmapped on drop; the file itself
/// is left for the next proxy to truncate, since the client may still
/// have it open.
pub struct SharedMemory {
    base: *mut u8,
    len: usize,
    to_wire: Ring,
    from_wire: Ring,
}

impl SharedMemory {
    /// Create (or truncate) `path` with rings of the given capacities,
    /// world read-write so a client under another account can map it,
    /// every page touched so none faults on the hot path. The state is
    /// `Starting` until [`set_state`](Self::set_state) says otherwise.
    pub fn create(
        path: &Path,
        to_wire_capacity: usize,
        from_wire_capacity: usize,
    ) -> Result<Self, String> {
        for (name, capacity) in [
            ("to-wire", to_wire_capacity),
            ("from-wire", from_wire_capacity),
        ] {
            if capacity == 0 || !capacity.is_power_of_two() {
                return Err(format!(
                    "the {name} ring capacity must be a power of two, not {capacity}"
                ));
            }
        }
        let len = HEADER_LEN + to_wire_capacity + from_wire_capacity;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| format!("cannot create {}: {e}", path.display()))?;
        file.set_len(len as u64)
            .map_err(|e| format!("cannot size {}: {e}", path.display()))?;
        // Explicitly, because the umask would otherwise strip the group
        // and world write bits the client needs.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))
            .map_err(|e| format!("cannot chmod {}: {e}", path.display()))?;

        // SAFETY: a fresh, sized, writable file descriptor; the mapping
        // is checked against MAP_FAILED before use and is `len` bytes.
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(format!(
                "cannot map {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            ));
        }
        let base = base.cast::<u8>();

        // SAFETY: `base` is a private, writable, `len`-byte mapping that
        // nothing else has seen yet.
        unsafe {
            ptr::write_bytes(base, 0, len);
            ptr::copy_nonoverlapping(MAGIC.as_ptr(), base.add(OFF_MAGIC), MAGIC.len());
            base.add(OFF_VERSION).cast::<u32>().write(VERSION);
            base.add(OFF_TO_WIRE_CAPACITY)
                .cast::<u64>()
                .write(to_wire_capacity as u64);
            base.add(OFF_FROM_WIRE_CAPACITY)
                .cast::<u64>()
                .write(from_wire_capacity as u64);
        }

        // SAFETY: every offset is inside the mapping, and the cursors are
        // 8-byte aligned at a page-aligned base.
        let (to_wire, from_wire) = unsafe {
            (
                Ring::new(
                    base.add(HEADER_LEN),
                    to_wire_capacity,
                    base.add(OFF_TO_WIRE_HEAD),
                    base.add(OFF_TO_WIRE_TAIL),
                ),
                Ring::new(
                    base.add(HEADER_LEN + to_wire_capacity),
                    from_wire_capacity,
                    base.add(OFF_FROM_WIRE_HEAD),
                    base.add(OFF_FROM_WIRE_TAIL),
                ),
            )
        };

        let link = Self {
            base,
            len,
            to_wire,
            from_wire,
        };
        // Already what the zero fill says; stated so the meaning of zero
        // is written down in one place.
        link.set_state(State::Starting);
        Ok(link)
    }

    pub fn set_state(&self, state: State) {
        self.state_word().store(state as u32, Ordering::Release);
    }

    /// Whether the client asked the proxy to go away.
    pub fn close_requested(&self) -> bool {
        // SAFETY: inside the mapping, 4-byte aligned.
        let flag = unsafe { &*self.base.add(OFF_CLOSE_REQUESTED).cast::<AtomicU32>() };
        flag.load(Ordering::Acquire) != 0
    }

    /// The to-wire ring: what the client sends, which the proxy consumes.
    pub fn outbound(&mut self) -> &mut Ring {
        &mut self.to_wire
    }

    /// The from-wire ring: what the server answers, which the proxy
    /// produces.
    pub fn inbound(&mut self) -> &mut Ring {
        &mut self.from_wire
    }

    fn state_word(&self) -> &AtomicU32 {
        // SAFETY: inside the mapping, 4-byte aligned.
        unsafe { &*self.base.add(OFF_STATE).cast::<AtomicU32>() }
    }

    #[cfg(test)]
    fn state(&self) -> u32 {
        self.state_word().load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn request_close(&self) {
        // SAFETY: inside the mapping, 4-byte aligned.
        let flag = unsafe { &*self.base.add(OFF_CLOSE_REQUESTED).cast::<AtomicU32>() };
        flag.store(1, Ordering::Release);
    }
}

impl Drop for SharedMemory {
    fn drop(&mut self) {
        // SAFETY: the mapping `create` made, unmapped exactly once.
        unsafe {
            libc::munmap(self.base.cast(), self.len);
        }
    }
}

/// One direction of the link. The same type serves both rings; which
/// cursor this side writes is decided by which methods it calls, and
/// each ring is used from exactly one side of the pair.
///
/// The data region is shared memory, so the slices handed out alias
/// another process's view of it. The protocol keeps that sound: a
/// producer only writes between the tail and the head-plus-capacity, a
/// consumer only reads between the head and the tail, and each cursor
/// moves only after its side is done with the bytes.
pub struct Ring {
    data: *mut u8,
    capacity: usize,
    mask: usize,
    head: *const AtomicU64,
    tail: *const AtomicU64,
}

impl Ring {
    /// # Safety
    /// `data` must point at `capacity` writable bytes and the cursors at
    /// aligned, mapped `u64`s, all outliving the ring.
    unsafe fn new(data: *mut u8, capacity: usize, head: *mut u8, tail: *mut u8) -> Self {
        Self {
            data,
            capacity,
            mask: capacity - 1,
            head: head.cast::<AtomicU64>(),
            tail: tail.cast::<AtomicU64>(),
        }
    }

    fn head(&self) -> &AtomicU64 {
        // SAFETY: `new`'s contract.
        unsafe { &*self.head }
    }

    fn tail(&self) -> &AtomicU64 {
        // SAFETY: `new`'s contract.
        unsafe { &*self.tail }
    }

    /// Consumer side: the bytes the producer has published and this side
    /// has not yet taken, as up to two slices because the ring wraps.
    /// Follow with [`consumed`](Self::consumed).
    pub fn readable(&self) -> (&[u8], &[u8]) {
        let head = self.head().load(Ordering::Relaxed);
        let tail = self.tail().load(Ordering::Acquire);
        let available = (tail - head) as usize;
        self.slices(head, available)
    }

    /// Consumer side: `n` bytes from the front of `readable` are done
    /// with.
    pub fn consumed(&self, n: usize) {
        let head = self.head().load(Ordering::Relaxed);
        self.head().store(head + n as u64, Ordering::Release);
    }

    /// Producer side: where the next bytes go, as up to two slices;
    /// empty when the consumer has not caught up. Follow with
    /// [`produced`](Self::produced).
    pub fn writable(&mut self) -> (&mut [u8], &mut [u8]) {
        let tail = self.tail().load(Ordering::Relaxed);
        let head = self.head().load(Ordering::Acquire);
        let space = self.capacity - (tail - head) as usize;
        let (first, second) = self.slices(tail, space);
        // SAFETY: the producer owns the region between the tail and the
        // consumer's head plus capacity; nobody else writes it until the
        // tail moves past it, and it does not overlap `readable`'s.
        unsafe {
            (
                std::slice::from_raw_parts_mut(first.as_ptr().cast_mut(), first.len()),
                std::slice::from_raw_parts_mut(second.as_ptr().cast_mut(), second.len()),
            )
        }
    }

    /// Producer side: `n` bytes were written at the front of `writable`.
    pub fn produced(&self, n: usize) {
        let tail = self.tail().load(Ordering::Relaxed);
        self.tail().store(tail + n as u64, Ordering::Release);
    }

    fn slices(&self, from: u64, len: usize) -> (&[u8], &[u8]) {
        let start = (from as usize) & self.mask;
        let first_len = len.min(self.capacity - start);
        // SAFETY: both ranges are inside the `capacity` bytes at `data`.
        unsafe {
            (
                std::slice::from_raw_parts(self.data.add(start), first_len),
                std::slice::from_raw_parts(self.data, len - first_len),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(dir: &tempfile::TempDir) -> SharedMemory {
        SharedMemory::create(&dir.path().join("link"), 16, 8).unwrap()
    }

    fn produce(ring: &mut Ring, bytes: &[u8]) -> usize {
        let (first, second) = ring.writable();
        let a = first.len().min(bytes.len());
        first[..a].copy_from_slice(&bytes[..a]);
        let b = second.len().min(bytes.len() - a);
        second[..b].copy_from_slice(&bytes[a..a + b]);
        ring.produced(a + b);
        a + b
    }

    fn consume(ring: &Ring) -> Vec<u8> {
        let (first, second) = ring.readable();
        let mut out = first.to_vec();
        out.extend_from_slice(second);
        ring.consumed(out.len());
        out
    }

    #[test]
    fn the_header_is_what_the_other_side_will_look_for() {
        let dir = tempfile::tempdir().unwrap();
        let link = link(&dir);
        let bytes = std::fs::read(dir.path().join("link")).unwrap();
        assert_eq!(bytes.len(), HEADER_LEN + 16 + 8);
        assert_eq!(&bytes[..8], b"MELINSHM");
        assert_eq!(u32::from_ne_bytes(bytes[8..12].try_into().unwrap()), 1);
        assert_eq!(u64::from_ne_bytes(bytes[16..24].try_into().unwrap()), 16);
        assert_eq!(u64::from_ne_bytes(bytes[24..32].try_into().unwrap()), 8);
        assert_eq!(link.state(), State::Starting as u32);
        assert!(!link.close_requested());

        link.set_state(State::Connected);
        let bytes = std::fs::read(dir.path().join("link")).unwrap();
        assert_eq!(u32::from_ne_bytes(bytes[32..36].try_into().unwrap()), 1);
    }

    #[test]
    fn bytes_come_out_in_order_across_the_wrap() {
        let dir = tempfile::tempdir().unwrap();
        let mut link = link(&dir);
        let ring = link.outbound();

        assert_eq!(produce(ring, b"0123456789"), 10);
        assert_eq!(consume(ring), b"0123456789");
        // Tail at 10 of 16: the next 10 bytes straddle the end.
        assert_eq!(produce(ring, b"abcdefghij"), 10);
        let (first, second) = ring.readable();
        assert_eq!(first, b"abcdef");
        assert_eq!(second, b"ghij");
        assert_eq!(consume(ring), b"abcdefghij");
    }

    #[test]
    fn a_full_ring_takes_nothing_until_the_consumer_moves() {
        let dir = tempfile::tempdir().unwrap();
        let mut link = link(&dir);
        let ring = link.inbound();

        assert_eq!(produce(ring, b"0123456789"), 8);
        assert_eq!(produce(ring, b"x"), 0);
        let (first, second) = ring.writable();
        assert!(first.is_empty() && second.is_empty());

        ring.consumed(3);
        assert_eq!(produce(ring, b"abcdef"), 3);
        let (first, second) = ring.readable();
        let mut all = first.to_vec();
        all.extend_from_slice(second);
        assert_eq!(all, b"34567abc");
    }

    #[test]
    fn the_cursors_live_where_the_layout_says() {
        let dir = tempfile::tempdir().unwrap();
        let mut link = link(&dir);
        produce(link.outbound(), b"abc");
        link.outbound().consumed(1);
        produce(link.inbound(), b"de");
        link.inbound().consumed(2);
        link.request_close();

        let bytes = std::fs::read(dir.path().join("link")).unwrap();
        let word = |off: usize| u64::from_ne_bytes(bytes[off..off + 8].try_into().unwrap());
        assert_eq!(word(OFF_TO_WIRE_TAIL), 3);
        assert_eq!(word(OFF_TO_WIRE_HEAD), 1);
        assert_eq!(word(OFF_FROM_WIRE_TAIL), 2);
        assert_eq!(word(OFF_FROM_WIRE_HEAD), 2);
        assert_eq!(&bytes[HEADER_LEN..HEADER_LEN + 3], b"abc");
        assert_eq!(&bytes[HEADER_LEN + 16..HEADER_LEN + 18], b"de");
        assert_eq!(
            u32::from_ne_bytes(
                bytes[OFF_CLOSE_REQUESTED..OFF_CLOSE_REQUESTED + 4]
                    .try_into()
                    .unwrap()
            ),
            1
        );
        assert!(link.close_requested());
    }

    #[test]
    fn capacities_must_be_powers_of_two() {
        let dir = tempfile::tempdir().unwrap();
        let err = SharedMemory::create(&dir.path().join("link"), 12, 8)
            .err()
            .expect("refused");
        assert!(err.contains("power of two"), "{err}");
    }
}

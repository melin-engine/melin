//! Ring-mapped provided-buffer ring (`IORING_REGISTER_PBUF_RING`).
//!
//! Replaces legacy `ProvideBuffers` SQEs for the reader's shared recv
//! pool: recycling a consumed buffer becomes one entry write plus one
//! release store of the ring tail — no SQE, no CQE, no syscall, and no
//! coupling between recycle volume and SQ depth. Requires kernel ≥ 5.19
//! with the `PBUF_RING` register opcode reachable — some virtualized
//! hosts filter it while reporting a modern uname, so the reader treats
//! registration failure as a runtime signal to fall back to legacy
//! `ProvideBuffers`.
//!
//! # Kernel contract (`io_uring_register_buf_ring(2)`)
//!
//! The ring is a page-aligned array of `io_uring_buf` entries (16 bytes:
//! addr / len / bid / resv) whose entry count is a power of two. The
//! `resv` field of entry 0 doubles as the shared tail. Userspace writes
//! an entry at `tail & (entries - 1)` and *then* publishes it with a
//! release store to the tail; the kernel pairs that with an acquire read
//! and never writes the tail itself (it tracks its consumption head
//! internally, invisible to userspace). The release/acquire pair is what
//! makes the entry's field writes visible before the slot becomes
//! claimable — the same protocol as liburing's
//! `io_uring_buf_ring_advance`.
//!
//! # Overrun discipline
//!
//! At most `entries` buffers may sit in the ring at once; the kernel
//! does not detect overrun (a stale entry would be claimed and DMA'd
//! into — silent corruption). This type never invents a buffer id: the
//! initial fill in [`BufRing::register`] adds each pool buffer exactly
//! once, and afterwards [`BufRing::push`] is only called with a bid the
//! kernel just handed back in a CQE. Every buffer is therefore always in
//! exactly one of three places — kernel-claimable (in the ring),
//! kernel-held (selected for an in-flight recv), or userspace-held
//! (between CQE and recycle) — and their sum is the pool size, which
//! equals `entries`. Overrun is structurally unreachable.

use std::alloc::Layout;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::types::BufRingEntry;

/// One registered provided-buffer ring plus the bookkeeping to recycle
/// buffers into it. Owns the ring memory; the buffer *pool* itself is
/// owned by the caller (the entries only point into it).
///
/// Lifetime rule: both the pool and this struct must outlive the
/// `IoUring` they are registered with — the kernel reads the ring and
/// writes the pool for as long as a multishot RECV is armed. Callers
/// enforce it by declaration order (see `reader_loop`): pool and
/// `BufRing` are declared *before* the `IoUring`, so on every exit path
/// — including panic unwind — the ring fd closes first.
pub(crate) struct BufRing {
    /// Page-aligned, zero-initialized ring memory (`entries` slots).
    /// Zeroing matters: the shared tail lives at entry 0 and must start
    /// at 0 to match `tail`'s initial value.
    ring: *mut BufRingEntry,
    /// Power-of-two slot count; the index mask is `entries - 1`.
    entries: u16,
    /// Local tail mirror. The kernel never writes the shared tail, so
    /// this copy is authoritative; it exists so pushes never read back
    /// from memory the kernel is concurrently reading. `u16` because
    /// that is the ABI's tail width — wrapping is deliberate and safe:
    /// indexing always masks, and `entries` divides 65536 (power of
    /// two), so the mapping stays aligned across wrap.
    tail: u16,
    /// Base of the caller-owned buffer pool the entries point into.
    pool_ptr: *mut u8,
    /// Size of each pool buffer.
    buf_size: usize,
}

impl BufRing {
    /// Allocate the ring memory (unregistered). `entries` must be a
    /// power of two and match the number of pool buffers at `pool_ptr`.
    pub(crate) fn new(entries: u16, pool_ptr: *mut u8, buf_size: usize) -> Self {
        assert!(
            entries.is_power_of_two(),
            "buf_ring entries must be a power of two (kernel ABI)"
        );
        let layout = Self::layout(entries);
        // SAFETY: the layout has non-zero size (entries ≥ 1 by the
        // power-of-two assert above).
        let ring = unsafe { std::alloc::alloc_zeroed(layout) } as *mut BufRingEntry;
        assert!(!ring.is_null(), "buf_ring allocation failed");
        Self {
            ring,
            entries,
            tail: 0,
            pool_ptr,
            buf_size,
        }
    }

    /// Ring memory layout: page-aligned as `IORING_REGISTER_PBUF_RING`
    /// requires.
    fn layout(entries: u16) -> Layout {
        Layout::from_size_align(entries as usize * size_of::<BufRingEntry>(), 4096)
            .expect("buf_ring layout")
    }

    /// Register the ring with the kernel under `bgid` and hand it every
    /// pool buffer. Call exactly once, after the `IoUring` exists and
    /// before the first buffer-selecting SQE is armed.
    pub(crate) fn register(&mut self, uring: &io_uring::IoUring, bgid: u16) -> std::io::Result<()> {
        // SAFETY: `self.ring` is a live, page-aligned, zeroed allocation
        // of exactly `entries` slots, and it outlives `uring` (the
        // declaration-order rule in the struct docs).
        unsafe {
            uring
                .submitter()
                .register_buf_ring_with_flags(self.ring as u64, self.entries, bgid, 0)
        }?;
        // Initial fill: every pool buffer, each exactly once — the base
        // case of the overrun-discipline invariant.
        for bid in 0..self.entries {
            self.push(bid);
        }
        Ok(())
    }

    /// Return buffer `bid` to the kernel.
    ///
    /// Published immediately (entry write, then release store of the
    /// advanced tail) rather than batched per CQE drain: on x86 a
    /// release store compiles to an ordinary store, so batching saves
    /// nothing measurable, while per-push publication minimizes the
    /// window in which an armed multishot RECV can find the pool empty
    /// and terminate with `ENOBUFS`.
    #[inline]
    pub(crate) fn push(&mut self, bid: u16) {
        // Hard assert: an out-of-range bid becomes an out-of-pool addr
        // in the entry — a future recv would DMA into arbitrary heap.
        // This path runs per CQE, off the per-frame budget.
        assert!(bid < self.entries, "bid {bid} out of range");
        let idx = (self.tail & (self.entries - 1)) as usize;
        // SAFETY: `idx < entries`, so the slot is inside the allocation,
        // and the kernel does not own it: the kernel only reads slots in
        // `[head, tail)` and this slot sits at `tail`.
        let entry = unsafe { &mut *self.ring.add(idx) };
        entry.set_addr(self.pool_ptr as u64 + bid as u64 * self.buf_size as u64);
        entry.set_len(self.buf_size as u32);
        entry.set_bid(bid);
        self.tail = self.tail.wrapping_add(1);
        // SAFETY: entry 0's resv field is the shared tail (ABI contract
        // encoded by `BufRingEntry::tail`); the pointer is in-bounds and
        // 2-aligned (byte offset 14 of a 16-byte-aligned allocation).
        let tail_ptr = unsafe { BufRingEntry::tail(self.ring) } as *mut u16;
        // Release: pairs with the kernel's acquire read of the tail, so
        // the entry field writes above are visible before the slot is
        // claimable. `AtomicU16::from_ptr` rather than a volatile write
        // because the ordering guarantee is the point, not just
        // non-elision.
        // SAFETY: valid for the allocation's lifetime, aligned, and no
        // other *userspace* accessor exists (single-threaded owner).
        unsafe { AtomicU16::from_ptr(tail_ptr) }.store(self.tail, Ordering::Release);
    }

    /// Read the shared (kernel-visible) tail back out of ring memory.
    #[cfg(test)]
    fn shared_tail(&self) -> u16 {
        let tail_ptr = unsafe { BufRingEntry::tail(self.ring) } as *mut u16;
        unsafe { AtomicU16::from_ptr(tail_ptr) }.load(Ordering::Acquire)
    }

    /// Read entry `idx` back out of ring memory (addr, len, bid).
    #[cfg(test)]
    fn entry_at(&self, idx: usize) -> (u64, u32, u16) {
        assert!(idx < self.entries as usize);
        let entry = unsafe { &*self.ring.add(idx) };
        (entry.addr(), entry.len(), entry.bid())
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this exact layout. The
        // `IoUring` this was registered with is already gone (the
        // declaration-order rule), and the reader's teardown quiesce
        // (see `reader_loop`, and `crate::uring_teardown` for the
        // shared policy) has *proven* no armed operation can still
        // select from this ring before letting it drop — when that
        // proof fails, the whole `BufRing` is `mem::forget`-leaked and
        // this dealloc never runs. Panic unwind skips the quiesce and
        // accepts the tiny process-is-dying exit-work window;
        // declaration order still closes the ring fd before this free.
        unsafe { std::alloc::dealloc(self.ring as *mut u8, Self::layout(self.entries)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::{IoUring, opcode, types};
    use std::io::Write as _;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::UnixStream;

    /// CQE flag: buffer ID is valid in the upper 16 bits of flags.
    const F_BUFFER: u32 = 1 << 0;
    /// CQE flag: more completions coming from this multishot op.
    const F_MORE: u32 = 1 << 1;
    const BUFFER_SHIFT: u32 = 16;

    /// A tiny pool + ring pair for tests. Keeps the pool allocation
    /// alive alongside the ring (same lifetime rule as production).
    struct Fixture {
        pool: Box<[u8]>,
        ring: BufRing,
        entries: u16,
        buf_size: usize,
    }

    fn fixture(entries: u16, buf_size: usize) -> Fixture {
        let mut pool = vec![0u8; entries as usize * buf_size].into_boxed_slice();
        let ring = BufRing::new(entries, pool.as_mut_ptr(), buf_size);
        Fixture {
            pool,
            ring,
            entries,
            buf_size,
        }
    }

    // ── Unit: layout and index math, no kernel involved ──

    #[test]
    fn push_writes_entry_fields_and_advances_shared_tail() {
        let mut f = fixture(8, 64);
        assert_eq!(f.ring.shared_tail(), 0, "zeroed allocation ⇒ tail 0");

        f.ring.push(3);
        let (addr, len, bid) = f.ring.entry_at(0);
        assert_eq!(
            addr,
            f.pool.as_ptr() as u64 + 3 * 64,
            "addr = pool + bid*size"
        );
        assert_eq!(len, 64);
        assert_eq!(bid, 3);
        assert_eq!(f.ring.shared_tail(), 1, "publish advances the shared tail");

        f.ring.push(5);
        let (addr, _, bid) = f.ring.entry_at(1);
        assert_eq!(addr, f.pool.as_ptr() as u64 + 5 * 64);
        assert_eq!(bid, 5);
        assert_eq!(f.ring.shared_tail(), 2);
    }

    /// The tail is a wrapping u16 while slot indexing masks by
    /// `entries - 1`. Walk far past 65536 pushes and verify the mapping
    /// never drifts — the failure mode would be an entry written to the
    /// wrong slot after wrap, i.e. the kernel claiming a stale buffer.
    #[test]
    fn tail_wraps_across_u16_without_index_drift() {
        let entries: u16 = 8;
        let mut f = fixture(entries, 32);
        // Not registered with any kernel ring: the memory is plain
        // userspace memory here, so "overrunning" is fine — this test
        // only checks our own arithmetic.
        for i in 0u32..70_000 {
            let bid = (i % entries as u32) as u16;
            f.ring.push(bid);
            let expected_idx = (i % entries as u32) as usize; // tail starts at 0
            let (_, _, got_bid) = f.ring.entry_at(expected_idx);
            assert_eq!(got_bid, bid, "push #{i} landed in the wrong slot");
            assert_eq!(
                f.ring.shared_tail(),
                (i + 1) as u16,
                "shared tail must track pushes mod 2^16"
            );
        }
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn non_power_of_two_entry_count_is_rejected() {
        let mut pool = vec![0u8; 6 * 32];
        let _ = BufRing::new(6, pool.as_mut_ptr(), 32);
    }

    /// An out-of-pool bid must never be written into a ring entry — it
    /// would point a future recv's DMA at arbitrary heap. Hard assert,
    /// pinned here so it can't be quietly downgraded to debug-only.
    #[test]
    #[should_panic(expected = "out of range")]
    fn out_of_range_bid_is_rejected() {
        let mut f = fixture(8, 32);
        f.ring.push(8); // == entries: one past the pool
    }

    // ── Kernel integration: real io_uring, real sockets ──
    //
    // These prove our use of the ABI against the actual kernel — the
    // memory-ordering and layout contract cannot be checked by any
    // userspace-only harness.
    //
    // Runtime-skipped (with a stderr note) where `PBUF_RING`
    // registration is rejected: the opcode needs kernel ≥ 5.19, and
    // some virtualized hosts filter newer io_uring register opcodes
    // while reporting a modern uname — the same environments where the
    // reader falls back to legacy ProvideBuffers. A runtime skip, not
    // `#[ignore]`: the tests run for real wherever the production
    // buf_ring path would actually engage.

    /// Probe whether this kernel/hypervisor accepts PBUF_RING.
    fn kernel_supports_buf_ring() -> bool {
        let mut pool = vec![0u8; 2 * 16].into_boxed_slice();
        let mut probe = BufRing::new(2, pool.as_mut_ptr(), 16);
        let uring = IoUring::new(2).expect("io_uring");
        probe.register(&uring, 42).is_ok()
        // `uring` drops before `probe` here (reverse declaration order),
        // upholding BufRing's lifetime rule.
    }

    macro_rules! skip_unless_buf_ring {
        () => {
            if !kernel_supports_buf_ring() {
                eprintln!("skipping: this kernel rejects IORING_REGISTER_PBUF_RING");
                return;
            }
        };
    }

    /// Arm a multishot RECV with buffer selection on `fd`.
    fn arm_recv(uring: &mut IoUring, fd: i32, bgid: u16, user_data: u64) {
        let sqe = opcode::RecvMulti::new(types::Fd(fd), bgid)
            .build()
            .user_data(user_data);
        unsafe {
            uring.submission().push(&sqe).expect("SQ full");
        }
    }

    /// Drain all currently available CQEs as (result, flags).
    fn drain(uring: &mut IoUring) -> Vec<(i32, u32)> {
        uring
            .completion()
            .map(|cqe| (cqe.result(), cqe.flags()))
            .collect()
    }

    #[test]
    fn kernel_delivers_data_through_buf_ring_buffers() {
        skip_unless_buf_ring!();
        let mut f = fixture(8, 32);
        let mut uring = IoUring::new(8).expect("io_uring");
        f.ring.register(&uring, 7).expect("register_buf_ring");

        let (mut tx, rx) = UnixStream::pair().expect("socketpair");
        arm_recv(&mut uring, rx.as_raw_fd(), 7, 1);
        uring.submit().expect("submit recv");

        let payload = *b"melin-buf-ring-abi-check-32bytes";
        assert_eq!(payload.len(), 32);
        tx.write_all(&payload).expect("peer write");

        uring.submit_and_wait(1).expect("wait for recv CQE");
        let cqes = drain(&mut uring);
        assert_eq!(cqes.len(), 1, "one recv completion");
        let (result, flags) = cqes[0];
        assert_eq!(result, 32, "full payload received");
        assert!(
            flags & F_BUFFER != 0,
            "kernel must report a selected buffer"
        );
        let bid = (flags >> BUFFER_SHIFT) as u16;
        assert!(bid < f.entries, "bid within the pool");
        let start = bid as usize * f.buf_size;
        assert_eq!(
            &f.pool[start..start + 32],
            &payload,
            "bytes landed in the buffer the CQE names"
        );
    }

    /// Full recycle soak crossing the u16 tail wrap *under the kernel*:
    /// 8 buffers × >8192 cycles ⇒ >65536 pushes. Every chunk is
    /// sequence-stamped and verified, so a post-wrap slot-mapping bug —
    /// the kernel claiming a stale entry and writing into the wrong
    /// buffer — surfaces as a payload mismatch, not silence.
    #[test]
    fn recycled_buffers_survive_tail_wrap_under_kernel() {
        skip_unless_buf_ring!();
        const ENTRIES: u16 = 8;
        const BUF: usize = 32;
        // 8500 cycles × 8 pushes = 68 000 pushes > 65 536.
        const CYCLES: u32 = 8_500;

        let mut f = fixture(ENTRIES, BUF);
        let mut uring = IoUring::new(32).expect("io_uring");
        f.ring.register(&uring, 3).expect("register_buf_ring");

        let (mut tx, rx) = UnixStream::pair().expect("socketpair");
        arm_recv(&mut uring, rx.as_raw_fd(), 3, 1);
        uring.submit().expect("submit recv");

        let mut chunk = [0u8; BUF];
        let mut next_seq: u32 = 0; // stamped into each chunk, verified on receipt
        let mut armed = true;
        for cycle in 0..CYCLES {
            // One pool's worth of data per cycle; SOCK_STREAM keeps the
            // bytes ordered and the 32-byte buffers slice it back into
            // exactly 8 completions (possibly across several drains).
            for i in 0..ENTRIES as u32 {
                let seq = cycle * ENTRIES as u32 + i;
                chunk[..4].copy_from_slice(&seq.to_le_bytes());
                chunk[4..].fill((seq % 251) as u8);
                tx.write_all(&chunk).expect("peer write");
            }

            let mut received = 0u32;
            while received < ENTRIES as u32 {
                if !armed {
                    arm_recv(&mut uring, rx.as_raw_fd(), 3, 1);
                    armed = true;
                }
                uring.submit_and_wait(1).expect("wait");
                for (result, flags) in drain(&mut uring) {
                    if result == -libc::ENOBUFS {
                        // Transient exhaustion mid-cycle: the recycles
                        // below refill the ring; re-arm and continue.
                        armed = false;
                        continue;
                    }
                    assert!(result > 0, "recv error {result} in cycle {cycle}");
                    assert_eq!(result as usize, BUF, "stream slices into whole buffers");
                    let bid = (flags >> BUFFER_SHIFT) as u16;
                    let start = bid as usize * BUF;
                    let got = &f.pool[start..start + BUF];
                    let seq = u32::from_le_bytes(got[..4].try_into().unwrap());
                    assert_eq!(seq, next_seq, "chunks arrive in stamp order");
                    assert!(
                        got[4..].iter().all(|&b| b == (seq % 251) as u8),
                        "payload intact for seq {seq} in bid {bid}"
                    );
                    next_seq += 1;
                    received += 1;
                    f.ring.push(bid);
                    if flags & F_MORE == 0 {
                        armed = false;
                    }
                }
            }
        }
        assert_eq!(next_seq, CYCLES * ENTRIES as u32, "every chunk verified");
    }

    /// Pool exhaustion must terminate the multishot with `ENOBUFS` and
    /// a re-arm after recycling must recover every byte. This is the
    /// exact sequence the reader loop depends on under burst — and the
    /// reason its CQE handler needs an explicit ENOBUFS branch (a bare
    /// `result <= 0 ⇒ disconnect` drops innocent clients).
    #[test]
    fn exhaustion_terminates_multishot_and_rearm_recovers() {
        skip_unless_buf_ring!();
        const ENTRIES: u16 = 2;
        const BUF: usize = 32;
        const CHUNKS: usize = 6; // 3× the pool: guarantees exhaustion

        let mut f = fixture(ENTRIES, BUF);
        let mut uring = IoUring::new(8).expect("io_uring");
        f.ring.register(&uring, 9).expect("register_buf_ring");

        let (mut tx, rx) = UnixStream::pair().expect("socketpair");
        arm_recv(&mut uring, rx.as_raw_fd(), 9, 1);
        uring.submit().expect("submit recv");

        for i in 0..CHUNKS {
            let chunk = [i as u8; BUF];
            tx.write_all(&chunk).expect("peer write");
        }

        let mut received = 0usize;
        let mut exhaustions = 0usize;
        let mut armed = true;
        while received < CHUNKS {
            if !armed {
                arm_recv(&mut uring, rx.as_raw_fd(), 9, 1);
                armed = true;
            }
            uring.submit_and_wait(1).expect("wait");
            for (result, flags) in drain(&mut uring) {
                if result == -libc::ENOBUFS {
                    exhaustions += 1;
                    armed = false;
                    continue;
                }
                assert!(result > 0, "unexpected recv error {result}");
                let bid = (flags >> BUFFER_SHIFT) as u16;
                let start = bid as usize * BUF;
                assert!(
                    f.pool[start..start + result as usize]
                        .iter()
                        .all(|&b| b == received as u8),
                    "chunk {received} intact after re-arm"
                );
                received += 1;
                f.ring.push(bid);
                if flags & F_MORE == 0 {
                    armed = false;
                }
            }
        }
        assert!(
            exhaustions > 0,
            "6 chunks against a 2-buffer pool must exhaust at least once"
        );
        assert_eq!(received, CHUNKS, "no bytes lost across exhaustion/re-arm");
    }
}

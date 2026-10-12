//! The proxy's DPDK path, end to end: the `shm-proxy` binary, run as the
//! Aeron harness runs it, with `--transport dpdk` on the runner's DPDK
//! client pair, dialing an echo node on DPDK, and a client on the far side
//! of its shared-memory rings speaking the protocol as the harness's Java
//! rig does.
//!
//! Built only with the crate's `dpdk` feature (`required-features`), and
//! run under `scripts/dpdk/netns-runner.sh`, which builds the network and
//! the client pair (see `docs/internal/dpdk-testing.md`).
//!
//! The binary owns the client pair's device in a process of its own, with
//! an EAL of its own, so this test never opens the launcher's client NIC
//! (`melin_test_node::open_dpdk_client`): it hands the binary the pair's
//! EAL arguments, address and the node's MAC instead.
//!
//! The ring client here speaks the protocol through `melin-client`'s I/O-free
//! pieces (the handshake, request framing, frame splitting and the reply's
//! classification), as the Java rig speaks it through its own. That is a
//! dev-dependency of this crate only: the `shm-proxy` binary depends on no
//! client crate, and must not (it knows the protocol's tags and nothing
//! more).

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use echo_server::{Echo, KIND_RESP_ECHO, MAX_PAYLOAD, RequestDecoder, ResponseEncoder};
use melin_client::framing::split_frame;
use melin_client::{
    Connection, Handshake, Reply, RequestFrameError, SigningKey, Step, classify, frame_request, key,
};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;

// ---------------------------------------------------------------------------
// The link file, as `src/shm.rs` documents it
// ---------------------------------------------------------------------------
//
// Spelled out here rather than taken from the crate: the layout is a
// contract with a client in another language, so the test reads it as
// such a client would, from the documented offsets.

const MAGIC: &[u8; 8] = b"MELINSHM";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 4096;
const OFF_VERSION: usize = 8;
const OFF_TO_WIRE_CAPACITY: usize = 16;
const OFF_FROM_WIRE_CAPACITY: usize = 24;
const OFF_STATE: usize = 32;
const OFF_CLOSE_REQUESTED: usize = 40;
const OFF_TO_WIRE_TAIL: usize = 64;
const OFF_TO_WIRE_HEAD: usize = 128;
const OFF_FROM_WIRE_TAIL: usize = 192;
const OFF_FROM_WIRE_HEAD: usize = 256;

const STATE_STARTING: u32 = 0;
const STATE_CONNECTED: u32 = 1;
const STATE_CLOSED: u32 = 2;
const STATE_FAILED: u32 = 3;

/// The client's side of the link: the file mapped, the to-wire ring
/// produced into and the from-wire ring consumed from, each cursor of its
/// own advanced with release after the bytes, each of the proxy's read
/// with acquire before them.
struct Link {
    base: *mut u8,
    len: usize,
    to_wire_capacity: u64,
    from_wire_capacity: u64,
    /// Bytes taken from the from-wire ring and not yet split into a frame.
    /// A `Vec` drained from the front: a frame or two at a time, so the
    /// shifting costs nothing a test notices, and `split_frame` wants the
    /// bytes contiguous, which a ring's wrap would not keep them.
    pending: Vec<u8>,
}

impl Link {
    /// Map the link file at `path`, which the proxy has created and
    /// written the header of.
    fn open(path: &Path) -> Link {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap_or_else(|e| panic!("open the link at {}: {e}", path.display()));
        let len = usize::try_from(file.metadata().expect("the link's size").len())
            .expect("a link that fits the address space");
        assert!(len > HEADER_LEN, "a link of {len} bytes has no rings");
        // SAFETY: a sized, writable file descriptor; the mapping is checked
        // against MAP_FAILED before use and is `len` bytes.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        assert_ne!(
            base,
            libc::MAP_FAILED,
            "map the link: {}",
            std::io::Error::last_os_error()
        );
        let mut link = Link {
            base: base.cast(),
            len,
            to_wire_capacity: 0,
            from_wire_capacity: 0,
            pending: Vec::new(),
        };
        // SAFETY: inside the mapping; the proxy wrote the header before it
        // said where the link is, and does not write it again.
        let magic = unsafe { std::slice::from_raw_parts(link.base, MAGIC.len()) };
        assert_eq!(magic, MAGIC, "the link's magic");
        // SAFETY: inside the mapping, aligned (a page-aligned base).
        let version = unsafe { link.base.add(OFF_VERSION).cast::<u32>().read() };
        assert_eq!(version, VERSION, "the link's layout version");
        // SAFETY: as above.
        unsafe {
            link.to_wire_capacity = link.base.add(OFF_TO_WIRE_CAPACITY).cast::<u64>().read();
            link.from_wire_capacity = link.base.add(OFF_FROM_WIRE_CAPACITY).cast::<u64>().read();
        }
        for capacity in [link.to_wire_capacity, link.from_wire_capacity] {
            assert!(capacity.is_power_of_two(), "a ring of {capacity} bytes");
        }
        assert_eq!(
            len as u64,
            HEADER_LEN as u64 + link.to_wire_capacity + link.from_wire_capacity,
            "the header and the two rings, end to end"
        );
        link
    }

    fn word32(&self, offset: usize) -> &AtomicU32 {
        // SAFETY: the header's offsets are inside the mapping and 4-byte
        // aligned; the mapping outlives `self`'s borrow.
        unsafe { &*self.base.add(offset).cast::<AtomicU32>() }
    }

    fn word64(&self, offset: usize) -> &AtomicU64 {
        // SAFETY: as `word32`, 8-byte aligned.
        unsafe { &*self.base.add(offset).cast::<AtomicU64>() }
    }

    /// The proxy's state word.
    fn state(&self) -> u32 {
        self.word32(OFF_STATE).load(Ordering::Acquire)
    }

    /// Ask the proxy to go away.
    fn request_close(&self) {
        self.word32(OFF_CLOSE_REQUESTED).store(1, Ordering::Release);
    }

    /// Produce as much of `bytes` as the to-wire ring has room for, and
    /// return how much that was.
    fn write(&self, bytes: &[u8]) -> usize {
        // This side's own cursor, which only it writes.
        let tail = self.word64(OFF_TO_WIRE_TAIL).load(Ordering::Relaxed);
        // The proxy's: everything before it has been taken off the ring.
        let head = self.word64(OFF_TO_WIRE_HEAD).load(Ordering::Acquire);
        let room = self.to_wire_capacity - (tail - head);
        let n = bytes.len().min(room as usize);
        let data = HEADER_LEN;
        let start = (tail & (self.to_wire_capacity - 1)) as usize;
        let first = n.min(self.to_wire_capacity as usize - start);
        // SAFETY: both pieces are inside the to-wire ring's data, in the
        // region between the tail and the proxy's head plus the capacity,
        // which the proxy does not read until the tail moves past it.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.base.add(data + start), first);
            std::ptr::copy_nonoverlapping(bytes[first..].as_ptr(), self.base.add(data), n - first);
        }
        self.word64(OFF_TO_WIRE_TAIL)
            .store(tail + n as u64, Ordering::Release);
        n
    }

    /// Take whatever the proxy has produced into the from-wire ring.
    fn take(&mut self) {
        let head = self.word64(OFF_FROM_WIRE_HEAD).load(Ordering::Relaxed);
        let tail = self.word64(OFF_FROM_WIRE_TAIL).load(Ordering::Acquire);
        let n = (tail - head) as usize;
        if n == 0 {
            return;
        }
        let data = HEADER_LEN + self.to_wire_capacity as usize;
        let start = (head & (self.from_wire_capacity - 1)) as usize;
        let first = n.min(self.from_wire_capacity as usize - start);
        // SAFETY: both pieces are inside the from-wire ring's data, between
        // this side's head and the proxy's tail, which the proxy does not
        // write again until the head moves past them.
        unsafe {
            self.pending.extend_from_slice(std::slice::from_raw_parts(
                self.base.add(data + start),
                first,
            ));
            self.pending
                .extend_from_slice(std::slice::from_raw_parts(self.base.add(data), n - first));
        }
        self.word64(OFF_FROM_WIRE_HEAD)
            .store(head + n as u64, Ordering::Release);
    }

    /// The next whole frame's payload from the node, if one has arrived.
    fn read_frame(&mut self) -> Option<Vec<u8>> {
        self.take();
        let frame = split_frame(&self.pending).expect("a frame within the protocol's limit")?;
        let payload = frame.payload.to_vec();
        let consumed = frame.consumed();
        self.pending.drain(..consumed);
        Some(payload)
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        // SAFETY: the mapping `open` made, unmapped once.
        unsafe {
            libc::munmap(self.base.cast(), self.len);
        }
    }
}

// ---------------------------------------------------------------------------
// The proxy, as a child process
// ---------------------------------------------------------------------------

/// The proxy's process, with its stderr and stdout collected as they come
/// by a thread each, so neither pipe fills and either is at hand for a
/// failure's message.
struct Proxy {
    child: Child,
    /// What the proxy wrote to stderr, kept apart from stdout: the lines
    /// the harness reads (`link at`, `connected to`) are a contract on
    /// stderr, and an assertion on a merged buffer would still pass if one
    /// moved to stdout.
    stderr: Arc<Mutex<String>>,
    /// What it wrote to stdout (the EAL's own logging among it), only for
    /// a failure's message.
    stdout: Arc<Mutex<String>>,
    /// The two collecting threads, joined once the proxy has exited, when
    /// its pipes have closed and every line is in its buffer.
    collectors: Vec<JoinHandle<()>>,
}

impl Proxy {
    fn spawn(args: &[String]) -> Proxy {
        let mut child = Command::new(env!("CARGO_BIN_EXE_shm-proxy"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn shm-proxy");
        let stderr = Arc::new(Mutex::new(String::new()));
        let stdout = Arc::new(Mutex::new(String::new()));
        let collectors = vec![
            collect(child.stderr.take().expect("piped"), Arc::clone(&stderr)),
            collect(child.stdout.take().expect("piped"), Arc::clone(&stdout)),
        ];
        Proxy {
            child,
            stderr,
            stdout,
            collectors,
        }
    }

    /// Once the proxy has exited: every line it wrote to stderr, and a
    /// report of both pipes for a failure's message.
    fn finish(&mut self) -> (String, String) {
        for collector in self.collectors.drain(..) {
            collector.join().expect("a collector does not panic");
        }
        (self.stderr(), self.report())
    }

    /// What the proxy has written to stderr so far.
    fn stderr(&self) -> String {
        self.stderr
            .lock()
            .expect("the collectors do not panic")
            .clone()
    }

    /// Both pipes so far, each under its own heading.
    fn report(&self) -> String {
        let stdout = self
            .stdout
            .lock()
            .expect("the collectors do not panic")
            .clone();
        format!(
            "--- shm-proxy stderr ---\n{}--- shm-proxy stdout ---\n{stdout}",
            self.stderr()
        )
    }

    /// Whether the proxy has exited, and how.
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().expect("the proxy's status")
    }

    /// Fail the test with `what`, and everything the proxy said.
    fn fail(&mut self, what: &str) -> ! {
        // Best effort: it may have exited already, which is no error here.
        let _ = self.child.kill();
        // Reaped so that its pipes close and what it printed last is
        // collected; the status is not what the test reports.
        let _ = self.child.wait();
        let (_, report) = self.finish();
        panic!("{what}\n{report}");
    }
}

/// Reaps a proxy still running when the test unwinds from a panic that did
/// not go through `fail` (a reply's `assert_eq!`, a ring's assert): the
/// runner has no PID namespace to take it down, and the proxy's bridge loop
/// spins until close-requested or its peer's FIN, neither of which a dead
/// test sends, so an orphan would hold a core indefinitely.
impl Drop for Proxy {
    fn drop(&mut self) {
        // Best effort: a status that cannot be read, or a kill racing the
        // proxy's own exit, leaves nothing more a destructor can do.
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Copy `from`'s lines into `into` until it closes.
fn collect(from: impl Read + Send + 'static, into: Arc<Mutex<String>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        for line in BufReader::new(from).lines() {
            // A pipe that cannot be read (output that is not UTF-8) ends
            // the collection: a line the test waits for or asserts is then
            // missing, and the test fails, printing what was collected.
            let Ok(line) = line else { return };
            let mut all = into.lock().expect("one writer per pipe, neither panics");
            all.push_str(&line);
            all.push('\n');
        }
    })
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

fn writer_key() -> SigningKey {
    SigningKey::from_bytes(&[0xAA; 32])
}

/// An echo node on DPDK, its journal and `authorized_keys` in `dir`.
fn start_node(dir: &Path) -> melin_test_node::Node {
    let authorized_keys = dir.join("authorized_keys");
    std::fs::write(
        &authorized_keys,
        key::authorized_keys_line("writer", &writer_key().verifying_key(), "shm-proxy-test"),
    )
    .expect("write authorized_keys");
    let config = ServerConfig {
        journal: dir.join("echo.journal"),
        authorized_keys,
        standalone: true,
        ack_policy: AckPolicy::Disk,
        // Unpinned, as every test node: the proxy's loop spins on a core
        // of its own beside the node's threads.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    };
    melin_test_node::start::<Echo>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// How long the proxy may take to bring its NIC up and dial: its own EAL
/// initialisation and port start, then its dial, which gives up after its
/// own few seconds.
const PROXY_CONNECT_LIMIT: Duration = Duration::from_secs(30);

/// How long a reply, or the proxy's exit once asked, may take.
const REPLY_LIMIT: Duration = Duration::from_secs(10);

/// The proxy's rings, in KiB: small enough that the requests and replies
/// below wrap both rings several times.
const RING_KIB: u64 = 4;

#[test]
fn the_proxy_bridges_a_client_to_a_dpdk_node_over_dpdk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_node(dir.path());
    // The launcher returns before the node serves, and the proxy dials
    // once: a kernel client, which retries until a deadline, waits for it.
    drop(
        Connection::connect_by(
            node.addr(),
            &writer_key(),
            Instant::now() + melin_test_node::STARTUP_LIMIT,
        )
        .expect("a serving node"),
    );

    let SocketAddr::V4(server) = node.addr() else {
        panic!("a DPDK node is on IPv4: {}", node.addr());
    };
    let seat = melin_test_node::dpdk_client();
    let node_mac = mac_string(melin_test_node::mac_of(node.addr()));
    let link_path = dir.path().join("link.shm");
    let args: Vec<String> = [
        "--transport",
        "dpdk",
        "--server",
        &server.to_string(),
        "--shm",
        link_path.to_str().expect("a UTF-8 temp path"),
        "--to-wire-kib",
        &RING_KIB.to_string(),
        "--from-wire-kib",
        &RING_KIB.to_string(),
        "--dpdk-ip",
        &seat.ip.to_string(),
        "--dpdk-prefix-len",
        &seat.prefix_len.to_string(),
        "--dpdk-peer-mac",
        &node_mac,
        // In full, with `=`: the proxy's default names a hugepage mount the
        // runner has none of, and the value starts with a dash.
        &format!(
            "--dpdk-eal-args={}",
            melin_test_node::dpdk_client_process_eal_args("shm-proxy-test").join(" ")
        ),
    ]
    .map(String::from)
    .to_vec();
    let mut proxy = Proxy::spawn(&args);

    // The link exists, header written, once the proxy says where it is.
    let started = Instant::now();
    while !proxy.stderr().contains("link at ") {
        if let Some(status) = proxy.exited() {
            proxy.fail(&format!(
                "the proxy exited ({status}) before creating the link"
            ));
        }
        if started.elapsed() > PROXY_CONNECT_LIMIT {
            proxy.fail("the proxy did not create the link");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut link = Link::open(&link_path);
    assert_eq!(link.to_wire_capacity, RING_KIB * 1024);
    assert_eq!(link.from_wire_capacity, RING_KIB * 1024);

    loop {
        match link.state() {
            STATE_CONNECTED => break,
            STATE_STARTING => {}
            STATE_FAILED => proxy.fail("the proxy could not connect"),
            other => proxy.fail(&format!("the link's state is {other} before connecting")),
        }
        if let Some(status) = proxy.exited() {
            proxy.fail(&format!("the proxy exited ({status}) before connecting"));
        }
        if started.elapsed() > PROXY_CONNECT_LIMIT {
            proxy.fail("the proxy did not connect");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // The handshake, over the rings: the node's challenge in, the signed
    // answer out, the node's verdict in.
    let mut handshake = Handshake::new(&writer_key());
    let challenge = next_frame(&mut link, &mut proxy);
    match handshake.feed(&challenge) {
        Ok(Step::Send(response)) => write_all(&link, &mut proxy, response),
        other => proxy.fail(&format!("the challenge was answered with {other:?}")),
    }
    let verdict = next_frame(&mut link, &mut proxy);
    match handshake.feed(&verdict) {
        Ok(Step::Ready) => {}
        other => proxy.fail(&format!("the node's verdict was {other:?}")),
    }

    // Echoes of every size either side of the length's `u8` boundary, up
    // to the cap, several in flight at a time and enough of them that both
    // rings wrap. A reply is matched to its request by order, as the node
    // answers them, and the non-empty payloads all differ, so a reply out
    // of place cannot pass for the right one.
    let sizes = [0, 1, 7, 255, 256, MAX_PAYLOAD];
    let payloads: Vec<Vec<u8>> = (0..96)
        .map(|i: usize| {
            let len = sizes[i % sizes.len()];
            (0..len)
                .map(|j| (i as u8).wrapping_mul(31).wrapping_add(j as u8))
                .collect()
        })
        .collect();
    let mut frame = vec![0u8; melin_client::REQUEST_HEADER_LEN + MAX_PAYLOAD];
    for batch in payloads.chunks(sizes.len()) {
        for payload in batch {
            let n = frame_request(&mut frame, |body: &mut [u8]| {
                body[..payload.len()].copy_from_slice(payload);
                Ok::<_, RequestFrameError>(payload.len())
            })
            .expect("a request within the cap");
            write_all(&link, &mut proxy, &frame[..n]);
        }
        for payload in batch {
            let reply = next_reply(&mut link, &mut proxy);
            assert_eq!(
                reply,
                [[KIND_RESP_ECHO].as_slice(), payload.as_slice()].concat(),
                "the echo of {} bytes",
                payload.len()
            );
        }
    }
    let wrapped = payloads.iter().map(|p| p.len() as u64).sum::<u64>();
    assert!(
        wrapped > 2 * RING_KIB * 1024,
        "the echoes carried {wrapped} bytes, too few to wrap the rings"
    );

    // Asked to close, the proxy exits cleanly and says so in the state.
    link.request_close();
    let asked = Instant::now();
    let status = loop {
        if let Some(status) = proxy.exited() {
            break status;
        }
        if asked.elapsed() > REPLY_LIMIT {
            proxy.fail("the proxy did not exit once asked to close");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let (stderr, report) = proxy.finish();
    assert!(status.success(), "{status}\n{report}");
    assert_eq!(link.state(), STATE_CLOSED, "{report}");
    // The line the harness's runner waits for on stderr, verbatim, naming
    // the DPDK transport: not the kernel's, the binary's default.
    let connected = format!("connected to {} over DPDK", node.addr());
    assert!(
        stderr.lines().any(|line| line == connected),
        "no `{connected}` line on stderr:\n{report}"
    );
    // And the NIC it dialed from is the client pair's device, the one port
    // of its EAL, with the pair's address and MAC.
    let nic = format!(
        "DPDK: port 0 as {} ({}), server {server} ",
        seat.ip,
        mac_string(seat.mac)
    );
    assert!(
        stderr.lines().any(|line| line.starts_with(&nic)),
        "no line starting `{nic}` on stderr:\n{report}"
    );

    drop(link);
    node.stop();
}

/// `mac` as the `--dpdk-*-mac` flags take it and the proxy prints it.
fn mac_string(mac: [u8; 6]) -> String {
    let [a, b, c, d, e, f] = mac;
    format!("{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}")
}

/// Write all of `bytes` into the to-wire ring, waiting for room.
fn write_all(link: &Link, proxy: &mut Proxy, bytes: &[u8]) {
    let started = Instant::now();
    let mut written = 0;
    while written < bytes.len() {
        written += link.write(&bytes[written..]);
        if started.elapsed() > REPLY_LIMIT {
            proxy.fail("the proxy did not take the request off its ring");
        }
        std::hint::spin_loop();
    }
}

/// The node's next frame, waiting for it.
fn next_frame(link: &mut Link, proxy: &mut Proxy) -> Vec<u8> {
    let started = Instant::now();
    loop {
        if let Some(frame) = link.read_frame() {
            return frame;
        }
        if link.state() != STATE_CONNECTED {
            proxy.fail(&format!(
                "the link's state became {} while a frame was owed",
                link.state()
            ));
        }
        if started.elapsed() > REPLY_LIMIT {
            proxy.fail("no frame from the node");
        }
        std::thread::yield_now();
    }
}

/// The one application frame of the next reply batch, through its batch
/// end, heartbeats skipped.
fn next_reply(link: &mut Link, proxy: &mut Proxy) -> Vec<u8> {
    let mut frames = Vec::new();
    loop {
        let payload = next_frame(link, proxy);
        match classify(&payload) {
            Ok(Reply::Response(body)) => frames.push(body.to_vec()),
            Ok(Reply::Heartbeat) => {}
            Ok(Reply::BatchEnd) => break,
            other => proxy.fail(&format!("a reply frame of {other:?}")),
        }
    }
    assert_eq!(frames.len(), 1, "one frame in an echo's batch: {frames:?}");
    frames.remove(0)
}

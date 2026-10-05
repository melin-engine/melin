//! DPDK Environment Abstraction Layer (EAL) initialization.
//!
//! The EAL must be initialized before any other DPDK calls. It sets up
//! hugepage memory, discovers NIC ports, and initializes per-lcore data
//! structures. It initializes once per process: `rte_eal_init` refuses a
//! second call, and after `rte_eal_cleanup` it cannot be initialized
//! again.
//!
//! Two ways to hold it:
//!
//! - [`Eal::init`]: the node initializes EAL for itself and cleans it up
//!   when it is done. One node per process, which is how a node is
//!   deployed.
//! - [`Eal::init_process_wide`]: initialized once for the whole process
//!   and never cleaned up, so the process can host several nodes, one
//!   after another or at once, each on a port of its own. A node started
//!   in such a process shares it rather than initializing its own (see
//!   `DpdkShared::init`). The test launcher uses this to run a cluster,
//!   or a restart, in one test process.

use std::ffi::CString;
use std::sync::OnceLock;

use crate::ffi;

/// The process-wide EAL, once [`Eal::init_process_wide`] has run, with the
/// outcome of that one attempt: EAL cannot be initialized twice, so a
/// failure is final too. A `OnceLock` because it is written once, by
/// whichever caller comes first, and read from any thread after. Never
/// dropped (a static), which is what keeps it from being cleaned up.
static PROCESS_WIDE: OnceLock<Result<Eal, EalError>> = OnceLock::new();

/// RAII wrapper for EAL initialization. Calls `rte_eal_cleanup` on drop.
pub struct Eal {
    _private: (),
}

impl Eal {
    /// Initialize the DPDK EAL with the given arguments.
    ///
    /// Typical args:
    /// - `["-l", "0-7"]` — logical core mask
    /// - `["--huge-dir", "/dev/hugepages"]` — hugepage mount point
    /// - `["--socket-mem", "1024"]` — memory per NUMA socket in MB
    /// - `["--vdev", "net_tap0"]` — virtual device for testing (no real NIC)
    ///
    /// # Errors
    /// Returns an error if EAL initialization fails (e.g., no hugepages,
    /// insufficient permissions, invalid arguments).
    pub fn init(args: &[&str]) -> Result<Self, EalError> {
        // Convert args to C strings. EAL expects argv[0] to be the program
        // name (it's ignored but must be present).
        let mut c_args: Vec<CString> = Vec::with_capacity(args.len() + 1);
        c_args.push(CString::new("melin-dpdk").expect("program name"));
        for arg in args {
            c_args.push(CString::new(*arg).map_err(|_| EalError::InvalidArg)?);
        }

        let mut c_ptrs: Vec<*mut libc::c_char> = c_args
            .iter()
            .map(|s| s.as_ptr() as *mut libc::c_char)
            .collect();

        let argc = c_ptrs.len() as libc::c_int;

        // SAFETY: rte_eal_init is called once at startup with valid argc/argv.
        // The CStrings remain alive for the duration of the call.
        let ret = unsafe { ffi::rte_eal_init(argc, c_ptrs.as_mut_ptr()) };

        if ret < 0 {
            return Err(EalError::InitFailed(ret));
        }

        tracing::info!(cores = ret, "DPDK EAL initialized");
        Ok(Eal { _private: () })
    }

    /// Initialize EAL for the whole process, once, and return it.
    ///
    /// The first call initializes it with `args`; every later call returns
    /// the outcome of that first one, whatever its own `args` (EAL cannot
    /// be initialized a second time, so there is nothing else it could
    /// do). Calls racing the first one wait for it.
    ///
    /// The process-wide EAL is never cleaned up: `rte_eal_cleanup` would
    /// end it for good, and the point is to outlive any one node. Process
    /// exit releases its memory. Every node started in this process after
    /// this call shares it, each on a port of its own (see
    /// [`Eal::attach_vdev`]), and must be given no EAL arguments of its
    /// own.
    ///
    /// # Errors
    /// The first call's initialization error, as [`Eal::init`].
    pub fn init_process_wide(args: &[&str]) -> Result<&'static Eal, EalError> {
        PROCESS_WIDE
            .get_or_init(|| Eal::init(args))
            .as_ref()
            .map_err(Clone::clone)
    }

    /// The outcome of [`Eal::init_process_wide`], if it has been called:
    /// the process-wide EAL, or the error its one initialization attempt
    /// failed with (EAL cannot be initialized again, so that error is
    /// final). `None` in a process that runs one node, which owns its EAL.
    pub fn process_wide() -> Option<Result<&'static Eal, EalError>> {
        PROCESS_WIDE
            .get()
            .map(|eal| eal.as_ref().map_err(Clone::clone))
    }

    /// Number of available DPDK ethernet ports.
    pub fn port_count(&self) -> u16 {
        // SAFETY: EAL is initialized (we hold `self`).
        unsafe { ffi::rte_eth_dev_count_avail() }
    }

    /// Probe virtual device `name` (`net_af_packet0`, say) with driver
    /// arguments `args` (`iface=veth0`), as `--vdev=name,args` would at
    /// EAL init, and return the port it created.
    ///
    /// For a process that hosts several nodes on the process-wide EAL:
    /// each node gets a device of its own. A node closes its port when it
    /// stops, which releases the port but leaves the device on the bus;
    /// [`Eal::detach_vdev`] removes it, after which the same name can be
    /// attached again for the next node.
    ///
    /// # Errors
    /// The probe failing (no such driver, bad arguments, a device of that
    /// name already attached), or the probe creating no port by that name.
    pub fn attach_vdev(&self, name: &str, args: &str) -> Result<u16, EalError> {
        let c_name = CString::new(name).map_err(|_| EalError::InvalidArg)?;
        let c_args = CString::new(args).map_err(|_| EalError::InvalidArg)?;
        // SAFETY: EAL is initialized (we hold `self`); every pointer is a
        // NUL-terminated string that outlives the call, which copies them.
        let ret =
            unsafe { ffi::rte_eal_hotplug_add(c"vdev".as_ptr(), c_name.as_ptr(), c_args.as_ptr()) };
        if ret != 0 {
            return Err(EalError::Hotplug {
                op: "attach",
                device: name.to_owned(),
                code: ret,
            });
        }
        let mut port_id: u16 = 0;
        // SAFETY: `c_name` is NUL-terminated and `port_id` writable, both
        // live for the call.
        let ret = unsafe { ffi::rte_eth_dev_get_port_by_name(c_name.as_ptr(), &mut port_id) };
        if ret != 0 {
            // Keep "attach failed => nothing attached": a device left on the
            // bus would make every later attach of this name fail with
            // EEXIST. The lookup failure is the error the caller needs; a
            // failed removal on top of it is only logged, since there is
            // nothing more the caller could do about it.
            if let Err(e) = self.detach_vdev(name) {
                tracing::warn!(device = name, error = %e, "failed to remove a DPDK virtual device that created no port");
            }
            return Err(EalError::NoSuchPort(name.to_owned()));
        }
        tracing::info!(device = name, port_id, "DPDK virtual device attached");
        Ok(port_id)
    }

    /// Remove virtual device `name` from the bus, closing its port first
    /// if it is still open. See [`Eal::attach_vdev`].
    ///
    /// # Errors
    /// No device of that name is attached, or the driver refused to
    /// remove it.
    pub fn detach_vdev(&self, name: &str) -> Result<(), EalError> {
        let c_name = CString::new(name).map_err(|_| EalError::InvalidArg)?;
        // SAFETY: EAL is initialized (we hold `self`); both pointers are
        // NUL-terminated strings that outlive the call.
        let ret = unsafe { ffi::rte_eal_hotplug_remove(c"vdev".as_ptr(), c_name.as_ptr()) };
        if ret != 0 {
            return Err(EalError::Hotplug {
                op: "detach",
                device: name.to_owned(),
                code: ret,
            });
        }
        tracing::info!(device = name, "DPDK virtual device detached");
        Ok(())
    }
}

impl Drop for Eal {
    fn drop(&mut self) {
        // SAFETY: EAL was initialized in `init()`. Cleanup is called once.
        unsafe {
            ffi::rte_eal_cleanup();
        }
        tracing::info!("DPDK EAL cleaned up");
    }
}

/// Errors from EAL initialization and device hotplug.
///
/// `Clone` because the process-wide EAL keeps the outcome of its one
/// initialization attempt and hands every caller a copy of it.
#[derive(Debug, Clone)]
pub enum EalError {
    /// An argument contained a null byte.
    InvalidArg,
    /// `rte_eal_init` returned an error code.
    InitFailed(libc::c_int),
    /// Attaching or detaching a virtual device failed.
    Hotplug {
        /// What was being done: `"attach"` or `"detach"`.
        op: &'static str,
        /// The virtual device's name (`net_af_packet0`, say).
        device: String,
        /// The negative errno `rte_eal_hotplug_add` or
        /// `rte_eal_hotplug_remove` returned.
        code: libc::c_int,
    },
    /// A device was attached but no port of its name exists.
    NoSuchPort(String),
}

impl std::fmt::Display for EalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EalError::InvalidArg => write!(f, "EAL argument contains null byte"),
            EalError::InitFailed(code) => write!(f, "rte_eal_init failed with code {code}"),
            EalError::Hotplug { op, device, code } => {
                write!(f, "{op} virtual device {device} failed with code {code}")
            }
            EalError::NoSuchPort(device) => {
                write!(
                    f,
                    "virtual device {device} attached, but no port by that name"
                )
            }
        }
    }
}

impl std::error::Error for EalError {}

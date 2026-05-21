# BIOS Tuning for Low-Latency Deployments

Platform-level configuration for AMD EPYC servers running Melin. Written for the operator setting up bare-metal hardware before the first production run. Pair with the kernel-side work in [CPU Tuning](operations.md#cpu-tuning): both are required to reach the latencies published in the README.

---

## Why this matters

A stock AMD EPYC BIOS leaves several firmware behaviours enabled that periodically interrupt or stall the cores:

- The memory controller periodically re-runs DDR signal-integrity training to compensate for thermal drift.
- The System Management Unit (SMU) samples each core to adjust voltage, frequency, and Infinity Fabric P-states.
- Data Fabric C-states gate the on-chip interconnect into low-power states.
- The memory controller also runs background ECC patrol scrubbing.
- The BIOS uses System Management Interrupts (SMI) to emulate legacy USB keyboards, poll the serial console, and enforce power caps.

These activities are largely invisible to standard OS counters (no IRQ in `/proc/interrupts`, no context switch) and individually cost only microseconds, but on a busy core they show up at the deep tail as a recurring periodic spike. Applying the settings below removes or substantially shrinks that periodic source.

Older AMD generations exhibit larger amplitudes of the same spike; the same BIOS profile applies and is expected to bring proportionally larger improvements on those parts.

## Quick reference

Settings grouped by expected impact, based on the AMD platform-level knobs commonly cited as contributors to deep-tail jitter. Apply the first nine for a quick pass.

| Setting | Value | Category |
|---|---|---|
| **Periodic Training Mode** (memory controller) | Disabled | Memory |
| **DF C-States** (Data Fabric) | Disabled | SMU / IF |
| **APBDIS** | 1 | SMU / IF |
| **Fixed SOC P-state** (a.k.a. DfPstate) | P0 | SMU / IF |
| **Global C-state Control** | Disabled | Power |
| **Memory Patrol Scrub** | Disabled | Memory |
| **USB Legacy Support** | Disabled | SMI sources |
| **PCIe ASPM** | Disabled | PCIe |
| **Determinism Slider** | Power | SMU / IF |
| cTDP | Max for SKU | Power |
| Package Power Limit / PPT | Max | Power |
| Core Performance Boost (CPB) | Disabled | CPU |
| SMT | Disabled | CPU |
| Memory Frequency | Max JEDEC rated for SKU | Memory |
| Memory Power Down Enable | Disabled | Memory |
| DRAM Refresh Rate | 1x | Memory |
| NPS (NUMA Per Socket) | NPS1 | Memory |
| Memory Interleaving | Auto | Memory |
| TSME / Transparent SME | Disabled | Encryption |
| SME / Secure Memory Encryption | Disabled | Encryption |
| SEV / SEV-SNP | Disabled | Encryption |
| IOMMU | Disabled (unless needed) | PCIe / Virt |
| SVM / AMD-V | Disabled (unless needed) | Virt |
| SR-IOV | Disabled (unless needed) | PCIe |
| Port 60/64 Emulation | Disabled | SMI sources |
| Console Redirection after POST | Disabled | SMI sources |
| BMC / IPMI Watchdog | Disabled | SMI sources |
| Power Capping | Disabled | SMI sources |
| HPET / High Precision Event Timer | Disabled | Misc |
| ErP Ready / Deep Sleep | Disabled | Power |
| USB 3.0 Wake / Wake on LAN | Disabled | Misc |
| PCIe Hot Plug | Disabled (unless needed) | PCIe |
| PCIe Relaxed Ordering | Enabled | PCIe |
| L1 / L2 / L3 Stream Prefetcher | Enabled | CPU |
| L3 Cache as NUMA Domain | Disabled | CPU |
| DRAM Scrub Time | Longest / Disabled | Memory |

## Detailed settings

### SMU and Infinity Fabric

- **DF C-States: Disabled.** Prevents the Data Fabric (the on-chip interconnect linking core complexes, memory controllers, and I/O hubs) from entering low-power states. When the fabric exits a C-state to service traffic, the wake-up takes microseconds and looks like a memory stall to any core issuing requests at that moment.
- **APBDIS: 1.** The name is a double negative: APBDIS = "Algorithmic Performance Boost Disable", and `1` *disables* the algorithm. The algorithm in question dynamically picks an Infinity Fabric / memory-controller P-state based on observed load. Each transition briefly affects memory bandwidth. With APBDIS=1, the IF P-state stops moving: it's then locked to whatever **Fixed SOC P-state** says.
- **Fixed SOC P-state (a.k.a. DfPstate): P0.** With APBDIS=1, this knob takes effect. P0 is the highest performance state: IF and memory clocks pinned at maximum.
- **DF P-State Frequency Optimizer: Disabled** (if present as a separate field). On some BIOSes the previous two settings appear gated behind this optimizer. Disable it; the manual fixed P-state then applies.
- **Determinism Slider: Power.** Counter-intuitive name: "Power Determinism" gives more *stable* timing than "Performance Determinism". The Performance variant lets each core chase its individual maximum boost, which involves frequent re-evaluation; the Power variant settles into a stable operating point and stays there.

### Power and C-states

- **Global C-state Control: Disabled.** Stops core C-state transitions entirely. The kernel's `processor.max_cstate=1` boot parameter helps, but the chip can still attempt internal CC6 transitions when the BIOS hasn't disabled it.
- **cTDP: set to the SKU's maximum.** Look up the configurable TDP range for your specific EPYC SKU and pick the top of the range. Removing the artificial cap stops the SMU from constantly throttling against it. Cooling-driven thermal throttling still works as a safety net.
- **Package Power Limit / PPT: same as cTDP, set to max.** Often the same knob under a different label; if both appear, set them equal.
- **ErP Ready / Deep Sleep: Disabled.** Off-state power-saving features that have no business firing on a server.

### CPU

- **Core Performance Boost (CPB): Disabled.** Locks cores at their base clock rather than letting them oscillate between base and boost on millisecond timescales. The cost is some peak throughput; the benefit is rock-steady latency. If you want maximum clock instead, leave CPB enabled and accept the occasional re-clock cost.
- **SMT: Disabled.** Hyperthread siblings share execution units; under a busy-spinning pipeline, the sibling starves the primary. Also matches the kernel boot parameter `nosmt`.
- **L1 / L2 / L3 Stream Prefetcher: Enabled.** Keep all prefetchers on. They help, not hurt, on this workload.
- **L3 Cache as NUMA Domain: Disabled.** Presents a single L3 view to the OS instead of one NUMA node per Core Complex Die. Simpler scheduling, lower variability.

### Memory

- **Periodic Training Mode: Disabled.** The memory controller periodically re-runs DDR signal-integrity calibration to compensate for thermal drift. The "Legacy" mode runs this on a fixed timer (roughly once per second), and each pass briefly stalls memory access while it executes, which on a busy core lands at the deep tail of every workload that touches DRAM. Modern DDR5 self-corrects sufficiently at runtime that disabling periodic re-training is safe on a stable thermal envelope. This setting often lives under *North Bridge Configuration* or *UMC Common Options*, not under a memory submenu.
- **Memory Frequency: Maximum JEDEC-rated for the SKU.** For EPYC 9255 this is DDR5-6000 (or the highest supported by your DIMM module population). Do not leave at *Auto*. Auto sometimes negotiates conservatively, and re-training during runtime is a stall source.
- **Memory Power Down Enable: Disabled.** Prevents DRAM ranks from entering power-down states between accesses.
- **DRAM Refresh Rate: 1x.** Not 2x. 2x doubles refresh frequency; useful in high-temperature environments but each refresh briefly blocks the bank, so 2x doubles the stall opportunities.
- **NPS (NUMA Per Socket): NPS1.** A single NUMA node per socket. Simplest topology. NPS2/NPS4 are useful for some workloads but add cross-CCD traffic complexity that isn't worth it for a single-process matching engine.
- **Memory Interleaving: Auto.** Let the BIOS pick the best interleave for the chosen NPS.
- **Memory Patrol Scrub: Disabled.** Background ECC scanning. Useful for long-uptime systems but creates periodic memory bandwidth bursts. Disable for low-latency workloads; on a freshly-rebooted system the scrub adds little safety in the first hours anyway.
- **DRAM Scrub Time: Longest interval, or Disabled.** Similar to Patrol Scrub but a separate knob on some boards.

### SMI sources

System Management Interrupts hand control to firmware code running in System Management Mode. The OS sees a "missing" microsecond on the core that handled the SMI; no IRQ counter increments. Eliminate every avoidable SMI source.

- **USB Legacy Support: Disabled.** The largest source of SMIs on most servers. The BIOS uses SMI to emulate PS/2 keyboard input from USB keyboards. Even without a USB keyboard plugged in, the emulation often runs anyway.
- **Port 60/64 Emulation: Disabled.** The companion knob to USB Legacy. Disable both.
- **Console Redirection / Serial Redirection after POST: Disabled.** Some BIOSes poll the serial port via SMI to redirect console output. Disable post-POST redirection (you can keep it for boot output if you need it).
- **BMC / IPMI Watchdog: Disabled** (where the BIOS exposes it). Periodic watchdog kicks can fire SMI on some platforms.
- **Power Capping: Disabled.** If the BIOS implements power caps via SMI rather than via the SMU, this fires on a timer. Set the cap mechanism to "SMU-enforced" if the BIOS offers the choice; otherwise disable entirely.

### Security and encryption

These are AMD's memory encryption and virtualisation features. Each one adds per-access overhead and, in the encryption cases, periodic SMU work for key management.

- **TSME / Transparent SME: Disabled.** Transparent SME encrypts every DRAM access. Adds per-access overhead plus SMU work to manage keys. Disable unless mandated by compliance.
- **SME / Secure Memory Encryption: Disabled.** Same family.
- **SEV / SEV-SNP: Disabled.** Encrypted VMs. Not needed for a bare-metal matching engine.
- **IOMMU: Disabled** if you do not need PCIe device pass-through or VFIO. The IOMMU adds a translation layer on every PCIe DMA. Enable it only if your deployment specifically requires it.
- **SVM Mode / AMD-V: Disabled** if you do not run virtual machines. One fewer set of CPU code paths active.
- **SR-IOV: Disabled** if you do not use SR-IOV NIC partitioning.

### PCIe

- **PCIe ASPM: Disabled** on all links. Active State Power Management lets PCIe links enter low-power states. Wake-ups add jitter to any I/O on the affected device, including NVMe and NIC traffic.
- **PCIe Hot Plug: Disabled** if you do not hot-plug devices in production. Removes background polling.
- **PCIe Relaxed Ordering: Enabled.** A performance knob, not a power knob; safe to leave on. Allows PCIe transactions to bypass strict ordering where the device permits.

### Miscellaneous

- **HPET (High Precision Event Timer): Disabled** if the BIOS exposes the knob. Modern Linux uses the TSC for all timing; HPET is a much slower fallback and can fire IRQs as a broadcast timer for idle CPUs. With `nohz_full` you do not need it at all.
- **USB 3.0 Wake / Wake on LAN: Disabled.** Wake-state polling adds background activity.

### Single-shot convenience

Some BIOSes expose a top-level **Workload Profile** or **Power Profile** dropdown with options like:

- *Latency Optimized*
- *Max Performance*
- *Balanced*
- *Power Saver*

If your BIOS has this, selecting **Latency Optimized** (or **Max Performance** where that's the closest option) typically sets 8–10 of the individual knobs above in one go. You can still layer the specific overrides on top; the explicit settings win where they disagree.

## Verification

After reboot, verify the OS sees the expected state:

```sh
# Clocksource should be tsc, not hpet
cat /sys/devices/system/clocksource/clocksource0/current_clocksource

# CPU frequency should be fixed (no scaling)
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor   # → performance
grep MHz /proc/cpuinfo | sort -u                            # → single value, not a range

# SMT off
cat /sys/devices/system/cpu/smt/active                      # → 0

# Confirm /proc/interrupts NMI/LOC counters on isolated cores stay flat under load
watch -n 1 'grep -E "^(NMI|LOC|RES|CAL|TLB):" /proc/interrupts'
```

A clean tuning leaves the isolated cores with zero LOC/RES/CAL/TLB counter movement during a sustained bench run.

Counter measured by `perf stat`:

```sh
# SMI count on a busy core (should be 0 over 30s)
perf stat -C <core> -e ls_smi_rx sleep 30
```

If `ls_smi_rx` is non-zero during a busy run, walk back through the [SMI sources](#smi-sources) section.

## Vendor notes

BIOS setting names vary across hardware vendors. Approximate translations:

- **Supermicro**: "AMD CBS" submenu contains most knobs verbatim.
- **Dell PowerEdge**: Knobs grouped under "Processor Settings", "Memory Settings", "System Profile Settings". The single-dropdown profile is "System Profile" → set to *Performance* or *Performance Per Watt (DAPC)*.
- **HPE ProLiant**: "Workload Profile" → *Low Latency*. Individual knobs under "System Options → Processor Options" and similar.
- **ASRock Rack / Tyan**: Most settings appear verbatim under "AMD CBS".

If a knob mentioned here is not exposed in your BIOS, ignore it; the commonly cited core settings (DF C-states, APBDIS+P0, Global C-state Control, USB Legacy, PCIe ASPM) are almost universally available.

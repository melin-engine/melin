//! Minimal `Application` implementation used by the transport-core test
//! suite. Not part of the public API.
//!
//! `TestApp` tracks a running sum and a per-key HWM map so round-trip
//! tests can assert state equality after snapshot/restore and after
//! journal replay. Kept deliberately small — the transport doesn't care
//! about semantics, only about byte-exact round-trips.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestEvent {
    Add(u64),
    Query,
}

impl AppEvent for TestEvent {
    fn encoded_size(&self) -> usize {
        match self {
            TestEvent::Add(_) => 1 + 8,
            TestEvent::Query => 1,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            TestEvent::Add(n) => {
                buf[0] = 0x01;
                buf[1..9].copy_from_slice(&n.to_le_bytes());
                9
            }
            TestEvent::Query => {
                buf[0] = 0x02;
                1
            }
        }
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        match buf.first().copied() {
            None => Err(CodecError::Truncated),
            Some(0x01) => {
                if buf.len() < 9 {
                    return Err(CodecError::Truncated);
                }
                let n =
                    u64::from_le_bytes(buf[1..9].try_into().expect("slice length checked above"));
                Ok(TestEvent::Add(n))
            }
            Some(0x02) => Ok(TestEvent::Query),
            Some(t) => Err(CodecError::UnknownTag(t)),
        }
    }

    fn is_query(&self) -> bool {
        matches!(self, TestEvent::Query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestReport {
    pub total_after: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestQuery {
    pub total: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TestApp {
    pub total: u64,
    pub ticks: u64,
    // HashMap for per-key dedup state, matching `Exchange::key_hwm`.
    // BTreeMap would give deterministic snapshot iteration for free, but
    // the snapshot() impl sorts explicitly so the choice doesn't matter.
    pub key_hwm: HashMap<u64, u64>,
}

impl TestApp {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Application for TestApp {
    type Event = TestEvent;
    type Report = TestReport;
    type QueryResponse = TestQuery;

    const APP_VERSION: u16 = 42;

    fn apply(
        &mut self,
        event: Self::Event,
        _ctx: &ApplyCtx,
        out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse> {
        match event {
            TestEvent::Add(n) => {
                self.total = self.total.wrapping_add(n);
                out.push(TestReport {
                    total_after: self.total,
                });
                None
            }
            TestEvent::Query => Some(TestQuery { total: self.total }),
        }
    }

    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Self::Report>) {
        self.ticks = self.ticks.wrapping_add(1);
    }

    fn check_request_seq(&mut self, key_hash: u64, seq: u64) -> bool {
        // Exempt internal/seed events (key_hash == 0) — same convention
        // as Exchange::check_request_seq.
        if key_hash == 0 {
            return true;
        }
        let hwm = self.key_hwm.entry(key_hash).or_insert(0);
        if seq > *hwm {
            *hwm = seq;
            true
        } else {
            false
        }
    }

    fn build_reject(_event: &Self::Event, _reason: RejectReason) -> Self::Report {
        TestReport {
            total_after: u64::MAX,
        }
    }

    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.total.to_le_bytes())?;
        w.write_all(&self.ticks.to_le_bytes())?;
        // Sort keys so the snapshot bytes are deterministic — HashMap
        // iteration order is nondeterministic and would break byte-eq
        // assertions across runs.
        let mut entries: Vec<(&u64, &u64)> = self.key_hwm.iter().collect();
        entries.sort_by_key(|(k, _)| **k);
        let len =
            u32::try_from(entries.len()).map_err(|_| io::Error::other("too many HWM entries"))?;
        w.write_all(&len.to_le_bytes())?;
        for (k, v) in entries {
            w.write_all(&k.to_le_bytes())?;
            w.write_all(&v.to_le_bytes())?;
        }
        Ok(())
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut u64_buf = [0u8; 8];
        r.read_exact(&mut u64_buf)?;
        let total = u64::from_le_bytes(u64_buf);
        r.read_exact(&mut u64_buf)?;
        let ticks = u64::from_le_bytes(u64_buf);
        let mut u32_buf = [0u8; 4];
        r.read_exact(&mut u32_buf)?;
        let len = u32::from_le_bytes(u32_buf) as usize;
        let mut key_hwm = HashMap::with_capacity(len);
        for _ in 0..len {
            let mut kb = [0u8; 8];
            r.read_exact(&mut kb)?;
            let mut vb = [0u8; 8];
            r.read_exact(&mut vb)?;
            key_hwm.insert(u64::from_le_bytes(kb), u64::from_le_bytes(vb));
        }
        Ok(Self {
            total,
            ticks,
            key_hwm,
        })
    }
}

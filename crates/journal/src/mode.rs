//! CLI-selectable writer mode. Lives separately from the concrete
//! writers so it can be parsed and serialised by `melin-server` and
//! `melin-bench` without dragging in any of the writer state.

/// Selects which concrete writer the server (or a bench) builds. Set
/// once at startup from the `--journal-writer` CLI flag (or its
/// config-file equivalent) and threaded through every boot path that
/// constructs a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalWriterMode {
    /// **Experimental.** `O_DIRECT` writes, sector-aligned, durability
    /// dependent on capacitor-backed PLP. Lowest-latency on enterprise
    /// NVMe with `VWC=0`; **silently loses acknowledged writes on power
    /// loss without PLP.** Under investigation for periodic ~1 Hz tail
    /// latency spikes on some NVMe firmware. Not recommended for
    /// production until both the durability guarantee is verified on
    /// the target drives and the spike root cause is identified.
    Sector,
    /// Page-cache writes with explicit `fdatasync` per batch. Honest
    /// durability on any drive at the cost of one device flush per
    /// flush boundary on `VWC=1` drives. Default and recommended for
    /// production.
    #[default]
    Buffered,
}

impl JournalWriterMode {
    /// Parse the value of the `--journal-writer` flag. Accepts
    /// `sector` / `buffered`, case-insensitive.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "sector" => Ok(Self::Sector),
            "buffered" => Ok(Self::Buffered),
            other => Err(format!(
                "unknown journal writer mode '{other}'; expected 'sector' or 'buffered'"
            )),
        }
    }

    /// Stable string form used by the CLI and config serialisation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sector => "sector",
            Self::Buffered => "buffered",
        }
    }
}

impl std::fmt::Display for JournalWriterMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JournalWriterMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_variants_case_insensitive() {
        assert_eq!(
            JournalWriterMode::parse("sector"),
            Ok(JournalWriterMode::Sector)
        );
        assert_eq!(
            JournalWriterMode::parse("Buffered"),
            Ok(JournalWriterMode::Buffered)
        );
        assert_eq!(
            JournalWriterMode::parse("BUFFERED"),
            Ok(JournalWriterMode::Buffered)
        );
        assert!(JournalWriterMode::parse("direct").is_err());
    }

    #[test]
    fn default_mode_is_buffered() {
        assert_eq!(JournalWriterMode::default(), JournalWriterMode::Buffered);
    }
}

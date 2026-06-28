//! Zero-reconstruction shred header parsing for the validator-granularity
//! source benchmark.
//!
//! Every Solana shred carries a fixed 83-byte common header at the start of the
//! raw UDP payload. We read slot / index / fec_set_index / data-vs-coding
//! directly from those bytes with no allocation, no signature verification and
//! no FEC reconstruction. This is the foundation of the whole benchmark: each
//! packet can be attributed to `(slot, fec_set_index, index, data|coding)` the
//! instant it arrives, alongside its source IP and receive timestamp.
//!
//! `slot`, `index` and the data/coding discriminator are read through
//! `solana_ledger::shred::layout` (the crate's own offset readers, already used
//! in `deshred.rs`). `fec_set_index` has no public `layout` helper in the locked
//! crate, so it is read directly from bytes `79..83` (little-endian u32). The
//! `verify_offsets_against_full_parse` test (milestone M0) proves these reads
//! match the full `Shred` parser over real captured shreds.

use std::net::IpAddr;

use solana_ledger::shred::{layout, ShredType};

/// Offset of the single shred-variant byte (encodes data/coding + merkle kind).
pub const OFFSET_SHRED_VARIANT: usize = 64;
/// Offset of the slot (u64 little-endian).
pub const OFFSET_SLOT: usize = 65;
/// Offset of the shred index (u32 little-endian).
pub const OFFSET_INDEX: usize = 73;
/// Offset of the shred version (u16 little-endian).
pub const OFFSET_VERSION: usize = 77;
/// Offset of the FEC set index (u32 little-endian).
pub const OFFSET_FEC_SET_INDEX: usize = 79;
/// Total size of the common header shared by every shred.
pub const SIZE_OF_COMMON_HEADER: usize = 83;

/// Identity of a single shred, used to match the same shred arriving from
/// multiple sources. `is_data` is included because a data shred and a coding
/// shred can share the same `(slot, index)` within a slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShredId {
    pub slot: u64,
    pub fec_set_index: u32,
    pub index: u32,
    pub is_data: bool,
}

/// The fields parsed from a raw shred header with zero reconstruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedHeader {
    pub slot: u64,
    pub index: u32,
    pub fec_set_index: u32,
    pub is_data: bool,
}

/// A single observed arrival of a shred from a given source.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    pub source: IpAddr,
    pub slot: u64,
    pub fec_set_index: u32,
    pub index: u32,
    pub is_data: bool,
    /// Receive timestamp, nanoseconds since the unix epoch. Kernel
    /// (SO_TIMESTAMPNS) when available, otherwise userspace `SystemTime::now()`.
    pub rx_ts_ns: i64,
}

impl Observation {
    #[inline]
    pub fn shred_id(&self) -> ShredId {
        ShredId {
            slot: self.slot,
            fec_set_index: self.fec_set_index,
            index: self.index,
            is_data: self.is_data,
        }
    }
}

/// Parse the minimal header fields from a raw shred payload. Returns `None` if
/// the payload is too short or the variant byte is not a recognized shred type.
#[inline]
pub fn parse_header(shred: &[u8]) -> Option<ParsedHeader> {
    // `layout::get_slot` / `get_index` already bounds-check, but we also need
    // bytes up to the end of the common header for fec_set_index.
    if shred.len() < SIZE_OF_COMMON_HEADER {
        return None;
    }
    let slot = layout::get_slot(shred)?;
    let index = layout::get_index(shred)?;
    let shred_type = layout::get_shred_type(shred).ok()?;
    let fec_set_index = u32::from_le_bytes(
        shred[OFFSET_FEC_SET_INDEX..OFFSET_FEC_SET_INDEX + 4]
            .try_into()
            .ok()?,
    );
    Some(ParsedHeader {
        slot,
        index,
        fec_set_index,
        is_data: matches!(shred_type, ShredType::Data),
    })
}

/// Parse a raw shred payload into an `Observation`, tagging it with the source
/// IP and receive timestamp. Returns `None` for non-shred / malformed packets.
#[inline]
pub fn parse_observation(shred: &[u8], source: IpAddr, rx_ts_ns: i64) -> Option<Observation> {
    let h = parse_header(shred)?;
    Some(Observation {
        source,
        slot: h.slot,
        fec_set_index: h.fec_set_index,
        index: h.index,
        is_data: h.is_data,
        rx_ts_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::BorshDeserialize;
    use solana_ledger::shred::merkle::Shred;

    #[derive(borsh::BorshDeserialize)]
    struct Packets {
        pub packets: Vec<Vec<u8>>,
    }

    fn load_fixture(path: &str) -> Option<Packets> {
        let buf = std::fs::read(path).ok()?;
        Packets::try_from_slice(&buf).ok()
    }

    /// Milestone M0: prove the fixed-offset reads (and the manual fec_set_index
    /// read at bytes 79..83) match the full `Shred` parser for EVERY real shred,
    /// across both data and coding shreds. Gated on the fixture being present.
    #[test]
    fn verify_offsets_against_full_parse() {
        let fixtures = ["../bins/serialized_shreds.bin", "../bins/serialized_shreds_data_complete_test.bin"];
        let mut checked = 0usize;
        let mut data_count = 0usize;
        let mut code_count = 0usize;

        for path in fixtures {
            let Some(pkts) = load_fixture(path) else {
                eprintln!("skipping missing fixture {path}");
                continue;
            };
            for raw in &pkts.packets {
                // Ground truth: only cross-check packets the real parser accepts.
                let Ok(shred) = Shred::from_payload(raw.clone()) else {
                    continue;
                };
                let ch = shred.common_header();
                let parsed = parse_header(raw)
                    .unwrap_or_else(|| panic!("parse_header returned None for a valid shred"));

                assert_eq!(parsed.slot, ch.slot, "slot mismatch");
                assert_eq!(parsed.index, ch.index, "index mismatch");
                assert_eq!(
                    parsed.fec_set_index, ch.fec_set_index,
                    "fec_set_index mismatch (manual 79..83 read)"
                );
                let truth_is_data = shred.shred_type() == ShredType::Data;
                assert_eq!(parsed.is_data, truth_is_data, "data/coding mismatch");

                if truth_is_data {
                    data_count += 1;
                } else {
                    code_count += 1;
                }
                checked += 1;
            }
        }

        // If neither fixture is present the test is a no-op; otherwise we must
        // have validated both data and coding shreds.
        if checked > 0 {
            assert!(data_count > 0, "expected at least one data shred");
            assert!(code_count > 0, "expected at least one coding shred");
            eprintln!("M0 verified {checked} shreds ({data_count} data, {code_count} coding)");
        }
    }

    #[test]
    fn rejects_short_payloads() {
        assert!(parse_header(&[0u8; 10]).is_none());
        assert!(parse_header(&[]).is_none());
    }
}

//! Zero-allocation structural validation of one deshredded micro-batch.
//!
//! A leader serializes every entry batch (or, under Alpenglow, every block component) on
//! its own and cuts it into data shreds, so the concatenated payload of the shreds between
//! two DATA_COMPLETE flags (`Shredder::deshred` concatenates each shred's `size`-bounded
//! data) is one serialized object followed by nothing but zero padding:
//! - Agave pads the last FEC set with data shreds that carry no data, so its payloads end
//!   exactly at the object end (`ledger/src/shred/merkle.rs` `shred_leftover_data`).
//! - Firedancer zero-fills the batch buffer itself up to a whole number of FEC-set payloads
//!   and shreds the padded buffer (`disco/shred/fd_shred_tile.c` `batch_sz_padded`), so
//!   its payloads end with up to one FEC set (~30 KB) of zeros inside the `size` fields.
//!
//! This module walks that byte string without building a single `Entry`, `Pubkey` or `Vec`
//! and reports whether it is well formed: the object parses, and every byte after it is 0.
//!
//! The deshredder uses it to decide whether a batch whose start was *guessed* (the shred
//! before it is still missing) really starts where the guess says. A guess that starts
//! inside another batch leaves the cursor mid-transaction; the chance that such a suffix
//! still parses as a complete `Vec<Entry>` ending exactly where the payload's nonzero bytes
//! end is negligible.
//!
//! Wire layout (bincode `Vec<Entry>`; byte-exact with Agave 4.x and the arb_bot consumer's
//! `integrations/shredstream/entry_decode.rs`):
//! - u64 LE entry count, then per entry: u64 LE `num_hashes`, 32-byte hash, u64 LE
//!   transaction count, then the transactions back to back.
//! - Legacy/v0 transaction: ShortU16 signature count (always one byte, `< 0x80`), the
//!   64-byte signatures, then the message. The first message byte is
//!   `num_required_signatures` (`< 0x80`) for legacy or the `0x80` prefix for v0.
//! - SIMD-0385 v1 transaction (live on mainnet since epoch 1035): `0x81`, legacy header, u32
//!   LE config mask, 32-byte lifetime specifier, u8 instruction count, u8 address count,
//!   addresses, config values, 4-byte instruction headers, instruction payloads, then
//!   exactly `num_required_signatures` 64-byte signatures with no length prefix.
//! - Alpenglow `BlockComponent::BlockMarker` (`entry/src/block_component.rs`): entry count 0,
//!   u16 LE marker version (1), then for `BlockMarkerV1` a u8 variant id, a u16 LE byte
//!   length and exactly that many bytes. An entry count of 0 with nothing (or only zeros)
//!   after it is an empty batch; `Shredder::deshred` also returns a zero-filled buffer when
//!   every composing shred carried no data.
//!
//! Beyond structure, only invariants that `sanitize` enforces before a transaction can be
//! recorded are checked: the signature count equals `num_required_signatures`, at least one
//! writable signer, and the signer/readonly regions fit in the static keys. Anything a
//! leader can put in a replayable block passes.

use std::fmt;

const SIGNATURE_BYTES: usize = 64;
const KEY_BYTES: usize = 32;
const HASH_BYTES: usize = 32;
const MESSAGE_VERSION_PREFIX: u8 = 0x80;
const V0_PREFIX: u8 = MESSAGE_VERSION_PREFIX;
const V1_PREFIX: u8 = MESSAGE_VERSION_PREFIX | 1;

/// num_hashes + hash + transaction count.
const MIN_ENTRY_BYTES: usize = 8 + HASH_BYTES + 8;
/// Smallest sanitizable transaction: legacy, one signature (count byte + 64), header (3),
/// one key (count byte + 32), blockhash (32), zero instructions (1). v0 and v1 are larger.
const MIN_TRANSACTION_BYTES: usize = 1 + SIGNATURE_BYTES + 3 + 1 + KEY_BYTES + HASH_BYTES + 1;

// SIMD-0385 limits (`solana-message` 4.x `versions/v1`).
const V1_MAX_SIGNATURES: u8 = 12;
const V1_MAX_INSTRUCTIONS: u8 = 64;
const V1_MAX_ADDRESSES: u8 = 64;
const V1_MASK_PRIORITY_FEE: u32 = 0b11;
const V1_MASK_COMPUTE_UNIT_LIMIT: u32 = 0b100;
const V1_MASK_LOADED_ACCOUNTS_DATA_SIZE: u32 = 0b1000;
const V1_MASK_HEAP_SIZE: u32 = 0b1_0000;
const V1_MASK_KNOWN_BITS: u32 = V1_MASK_PRIORITY_FEE
    | V1_MASK_COMPUTE_UNIT_LIMIT
    | V1_MASK_LOADED_ACCOUNTS_DATA_SIZE
    | V1_MASK_HEAP_SIZE;

/// Alpenglow marker layout (`VersionedBlockMarker::V1`, u16 tag 1).
const MARKER_VERSION_V1: u16 = 1;
/// entry count (8) + marker version (2) + variant id (1) + variant byte length (2).
const MARKER_V1_HEADER_BYTES: usize = 8 + 2 + 1 + 2;

/// What a well-formed payload contains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchKind {
    /// A `Vec<Entry>` with at least one entry.
    Entries { entries: u64, transactions: u64 },
    /// An Alpenglow block marker (header, footer, parent update, genesis certificate).
    BlockMarker { variant: u8 },
    /// Entry count 0 with no marker: an empty batch (Alpenglow block abort) or the
    /// zero-filled buffer `Shredder::deshred` returns when no shred carried data.
    Empty,
}

/// Why a payload is not a well-formed batch. `at` is a byte offset into the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkError {
    UnexpectedEof {
        at: usize,
    },
    /// A declared entry or transaction count the remaining bytes cannot possibly hold.
    ImplausibleCount {
        at: usize,
        count: u64,
    },
    ShortU16 {
        at: usize,
    },
    /// First transaction byte is neither a one-byte signature count nor the v1 prefix.
    InvalidDiscriminator {
        at: usize,
        byte: u8,
    },
    /// Versioned message prefix other than v0 in the legacy/v0 position.
    UnsupportedMessageVersion {
        at: usize,
        byte: u8,
    },
    /// Header violates a sanitize invariant (signature count, fee payer, key regions).
    InvalidHeader {
        at: usize,
    },
    /// Unknown v1 config bits (their payload size is unknowable) or a partial fee pair.
    InvalidV1ConfigMask {
        at: usize,
        mask: u32,
    },
    /// Entry count 0 followed by something that is not a v1 block marker.
    InvalidBlockMarker {
        at: usize,
    },
    /// The parse ended at `at`, but a nonzero byte follows (only zero padding may).
    TrailingBytes {
        at: usize,
        len: usize,
    },
}

impl fmt::Display for WalkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { at } => write!(f, "unexpected end of payload at {at}"),
            Self::ImplausibleCount { at, count } => {
                write!(f, "implausible count {count} at {at}")
            }
            Self::ShortU16 { at } => write!(f, "invalid ShortU16 at {at}"),
            Self::InvalidDiscriminator { at, byte } => {
                write!(f, "invalid transaction discriminator {byte:#04x} at {at}")
            }
            Self::UnsupportedMessageVersion { at, byte } => {
                write!(f, "unsupported message version byte {byte:#04x} at {at}")
            }
            Self::InvalidHeader { at } => write!(f, "invalid message header at {at}"),
            Self::InvalidV1ConfigMask { at, mask } => {
                write!(f, "invalid v1 config mask {mask:#010b} at {at}")
            }
            Self::InvalidBlockMarker { at } => write!(f, "invalid block marker at {at}"),
            Self::TrailingBytes { at, len } => {
                write!(
                    f,
                    "parse ended at {at} of {len} bytes; nonzero bytes follow"
                )
            }
        }
    }
}

impl std::error::Error for WalkError {}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    #[inline(always)]
    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    #[inline(always)]
    fn skip(&mut self, n: usize) -> Result<(), WalkError> {
        if n > self.remaining() {
            return Err(WalkError::UnexpectedEof { at: self.pos });
        }
        self.pos += n;
        Ok(())
    }

    #[inline(always)]
    fn take(&mut self, n: usize) -> Result<&'a [u8], WalkError> {
        if n > self.remaining() {
            return Err(WalkError::UnexpectedEof { at: self.pos });
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    #[inline(always)]
    fn u8(&mut self) -> Result<u8, WalkError> {
        match self.bytes.get(self.pos) {
            Some(byte) => {
                self.pos += 1;
                Ok(*byte)
            }
            None => Err(WalkError::UnexpectedEof { at: self.pos }),
        }
    }

    #[inline(always)]
    fn u16_le(&mut self) -> Result<u16, WalkError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    #[inline(always)]
    fn u32_le(&mut self) -> Result<u32, WalkError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    #[inline(always)]
    fn u64_le(&mut self) -> Result<u64, WalkError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    /// Solana `ShortU16` with the same validation as `solana-short-vec`: at most three
    /// bytes, no non-minimal encodings, third byte terminates, value fits in u16.
    #[inline(always)]
    fn short_u16(&mut self) -> Result<usize, WalkError> {
        let at = self.pos;
        let first = self.u8()?;
        if first & 0x80 == 0 {
            return Ok(usize::from(first));
        }
        let second = self.u8()?;
        if second == 0 {
            return Err(WalkError::ShortU16 { at });
        }
        let value = usize::from(first & 0x7f) | (usize::from(second & 0x7f) << 7);
        if second & 0x80 == 0 {
            return Ok(value);
        }
        let third = self.u8()?;
        if third == 0 || third > 0x03 {
            // 0: non-minimal; > 3: continues or exceeds u16::MAX (2 + 7 + 7 bits).
            return Err(WalkError::ShortU16 { at });
        }
        Ok(value | (usize::from(third) << 14))
    }
}

/// Walk `payload` as one deshredded micro-batch. Succeeds only if the batch parses and
/// everything after it is zero padding; never allocates.
pub fn validate_batch(payload: &[u8]) -> Result<BatchKind, WalkError> {
    let mut c = Cursor {
        bytes: payload,
        pos: 0,
    };
    let entry_count = c.u64_le()?;
    if entry_count == 0 {
        return walk_marker(payload);
    }
    if entry_count > (c.remaining() / MIN_ENTRY_BYTES) as u64 {
        return Err(WalkError::ImplausibleCount {
            at: 0,
            count: entry_count,
        });
    }
    let mut transactions = 0u64;
    for _ in 0..entry_count {
        // num_hashes + hash
        c.skip(8 + HASH_BYTES)?;
        let at = c.pos;
        let tx_count = c.u64_le()?;
        if tx_count > (c.remaining() / MIN_TRANSACTION_BYTES) as u64 {
            return Err(WalkError::ImplausibleCount {
                at,
                count: tx_count,
            });
        }
        for _ in 0..tx_count {
            walk_transaction(&mut c)?;
        }
        transactions += tx_count;
    }
    if !all_zero(&payload[c.pos..]) {
        return Err(WalkError::TrailingBytes {
            at: c.pos,
            len: payload.len(),
        });
    }
    Ok(BatchKind::Entries {
        entries: entry_count,
        transactions,
    })
}

/// Every byte is zero. Checked 64 bytes at a time with a branch-free OR so a Firedancer
/// padding tail of ~30 KB costs about a microsecond.
#[inline]
fn all_zero(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(64);
    for chunk in &mut chunks {
        if chunk.iter().fold(0u8, |acc, b| acc | b) != 0 {
            return false;
        }
    }
    chunks.remainder().iter().all(|b| *b == 0)
}

/// Entry count 0: an Alpenglow block marker, or an empty batch.
fn walk_marker(payload: &[u8]) -> Result<BatchKind, WalkError> {
    if all_zero(&payload[8..]) {
        return Ok(BatchKind::Empty);
    }
    let mut c = Cursor {
        bytes: payload,
        pos: 8,
    };
    let version = c.u16_le()?;
    if version != MARKER_VERSION_V1 {
        return Err(WalkError::InvalidBlockMarker { at: 8 });
    }
    // BlockMarkerV1 is TLV so unknown variants stay skippable: id, u16 length, bytes.
    let variant = c.u8()?;
    let len = usize::from(c.u16_le()?);
    // The TLV must fit, and only padding may follow it.
    let end = MARKER_V1_HEADER_BYTES + len;
    if end > payload.len() || !all_zero(&payload[end..]) {
        return Err(WalkError::InvalidBlockMarker { at: 11 });
    }
    Ok(BatchKind::BlockMarker { variant })
}

#[inline(always)]
fn walk_transaction(c: &mut Cursor<'_>) -> Result<(), WalkError> {
    let at = c.pos;
    let discriminator = c.u8()?;
    if discriminator & MESSAGE_VERSION_PREFIX == 0 {
        // Legacy or v0: the byte is the canonical one-byte ShortU16 signature count.
        let num_signatures = discriminator;
        c.skip(usize::from(num_signatures) * SIGNATURE_BYTES)?;
        let message_at = c.pos;
        let first = c.u8()?;
        let (num_required_signatures, is_v0) = if first & MESSAGE_VERSION_PREFIX == 0 {
            (first, false)
        } else if first == V0_PREFIX {
            (c.u8()?, true)
        } else {
            return Err(WalkError::UnsupportedMessageVersion {
                at: message_at,
                byte: first,
            });
        };
        let num_readonly_signed = c.u8()?;
        let num_readonly_unsigned = c.u8()?;
        if num_required_signatures != num_signatures
            || num_readonly_signed >= num_required_signatures
        {
            return Err(WalkError::InvalidHeader { at: message_at });
        }
        let num_keys = c.short_u16()?;
        if usize::from(num_required_signatures) + usize::from(num_readonly_unsigned) > num_keys {
            return Err(WalkError::InvalidHeader { at: message_at });
        }
        c.skip(num_keys * KEY_BYTES + HASH_BYTES)?;
        let num_instructions = c.short_u16()?;
        for _ in 0..num_instructions {
            // program id index, account indexes, data
            c.skip(1)?;
            let num_accounts = c.short_u16()?;
            c.skip(num_accounts)?;
            let data_len = c.short_u16()?;
            c.skip(data_len)?;
        }
        if is_v0 {
            let num_lookups = c.short_u16()?;
            for _ in 0..num_lookups {
                c.skip(KEY_BYTES)?;
                let writable = c.short_u16()?;
                c.skip(writable)?;
                let readonly = c.short_u16()?;
                c.skip(readonly)?;
            }
        }
        Ok(())
    } else if discriminator == V1_PREFIX {
        let num_required_signatures = c.u8()?;
        let num_readonly_signed = c.u8()?;
        let num_readonly_unsigned = c.u8()?;
        if num_required_signatures > V1_MAX_SIGNATURES
            || num_readonly_signed >= num_required_signatures
        {
            return Err(WalkError::InvalidHeader { at });
        }
        let mask_at = c.pos;
        let mask = c.u32_le()?;
        let priority_fee_bits = mask & V1_MASK_PRIORITY_FEE;
        if mask & !V1_MASK_KNOWN_BITS != 0
            || (priority_fee_bits != 0 && priority_fee_bits != V1_MASK_PRIORITY_FEE)
        {
            return Err(WalkError::InvalidV1ConfigMask { at: mask_at, mask });
        }
        c.skip(HASH_BYTES)?; // lifetime specifier
        let num_instructions = c.u8()?;
        let num_addresses = c.u8()?;
        if num_instructions > V1_MAX_INSTRUCTIONS
            || num_addresses > V1_MAX_ADDRESSES
            || usize::from(num_required_signatures) + usize::from(num_readonly_unsigned)
                > usize::from(num_addresses)
        {
            return Err(WalkError::InvalidHeader { at });
        }
        let config_bytes = if priority_fee_bits == V1_MASK_PRIORITY_FEE {
            8
        } else {
            0
        } + if mask & V1_MASK_COMPUTE_UNIT_LIMIT != 0 {
            4
        } else {
            0
        } + if mask & V1_MASK_LOADED_ACCOUNTS_DATA_SIZE != 0 {
            4
        } else {
            0
        } + if mask & V1_MASK_HEAP_SIZE != 0 { 4 } else { 0 };
        c.skip(usize::from(num_addresses) * KEY_BYTES + config_bytes)?;
        // Packed (u8 program index, u8 account count, u16 LE data length) headers, then
        // every instruction's account indexes and data in the same order.
        let headers = c.take(usize::from(num_instructions) * 4)?;
        let payload_bytes: usize = headers
            .chunks_exact(4)
            .map(|h| usize::from(h[1]) + usize::from(u16::from_le_bytes([h[2], h[3]])))
            .sum();
        c.skip(payload_bytes)?;
        c.skip(usize::from(num_required_signatures) * SIGNATURE_BYTES)
    } else {
        Err(WalkError::InvalidDiscriminator {
            at,
            byte: discriminator,
        })
    }
}

/// Real mainnet transactions from slot 447963115 (2026-09-18), fetched over RPC with
/// `encoding: base64, maxSupportedTransactionVersion: 1`. Copied from arb_bot
/// `src/integrations/shredstream/entry_decode.rs` `test_fixtures`.
#[cfg(test)]
pub(crate) mod test_fixtures {
    use base64::Engine;

    /// v1, 1 signature, 8 addresses, 3 instructions, fee + CU limit + loaded-data config.
    pub(crate) const V1_SMALL_B64: &str = "gQEABQ8AAABkUKHlJex71sD7WHa4drbcof35nNf39ic7UTuwELW4eAMIDQQy4ipPfDn3XTBx5zQxW7jPYOraWNnQXVAjsr4neADIgkaK+mgFQmpMbN0yUcXnMMgv83TppzMO+FRHvUalWPUzWqeGSqnxhO3xsWEIiO+R0A2OMLIMRJcNNgpgpy/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGp9UXGSxWjuCKhF9z0peIzwNcMUWyGrNE2AYuqUAAALlIs5Rf9vPrrCv+PPKR0viTOMMUVIjqhy6DG6uCSAy5znDW+1aOQ+v8lkqQFWw76VY7CBcGfKtpTryD792zfoboVybAsrW8+vZmi3wbg2n7AcmYX8wWfwtCmb8dfR7CpTYAAAAAAAAAsGgAAICWmAADAwQABgUrAAYFqgICBAAEAAAABgcABQEtm8g0Jda7jIgoAAAAAAAAAABteoOyoAEAAGUAEwCAAIAAgG16g7KgAQAABgcABQHxW4uqEinLFgoAAAABAAAAAAAAAAIAAAAAAAAAYhoAAGFLAAAPAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAA5XgAAIuIAAA8AAAAAAAAAAwAAAAAAAAAEAAAAAAAAAM38AQAAAAAADwAAAAAAAAAHAAAAAAAAAAgAAAAAAAAAPeoCAAAAAAAPAAAAAAAAAAsAAAAAAAAADAAAAAAAAAA9CQIAAAAAAA8AAAAAAAAADwAAAAAAAAAQAAAAAAAAAGRyAgAAAAAADwAAAAAAAAAUAAAAAAAAABUAAAAAAAAA8dkDAAAAAAAPAAAAAAAAABgAAAAAAAAAGQAAAAAAAACiIwoAAAAAAA8AAAAAAAAAHgAAAAAAAAAoAAAAAAAAAOpSAAAAAAAADwAAAAAAAACsAAAAAAAAAAkBAAAAAAAAOi0AAAAAAAA8AAAAAAAAAAoAAAABAAAAAAAAAAIAAAAAAAAAYhoAAGFLAAAPAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAA5XgAAIuIAAA8AAAAAAAAAAwAAAAAAAAAEAAAAAAAAAM38AQAAAAAADwAAAAAAAAAHAAAAAAAAAAgAAAAAAAAAPeoCAAAAAAAPAAAAAAAAAAsAAAAAAAAADAAAAAAAAAA9CQIAAAAAAA8AAAAAAAAADwAAAAAAAAAQAAAAAAAAAGRyAgAAAAAADwAAAAAAAAAUAAAAAAAAABUAAAAAAAAA8dkDAAAAAAAPAAAAAAAAABgAAAAAAAAAGQAAAAAAAACiIwoAAAAAAA8AAAAAAAAAHgAAAAAAAAAoAAAAAAAAAOpSAAAAAAAADwAAAAAAAACsAAAAAAAAAAkBAAAAAAAAOi0AAAAAAAA8AAAAAAAAAABteoOyoAEAAGUAEwCAAIAAgG16g7KgAQAAjB8u/qXS8JEVuJ0n8nvvFNYngsJSqPZT+utylRbnYchJwuxgDnz9huI5SmP+/r/TIEE8nwmr0uRdMCfK7XjkAw==";
    /// v1 above the 1232-byte legacy limit: 63 addresses, 2 instructions.
    pub(crate) const V1_LARGE_B64: &str = "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";
    /// v0 with address-table lookups.
    pub(crate) const V0_B64: &str = "AXUIAsrNGj8AE+TE//rHvIc+UkH5YfYtexcUfRHrS1rSRmK9wIaIi2+6UvUGP0a9d6N3rw5zo5fpgb1jT40M/wCAAQADCUuYF3gC2Os11et6KniuH+M11xrGCGWn4EY4BVI7+9HAICYQHsIDKJZKMqurE2xUBbkfOuOO5PZMtr3oebhoONJWmQKWFXfOipQt6Q+t/iUZxp089sFeqI8h7Ikq3UdqpyrRa1ravYjC9jBC/GDi4uvG6q5BRrV2uUxgyFNPgyoWtkkLuX89UJmlw5qHL3flwTNsfe74zctiArD0um6a4fXIKnojqYdjPSp5fO9vA5uqlVMm0PWeRMkgQ/oZPo/OOQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwZGb+UhFzL/7K26csOb57yM5bvF9xJrLEObOkAAAAD4VRO0ZkTnkh10Fh0gn10wGVxzrU+f3g1DV/NLl09+GEdxUWUTLdW9QmP/zzt2XmX3unLxYsg8cUmtyEWoYTRxBAcABQIyewUABgIAAQwCAAAAD2sAAAAAAAAIWgACJygGKQMqKywJLQoLLgwGKS8NMDEEMjM0NQ42DxAREjcXFxc4NhMUFRY3GBgYOAU5KywZLRobLgwGKS8cOjEEMjM7NQ42HR4fIDchISE4NiIjJCU3JiYmOBMUAAIAAAAAAAAAAAMAAgIDAAICBwAFBP0m9QAESXR7CT/e2jxBsfVw/eZBvctZBRlaGIppIljX1viZhjoO5OflIOI86ero6+7s7e8SAQIc3gwUGx0j4x8YFeY3AAgL1/4T1BoLiiijqdSmQnTGNkf2J6JsQ3gqnvA1tH1ON1gBCAD66T76s8pYF5g0PYxhfiEu3QUSzB2Lp85yb3Ux2oP/FQGLAAS2n2ALsA7jsb89Uaz2t4HevztYWJJ4TJdaQinOWRyeDgsQDAkWGRgUFwcKDggGAxIREw==";
    /// Legacy vote-style transaction.
    pub(crate) const LEGACY_B64: &str = "AeLmIX2Ihypz2IabgSoxqNcuiIW8Vd8F39k4nCFs1/RcuwtUQeRb/CGocbtFqQbk/4K6QFmQvLO2EEALmGRG/g0BAAEDg5vp2y2IjjFPrINJADiGTV3x0uH2+7x+j/pAXM1Ijbn4biDRQ+ZqOQ7kwmGBMSsSX/0s4kyf0D79E6/YZ0bSYQdhSB01dHS7fE12JOvTvbPYNV5z0RBD/A2jU4AAAAAAlw1ugdJPBuyET54b6AsiamUckT2DL9Hp1PAbohz0Ko4BAgIBAJQBDgAAAMtfsxoAAAAAHwEfAR4BHQEcARsBGgEZARgBFwEWARUBFAETARIBEQEQAQ8BDgENAQwBCwEKAQkBCAEHAQYBBQEEAQMBAgEBC+vMV0JVAAmNIXlPA9cE9rkR4N0Q7V7eeP7U64XTlZQB7KysagAAAAATH2IF/0JJdJhMNQaDxofGqLEdVgJlHh1GRrzuowaAOQ==";

    pub(crate) fn decode(b64: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap()
    }

    /// The four real transactions above, in (legacy, v0, v1 small, v1 large) order.
    pub(crate) fn real_transactions() -> [Vec<u8>; 4] {
        [
            decode(LEGACY_B64),
            decode(V0_B64),
            decode(V1_SMALL_B64),
            decode(V1_LARGE_B64),
        ]
    }

    /// One entry: (num_hashes, hash, raw serialized transactions).
    pub(crate) type EntrySpec<'a> = (u64, [u8; 32], &'a [&'a [u8]]);

    /// Serialize raw transaction byte strings as one bincode `Vec<Entry>` micro-batch.
    pub(crate) fn batch_of(entries: &[EntrySpec]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (num_hashes, hash, txs) in entries {
            out.extend_from_slice(&num_hashes.to_le_bytes());
            out.extend_from_slice(hash);
            out.extend_from_slice(&(txs.len() as u64).to_le_bytes());
            for tx in *txs {
                out.extend_from_slice(tx);
            }
        }
        out
    }

    /// Alpenglow `BlockComponent::BlockMarker(VersionedBlockMarker::V1(BlockHeader(..)))`
    /// as agave's wincode schema writes it: entry count 0, u16 marker version 1, u8 variant
    /// 1 (BlockHeader), u16 length, then `VersionedBlockHeader::V1` (u8 tag 1, parent slot,
    /// parent block id).
    pub(crate) fn block_header_marker(parent_slot: u64, parent_block_id: [u8; 32]) -> Vec<u8> {
        let mut inner = vec![1u8];
        inner.extend_from_slice(&parent_slot.to_le_bytes());
        inner.extend_from_slice(&parent_block_id);
        let mut out = Vec::new();
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(1);
        out.extend_from_slice(&(inner.len() as u16).to_le_bytes());
        out.extend_from_slice(&inner);
        out
    }
}

#[cfg(test)]
mod tests {
    use solana_entry::entry::Entry;
    use solana_sdk::{
        hash::Hash,
        instruction::CompiledInstruction,
        message::{v0, v0::MessageAddressTableLookup, MessageHeader, VersionedMessage},
        pubkey::Pubkey,
        signature::Signature,
        transaction::VersionedTransaction,
    };

    use super::{test_fixtures::*, *};

    fn sample_v0_tx(seed: u8) -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::from([seed; 64]), Signature::from([seed ^ 1; 64])],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 2,
                    num_readonly_signed_accounts: 1,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: (0..3u8)
                    .map(|i| Pubkey::new_from_array([seed ^ (i + 9); 32]))
                    .collect(),
                recent_blockhash: Hash::new_from_array([seed.wrapping_add(3); 32]),
                instructions: vec![CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 3, 4, 5],
                    // > 127 bytes forces a two-byte ShortU16
                    data: vec![seed; 300],
                }],
                address_table_lookups: vec![MessageAddressTableLookup {
                    account_key: Pubkey::new_from_array([seed ^ 0x55; 32]),
                    writable_indexes: vec![7, 8],
                    readonly_indexes: vec![9],
                }],
            }),
        }
    }

    #[test]
    fn real_mixed_version_batch_walks_exactly() {
        let [legacy, v0, v1_small, v1_large] = real_transactions();
        let bytes = batch_of(&[
            (5, [9; 32], &[&legacy, &v1_small, &v0, &v1_large]),
            (12_500, [8; 32], &[]),
            (6, [7; 32], &[&v1_large, &legacy]),
        ]);
        assert_eq!(
            validate_batch(&bytes),
            Ok(BatchKind::Entries {
                entries: 3,
                transactions: 6
            })
        );
    }

    #[test]
    fn bincode_entries_walk_exactly() {
        let entries = vec![
            Entry {
                num_hashes: 1,
                hash: Hash::new_from_array([1; 32]),
                transactions: vec![sample_v0_tx(1), sample_v0_tx(2)],
            },
            Entry {
                num_hashes: 0,
                hash: Hash::new_from_array([2; 32]),
                transactions: vec![],
            },
        ];
        let bytes = bincode::serialize(&entries).unwrap();
        assert_eq!(
            validate_batch(&bytes),
            Ok(BatchKind::Entries {
                entries: 2,
                transactions: 2
            })
        );
    }

    #[test]
    fn every_truncation_and_nonzero_extension_is_rejected() {
        let [legacy, v0, v1_small, v1_large] = real_transactions();
        let bytes = batch_of(&[(5, [9; 32], &[&legacy, &v1_small, &v0, &v1_large])]);
        for cut in 0..bytes.len() {
            assert!(validate_batch(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        for tail in [&[1u8][..], &[0, 0, 0, 7], &[0xff; 100]] {
            let mut longer = bytes.clone();
            longer.extend_from_slice(tail);
            assert!(matches!(
                validate_batch(&longer),
                Err(WalkError::TrailingBytes { .. })
            ));
        }
    }

    #[test]
    fn firedancer_zero_padding_is_accepted() {
        // fd_shred_tile.c pads each batch with zeros to a multiple of the FEC-set payload
        // (30816 bytes chained, 28768 resigned); the padding is inside the shred data.
        let [legacy, v0, v1_small, v1_large] = real_transactions();
        let bytes = batch_of(&[(5, [9; 32], &[&legacy, &v1_small, &v0, &v1_large])]);
        for fec_payload in [30_816usize, 28_768] {
            let mut padded = bytes.clone();
            padded.resize(bytes.len().next_multiple_of(fec_payload), 0);
            assert_eq!(
                validate_batch(&padded),
                Ok(BatchKind::Entries {
                    entries: 1,
                    transactions: 4
                })
            );
            // A single nonzero byte anywhere in the padding is not padding.
            let last = padded.len() - 1;
            padded[last] = 1;
            assert!(validate_batch(&padded).is_err());
        }
        let mut marker = block_header_marker(1, [2; 32]);
        marker.resize(30_816, 0);
        assert_eq!(
            validate_batch(&marker),
            Ok(BatchKind::BlockMarker { variant: 1 })
        );
    }

    #[test]
    fn every_misaligned_start_is_rejected() {
        // What a wrong unknown-start guess looks like: the tail of a real batch.
        let [legacy, v0, v1_small, v1_large] = real_transactions();
        let bytes = batch_of(&[
            (5, [9; 32], &[&legacy, &v1_small, &v0, &v1_large]),
            (6, [7; 32], &[&v0, &legacy, &v1_small]),
        ]);
        for skip in 1..bytes.len() {
            assert!(validate_batch(&bytes[skip..]).is_err(), "skip {skip}");
        }
    }

    #[test]
    fn header_invariants_and_versions_are_enforced() {
        let [legacy, v0, v1_small, _] = real_transactions();
        let wrap = |tx: &[u8]| batch_of(&[(1, [0; 32], &[tx])]);
        assert!(validate_batch(&wrap(&legacy)).is_ok());

        // Signature count disagreeing with the header (legacy: sig count at 0, header at 65).
        let mut bad = legacy.clone();
        bad[65] = 2;
        assert!(matches!(
            validate_batch(&wrap(&bad)),
            Err(WalkError::InvalidHeader { .. })
        ));
        // Unknown versioned message prefix in the legacy/v0 slot.
        let mut bad = v0.clone();
        bad[65] = 0x82;
        assert!(matches!(
            validate_batch(&wrap(&bad)),
            Err(WalkError::UnsupportedMessageVersion { byte: 0x82, .. })
        ));
        // Unknown transaction discriminator.
        let mut bad = v1_small.clone();
        bad[0] = 0x82;
        assert!(matches!(
            validate_batch(&wrap(&bad)),
            Err(WalkError::InvalidDiscriminator { byte: 0x82, .. })
        ));
        // v1 config mask with an unknown bit / a partial priority-fee pair.
        let mut bad = v1_small.clone();
        bad[4] |= 0b10_0000;
        assert!(matches!(
            validate_batch(&wrap(&bad)),
            Err(WalkError::InvalidV1ConfigMask { .. })
        ));
        let mut bad = v1_small.clone();
        bad[4] &= !0b10;
        assert!(matches!(
            validate_batch(&wrap(&bad)),
            Err(WalkError::InvalidV1ConfigMask { .. })
        ));
        // Non-minimal ShortU16 key count (legacy: 1 + 64 + 3 header bytes = offset 68).
        let mut bad = legacy.clone();
        assert_eq!(bad[68], 3);
        bad[68] = 0x83;
        bad.insert(69, 0x00);
        assert!(matches!(
            validate_batch(&wrap(&bad)),
            Err(WalkError::ShortU16 { .. })
        ));
    }

    #[test]
    fn short_u16_matches_solana_encoding() {
        let read = |bytes: &[u8]| {
            let mut c = Cursor { bytes, pos: 0 };
            c.short_u16().map(|v| (v, c.pos))
        };
        assert_eq!(read(&[0x00]), Ok((0, 1)));
        assert_eq!(read(&[0x7f]), Ok((127, 1)));
        assert_eq!(read(&[0x80, 0x01]), Ok((128, 2)));
        assert_eq!(read(&[0xff, 0x7f]), Ok((16_383, 2)));
        assert_eq!(read(&[0x80, 0x80, 0x01]), Ok((16_384, 3)));
        assert_eq!(read(&[0xff, 0xff, 0x03]), Ok((65_535, 3)));
        assert!(read(&[0xff, 0xff, 0x04]).is_err()); // > u16::MAX
        assert!(read(&[0x80, 0x00]).is_err()); // alias of 0
        assert!(read(&[0x80, 0x80, 0x00]).is_err()); // alias
        assert!(read(&[0x80, 0x80, 0x80]).is_err()); // third byte continues
        assert!(read(&[0x80]).is_err()); // truncated
                                         // Cross-check every value against the reference encoder.
        for value in 0..=u16::MAX {
            let mut encoded = Vec::new();
            let mut rem = value;
            loop {
                let mut byte = (rem & 0x7f) as u8;
                rem >>= 7;
                if rem != 0 {
                    byte |= 0x80;
                }
                encoded.push(byte);
                if rem == 0 {
                    break;
                }
            }
            assert_eq!(read(&encoded), Ok((usize::from(value), encoded.len())));
        }
    }

    #[test]
    fn block_markers_and_empty_batches() {
        let marker = block_header_marker(447_963_114, [3; 32]);
        assert_eq!(
            validate_batch(&marker),
            Ok(BatchKind::BlockMarker { variant: 1 })
        );
        // Unknown TLV variant with a consistent length is still a well-formed marker.
        let mut other = marker.clone();
        other[10] = 9;
        assert_eq!(
            validate_batch(&other),
            Ok(BatchKind::BlockMarker { variant: 9 })
        );
        // TLV longer than the payload, nonzero bytes after it, or an unknown marker version.
        assert!(validate_batch(&marker[..marker.len() - 1]).is_err());
        let mut longer = marker.clone();
        longer.push(1);
        assert!(validate_batch(&longer).is_err());
        let mut bad_version = marker.clone();
        bad_version[8] = 2;
        assert!(matches!(
            validate_batch(&bad_version),
            Err(WalkError::InvalidBlockMarker { .. })
        ));
        // Empty batch (Alpenglow abort) and deshred's zero-filled no-data buffer.
        assert_eq!(validate_batch(&0u64.to_le_bytes()), Ok(BatchKind::Empty));
        assert_eq!(validate_batch(&[0u8; 1015]), Ok(BatchKind::Empty));
        assert!(validate_batch(&[0u8; 7]).is_err());
        assert!(validate_batch(&[]).is_err());
    }

    #[test]
    fn garbage_and_absurd_counts_are_rejected_without_panicking() {
        let mut huge = u64::MAX.to_le_bytes().to_vec();
        huge.extend_from_slice(&[0xAB; 64]);
        assert!(matches!(
            validate_batch(&huge),
            Err(WalkError::ImplausibleCount { .. })
        ));
        // One entry claiming u64::MAX transactions.
        let mut bytes = 1u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0; 40]);
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        bytes.extend_from_slice(&[0x11; 200]);
        assert!(matches!(
            validate_batch(&bytes),
            Err(WalkError::ImplausibleCount { .. })
        ));
        // Deterministic pseudo-random garbage never validates (and never panics).
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut buf = vec![0u8; 4096];
        for len in [9usize, 64, 300, 1203, 4096] {
            for _ in 0..2_000 {
                for b in buf.iter_mut().take(len) {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    *b = state as u8;
                }
                assert!(validate_batch(&buf[..len]).is_err());
            }
        }
    }
}

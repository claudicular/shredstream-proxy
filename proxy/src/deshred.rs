use std::{
    collections::HashSet,
    hash::Hash,
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use itertools::Itertools;
use jito_protos::shredstream::TraceShred;
use log::{debug, warn};
use prost::Message;
use solana_ledger::{
    blockstore::MAX_DATA_SHREDS_PER_SLOT,
    shred::{
        merkle::{Shred, ShredCode},
        ReedSolomonCache, ShredType, Shredder,
    },
};
use solana_metrics::datapoint_warn;
use solana_sdk::clock::{Slot, MAX_PROCESSING_AGE};

use crate::{
    entry_walk::{self, BatchKind},
    forwarder::ShredMetrics,
};

#[derive(Default, Debug, Copy, Clone, Eq, PartialEq)]
enum ShredStatus {
    #[default]
    Unknown,
    /// Shred that is **not** marked as [ShredFlags::DATA_COMPLETE_SHRED]
    NotDataComplete,
    /// Shred that is marked as [ShredFlags::DATA_COMPLETE_SHRED]
    DataComplete,
}

/// Tracks per-slot shred information for data shreds
/// Guaranteed to have MAX_DATA_SHREDS_PER_SLOT entries in each Vec
#[derive(Debug)]
pub struct ShredsStateTracker {
    /// Compact status of each data shred for fast iteration.
    data_status: Vec<ShredStatus>,
    /// Data shreds received for the slot (not coding!)
    data_shreds: Vec<Option<Shred>>,
    /// array of bools that track which FEC set indexes have been already recovered
    already_recovered_fec_sets: Vec<bool>,
    /// array of bools that track which data shred indexes have been already deshredded
    already_deshredded: Vec<bool>,
    /// Bitset of data shred indexes at which a guessed batch start was tried and rejected
    /// (failed the structural walk or failed to deshred). The payload of a guessed batch is
    /// fixed until the shred before its start arrives, and that arrival changes the
    /// candidate start, so a set bit means "retrying this exact guess is pointless".
    rejected_guess_starts: Vec<u64>,
}
impl Default for ShredsStateTracker {
    fn default() -> Self {
        Self {
            data_status: vec![ShredStatus::Unknown; MAX_DATA_SHREDS_PER_SLOT],
            data_shreds: vec![None; MAX_DATA_SHREDS_PER_SLOT],
            already_recovered_fec_sets: vec![false; MAX_DATA_SHREDS_PER_SLOT],
            already_deshredded: vec![false; MAX_DATA_SHREDS_PER_SLOT],
            rejected_guess_starts: vec![0; MAX_DATA_SHREDS_PER_SLOT.div_ceil(64)],
        }
    }
}

impl ShredsStateTracker {
    fn is_rejected_guess(&self, start: usize) -> bool {
        self.rejected_guess_starts[start / 64] & (1 << (start % 64)) != 0
    }

    fn reject_guess(&mut self, start: usize) {
        self.rejected_guess_starts[start / 64] |= 1 << (start % 64);
    }
}

/// Clears every bit in `start..=end` of a rejected-guess bitset; true if any was set, i.e.
/// the batch being emitted had been held back by a rejected guess. A free function so it
/// can run while the tracker's `data_shreds` is borrowed.
fn take_rejected_guesses(bits: &mut [u64], start: usize, end: usize) -> bool {
    let (first, last) = (start / 64, end / 64);
    let mut any = false;
    for (word, value) in bits.iter_mut().enumerate().take(last + 1).skip(first) {
        let lo = if word == first { start % 64 } else { 0 };
        let hi = if word == last { end % 64 } else { 63 };
        let mask = (u64::MAX >> (63 - hi)) & (u64::MAX << lo);
        any |= *value & mask != 0;
        *value &= !mask;
    }
    any
}

/// [`reconstruct_shred_payloads`] over a `PacketBatch`.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub fn reconstruct_shreds(
    packet_batch: solana_perf::packet::PacketBatch,
    all_shreds: &mut SlotShreds,
    slot_fec_indexes_to_iterate: &mut Vec<(Slot, u32)>,
    deshredded_entries: &mut Vec<(Slot, Vec<u8>)>,
    // Parallel to `deshredded_entries` (same order/length): the inclusive composing
    // data-shred index range `(start, end)` and whether the left boundary was guessed
    // (`unknown_start`), for pipeline-latency attribution. Filled unconditionally
    // (negligible); the reconstruct thread ignores it when pipeline latency is off.
    entry_ranges: &mut Vec<(u32, u32, bool)>,
    highest_slot_seen: &mut Slot,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
) -> usize {
    reconstruct_shred_payloads(
        packet_batch.iter().filter_map(|p| p.data(..)),
        all_shreds,
        slot_fec_indexes_to_iterate,
        deshredded_entries,
        entry_ranges,
        highest_slot_seen,
        rs_cache,
        metrics,
    )
}

/// Per-slot shred state: FEC-set shred sets plus the data-shred tracker.
pub type SlotShreds = ahash::HashMap<
    Slot,
    (
        ahash::HashMap<u32 /* fec_set_index */, HashSet<ComparableShred>>,
        ShredsStateTracker,
    ),
>;

/// Returns the number of shreds reconstructed
/// Updates all_shreds with current state, and deshredded_entries with returned values
/// receive shreds per FEC set, attempting to recover the other shreds in the fec set so you do not have to wait until all data shreds have arrived.
/// every time a fec is recovered, scan for neighbouring DATA_COMPLETE_SHRED flags in the shreds, attempting to deserialize into solana entries when there are no missing shreds between the DATA_COMPLETE_SHRED flags.
/// note that an FEC set doesn't necessarily contain DATA_COMPLETE_SHRED in the last shred. when deserializing the bincode data, you must use data between shreds starting at the last DATA_COMPLETE_SHRED (not inclusive) to the next DATA_COMPLETE_SHRED (inclusive)
///
/// Batch starts. A batch whose preceding shred is DATA_COMPLETE (or that starts at index 0)
/// has a proven start and is emitted as soon as it is contiguous. When the preceding shred
/// is still missing the start is a guess. Leaders serialize and shred every batch on its
/// own, so a batch always begins on an FEC-set boundary: a guess inside an FEC set is always
/// wrong and is never emitted (the set's missing shred arrives or is recovered first). A
/// guess on a boundary is right only if the previous set ended a batch, so it is emitted
/// only if the payload passes a structural walk that must end where the payload's nonzero
/// bytes end (`entry_walk::validate_batch`). A rejected guess marks nothing as consumed:
/// the batch is emitted later, whole, once its real start resolves. `rejected_guess_starts`
/// stops the same guess from being re-deshredded on every later shred of the slot.
///
/// `slot_fec_indexes_to_iterate` is scratch space; after recovery it also holds the index
/// after every newly known DATA_COMPLETE shred, whose batch just gained a proven start.
///
/// Takes raw shred payloads (one received datagram each), so a caller that owns its
/// receive buffers needs no `PacketBatch`.
#[allow(clippy::too_many_arguments)]
pub fn reconstruct_shred_payloads<'a>(
    payloads: impl IntoIterator<Item = &'a [u8]>,
    all_shreds: &mut SlotShreds,
    slot_fec_indexes_to_iterate: &mut Vec<(Slot, u32)>,
    deshredded_entries: &mut Vec<(Slot, Vec<u8>)>,
    entry_ranges: &mut Vec<(u32, u32, bool)>,
    highest_slot_seen: &mut Slot,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
) -> usize {
    deshredded_entries.clear();
    entry_ranges.clear();
    slot_fec_indexes_to_iterate.clear();
    // (slot, index after a newly known DATA_COMPLETE data shred): batches whose start just
    // became proven. Usually empty or one element per packet batch.
    let mut proven_starts = Vec::<(Slot, u32)>::new();
    // ingest all packets
    for packet in payloads {
        match solana_ledger::shred::Shred::new_from_serialized_shred(packet.to_vec())
            .and_then(Shred::try_from)
        {
            Ok(shred) => {
                let slot = shred.common_header().slot;
                let index = shred.index() as usize;
                let fec_set_index = shred.fec_set_index();
                let (all_shreds, state_tracker) = all_shreds.entry(slot).or_default();
                if highest_slot_seen.saturating_sub(SLOT_LOOKBACK) > slot {
                    debug!(
                        "Old shred slot: {slot}, fec_set_index: {fec_set_index}, index: {index}"
                    );
                    continue;
                }
                if state_tracker.already_recovered_fec_sets[fec_set_index as usize]
                    || state_tracker.already_deshredded[index]
                {
                    debug!("Already completed slot: {slot}, fec_set_index: {fec_set_index}, index: {index}");
                    continue;
                }
                let Some(_shred_index) = update_state_tracker(&shred, state_tracker) else {
                    continue;
                };
                if shred.shred_type() == ShredType::Data
                    && state_tracker.data_status[index] == ShredStatus::DataComplete
                {
                    proven_starts.push((slot, index as u32 + 1));
                }

                all_shreds
                    .entry(fec_set_index)
                    .or_default()
                    .insert(ComparableShred(shred));
                slot_fec_indexes_to_iterate.push((slot, fec_set_index)); // use Vec so we can sort to make sure if any earlier FEC sets have DATA_SHRED_COMPLETE, later entries can use the flag to find the bounds
                *highest_slot_seen = std::cmp::max(*highest_slot_seen, slot);
            }
            Err(e) => {
                if TraceShred::decode(packet).is_ok() {
                    continue;
                }
                warn!("Failed to decode shred. Err: {e:?}");
            }
        }
    }
    slot_fec_indexes_to_iterate.sort_unstable();
    slot_fec_indexes_to_iterate.dedup();

    // try recovering by FEC set
    // already checked if FEC set is completed or deserialized
    let mut total_recovered_count = 0;
    for (slot, fec_set_index) in slot_fec_indexes_to_iterate.iter() {
        let (all_shreds, state_tracker) = all_shreds.entry(*slot).or_default();
        let shreds = all_shreds.entry(*fec_set_index).or_default();
        let (
            num_expected_data_shreds,
            num_expected_coding_shreds,
            num_data_shreds,
            num_coding_shreds,
        ) = get_data_shred_info(shreds);

        // haven't received last data shred, haven't seen any coding shreds, so wait until more arrive
        let min_shreds_needed_to_recover = num_expected_data_shreds as usize;
        if num_expected_data_shreds == 0
            || shreds.len() < min_shreds_needed_to_recover
            || num_data_shreds == num_expected_data_shreds
        {
            continue;
        }

        // try to recover if we have enough shreds in the FEC set
        let merkle_shreds = shreds
            .iter()
            .sorted_by_key(|s| (u8::MAX - s.shred_type() as u8, s.index()))
            .map(|s| s.0.clone())
            .collect_vec();
        let recovered = match solana_ledger::shred::merkle::recover(merkle_shreds, rs_cache) {
            Ok(r) => r, // data shreds followed by code shreds (whatever was missing from to_deshred_payload)
            Err(e) => {
                warn!(
                    "Failed to recover shreds for slot {slot} fec_set_index {fec_set_index}. num_expected_data_shreds: {num_expected_data_shreds}, num_data_shreds: {num_data_shreds} num_expected_coding_shreds: {num_expected_coding_shreds} num_coding_shreds: {num_coding_shreds} Err: {e}",
                );
                continue;
            }
        };

        let mut fec_set_recovered_count = 0;
        for shred in recovered {
            match shred {
                Ok(shred) => {
                    let Some(index) = update_state_tracker(&shred, state_tracker) else {
                        continue; // already seen before in state tracker
                    };
                    if shred.shred_type() == ShredType::Data
                        && state_tracker.data_status[index] == ShredStatus::DataComplete
                    {
                        proven_starts.push((*slot, index as u32 + 1));
                    }
                    // shreds.insert(ComparableShred(shred)); // optional since all data shreds are in state_tracker
                    total_recovered_count += 1;
                    fec_set_recovered_count += 1;
                }
                Err(e) => warn!(
                    "Failed to recover shred for slot {slot}, fec set: {fec_set_index}. Err: {e}"
                ),
            }
        }

        if fec_set_recovered_count > 0 {
            debug!("recovered slot: {slot}, fec_index: {fec_set_index}, recovered count: {fec_set_recovered_count}");
            state_tracker.already_recovered_fec_sets[*fec_set_index as usize] = true;
            shreds.clear();
        }
    }

    // Candidates for emission: every touched FEC set, plus every batch whose start was just
    // proven by a newly known DATA_COMPLETE shred. The latter matters when that batch is
    // not touched in this packet batch (e.g. it was held back by a rejected guess).
    if !proven_starts.is_empty() {
        slot_fec_indexes_to_iterate.extend(proven_starts);
        slot_fec_indexes_to_iterate.sort_unstable();
        slot_fec_indexes_to_iterate.dedup();
    }

    // deshred; validate guessed starts
    for (slot, candidate_index) in slot_fec_indexes_to_iterate.iter() {
        let Some((_all_shreds, state_tracker)) = all_shreds.get_mut(slot) else {
            continue;
        };
        let Some((start_data_complete_idx, end_data_complete_idx, unknown_start)) =
            get_indexes(state_tracker, *candidate_index as usize)
        else {
            continue;
        };
        if unknown_start {
            // A batch always starts on an FEC-set boundary; a guess inside a set is wrong.
            let on_fec_boundary = state_tracker.data_shreds[start_data_complete_idx]
                .as_ref()
                .is_some_and(|s| s.fec_set_index() as usize == start_data_complete_idx);
            if !on_fec_boundary {
                metrics
                    .unknown_start_mid_fec_count
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if state_tracker.is_rejected_guess(start_data_complete_idx) {
                metrics
                    .unknown_start_retry_skipped_count
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            metrics
                .unknown_start_position_count
                .fetch_add(1, Ordering::Relaxed);
        }

        let to_deshred =
            &state_tracker.data_shreds[start_data_complete_idx..=end_data_complete_idx];
        let deshredded_payload = match Shredder::deshred(
            to_deshred.iter().map(|s| s.as_ref().unwrap().payload()),
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!("slot {slot} failed to deshred slot: {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}. Err: {e}");
                metrics
                    .fec_recovery_error_count
                    .fetch_add(1, Ordering::Relaxed);
                if unknown_start {
                    metrics
                        .unknown_start_position_error_count
                        .fetch_add(1, Ordering::Relaxed);
                    state_tracker.reject_guess(start_data_complete_idx);
                }
                continue;
            }
        };

        if unknown_start {
            let started = Instant::now();
            let walk = entry_walk::validate_batch(&deshredded_payload);
            record_walk_time(metrics, started);
            match walk {
                Ok(kind) => {
                    metrics
                        .unknown_start_validated_count
                        .fetch_add(1, Ordering::Relaxed);
                    record_batch_kind(metrics, kind);
                }
                Err(e) => {
                    // Most likely the guess landed inside a multi-set batch. Keep every
                    // shred; the whole batch is emitted once the real start resolves.
                    metrics
                        .unknown_start_invalid_count
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        "slot {slot}: guessed batch start {start_data_complete_idx}..={end_data_complete_idx} rejected ({} bytes): {e}",
                        deshredded_payload.len()
                    );
                    state_tracker.reject_guess(start_data_complete_idx);
                    continue;
                }
            }
        }

        // Read entry count from bincode header (first 8 bytes = Vec length as little-endian u64)
        // Skip full deserialization — consumer deserializes on its end
        if deshredded_payload.len() >= 8 {
            let entry_count = u64::from_le_bytes(deshredded_payload[..8].try_into().unwrap());
            metrics
                .entry_count
                .fetch_add(entry_count, Ordering::Relaxed);
        }

        if take_rejected_guesses(
            &mut state_tracker.rejected_guess_starts,
            start_data_complete_idx,
            end_data_complete_idx,
        ) {
            metrics
                .held_batch_emitted_count
                .fetch_add(1, Ordering::Relaxed);
            debug!("slot {slot}: held batch {start_data_complete_idx}..={end_data_complete_idx} emitted after its start resolved");
        }

        deshredded_entries.push((*slot, deshredded_payload));
        entry_ranges.push((
            start_data_complete_idx as u32,
            end_data_complete_idx as u32,
            unknown_start,
        ));
        // Before marking: a composing set already flagged here was completed by the RS
        // recovery above (a batch shares no FEC set with another batch).
        let needed_recovery = to_deshred
            .iter()
            .flatten()
            .any(|shred| state_tracker.already_recovered_fec_sets[shred.fec_set_index() as usize]);
        to_deshred.iter().for_each(|shred| {
            let Some(shred) = shred.as_ref() else {
                return;
            };
            state_tracker.already_recovered_fec_sets[shred.fec_set_index() as usize] = true;
            state_tracker.already_deshredded[shred.index() as usize] = true;
        });
        if needed_recovery {
            metrics
                .recovered_batch_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    if all_shreds.len() > MAX_PROCESSING_AGE {
        let slot_threshold = highest_slot_seen.saturating_sub(SLOT_LOOKBACK);
        let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
        let mut incomplete_fec_sets_count = 0;
        all_shreds.retain(|slot, (fec_set_indexes, state_tracker)| {
            if *slot >= slot_threshold {
                return true;
            }

            // count missing fec sets before clearing
            for (fec_set_index, shreds) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[*fec_set_index as usize] {
                    continue;
                }
                let (
                    num_expected_data_shreds,
                    _num_expected_coding_shreds,
                    _num_data_shreds,
                    _num_coding_shreds,
                ) = get_data_shred_info(shreds);

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .and_modify(|fec_set_data| {
                        fec_set_data.push((*fec_set_index, num_expected_data_shreds, shreds.len()))
                    })
                    .or_insert_with(|| {
                        vec![(*fec_set_index, num_expected_data_shreds, shreds.len())]
                    });
            }

            false
        });
        if incomplete_fec_sets_count > 0 {
            incomplete_fec_sets
                .iter_mut()
                .for_each(|(_slot, fec_set_indexes)| fec_set_indexes.sort_unstable());
            datapoint_warn!(
                "shredstream_proxy-deshred_missed_fec_sets",
                (
                    "slot_fec_set_indexes",
                    format!("{:?}", incomplete_fec_sets.iter().sorted().collect_vec()),
                    String
                ),
                ("slot_count", incomplete_fec_sets.len(), i64),
                ("fec_set_count", incomplete_fec_sets_count, i64),
            );
        }
    }

    if total_recovered_count > 0 {
        metrics
            .recovered_count
            .fetch_add(total_recovered_count as u64, Ordering::Relaxed);
    }

    total_recovered_count
}

/// Structural check of a batch whose start was proven (the shred before it is
/// DATA_COMPLETE, or it starts the slot), run by the reconstruct thread *after* the batch
/// was published so the known-start path pays nothing for it. Such a batch is published
/// regardless: its start cannot be wrong, so a failure here means a transaction format this
/// walk does not know (the SIMD-0385 failure mode) or an equivocating leader's mixed shreds,
/// and the consumer is the right place to decide. Failures are counted and logged (rate
/// limited) so a new wire format is loud instead of silent.
pub fn observe_known_start_batch(
    slot: Slot,
    start_index: u32,
    end_index: u32,
    payload: &[u8],
    metrics: &ShredMetrics,
) {
    let started = Instant::now();
    let walk = entry_walk::validate_batch(payload);
    record_walk_time(metrics, started);
    match walk {
        Ok(kind) => record_batch_kind(metrics, kind),
        Err(e) => {
            metrics
                .known_start_invalid_count
                .fetch_add(1, Ordering::Relaxed);
            static LAST_WARN_SECS: AtomicU64 = AtomicU64::new(0);
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if LAST_WARN_SECS.swap(now_secs, Ordering::Relaxed) != now_secs {
                warn!(
                    "slot {slot}: published batch {start_index}..={end_index} ({} bytes) has a proven start but does not parse: {e}; first bytes {:02x?}",
                    payload.len(),
                    &payload[..payload.len().min(16)]
                );
            }
        }
    }
}

fn record_batch_kind(metrics: &ShredMetrics, kind: BatchKind) {
    match kind {
        BatchKind::Entries { transactions, .. } => {
            metrics.txn_count.fetch_add(transactions, Ordering::Relaxed);
        }
        BatchKind::BlockMarker { .. } | BatchKind::Empty => {
            metrics.block_marker_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn record_walk_time(metrics: &ShredMetrics, started: Instant) {
    metrics
        .batch_walk_ns
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    metrics.batch_walk_count.fetch_add(1, Ordering::Relaxed);
}

#[allow(unused)]
fn debug_remaining_shreds(
    all_shreds: &mut ahash::HashMap<
        Slot,
        (
            ahash::HashMap<u32, HashSet<ComparableShred>>,
            ShredsStateTracker,
        ),
    >,
) {
    let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
    let mut incomplete_fec_sets_count = 0;
    all_shreds
        .iter()
        .for_each(|(slot, (fec_set_indexes, state_tracker))| {
            // count missing fec sets before clearing
            for (fec_set_index, shreds) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[*fec_set_index as usize] {
                    continue;
                }
                let (
                    num_expected_data_shreds,
                    _num_expected_coding_shreds,
                    _num_data_shreds,
                    _num_coding_shreds,
                ) = get_data_shred_info(shreds);

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .and_modify(|fec_set_data| {
                        fec_set_data.push((*fec_set_index, num_expected_data_shreds, shreds.len()))
                    })
                    .or_insert_with(|| {
                        vec![(*fec_set_index, num_expected_data_shreds, shreds.len())]
                    });
            }
        });
    incomplete_fec_sets
        .iter_mut()
        .for_each(|(_slot, fec_set_indexes)| fec_set_indexes.sort_unstable());
    println!("{:?}", incomplete_fec_sets.iter().sorted().collect_vec());
}

/// Return the inclusive range of shreds that constitute one complete segment: [0+ NotDataComplete, DataComplete]
/// Rules:
/// * A segment **ends** at the first `DataComplete` *at or after* `index`.
/// * It **starts** one position after the previous `DataComplete`, or at the beginning of the vector if there is none.
/// * If an `Unknown` is seen while searching towards the right, the segment is discarded and `None` is returned.
/// * We allow `Unknown` towards the left since sometimes entire FEC sets are not sent out
fn get_indexes(
    tracker: &ShredsStateTracker,
    index: usize,
) -> Option<(
    usize, /* start_data_complete_idx */
    usize, /* end_data_complete_idx */
    bool,  /* unknown start index */
)> {
    if index >= tracker.data_status.len() {
        return None;
    }

    // find the right boundary (first DataComplete ≥ index)
    let mut end = index;
    while end < tracker.data_status.len() {
        if tracker.already_deshredded[end] {
            return None;
        }
        match &tracker.data_status[end] {
            ShredStatus::Unknown => return None,
            ShredStatus::DataComplete => break,
            ShredStatus::NotDataComplete => end += 1,
        }
    }
    if end == tracker.data_status.len() {
        return None; // never saw a DataComplete
    }

    if end == 0 {
        return Some((0, 0, false)); // the vec *starts* with DataComplete
    }
    if index == 0 {
        return Some((0, end, false));
    }

    // find the left boundary (prev DataComplete + 1)
    let mut start = index;
    let mut next = start - 1;
    loop {
        match tracker.data_status[next] {
            ShredStatus::NotDataComplete => {
                if tracker.already_deshredded[next] {
                    return None; // already covered by some other iteration
                }
                if next == 0 {
                    return Some((0, end, false)); // no earlier DataComplete
                }
                start = next;
                next -= 1;
            }
            ShredStatus::DataComplete => return Some((start, end, false)),
            ShredStatus::Unknown => return Some((start, end, true)), // sometimes we don't have the previous starting shreds, make best guess
        }
    }
}

/// Upon receiving a new shred (either from recovery or receiving a UDP packet), update the state tracker
/// Returns shred index on new insert, None if already exists
fn update_state_tracker(shred: &Shred, state_tracker: &mut ShredsStateTracker) -> Option<usize> {
    let index = shred.index() as usize;
    if state_tracker.already_recovered_fec_sets[shred.fec_set_index() as usize] {
        return None;
    }
    if shred.shred_type() == ShredType::Data
        && (state_tracker.data_shreds[index].is_some()
            || !matches!(state_tracker.data_status[index], ShredStatus::Unknown))
    {
        return None;
    }
    if let Shred::ShredData(s) = &shred {
        state_tracker.data_shreds[index] = Some(shred.clone());
        if s.data_complete() || s.last_in_slot() {
            state_tracker.data_status[index] = ShredStatus::DataComplete;
        } else {
            state_tracker.data_status[index] = ShredStatus::NotDataComplete;
        }
    };
    Some(index)
}

const SLOT_LOOKBACK: Slot = 50;

/// check if we can reconstruct (having minimum number of data + coding shreds)
fn get_data_shred_info(
    shreds: &HashSet<ComparableShred>,
) -> (
    u16, /* num_expected_data_shreds */
    u16, /* num_expected_coding_shreds */
    u16, /* num_data_shreds */
    u16, /* num_coding_shreds */
) {
    let mut num_expected_data_shreds = 0;
    let mut num_expected_coding_shreds = 0;
    let mut num_data_shreds = 0;
    let mut num_coding_shreds = 0;
    for shred in shreds {
        match &shred.0 {
            Shred::ShredCode(s) => {
                num_coding_shreds += 1;
                num_expected_data_shreds = s.coding_header.num_data_shreds;
                num_expected_coding_shreds = s.coding_header.num_coding_shreds;
            }
            Shred::ShredData(s) => {
                num_data_shreds += 1;
                if num_expected_data_shreds == 0 && (s.data_complete() || s.last_in_slot()) {
                    num_expected_data_shreds =
                        (shred.0.index() - shred.0.fec_set_index()) as u16 + 1;
                }
            }
        }
    }
    (
        num_expected_data_shreds,
        num_expected_coding_shreds,
        num_data_shreds,
        num_coding_shreds,
    )
}

/// Issue: datashred equality comparison is wrong due to data size being smaller than the 1203 bytes allocated
#[derive(Clone, Debug, Eq)]
pub struct ComparableShred(Shred);

impl std::ops::Deref for ComparableShred {
    type Target = Shred;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for ComparableShred {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Shred::ShredCode(s) => {
                s.common_header.hash(state);
                s.coding_header.hash(state);
            }
            Shred::ShredData(s) => {
                s.common_header.hash(state);
                s.data_header.hash(state);
            }
        }
    }
}

impl PartialEq for ComparableShred {
    // Custom comparison to avoid random bytes that are part of payload
    fn eq(&self, other: &Self) -> bool {
        match &self.0 {
            Shred::ShredCode(s1) => match &other.0 {
                Shred::ShredCode(s2) => {
                    let solana_ledger::shred::ShredVariant::MerkleCode {
                        proof_size,
                        chained: _,
                        resigned,
                    } = s1.common_header.shred_variant
                    else {
                        return false;
                    };

                    // see https://github.com/jito-foundation/jito-solana/blob/d6c73374e3b4f863436e4b7d4d1ce5eea01cd262/ledger/src/shred/merkle.rs#L346, and re-add the proof component
                    let comparison_len =
                        <ShredCode as solana_ledger::shred::traits::Shred>::SIZE_OF_PAYLOAD
                            .saturating_sub(
                                usize::from(proof_size)
                                    * solana_ledger::shred::merkle::SIZE_OF_MERKLE_PROOF_ENTRY
                                    + if resigned {
                                        solana_ledger::shred::SIZE_OF_SIGNATURE
                                    } else {
                                        0
                                    },
                            );

                    s1.coding_header == s2.coding_header
                        && s1.common_header == s2.common_header
                        && s1.payload[..comparison_len] == s2.payload[..comparison_len]
                }
                Shred::ShredData(_) => false,
            },
            Shred::ShredData(s1) => match &other.0 {
                Shred::ShredCode(_) => false,
                Shred::ShredData(s2) => {
                    let Ok(s1_data) = solana_ledger::shred::layout::get_data(self.payload()) else {
                        return false;
                    };
                    let Ok(s2_data) = solana_ledger::shred::layout::get_data(other.payload())
                    else {
                        return false;
                    };
                    s1.data_header == s2.data_header
                        && s1.common_header == s2.common_header
                        && s1_data == s2_data
                }
            },
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashSet},
        io::{Read, Write},
        net::UdpSocket,
        sync::Arc,
    };

    use borsh::BorshDeserialize;
    use itertools::Itertools;
    use rand::Rng;
    use solana_ledger::{
        blockstore::make_slot_entries_with_transactions,
        shred::{merkle::Shred, ProcessShredsStats, ReedSolomonCache, ShredCommonHeader, Shredder},
    };
    use solana_perf::packet::{Packet, PacketBatch};
    use solana_sdk::{clock::Slot, hash::Hash, signature::Keypair};

    use crate::{
        deshred::{reconstruct_shreds, ComparableShred},
        forwarder::ShredMetrics,
    };

    /// For serializing packets to disk
    #[derive(borsh::BorshSerialize, borsh::BorshDeserialize, PartialEq, Debug)]
    struct Packets {
        pub packets: Vec<Vec<u8>>,
    }

    #[allow(unused)]
    fn listen_and_write_shreds() -> std::io::Result<()> {
        let socket = UdpSocket::bind("127.0.0.1:5000")?;
        println!("Listening on {}", socket.local_addr()?);

        let mut map = ahash::HashMap::<usize, usize>::default();
        let mut buf = [0u8; 1500];
        let mut vec = Packets {
            packets: Vec::new(),
        };

        let mut i = 0;
        loop {
            i += 1;
            match socket.recv_from(&mut buf) {
                Ok((amt, _src)) => {
                    vec.packets.push(buf[..amt].to_vec());
                    match map.entry(amt) {
                        Entry::Occupied(mut e) => *e.get_mut() += 1,
                        Entry::Vacant(e) => {
                            e.insert(1);
                        }
                    }
                    *map.get_mut(&amt).unwrap_or(&mut 0) += 1;
                }
                Err(e) => {
                    eprintln!("Error receiving data: {}", e);
                }
            }
            if i % 50000 == 0 {
                dbg!(&map);
                // size 1203 are data shreds: https://github.com/jito-foundation/jito-solana/blob/1742826fca975bd6d17daa5693abda861bbd2adf/ledger/src/shred/merkle.rs#L42
                // size 1228 are coding shreds: https://github.com/jito-foundation/jito-solana/blob/1742826fca975bd6d17daa5693abda861bbd2adf/ledger/src/shred/shred_code.rs#L16
                let mut file = std::fs::File::create("serialized_shreds.bin")?;
                file.write_all(&borsh::to_vec(&vec)?)?;
                return Ok(());
            }
        }
    }

    #[test]
    fn test_reconstruct_live_shreds() {
        let packets = {
            let mut file = std::fs::File::open("../bins/serialized_shreds.bin").unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            Packets::try_from_slice(&buffer).unwrap()
        };
        assert_eq!(packets.packets.len(), 50_000);

        let shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok())
            .collect::<Vec<_>>();
        assert_eq!(shreds.len(), 49989);

        let unique_shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok().map(ComparableShred))
            .collect::<HashSet<ComparableShred>>();
        assert_eq!(unique_shreds.len(), 44900);

        let unique_slot_fec_shreds = packets
            .packets
            .iter()
            .filter_map(|p| {
                Shred::from_payload(p.clone())
                    .ok()
                    .map(|s| *s.common_header())
            })
            .collect::<HashSet<ShredCommonHeader>>();
        assert_eq!(unique_slot_fec_shreds.len(), 44900);

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .map(|x| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&mut deshredded_entries);
        assert!(recovered_count < deshredded_entries.len());
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries_bytes)| {
                        bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes)
                            .unwrap()
                            .len()
                    })
                .sum::<usize>(),
            13580
        );
        assert_all_batches_walk(&deshredded_entries);
        assert_eq!(all_shreds.len(), 30);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries_bytes)| *slot);
        // slot_to_entry
        //     .iter()
        //     .sorted_by_key(|(slot, _)| *slot)
        //     .for_each(|(slot, entry)| {
        //         println!(
        //             "slot {slot} entry count: {:?}, txn count: {}",
        //             entry.len(),
        //             entry
        //                 .iter()
        //                 .map(|(_slot, entry)| entry.transactions.len())
        //                 .sum::<usize>()
        //         );
        //     });
        assert_eq!(slot_to_entry.len(), 29);

        // Test 2: 33% of shreds missing
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| (index + 1) % 3 != 0)
                    .map(|(_i, x)| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&deshredded_entries, "new.txt");
        assert!(recovered_count > (deshredded_entries.len() / 4));
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries_bytes)| {
                        bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes)
                            .unwrap()
                            .len()
                    })
                .sum::<usize>(),
            13580
        );
        assert_all_batches_walk(&deshredded_entries);
        assert!(all_shreds.len() > 15);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries_bytes)| *slot);
        assert_eq!(slot_to_entry.len(), 29);
    }

    /// Every emitted batch passes the structural walk, which agrees with bincode on the
    /// entry and transaction counts.
    fn assert_all_batches_walk(deshredded_entries: &[(Slot, Vec<u8>)]) {
        for (slot, bytes) in deshredded_entries {
            let entries = bincode::deserialize::<Vec<solana_entry::entry::Entry>>(bytes).unwrap();
            let transactions = entries.iter().map(|e| e.transactions.len() as u64).sum();
            let walked = crate::entry_walk::validate_batch(bytes)
                .unwrap_or_else(|e| panic!("slot {slot}: {e}"));
            if entries.is_empty() {
                assert_eq!(walked, crate::entry_walk::BatchKind::Empty, "slot {slot}");
            } else {
                assert_eq!(
                    walked,
                    crate::entry_walk::BatchKind::Entries {
                        entries: entries.len() as u64,
                        transactions
                    },
                    "slot {slot}"
                );
            }
        }
    }

    /// Timing only:
    /// `cargo test --release -p jito-shredstream-proxy bench_batch_walk -- --ignored --nocapture`.
    /// Walk cost on every batch deshredded from the captured mainnet shreds, next to the
    /// deshred concatenation that precedes it and the bincode decode `caa9daf` removed.
    #[test]
    #[ignore = "timing only; run with --ignored --nocapture"]
    fn bench_batch_walk() {
        use std::{hint::black_box, time::Instant};

        let packets = {
            let mut file = std::fs::File::open("../bins/serialized_shreds.bin").unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            Packets::try_from_slice(&buffer).unwrap()
        };
        let mut all_shreds = ahash::HashMap::default();
        let mut deshredded_entries = Vec::new();
        reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .map(|x| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut Vec::new(),
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut 0,
            &ReedSolomonCache::default(),
            &ShredMetrics::default(),
        );
        let batches: Vec<&[u8]> = deshredded_entries
            .iter()
            .map(|(_, b)| b.as_slice())
            .collect();
        let total_bytes: usize = batches.iter().map(|b| b.len()).sum();
        let total_txs: u64 = batches
            .iter()
            .map(|b| match crate::entry_walk::validate_batch(b).unwrap() {
                crate::entry_walk::BatchKind::Entries { transactions, .. } => transactions,
                _ => 0,
            })
            .sum();
        let time_per_pass = |f: &dyn Fn()| {
            for _ in 0..3 {
                f();
            }
            let passes = 30u32;
            let start = Instant::now();
            for _ in 0..passes {
                f();
            }
            start.elapsed().as_nanos() as f64 / f64::from(passes)
        };
        let walk_ns = time_per_pass(&|| {
            for b in &batches {
                black_box(crate::entry_walk::validate_batch(black_box(b)).unwrap());
            }
        });
        let bincode_ns = time_per_pass(&|| {
            for b in &batches {
                black_box(
                    bincode::deserialize::<Vec<solana_entry::entry::Entry>>(black_box(b)).unwrap(),
                );
            }
        });
        let per_kb = |ns: f64| ns / (total_bytes as f64 / 1024.0);
        println!(
            "{} batches, {} txs, {:.1} KB mean batch: walk {:.2} us/batch = {:.1} ns/KB \
             (= {:.2} us per 60 KB); bincode Vec<Entry> {:.2} us/batch = {:.1} ns/KB",
            batches.len(),
            total_txs,
            total_bytes as f64 / batches.len() as f64 / 1024.0,
            walk_ns / batches.len() as f64 / 1000.0,
            per_kb(walk_ns),
            per_kb(walk_ns) * 60.0 / 1000.0,
            bincode_ns / batches.len() as f64 / 1000.0,
            per_kb(bincode_ns),
        );

        // A current-format 2-FEC-set batch (61,632 bytes) of real legacy/v0/v1 txs.
        let [legacy, v0, v1_small, v1_large] =
            crate::entry_walk::test_fixtures::real_transactions();
        let cycle: Vec<&[u8]> = vec![&legacy, &v0, &legacy, &v1_small, &legacy, &v0, &v1_large];
        let mut txs: Vec<&[u8]> = vec![];
        let mut len = 8 + 48;
        while len < 61_632 - 2_000 {
            let tx = cycle[txs.len() % cycle.len()];
            txs.push(tx);
            len += tx.len();
        }
        let mut modern = crate::entry_walk::test_fixtures::batch_of(&[(1, [1; 32], &txs)]);
        modern.resize(61_632, 0); // Firedancer-style zero padding to two FEC sets
        let modern_ns = time_per_pass(&|| {
            for _ in 0..100 {
                black_box(crate::entry_walk::validate_batch(black_box(&modern)).unwrap());
            }
        }) / 100.0;
        println!(
            "61,632-byte legacy/v0/v1 batch ({} txs, zero padded): walk {:.2} us",
            txs.len(),
            modern_ns / 1000.0
        );
    }

    /// Helper function to compare all shred output
    #[allow(unused)]
    fn debug_to_disk(
        deshredded_entries: &[(Slot, Vec<u8>)],
        filepath: &str,
    ) {
        let entries = deshredded_entries
            .iter()
            .map(|(slot, entries_bytes)| {
                let entries = bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes).unwrap();
                (slot, entries)
            })
            .into_group_map_by(|(slot, _entries)| *slot)
            .into_iter()
            .map(|(key, values)| {
                (
                    key,
                    values.into_iter().fold(Vec::new(), |mut acc, (_, v)| {
                        acc.extend(v);
                        acc
                    }),
                )
            })
            .map(|(slot, entries)| {
                let mut vec = entries
                    .iter()
                    .flat_map(|x| x.transactions.iter())
                    .map(|x| x.signatures[0])
                    .collect::<Vec<_>>();
                vec.sort();
                vec.dedup();
                (slot, vec)
            })
            .sorted_by_key(|x| x.0)
            .dedup_by(|lhs, rhs| lhs.0 == rhs.0)
            .collect_vec();
        let mut file = std::fs::File::create(filepath).unwrap();
        write!(file, "entries: {:#?}", &entries).unwrap();
    }

    #[test]
    /// Test if DATA_COMPLETE_SHRED across multiple FEC sets is handled correctly
    fn test_reconstruct_live_data_complete_shred() {
        let packets = {
            let mut file =
                std::fs::File::open("../bins/serialized_shreds_data_complete_test.bin").unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            Packets::try_from_slice(&buffer).unwrap()
        };
        assert_eq!(packets.packets.len(), 150_000);

        let shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok())
            .collect::<Vec<_>>();
        assert_eq!(shreds.len(), 149977);

        let unique_shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok().map(ComparableShred))
            .collect::<HashSet<ComparableShred>>();
        assert_eq!(unique_shreds.len(), 109221);

        let unique_slot_fec_shreds = packets
            .packets
            .iter()
            .filter_map(|p| {
                Shred::from_payload(p.clone())
                    .ok()
                    .map(|s| *s.common_header())
            })
            .collect::<HashSet<ShredCommonHeader>>();
        assert_eq!(unique_slot_fec_shreds.len(), 109221);

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .map(|x| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&mut deshredded_entries);
        assert!(recovered_count < deshredded_entries.len());
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries_bytes)| {
                        bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes)
                            .unwrap()
                            .len()
                    })
                .sum::<usize>(),
            43170
        );
        assert_all_batches_walk(&deshredded_entries);
        assert_eq!(all_shreds.len(), 61);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries_bytes)| *slot);
        // slot_to_entry
        //     .iter()
        //     .sorted_by_key(|(slot, _)| *slot)
        //     .for_each(|(slot, entry)| {
        //         println!(
        //             "slot {slot} entry count: {:?}, txn count: {}",
        //             entry.len(),
        //             entry
        //                 .iter()
        //                 .map(|(_slot, entry)| entry.transactions.len())
        //                 .sum::<usize>()
        //         );
        //     });
        assert_eq!(slot_to_entry.len(), 61);

        // Test 2: 33% of shreds missing
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| (index + 1) % 3 != 0)
                    .map(|(_i, x)| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&deshredded_entries, "new.txt");
        assert!(recovered_count > (deshredded_entries.len() / 4));
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries_bytes)| {
                        bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes)
                            .unwrap()
                            .len()
                    })
                .sum::<usize>(),
            43170
        );
        assert_all_batches_walk(&deshredded_entries);
        assert!(all_shreds.len() > 15);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries_bytes)| *slot);
        assert_eq!(slot_to_entry.len(), 61);
    }

    #[test]
    fn test_recover_shreds() {
        let mut rng = rand::thread_rng();
        let slot = 11_111;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let chained_merkle_root = Some(Hash::new_from_array(rng.gen()));
        let num_entry_groups = 10;
        let num_entries = 10;
        let mut entries = Vec::new();
        let mut data_shreds = Vec::new();
        let mut coding_shreds = Vec::new();

        let mut index = 0;
        (0..num_entry_groups).for_each(|_i| {
            let _entries = make_slot_entries_with_transactions(num_entries);
            let (_data_shreds, _coding_shreds) = shredder.entries_to_shreds(
                &leader_keypair,
                _entries.as_slice(),
                true,
                chained_merkle_root,
                index as u32, // next_shred_index
                index as u32, // next_code_index,
                true,         // merkle_variant
                &reed_solomon_cache,
                &mut ProcessShredsStats::default(),
            );
            index += _data_shreds.len();
            entries.extend(_entries);
            data_shreds.extend(_data_shreds);
            coding_shreds.extend(_coding_shreds);
        });

        let packets = data_shreds
            .iter()
            .chain(coding_shreds.iter())
            .map(|s| {
                let mut p = Packet::default();
                s.copy_to_packet(&mut p);
                p
            })
            .collect_vec();
        assert_eq!(data_shreds.len(), 320);
        assert_eq!(
            data_shreds
                .iter()
                .map(|s| s.fec_set_index())
                .dedup()
                .count(),
            num_entry_groups
        );

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(packets.clone()),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );
        assert_eq!(recovered_count, 0);
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries_bytes)| {
                        bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes)
                            .unwrap()
                            .len()
                    })
                .sum::<usize>(),
            entries.len()
        );
        assert_eq!(
            all_shreds.len(),
            1, // slot 11111
        );

        // Test 2: 33% of shreds missing
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| (index + 1) % 3 != 0)
                    .map(|(_i, p)| p.clone())
                    .collect(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut Vec::new(),
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );
        assert!(recovered_count > 0);
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries_bytes)| {
                        bincode::deserialize::<Vec<solana_entry::entry::Entry>>(entries_bytes)
                            .unwrap()
                            .len()
                    })
                .sum::<usize>(),
            entries.len()
        );
        assert_eq!(
            all_shreds.len(),
            1, // slot 11111
        );
    }
}
#[cfg(test)]
mod get_indexes_tests {
    use super::{get_indexes, ShredStatus, ShredsStateTracker};

    fn make_test_statustracker(statuses: &[ShredStatus]) -> ShredsStateTracker {
        let mut tracker = ShredsStateTracker::default();
        tracker.data_status[..statuses.len()].copy_from_slice(statuses);
        tracker
    }

    #[test]
    fn start_at_index_zero() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), Some((0, 2, false)));

        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), Some((0, 0, false)));

        let s = [
            ShredStatus::Unknown,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);
    }

    #[test]
    fn start_just_after_data_complete() {
        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 3, false)));
    }

    #[test]
    fn start_just_before_data_complete() {
        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 2, false)));
    }

    #[test]
    fn two_consecutive_data_complete() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((0, 1, false)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
    }

    #[test]
    fn three_consecutive_data_complete() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((0, 1, false)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
        assert_eq!(get_indexes(&tracker, 3), Some((3, 3, false)));
    }

    #[test]
    fn unknown_discards_segment() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::Unknown,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);

        let s = [
            ShredStatus::Unknown,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 2, true)));
    }

    #[test]
    fn test_unknown() {
        let s = [
            ShredStatus::Unknown,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 1, true)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
        assert_eq!(get_indexes(&tracker, 3), Some((3, 4, false)));
    }
}

/// End-to-end unknown-start behaviour on real merkle shreds: a leader shreds payloads made
/// of real mainnet legacy/v0/v1 transactions, and packets are delivered out of order or
/// with gaps.
#[cfg(test)]
pub(crate) mod validated_start_tests {
    use std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    };

    use solana_ledger::shred::{layout, merkle, ProcessShredsStats, ReedSolomonCache, ShredType};
    use solana_perf::packet::{Packet, PacketBatch};
    use solana_sdk::{clock::Slot, hash::Hash, signature::Keypair};

    use super::{
        observe_known_start_batch, reconstruct_shreds, ComparableShred, ShredsStateTracker,
    };
    use crate::{
        entry_walk::{self, test_fixtures::*, BatchKind},
        forwarder::ShredMetrics,
    };

    pub(crate) const SLOT: Slot = 1_000;

    type AllShreds = ahash::HashMap<
        Slot,
        (
            ahash::HashMap<u32, HashSet<ComparableShred>>,
            ShredsStateTracker,
        ),
    >;

    /// A leader that shreds raw payloads into consecutive chained merkle batches.
    pub(crate) struct Leader {
        keypair: Keypair,
        pool: rayon::ThreadPool,
        rs_cache: ReedSolomonCache,
        next_data: u32,
        next_code: u32,
    }

    impl Leader {
        pub(crate) fn new() -> Self {
            Self {
                keypair: Keypair::new(),
                pool: rayon::ThreadPoolBuilder::new()
                    .num_threads(2)
                    .build()
                    .unwrap(),
                rs_cache: ReedSolomonCache::default(),
                next_data: 0,
                next_code: 0,
            }
        }

        fn shred(&self, payload: &[u8], next_data: u32, next_code: u32) -> Vec<merkle::Shred> {
            merkle::make_shreds_from_data(
                &self.pool,
                &self.keypair,
                Some(Hash::new_from_array([7; 32])),
                payload,
                SLOT,
                SLOT - 1,
                0, // shred_version
                0, // reference_tick
                false,
                next_data,
                next_code,
                &self.rs_cache,
                &mut ProcessShredsStats::default(),
            )
            .unwrap()
        }

        /// Data bytes one full 32-data-shred FEC set carries.
        pub(crate) fn fec_set_payload_bytes(&self) -> usize {
            let shreds = self.shred(&vec![1u8; 200_000], 0, 0);
            let first = shreds
                .iter()
                .find(|s| s.shred_type() == ShredType::Data)
                .unwrap();
            32 * layout::get_data(first.payload().as_ref()).unwrap().len()
        }

        /// Shred the next batch of the slot.
        pub(crate) fn batch(&mut self, payload: &[u8]) -> Vec<merkle::Shred> {
            let shreds = self.shred(payload, self.next_data, self.next_code);
            for s in &shreds {
                let next = match s.shred_type() {
                    ShredType::Data => &mut self.next_data,
                    ShredType::Code => &mut self.next_code,
                };
                *next = (*next).max(s.index() + 1);
            }
            shreds
        }
    }

    /// A legacy transaction padding a batch to an exact length (300..=16_000 bytes).
    fn filler_tx(len: usize) -> Vec<u8> {
        // sig count, sig, header, key count, 2 keys, blockhash, ix count, program index,
        // account count, 1 account index, then a 2-byte ShortU16 data length and the data.
        const FIXED: usize = 1 + 64 + 3 + 1 + 64 + 32 + 1 + 1 + 1 + 1;
        let data_len = len - FIXED - 2;
        assert!((128..16_384).contains(&data_len));
        let mut tx = vec![1u8];
        tx.extend_from_slice(&[0x5a; 64]);
        tx.extend_from_slice(&[1, 0, 1, 2]);
        tx.extend_from_slice(&[0x11; 32]);
        tx.extend_from_slice(&[0x22; 32]);
        tx.extend_from_slice(&[0x33; 32]);
        tx.extend_from_slice(&[1, 1, 1, 0]);
        tx.push((data_len & 0x7f) as u8 | 0x80);
        tx.push((data_len >> 7) as u8);
        tx.extend(std::iter::repeat_n(0x44, data_len));
        assert_eq!(tx.len(), len);
        tx
    }

    /// A `Vec<Entry>` of exactly `len` bytes cycling through the real transactions (plus
    /// `extra`), each entry holding up to 7 transactions, with one filler transaction last.
    pub(crate) fn payload_of_len(len: usize, extra: &[Vec<u8>]) -> Vec<u8> {
        let real = real_transactions();
        let pool: Vec<&[u8]> = real
            .iter()
            .chain(extra.iter())
            .map(|t| t.as_slice())
            .collect();
        let mut entries: Vec<Vec<&[u8]>> = vec![];
        let mut total = 8usize;
        let mut i = 0;
        loop {
            let tx = pool[i % pool.len()];
            let new_entry = entries.last().is_none_or(|e| e.len() == 7);
            let cost = tx.len() + if new_entry { 48 } else { 0 };
            // Leave room for a last entry holding one 300..=16_000-byte filler.
            if total + cost + 48 + 300 > len {
                break;
            }
            if new_entry {
                entries.push(vec![]);
            }
            entries.last_mut().unwrap().push(tx);
            total += cost;
            i += 1;
        }
        let filler = filler_tx(len - total - 48);
        entries.push(vec![&filler]);
        let hashes: Vec<[u8; 32]> = (0..entries.len()).map(|k| [k as u8; 32]).collect();
        let spec: Vec<EntrySpec> = entries
            .iter()
            .zip(&hashes)
            .map(|(txs, h)| (1u64, *h, txs.as_slice()))
            .collect();
        let bytes = batch_of(&spec);
        assert_eq!(bytes.len(), len);
        assert!(entry_walk::validate_batch(&bytes).is_ok());
        bytes
    }

    fn packets<'a>(shreds: impl IntoIterator<Item = &'a merkle::Shred>) -> PacketBatch {
        PacketBatch::new(
            shreds
                .into_iter()
                .map(|s| {
                    let bytes: &[u8] = s.payload().as_ref();
                    let mut p = Packet::default();
                    p.buffer_mut()[..bytes.len()].copy_from_slice(bytes);
                    p.meta_mut().size = bytes.len();
                    p
                })
                .collect(),
        )
    }

    struct Proxy {
        all_shreds: AllShreds,
        scratch: Vec<(Slot, u32)>,
        highest_slot_seen: Slot,
        rs_cache: ReedSolomonCache,
        metrics: Arc<ShredMetrics>,
    }

    /// One emitted batch: (payload, start index, end index, unknown_start).
    type Emitted = (Vec<u8>, u32, u32, bool);

    impl Proxy {
        fn new() -> Self {
            Self {
                all_shreds: AllShreds::default(),
                scratch: vec![],
                highest_slot_seen: 0,
                rs_cache: ReedSolomonCache::default(),
                metrics: Arc::new(ShredMetrics::default()),
            }
        }

        /// Feed one packet batch; returns what was emitted, running the known-start walk
        /// exactly as the reconstruct thread does after publishing.
        fn feed<'a>(
            &mut self,
            shreds: impl IntoIterator<Item = &'a merkle::Shred>,
        ) -> Vec<Emitted> {
            let mut entries = vec![];
            let mut ranges = vec![];
            reconstruct_shreds(
                packets(shreds),
                &mut self.all_shreds,
                &mut self.scratch,
                &mut entries,
                &mut ranges,
                &mut self.highest_slot_seen,
                &self.rs_cache,
                &self.metrics,
            );
            entries
                .into_iter()
                .zip(ranges)
                .map(|((slot, bytes), (start, end, unknown_start))| {
                    assert_eq!(slot, SLOT);
                    if !unknown_start {
                        observe_known_start_batch(slot, start, end, &bytes, &self.metrics);
                    }
                    (bytes, start, end, unknown_start)
                })
                .collect()
        }
    }

    fn count(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn is_data(s: &merkle::Shred) -> bool {
        s.shred_type() == ShredType::Data
    }

    /// Z = 1 FEC set (data 0..=31), A = 2 FEC sets (32..=95), B = 1 FEC set (96..=127).
    struct Slot3 {
        z: Vec<u8>,
        a: Vec<u8>,
        b: Vec<u8>,
        shreds: Vec<merkle::Shred>,
    }

    fn three_batches(a_extra: &[Vec<u8>]) -> Slot3 {
        let mut leader = Leader::new();
        let set = leader.fec_set_payload_bytes();
        let z = payload_of_len(set, &[]);
        let a = payload_of_len(2 * set, a_extra);
        let b = payload_of_len(set, &[]);
        let mut shreds = leader.batch(&z);
        shreds.extend(leader.batch(&a));
        shreds.extend(leader.batch(&b));
        // Every set is a full 32:32 set, as on mainnet.
        let fec_sets: Vec<u32> = shreds
            .iter()
            .filter(|s| is_data(s))
            .map(|s| s.fec_set_index())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_eq!(fec_sets, vec![0, 32, 64, 96]);
        Slot3 { z, a, b, shreds }
    }

    #[test]
    fn known_starts_emit_every_batch_once_and_validate() {
        let s = three_batches(&[]);
        let mut proxy = Proxy::new();
        let out = proxy.feed(&s.shreds);
        let got: Vec<_> = out
            .iter()
            .map(|(b, st, en, u)| (b.clone(), *st, *en, *u))
            .collect();
        assert_eq!(
            got,
            vec![
                (s.z.clone(), 0, 31, false),
                (s.a.clone(), 32, 95, false),
                (s.b.clone(), 96, 127, false),
            ]
        );
        let m = proxy.metrics.clone();
        assert_eq!(count(&m.known_start_invalid_count), 0);
        assert_eq!(count(&m.batch_walk_count), 3);
        assert_eq!(count(&m.unknown_start_position_count), 0);
        // Real transactions (4 per cycle) plus one filler per batch were counted.
        assert!(count(&m.txn_count) > 3 * 20);
    }

    #[test]
    fn wrong_guess_is_rejected_then_the_whole_batch_is_emitted() {
        let s = three_batches(&[]);
        // Lose data shred 63 (last of A's first set) and all of that set's coding shreds,
        // and hold back the coding shreds of A's second set for a later packet batch.
        let first = s.shreds.iter().filter(|x| {
            let lost = is_data(x) && x.index() == 63;
            let set32_code = !is_data(x) && x.fec_set_index() == 32;
            let set64_code = !is_data(x) && x.fec_set_index() == 64;
            !(lost || set32_code || set64_code)
        });
        let mut proxy = Proxy::new();
        let out = proxy.feed(first);
        // Z and B have proven starts; A's second set alone was guessed and rejected.
        let got: Vec<_> = out.iter().map(|(_, st, en, u)| (*st, *en, *u)).collect();
        assert_eq!(got, vec![(0, 31, false), (96, 127, false)]);
        let m = proxy.metrics.clone();
        assert_eq!(count(&m.unknown_start_position_count), 1);
        assert_eq!(count(&m.unknown_start_invalid_count), 1);
        assert_eq!(count(&m.unknown_start_validated_count), 0);

        // More shreds of the held set: the same guess is not deshredded again.
        let out = proxy.feed(
            s.shreds
                .iter()
                .filter(|x| !is_data(x) && x.fec_set_index() == 64),
        );
        assert!(out.is_empty());
        assert_eq!(count(&m.unknown_start_position_count), 1);
        assert!(count(&m.unknown_start_retry_skipped_count) >= 1);

        // The missing shred arrives: A is emitted whole, with its proven start.
        let out = proxy.feed(s.shreds.iter().filter(|x| is_data(x) && x.index() == 63));
        assert_eq!(out, vec![(s.a.clone(), 32, 95, false)]);
        assert_eq!(count(&m.held_batch_emitted_count), 1);
        assert_eq!(count(&m.known_start_invalid_count), 0);
    }

    #[test]
    fn right_guess_is_validated_and_emitted_immediately() {
        let s = three_batches(&[]);
        // Lose Z's DATA_COMPLETE shred (31) and Z's coding shreds.
        let mut proxy = Proxy::new();
        let out = proxy.feed(s.shreds.iter().filter(|x| {
            !((is_data(x) && x.index() == 31) || (!is_data(x) && x.fec_set_index() == 0))
        }));
        assert_eq!(
            out,
            vec![(s.a.clone(), 32, 95, true), (s.b.clone(), 96, 127, false)]
        );
        let m = proxy.metrics.clone();
        assert_eq!(count(&m.unknown_start_validated_count), 1);
        assert_eq!(count(&m.unknown_start_invalid_count), 0);

        // Shred 31 arrives: Z is emitted; A is not emitted twice.
        let out = proxy.feed(s.shreds.iter().filter(|x| is_data(x) && x.index() == 31));
        assert_eq!(out, vec![(s.z.clone(), 0, 31, false)]);
        assert_eq!(count(&m.held_batch_emitted_count), 0);
    }

    #[test]
    fn mid_fec_set_guess_is_never_emitted() {
        let s = three_batches(&[]);
        // Lose data shred 40 (inside A's first set) and hold back that set's coding.
        let mut proxy = Proxy::new();
        let out = proxy.feed(s.shreds.iter().filter(|x| {
            !((is_data(x) && x.index() == 40) || (!is_data(x) && x.fec_set_index() == 32))
        }));
        // The old code emitted 41..=95 as a guessed batch; now only Z and B go out.
        let got: Vec<_> = out.iter().map(|(_, st, en, u)| (*st, *en, *u)).collect();
        assert_eq!(got, vec![(0, 31, false), (96, 127, false)]);
        let m = proxy.metrics.clone();
        assert!(count(&m.unknown_start_mid_fec_count) >= 1);
        assert_eq!(count(&m.unknown_start_position_count), 0);

        // The set's coding shreds arrive: FEC recovery fills 40 and A is emitted whole.
        let out = proxy.feed(
            s.shreds
                .iter()
                .filter(|x| !is_data(x) && x.fec_set_index() == 32),
        );
        assert_eq!(out, vec![(s.a.clone(), 32, 95, false)]);
    }

    #[test]
    fn any_32_of_64_shreds_recover_the_set_and_emit_in_the_same_call() {
        let s = three_batches(&[]);
        // A's first set (32..=63): only 16 data shreds (32..=47) arrive, plus 15 of its
        // coding shreds. Everything else of Z and A arrives too.
        let set32_code: Vec<u32> = s
            .shreds
            .iter()
            .filter(|x| !is_data(x) && x.fec_set_index() == 32)
            .map(|x| x.index())
            .collect();
        assert_eq!(set32_code.len(), 32);
        let first = s.shreds.iter().filter(|x| {
            let fec = x.fec_set_index();
            if fec == 96 {
                return false; // B
            }
            if fec != 32 {
                return true;
            }
            if is_data(x) {
                x.index() <= 47
            } else {
                set32_code[..15].contains(&x.index())
            }
        });
        let mut proxy = Proxy::new();
        let out = proxy.feed(first);
        // 31 of 64: Z only.
        assert_eq!(out, vec![(s.z.clone(), 0, 31, false)]);
        assert_eq!(count(&proxy.metrics.recovered_batch_count), 0);

        // The 32nd shred (a coding shred) recovers the 16 missing data shreds and A goes
        // out in the same call, without waiting for any late data shred.
        let out = proxy.feed(
            s.shreds
                .iter()
                .filter(|x| !is_data(x) && x.index() == set32_code[15]),
        );
        assert_eq!(out, vec![(s.a.clone(), 32, 95, false)]);
        assert_eq!(count(&proxy.metrics.recovered_batch_count), 1);
    }

    #[test]
    fn unknown_format_batch_held_by_guess_is_released_by_its_proven_start() {
        // A contains a transaction with an unknown message version (0x82): the walk
        // rejects it as a guess, but once its start is proven it must still go out.
        let mut unknown = decode(V0_B64);
        unknown[65] = 0x82;
        let s = {
            let mut leader = Leader::new();
            let set = leader.fec_set_payload_bytes();
            let z = payload_of_len(set, &[]);
            // A = real payload + one more entry holding only the unknown-format tx.
            let mut a = payload_of_len(2 * set - 48 - unknown.len(), &[]);
            let n = u64::from_le_bytes(a[..8].try_into().unwrap());
            a[..8].copy_from_slice(&(n + 1).to_le_bytes());
            a.extend_from_slice(&1u64.to_le_bytes()); // num_hashes
            a.extend_from_slice(&[0xcc; 32]); // hash
            a.extend_from_slice(&1u64.to_le_bytes()); // tx count
            a.extend_from_slice(&unknown);
            assert_eq!(a.len(), 2 * set);
            assert!(entry_walk::validate_batch(&a).is_err());
            let b = payload_of_len(set, &[]);
            let mut shreds = leader.batch(&z);
            shreds.extend(leader.batch(&a));
            shreds.extend(leader.batch(&b));
            Slot3 { z, a, b, shreds }
        };
        // Lose Z's DATA_COMPLETE shred (31) and Z's coding: A's start is a (right) guess.
        let mut proxy = Proxy::new();
        let out = proxy.feed(s.shreds.iter().filter(|x| {
            !((is_data(x) && x.index() == 31) || (!is_data(x) && x.fec_set_index() == 0))
        }));
        assert_eq!(out, vec![(s.b.clone(), 96, 127, false)]);
        let m = proxy.metrics.clone();
        assert_eq!(count(&m.unknown_start_invalid_count), 1);

        // Only shred 31 arrives; set 32 is not touched, yet A's now-proven start releases
        // it, and the post-publish walk flags it.
        let out = proxy.feed(s.shreds.iter().filter(|x| is_data(x) && x.index() == 31));
        assert_eq!(
            out,
            vec![(s.z.clone(), 0, 31, false), (s.a.clone(), 32, 95, false)]
        );
        assert_eq!(count(&m.held_batch_emitted_count), 1);
        assert_eq!(count(&m.known_start_invalid_count), 1);
    }

    #[test]
    fn block_marker_batch_is_emitted_and_does_not_block_later_batches() {
        let mut leader = Leader::new();
        let set = leader.fec_set_payload_bytes();
        let z = payload_of_len(set, &[]);
        let marker = block_header_marker(SLOT - 1, [9; 32]);
        let a = payload_of_len(set, &[]);
        let mut shreds = leader.batch(&z);
        let marker_shreds = leader.batch(&marker);
        let marker_start = marker_shreds[0].index();
        let marker_end = marker_shreds
            .iter()
            .filter(|x| is_data(x))
            .map(|x| x.index())
            .max()
            .unwrap();
        shreds.extend(marker_shreds);
        shreds.extend(leader.batch(&a));
        let a_start = marker_end + 1;

        // Everything present: all three batches with proven starts.
        let mut proxy = Proxy::new();
        let out = proxy.feed(&shreds);
        let got: Vec<_> = out
            .iter()
            .map(|(b, st, _, u)| (b.clone(), *st, *u))
            .collect();
        assert_eq!(
            got,
            vec![
                (z.clone(), 0, false),
                (marker.clone(), marker_start, false),
                (a.clone(), a_start, false)
            ]
        );
        assert_eq!(count(&proxy.metrics.block_marker_count), 1);
        assert_eq!(count(&proxy.metrics.known_start_invalid_count), 0);

        // Z's end lost: the marker's start is a guess, it validates as a marker, and the
        // batch after it keeps its proven start.
        let mut proxy = Proxy::new();
        let out = proxy.feed(shreds.iter().filter(|x| {
            !((is_data(x) && x.index() == 31) || (!is_data(x) && x.fec_set_index() == 0))
        }));
        let got: Vec<_> = out
            .iter()
            .map(|(b, st, _, u)| (b.clone(), *st, *u))
            .collect();
        assert_eq!(
            got,
            vec![
                (marker.clone(), marker_start, true),
                (a.clone(), a_start, false)
            ]
        );
        assert_eq!(count(&proxy.metrics.unknown_start_validated_count), 1);
        assert_eq!(count(&proxy.metrics.block_marker_count), 1);
        assert_eq!(
            entry_walk::validate_batch(&marker),
            Ok(BatchKind::BlockMarker { variant: 1 })
        );
    }
}

//! The benchmark aggregator: a single thread that owns all matching/stat state
//! so the hot ingest path stays lock-free (it only parses headers and pushes
//! observations onto a bounded channel).
//!
//! Pipeline per the design:
//!  1. raw CSV dump of every observation (milestone M1), optional.
//!  2. match the same shred across sources via `ShredId` -> earliest-arrival per
//!     source (M2). All jito PoPs collapse into one `SourceId::Jito` baseline.
//!  3. on slot-age eviction, resolve `slot -> leader` and bucket per
//!     `(leader, source)`: win-rate / lead-time / coverage / exclusivity, the
//!     symmetric pairwise series, and the jito-oriented "vs-jito" series (M3).
//!  4. periodically emit to influx (`datapoint_info!`) + a log summary (M4).

use std::{
    collections::HashMap,
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::Receiver;
use log::{info, warn};
use solana_sdk::{clock::Slot, pubkey::Pubkey};

use super::{
    influx::{append_point, InfluxWriter},
    leader::LeaderScheduleHandle,
    parse::{Observation, ShredId},
    sources::{self, SourceId},
    stats::{basis_points, mean, quantiles},
    validators::ValidatorMap,
    BenchmarkConfig,
};

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Slots at/above this are treated as garbage (a stray/corrupt UDP datagram with
/// a plausible variant byte). Real mainnet slots are ~4.3e8; 2^40 (~1.1e12) is
/// ~10000 years away at 400ms/slot. Guards `current_max_slot` from poisoning.
const MAX_PLAUSIBLE_SLOT: u64 = 1 << 40;
/// When no leader window is available, only let `current_max_slot` advance by at
/// most this many slots at once (defends against a plausible-but-garbage slot
/// poisoning eviction). Larger than any real window + leader backfill.
const MAX_SLOT_JUMP: u64 = 10_000;
/// Slack (slots) around the leader window when gating observed slots.
const LEADER_MARGIN: u64 = 128;
/// Cap on samples retained per series, per flush window. Bounds aggregator
/// memory and emit-time sort/copy cost independent of shred rate; p50/p90/p99
/// are accurate from this many samples.
const MAX_SAMPLES_PER_SERIES: usize = 16_384;
/// Max batches drained per outer-loop iteration before yielding to sweep/flush,
/// so sustained load can't starve eviction (which would grow the map unbounded).
const DRAIN_CAP: usize = 4_096;
/// Reject pipeline-latency samples outside `[0, this]` as clock anomalies (an NTP
/// step / CLOCK_REALTIME jump) instead of poisoning the percentiles. Well above any
/// real reconstruct latency, well under a slot (400ms).
const PIPELINE_MAX_PLAUSIBLE_NS: i64 = 200_000_000; // 200ms
/// Defer joining a publish event until its slot is this many slots behind the
/// frontier. By then every composing shred's observation has been ingested, so an
/// index absent from the arrival map reliably means "unobserved" (FEC-recovered or
/// dropped) rather than "the drain hasn't caught up yet". Must stay far below
/// `window_slots` (the arrival-map eviction horizon) so the observations are still
/// present when we join.
const PUBLISH_DEFER_SLOTS: u64 = 3;

/// Reported by the reconstruct thread each time it publishes one `Vec<Entry>`. The
/// aggregator joins it against observed per-shred arrival timestamps
/// (`(slot, data_index) -> earliest rx`) to derive pipeline latency, so the
/// reconstruct thread itself computes nothing. Tiny and `Copy` (drop-on-full).
#[derive(Clone, Copy, Debug)]
pub struct PublishEvent {
    pub slot: Slot,
    /// Inclusive composing data-shred index range `[start_index, end_index]`.
    pub start_index: u32,
    pub end_index: u32,
    /// The left DATA_COMPLETE boundary was missing and `get_indexes` guessed the
    /// range; such batches are excluded from the clean distribution.
    pub unknown_start: bool,
    /// CLOCK_REALTIME nanos captured right after the shmem publish.
    pub publish_ts_ns: i64,
}

/// One latency distribution: a reservoir sample for percentiles + an exact running
/// max and count that are independent of the sampling cap (so `max`/`n` stay true
/// even after the reservoir fills, and late-window tail spikes still land in the
/// percentiles via reservoir replacement).
#[derive(Default)]
struct LatSeries {
    n: u64,
    max_ns: i64,
    samples: Vec<i64>,
}

impl LatSeries {
    fn push(&mut self, v: i64, rng: &mut impl rand::Rng) {
        self.n += 1;
        if self.n == 1 || v > self.max_ns {
            self.max_ns = v;
        }
        if self.samples.len() < MAX_SAMPLES_PER_SERIES {
            self.samples.push(v);
        } else {
            // Reservoir sampling (Algorithm R): keep a uniform sample of all `n`.
            let j = rng.gen_range(0..self.n);
            if (j as usize) < MAX_SAMPLES_PER_SERIES {
                self.samples[j as usize] = v;
            }
        }
    }
    fn quantiles_us(&self) -> [i64; 3] {
        let mut s = self.samples.clone();
        let q = quantiles(&mut s, &[0.5, 0.9, 0.99]);
        [q[0] / 1000, q[1] / 1000, q[2] / 1000]
    }
}

/// Per-flush pipeline-latency accumulators. `clean` = every composing shred was
/// observed (`debt = T_publish - T_ready`, `spread = T_ready - T_first`).
/// `incomplete` = >=1 composing index was ABSENT from the arrival map — FEC-recovered
/// OR its observation was dropped/lost — but >=1 was observed, so a lower-confidence
/// (over-estimated) debt is still measurable. `no_anchor` = no composing index was
/// observed at all (no debt). The `incomplete`/`no_anchor` split intentionally does
/// NOT claim to be pure FEC recovery; the `-pipeline` meta row surfaces the
/// observation-drop / cmsg-missing counts so operators can discount it.
#[derive(Default)]
struct PipelineAcc {
    clean_debt: LatSeries,
    clean_spread: LatSeries,
    incomplete_debt: LatSeries,
    no_anchor_n: u64,
    clock_anomalies: u64,
    unknown_start_excluded: u64,
}

/// Join one publish event against the observed arrival map and fold the resulting
/// latency into `acc`. `data_rx` = `(slot, data_index) -> earliest observed rx`.
fn process_publish_event(
    ev: &PublishEvent,
    data_rx: &ahash::HashMap<(Slot, u32), i64>,
    acc: &mut PipelineAcc,
    rng: &mut impl rand::Rng,
) {
    // Guessed left boundary => composing set is uncertain; exclude.
    if ev.unknown_start {
        acc.unknown_start_excluded += 1;
        return;
    }
    let mut min_rx = i64::MAX;
    let mut max_rx = i64::MIN;
    let mut missing = false; // >=1 composing index was absent from the arrival map
    for idx in ev.start_index..=ev.end_index {
        match data_rx.get(&(ev.slot, idx)) {
            Some(&t) => {
                if t < min_rx {
                    min_rx = t;
                }
                if t > max_rx {
                    max_rx = t;
                }
            }
            None => missing = true,
        }
    }
    if max_rx == i64::MIN {
        // No composing index observed (fully recovered, all dropped, or evicted):
        // no arrival anchor to measure against.
        acc.no_anchor_n += 1;
        return;
    }
    // T_ready = max availability over composing shreds; among observed shreds that
    // is the max arrival. debt = publish - T_ready.
    let debt = ev.publish_ts_ns - max_rx;
    if !(0..=PIPELINE_MAX_PLAUSIBLE_NS).contains(&debt) {
        acc.clock_anomalies += 1;
        return;
    }
    if missing {
        // Approximate: the true completion may be a coding-shred arrival we don't
        // track here (or a dropped observation), so this can over-estimate debt.
        acc.incomplete_debt.push(debt, rng);
    } else {
        let spread = max_rx - min_rx; // >= 0
        // spread shares the CLOCK_REALTIME domain; guard it like debt so a clock
        // step between the first and last intra-batch arrival can't poison it.
        if !(0..=PIPELINE_MAX_PLAUSIBLE_NS).contains(&spread) {
            acc.clock_anomalies += 1;
            return;
        }
        acc.clean_debt.push(debt, rng);
        acc.clean_spread.push(spread, rng);
    }
}

/// Earliest arrival timestamp per source for one shred.
#[derive(Default)]
struct PerSourceArrival {
    /// (source, earliest rx_ts_ns). All jito PoPs share `SourceId::Jito`, so its
    /// timestamp is automatically the min across jito's IPs.
    arrivals: Vec<(SourceId, i64)>,
}

impl PerSourceArrival {
    #[inline]
    fn observe(&mut self, source: SourceId, ts: i64) {
        for (s, t) in self.arrivals.iter_mut() {
            if *s == source {
                if ts < *t {
                    *t = ts;
                }
                return;
            }
        }
        self.arrivals.push((source, ts));
    }
}

/// Per-(leader, source) accumulator for one flush window.
#[derive(Default, Clone)]
struct SourceAgg {
    delivered: u64,
    exclusive: u64,
    contested_delivered: u64,
    contested_firsts: u64,
    lead_samples_ns: Vec<i64>,
}

impl SourceAgg {
    fn merge(&mut self, other: &SourceAgg) {
        self.delivered += other.delivered;
        self.exclusive += other.exclusive;
        self.contested_delivered += other.contested_delivered;
        self.contested_firsts += other.contested_firsts;
        for &x in &other.lead_samples_ns {
            if self.lead_samples_ns.len() >= MAX_SAMPLES_PER_SERIES {
                break;
            }
            self.lead_samples_ns.push(x);
        }
    }

    fn push_lead(&mut self, ns: i64) {
        if self.lead_samples_ns.len() < MAX_SAMPLES_PER_SERIES {
            self.lead_samples_ns.push(ns);
        }
    }
}

/// Per-pair accumulator. delta = ts[b] - ts[a] for canonical (a<b); positive =>
/// source_a was earlier.
#[derive(Default)]
struct PairAgg {
    a_faster: u64,
    b_faster: u64,
    ties: u64,
    deltas: Vec<i64>,
}

/// jito-oriented accumulator for one non-jito source vs the jito baseline.
#[derive(Default)]
struct VsJitoAgg {
    /// shreds where jito AND this source both delivered.
    contested: u64,
    /// source arrived before jito.
    beats: u64,
    /// jito arrived before source.
    losses: u64,
    ties: u64,
    /// Σ(source_rx - jito_rx) ns; negative total => source faster on average.
    delta_sum_ns: i64,
    /// shreds this source delivered that jito did NOT (backroom exclusive).
    source_excl: u64,
    delta_samples_ns: Vec<i64>,
}

impl VsJitoAgg {
    fn merge(&mut self, other: &VsJitoAgg) {
        self.contested += other.contested;
        self.beats += other.beats;
        self.losses += other.losses;
        self.ties += other.ties;
        self.delta_sum_ns += other.delta_sum_ns;
        self.source_excl += other.source_excl;
        for &x in &other.delta_samples_ns {
            if self.delta_samples_ns.len() >= MAX_SAMPLES_PER_SERIES {
                break;
            }
            self.delta_samples_ns.push(x);
        }
    }
}

/// Per-leader accumulator for one flush window.
#[derive(Default)]
struct LeaderAgg {
    total_shreds: u64,
    contested_total: u64,
    /// shreds (of this leader) jito delivered — denominator for vs-jito coverage.
    jito_delivered: u64,
    per_source: HashMap<SourceId, SourceAgg>,
    per_pair: HashMap<(SourceId, SourceId), PairAgg>,
    /// Keyed by the non-jito source (any custom IP source, or DoubleZero). jito
    /// is the baseline and never a key here.
    vs_jito: HashMap<SourceId, VsJitoAgg>,
}

type LeaderKey = Option<Pubkey>;
/// Accumulators keyed by (leader, is_data) so coding shreds (when included) form
/// a separate `shred_type` series and never pollute the data headline.
type AccKey = (LeaderKey, bool);

#[allow(clippy::too_many_arguments)]
pub fn run(
    rx: Receiver<Vec<Observation>>,
    cfg: BenchmarkConfig,
    leader: Option<LeaderScheduleHandle>,
    validators: Option<Arc<ValidatorMap>>,
    dropped: Arc<AtomicU64>,
    ts_missing: Arc<AtomicU64>,
    pipeline_rx: Option<Receiver<PublishEvent>>,
    pipeline_dropped: Arc<AtomicU64>,
    exit: Arc<AtomicBool>,
) {
    let mut map: ahash::HashMap<ShredId, PerSourceArrival> = ahash::HashMap::default();
    let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();
    let mut current_max_slot: Slot = 0;

    // Pipeline-latency (Design B): a windowed (slot, data_index) -> earliest rx map
    // populated from the observation stream, joined against reconstruct's publish
    // events. Empty/unused when pipeline latency is off.
    let pipeline_on = pipeline_rx.is_some();
    let mut data_rx: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
    let mut pipeline_acc = PipelineAcc::default();
    // Publish events buffered until their slot is `PUBLISH_DEFER_SLOTS` behind the
    // frontier, so the join sees a fully-ingested arrival map (no drain-order race).
    let mut pending_publish: Vec<PublishEvent> = Vec::new();
    let mut rng = rand::thread_rng();

    let mut csv = open_csv(&cfg);
    let influx = cfg.influx.as_ref().and_then(InfluxWriter::new);

    let sweep_interval = Duration::from_secs(1);
    let mut last_sweep = Instant::now();
    let mut last_flush = Instant::now();

    info!(
        "benchmark aggregator started (data_only={}, window_slots={}, flush={}s, csv={:?}, leader_schedule={}, validator_map={}, jito_ips={})",
        cfg.data_only,
        cfg.window_slots,
        cfg.flush_interval.as_secs(),
        cfg.csv_path,
        leader.is_some(),
        validators.as_ref().map(|v| v.len()).unwrap_or(0),
        sources::jito_ip_count(),
    );

    while !exit.load(Ordering::Relaxed) {
        let leader_bounds = leader.as_ref().and_then(|h| h.bounds());
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(batch) => {
                ingest(&mut map, &mut data_rx, pipeline_on, &mut current_max_slot, &cfg, &mut csv, batch, leader_bounds);
                let mut drained = 1;
                while drained < DRAIN_CAP {
                    match rx.try_recv() {
                        Ok(batch) => {
                            ingest(&mut map, &mut data_rx, pipeline_on, &mut current_max_slot, &cfg, &mut csv, batch, leader_bounds);
                            drained += 1;
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }

        // Buffer publish events, then join only those whose slot is
        // `PUBLISH_DEFER_SLOTS` behind the frontier — by then all their shreds'
        // observations have been ingested, so an index absent from `data_rx`
        // reliably means unobserved, not a drain-order race.
        if let Some(prx) = pipeline_rx.as_ref() {
            let mut drained = 0;
            while drained < DRAIN_CAP {
                match prx.try_recv() {
                    Ok(ev) => {
                        pending_publish.push(ev);
                        drained += 1;
                    }
                    Err(_) => break,
                }
            }
            let ready_threshold = current_max_slot.saturating_sub(PUBLISH_DEFER_SLOTS);
            pending_publish.retain(|ev| {
                if ev.slot <= ready_threshold {
                    process_publish_event(ev, &data_rx, &mut pipeline_acc, &mut rng);
                    false // processed -> drop from the buffer
                } else {
                    true // not yet ripe -> keep
                }
            });
        }

        if last_sweep.elapsed() >= sweep_interval {
            sweep_finalize(&mut map, current_max_slot, cfg.window_slots, leader.as_ref(), &mut acc);
            if pipeline_on {
                // Evict the arrival map on the same slot-age window as the match map.
                let threshold = current_max_slot.saturating_sub(cfg.window_slots);
                data_rx.retain(|(slot, _), _| *slot >= threshold);
            }
            last_sweep = Instant::now();
            if let Some(w) = csv.as_mut() {
                let _ = w.flush();
            }
        }

        if last_flush.elapsed() >= cfg.flush_interval {
            // emit_pipeline first: it PEEKS the observation dropped/ts_missing
            // counters (to surface drop-induced misclassification), then emit() resets
            // them for the -health series.
            if pipeline_on {
                emit_pipeline(&pipeline_acc, &pipeline_dropped, &dropped, &ts_missing, influx.as_ref());
                pipeline_acc = PipelineAcc::default();
            }
            emit(&acc, validators.as_deref(), leader.as_ref(), &cfg, &dropped, &ts_missing, influx.as_ref());
            acc.clear();
            last_flush = Instant::now();
        }
    }

    sweep_finalize(&mut map, current_max_slot, 0, leader.as_ref(), &mut acc);
    if pipeline_on {
        // Flush any still-deferred publish events (ignore the ripeness threshold).
        for ev in pending_publish.drain(..) {
            process_publish_event(&ev, &data_rx, &mut pipeline_acc, &mut rng);
        }
        emit_pipeline(&pipeline_acc, &pipeline_dropped, &dropped, &ts_missing, influx.as_ref());
    }
    emit(&acc, validators.as_deref(), leader.as_ref(), &cfg, &dropped, &ts_missing, influx.as_ref());
    if let Some(w) = csv.as_mut() {
        let _ = w.flush();
    }
    info!("benchmark aggregator stopped");
}

#[inline]
fn slot_is_plausible(slot: Slot, current_max: Slot, leader_bounds: Option<(Slot, Slot)>) -> bool {
    if slot >= MAX_PLAUSIBLE_SLOT {
        return false;
    }
    match leader_bounds {
        Some((base, end)) => slot + LEADER_MARGIN >= base && slot < end.saturating_add(LEADER_MARGIN),
        None => current_max == 0 || slot <= current_max.saturating_add(MAX_SLOT_JUMP),
    }
}

#[allow(clippy::too_many_arguments)]
fn ingest(
    map: &mut ahash::HashMap<ShredId, PerSourceArrival>,
    data_rx: &mut ahash::HashMap<(Slot, u32), i64>,
    pipeline_on: bool,
    current_max_slot: &mut Slot,
    cfg: &BenchmarkConfig,
    csv: &mut Option<BufWriter<std::fs::File>>,
    batch: Vec<Observation>,
    leader_bounds: Option<(Slot, Slot)>,
) {
    for obs in batch {
        if let Some(w) = csv.as_mut() {
            let _ = writeln!(
                w,
                "{},{},{},{},{},{}",
                obs.rx_ts_ns,
                obs.source,
                obs.slot,
                obs.fec_set_index,
                obs.index,
                if obs.is_data { "data" } else { "code" },
            );
        }
        if !slot_is_plausible(obs.slot, *current_max_slot, leader_bounds) {
            continue;
        }
        if obs.slot > *current_max_slot {
            *current_max_slot = obs.slot;
        }
        // Pipeline latency: record the earliest arrival per data-shred index (min
        // across sources = true earliest). `rx_ts_ns` is always > 0 here (the tap
        // skips the no-kernel-timestamp sentinel).
        if pipeline_on && obs.is_data {
            data_rx
                .entry((obs.slot, obs.index))
                .and_modify(|t| {
                    if obs.rx_ts_ns < *t {
                        *t = obs.rx_ts_ns;
                    }
                })
                .or_insert(obs.rx_ts_ns);
        }
        if cfg.data_only && !obs.is_data {
            continue;
        }
        map.entry(obs.shred_id())
            .or_default()
            .observe(sources::classify(obs.source), obs.rx_ts_ns);
    }
}

fn sweep_finalize(
    map: &mut ahash::HashMap<ShredId, PerSourceArrival>,
    current_max_slot: Slot,
    horizon: u64,
    leader: Option<&LeaderScheduleHandle>,
    acc: &mut HashMap<AccKey, LeaderAgg>,
) {
    let threshold = current_max_slot.saturating_sub(horizon);
    map.retain(|id, psa| {
        if id.slot < threshold || horizon == 0 {
            finalize_one(*id, psa, leader, acc);
            false
        } else {
            true
        }
    });
}

fn finalize_one(
    id: ShredId,
    psa: &PerSourceArrival,
    leader: Option<&LeaderScheduleHandle>,
    acc: &mut HashMap<AccKey, LeaderAgg>,
) {
    let arrivals = &psa.arrivals;
    if arrivals.is_empty() {
        return;
    }
    let leader_key: LeaderKey = leader.and_then(|h| h.leader_for_slot(id.slot));
    let lagg = acc.entry((leader_key, id.is_data)).or_default();
    lagg.total_shreds += 1;

    let contested = arrivals.len() >= 2;
    if contested {
        lagg.contested_total += 1;
    }

    let (winner_src, winner_ts) = arrivals
        .iter()
        .min_by_key(|(_, t)| *t)
        .copied()
        .expect("non-empty");

    for &(src, ts) in arrivals {
        let sa = lagg.per_source.entry(src).or_default();
        sa.delivered += 1;
        if contested {
            sa.contested_delivered += 1;
            sa.push_lead(ts - winner_ts);
            if src == winner_src {
                sa.contested_firsts += 1;
            }
        } else {
            sa.exclusive += 1;
        }
    }

    // Symmetric pairwise series.
    for i in 0..arrivals.len() {
        for j in (i + 1)..arrivals.len() {
            let (a_src, a_ts) = arrivals[i];
            let (b_src, b_ts) = arrivals[j];
            let ((ka, kb), delta) = if a_src <= b_src {
                ((a_src, b_src), b_ts - a_ts)
            } else {
                ((b_src, a_src), a_ts - b_ts)
            };
            let pa = lagg.per_pair.entry((ka, kb)).or_default();
            match delta.cmp(&0) {
                std::cmp::Ordering::Greater => pa.a_faster += 1,
                std::cmp::Ordering::Less => pa.b_faster += 1,
                std::cmp::Ordering::Equal => pa.ties += 1,
            }
            if pa.deltas.len() < MAX_SAMPLES_PER_SERIES {
                pa.deltas.push(delta);
            }
        }
    }

    // jito-oriented series: every non-jito source measured against the jito baseline.
    let jito_ts = arrivals
        .iter()
        .find(|(s, _)| s.is_jito())
        .map(|(_, t)| *t);
    if jito_ts.is_some() {
        lagg.jito_delivered += 1;
    }
    for &(src, ts) in arrivals {
        if src.is_jito() {
            continue;
        }
        let va = lagg.vs_jito.entry(src).or_default();
        match jito_ts {
            Some(jts) => {
                va.contested += 1;
                let d = ts - jts; // negative => source beat jito
                va.delta_sum_ns += d;
                match d.cmp(&0) {
                    std::cmp::Ordering::Less => va.beats += 1,
                    std::cmp::Ordering::Greater => va.losses += 1,
                    std::cmp::Ordering::Equal => va.ties += 1,
                }
                if va.delta_samples_ns.len() < MAX_SAMPLES_PER_SERIES {
                    va.delta_samples_ns.push(d);
                }
            }
            None => va.source_excl += 1,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit(
    acc: &HashMap<AccKey, LeaderAgg>,
    validators: Option<&ValidatorMap>,
    leader: Option<&LeaderScheduleHandle>,
    cfg: &BenchmarkConfig,
    dropped: &AtomicU64,
    ts_missing: &AtomicU64,
    influx: Option<&InfluxWriter>,
) {
    let dropped_now = dropped.swap(0, Ordering::Relaxed);
    let ts_missing_now = ts_missing.swap(0, Ordering::Relaxed);
    if dropped_now > 0 {
        warn!("benchmark dropped {dropped_now} observations since last flush (channel full)");
    }
    if ts_missing_now > 0 {
        warn!("benchmark skipped {ts_missing_now} packets with no kernel timestamp since last flush");
    }
    let want_influx = influx.is_some();
    let ts = now_unix_nanos();
    let mut buf = String::new();

    if want_influx {
        let leader_base_slot = leader.and_then(|h| h.bounds()).map(|(b, _)| b).unwrap_or(0);
        append_point(
            &mut buf,
            "shredstream_bench-health",
            &[],
            &[
                ("dropped_observations", dropped_now as i64),
                ("ts_missing", ts_missing_now as i64),
                ("series_tracked", acc.len() as i64),
                ("leader_base_slot", leader_base_slot as i64),
            ],
            ts,
        );
    }

    // Global (all-leaders, data-only) rollups — for the log summary and the
    // leader="ALL" rows.
    let mut global_source: HashMap<SourceId, SourceAgg> = HashMap::new();
    let mut global_total: u64 = 0;
    let mut global_vs: HashMap<SourceId, VsJitoAgg> = HashMap::new();
    let mut global_jito_delivered: u64 = 0;

    for ((leader_key, is_data), lagg) in acc.iter() {
        let shred_type = if *is_data { "data" } else { "code" };
        let leader_label = leader_key
            .map(|p| p.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let (region, in_region) = match (leader_key, validators) {
            (Some(pk), Some(vm)) => (
                vm.region_label(pk),
                vm.is_in_region(pk, &cfg.node_country, cfg.region_max_rtt_us),
            ),
            _ => ("unknown".to_string(), false),
        };

        if *is_data {
            global_total += lagg.total_shreds;
            global_jito_delivered += lagg.jito_delivered;
            for (src, sa) in lagg.per_source.iter() {
                global_source.entry(*src).or_default().merge(sa);
            }
            for (src, va) in lagg.vs_jito.iter() {
                global_vs.entry(*src).or_default().merge(va);
            }
        }

        if !want_influx || lagg.total_shreds < cfg.min_samples {
            continue;
        }

        // Headline: vs-jito rows, always emitted (when jito delivered for this leader).
        if lagg.jito_delivered >= cfg.min_samples {
            for (src, va) in lagg.vs_jito.iter() {
                append_vs_jito(&mut buf, ts, &leader_label, &region, in_region, shred_type, *src, va, lagg.jito_delivered);
            }
        }
        // Optional per-source + symmetric-pair detail.
        if cfg.emit_source_pair {
            for (src, sa) in lagg.per_source.iter() {
                append_source(&mut buf, ts, &leader_label, &region, in_region, shred_type, &src.label(), sa, lagg.total_shreds);
            }
            for ((a, b), pa) in lagg.per_pair.iter() {
                append_pair(&mut buf, ts, &leader_label, &region, in_region, shred_type, &a.label(), &b.label(), pa);
            }
        }
    }

    // Global rollup rows (leader = "ALL").
    if want_influx && global_jito_delivered >= cfg.min_samples {
        for (src, va) in global_vs.iter() {
            append_vs_jito(&mut buf, ts, "ALL", "ALL", false, "data", *src, va, global_jito_delivered);
        }
    }
    if want_influx && cfg.emit_source_pair && global_total >= cfg.min_samples {
        for (src, sa) in global_source.iter() {
            append_source(&mut buf, ts, "ALL", "ALL", false, "data", &src.label(), sa, global_total);
        }
    }

    if let Some(w) = influx {
        w.write(&buf);
    }
    if global_total >= cfg.min_samples {
        log_summary(&global_source, global_total);
    }
}

#[allow(clippy::too_many_arguments)]
fn append_source(
    buf: &mut String,
    ts: u128,
    leader_label: &str,
    region: &str,
    in_region: bool,
    shred_type: &str,
    source: &str,
    sa: &SourceAgg,
    total: u64,
) {
    let mut samples = sa.lead_samples_ns.clone();
    let q = quantiles(&mut samples, &[0.5, 0.9, 0.99]);
    let lead_mean = mean(&samples);
    let in_region_s = in_region.to_string();
    append_point(
        buf,
        "shredstream_bench-source",
        &[
            ("leader", leader_label),
            ("region", region),
            ("in_region", in_region_s.as_str()),
            ("shred_type", shred_type),
            ("source", source),
        ],
        &[
            ("total", total as i64),
            ("delivered", sa.delivered as i64),
            ("contested", sa.contested_delivered as i64),
            ("contested_firsts", sa.contested_firsts as i64),
            ("win_rate_bps", basis_points(sa.contested_firsts, sa.contested_delivered)),
            ("coverage_bps", basis_points(sa.delivered, total)),
            ("exclusive_bps", basis_points(sa.exclusive, total)),
            ("lead_mean_us", lead_mean / 1000),
            ("lead_p50_us", q[0] / 1000),
            ("lead_p90_us", q[1] / 1000),
            ("lead_p99_us", q[2] / 1000),
        ],
        ts,
    );
}

#[allow(clippy::too_many_arguments)]
fn append_pair(
    buf: &mut String,
    ts: u128,
    leader_label: &str,
    region: &str,
    in_region: bool,
    shred_type: &str,
    a: &str,
    b: &str,
    pa: &PairAgg,
) {
    let mut samples = pa.deltas.clone();
    let q = quantiles(&mut samples, &[0.5, 0.9, 0.99]);
    let decided = pa.a_faster + pa.b_faster;
    let in_region_s = in_region.to_string();
    append_point(
        buf,
        "shredstream_bench-pair",
        &[
            ("leader", leader_label),
            ("region", region),
            ("in_region", in_region_s.as_str()),
            ("shred_type", shred_type),
            ("source_a", a),
            ("source_b", b),
        ],
        &[
            ("samples", pa.deltas.len() as i64),
            ("a_faster", pa.a_faster as i64),
            ("b_faster", pa.b_faster as i64),
            ("ties", pa.ties as i64),
            ("a_win_rate_bps", basis_points(pa.a_faster, decided)),
            ("delta_p50_us", q[0] / 1000),
            ("delta_p90_us", q[1] / 1000),
            ("delta_p99_us", q[2] / 1000),
        ],
        ts,
    );
}

/// The headline series for "per validator, how does this source do vs jito".
/// `delta_sum_us` and the percentiles are signed: negative => source faster than jito.
#[allow(clippy::too_many_arguments)]
fn append_vs_jito(
    buf: &mut String,
    ts: u128,
    leader_label: &str,
    region: &str,
    in_region: bool,
    shred_type: &str,
    source: SourceId,
    va: &VsJitoAgg,
    jito_delivered: u64,
) {
    let mut samples = va.delta_samples_ns.clone();
    let q = quantiles(&mut samples, &[0.5, 0.9, 0.99]);
    let in_region_s = in_region.to_string();
    let src = source.label();
    append_point(
        buf,
        "shredstream_bench-vs-jito",
        &[
            ("leader", leader_label),
            ("region", region),
            ("in_region", in_region_s.as_str()),
            ("shred_type", shred_type),
            ("source", src.as_str()),
        ],
        &[
            ("contested", va.contested as i64),
            ("beats", va.beats as i64),
            ("losses", va.losses as i64),
            ("ties", va.ties as i64),
            ("beat_rate_bps", basis_points(va.beats, va.contested)),
            ("coverage_vs_jito_bps", basis_points(va.contested, jito_delivered)),
            ("source_excl", va.source_excl as i64),
            ("delta_sum_us", va.delta_sum_ns / 1000),
            ("delta_p50_us", q[0] / 1000),
            ("delta_p90_us", q[1] / 1000),
            ("delta_p99_us", q[2] / 1000),
        ],
        ts,
    );
}

/// Emit the pipeline-latency series (`shredstream_bench-pipeline`). Runs on the
/// aggregator thread at flush; never touches the reconstruct hot path. `obs_dropped`
/// and `ts_missing` are PEEKED (loaded, not reset) so the meta row surfaces the
/// observation loss that can misclassify clean batches as incomplete; the -health
/// series (emit(), called after this) resets them.
fn emit_pipeline(
    acc: &PipelineAcc,
    pipeline_dropped: &AtomicU64,
    obs_dropped: &AtomicU64,
    ts_missing: &AtomicU64,
    influx: Option<&InfluxWriter>,
) {
    let pipeline_dropped_now = pipeline_dropped.swap(0, Ordering::Relaxed);
    let obs_dropped_now = obs_dropped.load(Ordering::Relaxed);
    let ts_missing_now = ts_missing.load(Ordering::Relaxed);
    let Some(w) = influx else {
        if acc.clean_debt.n + acc.incomplete_debt.n + acc.no_anchor_n > 0 {
            let q = acc.clean_debt.quantiles_us();
            info!(
                "pipeline latency: clean n={} p50={}us p99={}us max={}us | incomplete n={} \
                 no_anchor={} anomalies={} obs_dropped={} ts_missing={} pub_dropped={}",
                acc.clean_debt.n,
                q[0],
                q[2],
                acc.clean_debt.max_ns / 1000,
                acc.incomplete_debt.n,
                acc.no_anchor_n,
                acc.clock_anomalies,
                obs_dropped_now,
                ts_missing_now,
                pipeline_dropped_now,
            );
        }
        return;
    };
    let ts = now_unix_nanos();
    let mut buf = String::new();
    append_lat_series(&mut buf, ts, "clean", &acc.clean_debt, Some(&acc.clean_spread));
    append_lat_series(&mut buf, ts, "incomplete", &acc.incomplete_debt, None);
    // Per-window counters that are NOT per-class (so they aren't mis-attributed to
    // one class). obs_dropped/ts_missing let a query discount the clean/incomplete
    // split during observation-loss windows.
    append_point(
        &mut buf,
        "shredstream_bench-pipeline",
        &[("leader", "ALL"), ("class", "meta")],
        &[
            ("no_anchor_n", acc.no_anchor_n as i64),
            ("clock_anomalies", acc.clock_anomalies as i64),
            ("unknown_start_excluded", acc.unknown_start_excluded as i64),
            ("pipeline_dropped", pipeline_dropped_now as i64),
            ("obs_dropped", obs_dropped_now as i64),
            ("ts_missing", ts_missing_now as i64),
        ],
        ts,
    );
    w.write(&buf);
}

/// Append one class row: `n` (exact count) + debt p50/p90/p99/max, and optional
/// spread p50/p90/p99/max. Percentiles come from the reservoir; `n`/`max` are exact.
fn append_lat_series(
    buf: &mut String,
    ts: u128,
    class: &str,
    debt: &LatSeries,
    spread: Option<&LatSeries>,
) {
    let dq = debt.quantiles_us();
    let mut fields: Vec<(&str, i64)> = vec![
        ("n", debt.n as i64),
        ("debt_p50_us", dq[0]),
        ("debt_p90_us", dq[1]),
        ("debt_p99_us", dq[2]),
        ("debt_max_us", debt.max_ns / 1000),
    ];
    if let Some(sp) = spread {
        let sq = sp.quantiles_us();
        fields.push(("spread_p50_us", sq[0]));
        fields.push(("spread_p90_us", sq[1]));
        fields.push(("spread_p99_us", sq[2]));
        fields.push(("spread_max_us", sp.max_ns / 1000));
    }
    append_point(
        buf,
        "shredstream_bench-pipeline",
        &[("leader", "ALL"), ("class", class)],
        &fields,
        ts,
    );
}

fn log_summary(global: &HashMap<SourceId, SourceAgg>, total: u64) {
    let mut rows: Vec<(SourceId, &SourceAgg)> = global.iter().map(|(k, v)| (*k, v)).collect();
    rows.sort_by_key(|(_, sa)| std::cmp::Reverse(sa.contested_firsts));
    let summary: Vec<String> = rows
        .iter()
        .map(|(sid, sa)| {
            let mut s = sa.lead_samples_ns.clone();
            let q = quantiles(&mut s, &[0.5]);
            format!(
                "{}: win={:.1}% cover={:.1}% excl={:.1}% p50_lead={}us",
                sid.label(),
                basis_points(sa.contested_firsts, sa.contested_delivered) as f64 / 100.0,
                basis_points(sa.delivered, total) as f64 / 100.0,
                basis_points(sa.exclusive, total) as f64 / 100.0,
                q[0] / 1000,
            )
        })
        .collect();
    info!("benchmark global ({total} data shreds): {}", summary.join(" | "));
}

fn open_csv(cfg: &BenchmarkConfig) -> Option<BufWriter<std::fs::File>> {
    let path = cfg.csv_path.as_ref()?;
    match std::fs::File::create(path) {
        Ok(f) => {
            let mut w = BufWriter::new(f);
            let _ = writeln!(w, "rx_ts_ns,source,slot,fec_set_index,index,type");
            info!("benchmark raw CSV -> {}", path.display());
            Some(w)
        }
        Err(e) => {
            warn!("failed to open benchmark CSV {}: {e}", path.display());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, n))
    }
    fn sid(n: u8) -> SourceId {
        SourceId::Ip(ip(n))
    }

    #[test]
    fn slot_guard_rejects_garbage_and_bounds_jumps() {
        assert!(!slot_is_plausible(MAX_PLAUSIBLE_SLOT, 0, None));
        assert!(!slot_is_plausible(u64::MAX, 1_000, None));
        assert!(slot_is_plausible(500_000_000, 0, None));
        assert!(slot_is_plausible(1_000 + MAX_SLOT_JUMP, 1_000, None));
        assert!(!slot_is_plausible(1_000 + MAX_SLOT_JUMP + 1, 1_000, None));
        let bounds = Some((1_000u64, 6_000u64));
        assert!(slot_is_plausible(3_000, 0, bounds));
        assert!(!slot_is_plausible(50_000, 5_999, bounds));
        assert!(!slot_is_plausible(500, 0, bounds));
    }

    #[test]
    fn finalize_contested_and_exclusive() {
        let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();
        let (a, b) = (sid(1), sid(2)); // Ip(10.0.0.1) < Ip(10.0.0.2)

        let mut psa = PerSourceArrival::default();
        psa.observe(a, 1000);
        psa.observe(b, 1500);
        finalize_one(ShredId { slot: 100, fec_set_index: 0, index: 5, is_data: true }, &psa, None, &mut acc);

        let mut psa2 = PerSourceArrival::default();
        psa2.observe(a, 2000);
        finalize_one(ShredId { slot: 101, fec_set_index: 0, index: 1, is_data: true }, &psa2, None, &mut acc);

        let lagg = acc.get(&(None, true)).unwrap();
        assert_eq!(lagg.total_shreds, 2);
        assert_eq!(lagg.contested_total, 1);

        let sa_a = lagg.per_source.get(&a).unwrap();
        assert_eq!(sa_a.delivered, 2);
        assert_eq!(sa_a.exclusive, 1);
        assert_eq!(sa_a.contested_firsts, 1);
        assert_eq!(sa_a.lead_samples_ns, vec![0]);

        let sa_b = lagg.per_source.get(&b).unwrap();
        assert_eq!(sa_b.contested_firsts, 0);
        assert_eq!(sa_b.lead_samples_ns, vec![500]);

        let pa = lagg.per_pair.get(&(a, b)).unwrap();
        assert_eq!((pa.a_faster, pa.b_faster, pa.ties), (1, 0, 0));
    }

    #[test]
    fn vs_jito_orientation_and_exclusive() {
        let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();
        let custom = sid(7);

        // shred 1: jito 1000, custom 800 -> custom beats jito by 200ns
        let mut s1 = PerSourceArrival::default();
        s1.observe(SourceId::Jito, 1000);
        s1.observe(custom, 800);
        finalize_one(ShredId { slot: 10, fec_set_index: 0, index: 0, is_data: true }, &s1, None, &mut acc);

        // shred 2: jito 1000, custom 1300 -> jito faster
        let mut s2 = PerSourceArrival::default();
        s2.observe(SourceId::Jito, 1000);
        s2.observe(custom, 1300);
        finalize_one(ShredId { slot: 10, fec_set_index: 0, index: 1, is_data: true }, &s2, None, &mut acc);

        // shred 3: custom only (jito never delivered) -> source exclusive
        let mut s3 = PerSourceArrival::default();
        s3.observe(custom, 500);
        finalize_one(ShredId { slot: 10, fec_set_index: 0, index: 2, is_data: true }, &s3, None, &mut acc);

        let lagg = acc.get(&(None, true)).unwrap();
        assert_eq!(lagg.jito_delivered, 2); // shreds 1 and 2
        let va = lagg.vs_jito.get(&sid(7)).unwrap();
        assert_eq!(va.contested, 2);
        assert_eq!(va.beats, 1);
        assert_eq!(va.losses, 1);
        assert_eq!(va.source_excl, 1);
        assert_eq!(va.delta_sum_ns, -200 + 300); // = +100 ns net
        // coverage = contested(2) / jito_delivered(2) = 100%
        assert_eq!(basis_points(va.contested, lagg.jito_delivered), 10_000);
    }

    #[test]
    fn doublezero_measured_vs_jito() {
        // DoubleZero (identified by ingress, not IP) must flow through the
        // generalized vs-jito series just like any custom IP source.
        let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();

        // jito 1000, doublezero 700 -> doublezero beats jito by 300ns
        let mut s = PerSourceArrival::default();
        s.observe(SourceId::Jito, 1000);
        s.observe(SourceId::DoubleZero, 700);
        finalize_one(ShredId { slot: 5, fec_set_index: 0, index: 0, is_data: true }, &s, None, &mut acc);

        let lagg = acc.get(&(None, true)).unwrap();
        let va = lagg.vs_jito.get(&SourceId::DoubleZero).unwrap();
        assert_eq!(va.contested, 1);
        assert_eq!(va.beats, 1);
        assert_eq!(va.delta_sum_ns, -300);
        // jito is the baseline; it must never be a vs_jito key.
        assert!(lagg.vs_jito.get(&SourceId::Jito).is_none());
    }

    // ---- pipeline-latency join (Design B) ----

    fn ev(slot: Slot, start: u32, end: u32, unknown: bool, publish: i64) -> PublishEvent {
        PublishEvent {
            slot,
            start_index: start,
            end_index: end,
            unknown_start: unknown,
            publish_ts_ns: publish,
        }
    }

    #[test]
    fn pipeline_clean_sample() {
        // All composing indices observed. T_ready = max arrival, T_first = min.
        let mut m: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
        m.insert((100, 0), 1000);
        m.insert((100, 1), 1500); // latest -> T_ready
        m.insert((100, 2), 1200);
        let mut acc = PipelineAcc::default();
        let mut rng = rand::thread_rng();
        process_publish_event(&ev(100, 0, 2, false, 1800), &m, &mut acc, &mut rng);
        assert_eq!(acc.clean_debt.n, 1);
        assert_eq!(acc.incomplete_debt.n, 0);
        assert_eq!(acc.clean_debt.samples, vec![300]); // publish - T_ready
        assert_eq!(acc.clean_debt.max_ns, 300);
        assert_eq!(acc.clean_spread.samples, vec![500]); // T_ready - T_first
    }

    #[test]
    fn pipeline_incomplete_sample() {
        // Index 1 absent (FEC-recovered or observation lost) -> incomplete class,
        // approx debt anchored on the max observed arrival.
        let mut m: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
        m.insert((100, 0), 1000);
        m.insert((100, 2), 1200);
        let mut acc = PipelineAcc::default();
        let mut rng = rand::thread_rng();
        process_publish_event(&ev(100, 0, 2, false, 1600), &m, &mut acc, &mut rng);
        assert_eq!(acc.clean_debt.n, 0);
        assert_eq!(acc.incomplete_debt.n, 1);
        assert_eq!(acc.incomplete_debt.samples, vec![400]);
        assert!(acc.clean_spread.samples.is_empty()); // spread only for clean
    }

    #[test]
    fn pipeline_clock_anomaly_rejected() {
        let mut m: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
        m.insert((100, 0), 2000);
        let mut acc = PipelineAcc::default();
        let mut rng = rand::thread_rng();
        // negative debt (publish before ready)
        process_publish_event(&ev(100, 0, 0, false, 1000), &m, &mut acc, &mut rng);
        // absurd debt (> CAP)
        process_publish_event(
            &ev(100, 0, 0, false, 2000 + PIPELINE_MAX_PLAUSIBLE_NS + 1),
            &m,
            &mut acc,
            &mut rng,
        );
        assert_eq!(acc.clock_anomalies, 2);
        assert_eq!(acc.clean_debt.n, 0);
        assert_eq!(acc.incomplete_debt.n, 0);
    }

    #[test]
    fn pipeline_spread_clock_guard() {
        // A huge intra-batch spread (a CLOCK_REALTIME step landing between arrivals)
        // is rejected even though debt is in range — spread must not poison the
        // percentiles unguarded.
        let mut m: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
        m.insert((100, 0), 1000);
        let max_rx = 1000 + PIPELINE_MAX_PLAUSIBLE_NS + 1000;
        m.insert((100, 1), max_rx);
        let mut acc = PipelineAcc::default();
        let mut rng = rand::thread_rng();
        process_publish_event(&ev(100, 0, 1, false, max_rx + 100), &m, &mut acc, &mut rng);
        assert_eq!(acc.clock_anomalies, 1);
        assert_eq!(acc.clean_debt.n, 0);
        assert_eq!(acc.clean_spread.n, 0);
    }

    #[test]
    fn pipeline_unknown_start_excluded() {
        let m: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
        let mut acc = PipelineAcc::default();
        let mut rng = rand::thread_rng();
        process_publish_event(&ev(100, 0, 2, true, 5000), &m, &mut acc, &mut rng);
        assert_eq!(acc.unknown_start_excluded, 1);
        assert_eq!(acc.clean_debt.n, 0);
        assert_eq!(acc.incomplete_debt.n, 0);
    }

    #[test]
    fn pipeline_no_anchor_counts_no_sample() {
        // No composing index observed: count it, but no arrival anchor -> no sample.
        let m: ahash::HashMap<(Slot, u32), i64> = ahash::HashMap::default();
        let mut acc = PipelineAcc::default();
        let mut rng = rand::thread_rng();
        process_publish_event(&ev(100, 5, 6, false, 9999), &m, &mut acc, &mut rng);
        assert_eq!(acc.no_anchor_n, 1);
        assert!(acc.incomplete_debt.samples.is_empty());
        assert_eq!(acc.clean_debt.n, 0);
    }
}

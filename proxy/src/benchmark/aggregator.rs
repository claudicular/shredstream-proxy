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

pub fn run(
    rx: Receiver<Vec<Observation>>,
    cfg: BenchmarkConfig,
    leader: Option<LeaderScheduleHandle>,
    validators: Option<Arc<ValidatorMap>>,
    dropped: Arc<AtomicU64>,
    exit: Arc<AtomicBool>,
) {
    let mut map: ahash::HashMap<ShredId, PerSourceArrival> = ahash::HashMap::default();
    let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();
    let mut current_max_slot: Slot = 0;

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
                ingest(&mut map, &mut current_max_slot, &cfg, &mut csv, batch, leader_bounds);
                let mut drained = 1;
                while drained < DRAIN_CAP {
                    match rx.try_recv() {
                        Ok(batch) => {
                            ingest(&mut map, &mut current_max_slot, &cfg, &mut csv, batch, leader_bounds);
                            drained += 1;
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }

        if last_sweep.elapsed() >= sweep_interval {
            sweep_finalize(&mut map, current_max_slot, cfg.window_slots, leader.as_ref(), &mut acc);
            last_sweep = Instant::now();
            if let Some(w) = csv.as_mut() {
                let _ = w.flush();
            }
        }

        if last_flush.elapsed() >= cfg.flush_interval {
            emit(&acc, validators.as_deref(), leader.as_ref(), &cfg, &dropped, influx.as_ref());
            acc.clear();
            last_flush = Instant::now();
        }
    }

    sweep_finalize(&mut map, current_max_slot, 0, leader.as_ref(), &mut acc);
    emit(&acc, validators.as_deref(), leader.as_ref(), &cfg, &dropped, influx.as_ref());
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

fn ingest(
    map: &mut ahash::HashMap<ShredId, PerSourceArrival>,
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

fn emit(
    acc: &HashMap<AccKey, LeaderAgg>,
    validators: Option<&ValidatorMap>,
    leader: Option<&LeaderScheduleHandle>,
    cfg: &BenchmarkConfig,
    dropped: &AtomicU64,
    influx: Option<&InfluxWriter>,
) {
    let dropped_now = dropped.swap(0, Ordering::Relaxed);
    if dropped_now > 0 {
        warn!("benchmark dropped {dropped_now} observations since last flush (channel full)");
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
}

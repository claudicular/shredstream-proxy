//! The benchmark aggregator: a single thread that owns all matching/stat state
//! so the hot ingest path stays lock-free (it only parses headers and pushes
//! observations onto a bounded channel).
//!
//! Pipeline per the design:
//!  1. raw CSV dump of every observation (milestone M1), optional.
//!  2. match the same shred across sources via `ShredId` -> earliest-arrival per
//!     source (M2).
//!  3. on slot-age eviction, resolve `slot -> leader` and bucket win-rate /
//!     lead-time / pairwise deltas / coverage / exclusivity per
//!     `(leader, source)` (M3).
//!  4. periodically emit to influx (`datapoint_info!`) + a log summary (M4).

use std::{
    collections::HashMap,
    io::{BufWriter, Write},
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crossbeam_channel::Receiver;
use log::{info, warn};
use solana_metrics::datapoint_info;
use solana_sdk::{clock::Slot, pubkey::Pubkey};

use super::{
    leader::LeaderScheduleHandle,
    parse::{Observation, ShredId},
    stats::{basis_points, mean, quantiles},
    validators::ValidatorMap,
    BenchmarkConfig,
};

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
/// Cap on samples retained per (leader, source) and per pair, per flush window.
/// Bounds aggregator memory and emit-time sort/copy cost independent of shred
/// rate; p50/p90/p99 are accurate from this many samples.
const MAX_SAMPLES_PER_SERIES: usize = 16_384;
/// Max batches drained per outer-loop iteration before yielding to sweep/flush,
/// so sustained load can't starve eviction (which would grow the map unbounded).
const DRAIN_CAP: usize = 4_096;

/// Earliest arrival timestamp per source for one shred.
#[derive(Default)]
struct PerSourceArrival {
    /// (source, earliest rx_ts_ns). N is the number of distinct sources (small).
    arrivals: Vec<(IpAddr, i64)>,
}

impl PerSourceArrival {
    #[inline]
    fn observe(&mut self, source: IpAddr, ts: i64) {
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
    /// shreds (of this leader) this source delivered at all (union numerator).
    delivered: u64,
    /// shreds where this source was the ONLY deliverer (backroom exclusive).
    exclusive: u64,
    /// contested shreds (>=2 sources) this source participated in.
    contested_delivered: u64,
    /// contested shreds this source delivered first.
    contested_firsts: u64,
    /// lead time vs the winner (ns) over CONTESTED shreds; 0 when this source
    /// won. Capped at MAX_SAMPLES_PER_SERIES.
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

/// Per-pair accumulator. delta = ts[max_ip] - ts[min_ip]; positive => the
/// min-ip source (source_a) was earlier.
#[derive(Default)]
struct PairAgg {
    a_faster: u64,
    b_faster: u64,
    ties: u64,
    deltas: Vec<i64>,
}

/// Per-leader accumulator for one flush window.
#[derive(Default)]
struct LeaderAgg {
    total_shreds: u64,
    contested_total: u64,
    per_source: HashMap<IpAddr, SourceAgg>,
    per_pair: HashMap<(IpAddr, IpAddr), PairAgg>,
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

    let sweep_interval = Duration::from_secs(1);
    let mut last_sweep = Instant::now();
    let mut last_flush = Instant::now();

    info!(
        "benchmark aggregator started (data_only={}, window_slots={}, flush={}s, csv={:?}, leader_schedule={}, validator_map={})",
        cfg.data_only,
        cfg.window_slots,
        cfg.flush_interval.as_secs(),
        cfg.csv_path,
        leader.is_some(),
        validators.as_ref().map(|v| v.len()).unwrap_or(0),
    );

    while !exit.load(Ordering::Relaxed) {
        let leader_bounds = leader.as_ref().and_then(|h| h.bounds());
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(batch) => {
                ingest(&mut map, &mut current_max_slot, &cfg, &mut csv, batch, leader_bounds);
                // Bounded drain so sweep/flush still run under sustained load.
                let mut drained = 1;
                while drained < DRAIN_CAP {
                    match rx.try_recv() {
                        Ok(batch) => {
                            ingest(
                                &mut map,
                                &mut current_max_slot,
                                &cfg,
                                &mut csv,
                                batch,
                                leader_bounds,
                            );
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
            sweep_finalize(
                &mut map,
                current_max_slot,
                cfg.window_slots,
                leader.as_ref(),
                &mut acc,
            );
            last_sweep = Instant::now();
            if let Some(w) = csv.as_mut() {
                let _ = w.flush();
            }
        }

        if last_flush.elapsed() >= cfg.flush_interval {
            emit(&acc, validators.as_deref(), leader.as_ref(), &cfg, &dropped);
            acc.clear();
            last_flush = Instant::now();
        }
    }

    // Final finalize + flush on shutdown.
    sweep_finalize(&mut map, current_max_slot, 0, leader.as_ref(), &mut acc);
    emit(&acc, validators.as_deref(), leader.as_ref(), &cfg, &dropped);
    if let Some(w) = csv.as_mut() {
        let _ = w.flush();
    }
    info!("benchmark aggregator stopped");
}

/// Whether an observed slot is plausible enough to drive matching/eviction.
#[inline]
fn slot_is_plausible(slot: Slot, current_max: Slot, leader_bounds: Option<(Slot, Slot)>) -> bool {
    if slot >= MAX_PLAUSIBLE_SLOT {
        return false;
    }
    match leader_bounds {
        // Leader window is authoritative: live shreds always fall inside it.
        Some((base, end)) => {
            slot + LEADER_MARGIN >= base && slot < end.saturating_add(LEADER_MARGIN)
        }
        // No window yet (startup / no RPC): bootstrap on first, then bound jumps.
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
            // rx_ts_ns,source,slot,fec_set_index,index,type
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
        // Guard eviction/matching against garbage slots from stray/corrupt UDP.
        if !slot_is_plausible(obs.slot, *current_max_slot, leader_bounds) {
            continue;
        }
        if obs.slot > *current_max_slot {
            *current_max_slot = obs.slot;
        }
        // Headline stats are computed on DATA shreds only unless include_coding
        // is set; coding still appears in the raw CSV above.
        if cfg.data_only && !obs.is_data {
            continue;
        }
        map.entry(obs.shred_id())
            .or_default()
            .observe(obs.source, obs.rx_ts_ns);
    }
}

/// Finalize every shred whose slot is older than `current_max_slot - horizon`.
fn sweep_finalize(
    map: &mut ahash::HashMap<ShredId, PerSourceArrival>,
    current_max_slot: Slot,
    horizon: u64,
    leader: Option<&LeaderScheduleHandle>,
    acc: &mut HashMap<AccKey, LeaderAgg>,
) {
    let threshold = current_max_slot.saturating_sub(horizon);
    map.retain(|id, psa| {
        // horizon==0 (shutdown) finalizes everything.
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

    let (winner_ip, winner_ts) = arrivals
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
            if src == winner_ip {
                sa.contested_firsts += 1;
            }
        } else {
            sa.exclusive += 1;
        }
    }

    // Pairwise signed deltas (only exist when >= 2 sources).
    for i in 0..arrivals.len() {
        for j in (i + 1)..arrivals.len() {
            let (a_ip, a_ts) = arrivals[i];
            let (b_ip, b_ts) = arrivals[j];
            let ((ka, kb), delta) = if a_ip <= b_ip {
                ((a_ip, b_ip), b_ts - a_ts)
            } else {
                ((b_ip, a_ip), a_ts - b_ts)
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
}

fn emit(
    acc: &HashMap<AccKey, LeaderAgg>,
    validators: Option<&ValidatorMap>,
    leader: Option<&LeaderScheduleHandle>,
    cfg: &BenchmarkConfig,
    dropped: &AtomicU64,
) {
    let dropped_now = dropped.swap(0, Ordering::Relaxed);
    if dropped_now > 0 {
        warn!("benchmark dropped {dropped_now} observations since last flush (channel full)");
    }
    let leader_base_slot = leader.and_then(|h| h.bounds()).map(|(b, _)| b).unwrap_or(0);
    datapoint_info!(
        "shredstream_bench-health",
        ("dropped_observations", dropped_now as i64, i64),
        ("series_tracked", acc.len() as i64, i64),
        ("leader_base_slot", leader_base_slot as i64, i64),
    );

    // Global (all-leaders) rollup, DATA shreds only (the headline series).
    let mut global: HashMap<IpAddr, SourceAgg> = HashMap::new();
    let mut global_total: u64 = 0;

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

        // accumulate the global (data-only) rollup
        if *is_data {
            global_total += lagg.total_shreds;
            for (src, sa) in lagg.per_source.iter() {
                global.entry(*src).or_default().merge(sa);
            }
        }

        if lagg.total_shreds < cfg.min_samples {
            continue;
        }

        for (src, sa) in lagg.per_source.iter() {
            emit_source_row(&leader_label, &region, in_region, shred_type, *src, sa, lagg.total_shreds);
        }
        for ((a, b), pa) in lagg.per_pair.iter() {
            emit_pair_row(&leader_label, &region, shred_type, *a, *b, pa);
        }
    }

    // Global rollup row (leader = "ALL").
    if global_total >= cfg.min_samples {
        for (src, sa) in global.iter() {
            emit_source_row("ALL", "ALL", false, "data", *src, sa, global_total);
        }
        log_summary(&global, global_total);
    }
}

fn emit_source_row(
    leader_label: &str,
    region: &str,
    in_region: bool,
    shred_type: &str,
    src: IpAddr,
    sa: &SourceAgg,
    total: u64,
) {
    let mut samples = sa.lead_samples_ns.clone();
    let q = quantiles(&mut samples, &[0.5, 0.9, 0.99]);
    let lead_mean = mean(&samples);
    datapoint_info!(
        "shredstream_bench-source",
        "leader" => leader_label,
        "region" => region,
        "in_region" => in_region.to_string(),
        "shred_type" => shred_type,
        "source" => src.to_string(),
        ("total", total as i64, i64),
        ("delivered", sa.delivered as i64, i64),
        // win-rate among contested races this source took part in
        ("contested", sa.contested_delivered as i64, i64),
        ("contested_firsts", sa.contested_firsts as i64, i64),
        ("win_rate_bps", basis_points(sa.contested_firsts, sa.contested_delivered), i64),
        // fraction of this leader's shreds the source delivered, and was alone on
        ("coverage_bps", basis_points(sa.delivered, total), i64),
        ("exclusive_bps", basis_points(sa.exclusive, total), i64),
        ("lead_mean_us", lead_mean / 1000, i64),
        ("lead_p50_us", q[0] / 1000, i64),
        ("lead_p90_us", q[1] / 1000, i64),
        ("lead_p99_us", q[2] / 1000, i64),
    );
}

fn emit_pair_row(leader_label: &str, region: &str, shred_type: &str, a: IpAddr, b: IpAddr, pa: &PairAgg) {
    let mut samples = pa.deltas.clone();
    let q = quantiles(&mut samples, &[0.5, 0.9, 0.99]);
    let decided = pa.a_faster + pa.b_faster;
    datapoint_info!(
        "shredstream_bench-pair",
        "leader" => leader_label,
        "region" => region,
        "shred_type" => shred_type,
        "source_a" => a.to_string(),
        "source_b" => b.to_string(),
        ("samples", pa.deltas.len() as i64, i64),
        ("a_faster", pa.a_faster as i64, i64),
        ("b_faster", pa.b_faster as i64, i64),
        ("ties", pa.ties as i64, i64),
        // a_win_rate over DECIDED comparisons (ties excluded from denominator)
        ("a_win_rate_bps", basis_points(pa.a_faster, decided), i64),
        ("delta_p50_us", q[0] / 1000, i64),
        ("delta_p90_us", q[1] / 1000, i64),
        ("delta_p99_us", q[2] / 1000, i64),
    );
}

fn log_summary(global: &HashMap<IpAddr, SourceAgg>, total: u64) {
    let mut rows: Vec<(IpAddr, &SourceAgg)> = global.iter().map(|(k, v)| (*k, v)).collect();
    rows.sort_by_key(|(_, sa)| std::cmp::Reverse(sa.contested_firsts));
    let summary: Vec<String> = rows
        .iter()
        .map(|(ip, sa)| {
            let mut s = sa.lead_samples_ns.clone();
            let q = quantiles(&mut s, &[0.5]);
            format!(
                "{ip}: win={:.1}% cover={:.1}% excl={:.1}% p50_lead={}us",
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
    use std::net::Ipv4Addr;

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, n))
    }

    #[test]
    fn slot_guard_rejects_garbage_and_bounds_jumps() {
        // absolute implausible slot is always rejected
        assert!(!slot_is_plausible(MAX_PLAUSIBLE_SLOT, 0, None));
        assert!(!slot_is_plausible(u64::MAX, 1_000, None));
        // no window: bootstrap on first, then bounded forward jump
        assert!(slot_is_plausible(500_000_000, 0, None));
        assert!(slot_is_plausible(1_000 + MAX_SLOT_JUMP, 1_000, None));
        assert!(!slot_is_plausible(1_000 + MAX_SLOT_JUMP + 1, 1_000, None));
        // with a leader window, only in-window (± margin) slots are accepted
        let bounds = Some((1_000u64, 6_000u64));
        assert!(slot_is_plausible(3_000, 0, bounds));
        assert!(slot_is_plausible(1_000, 0, bounds));
        assert!(!slot_is_plausible(50_000, 5_999, bounds)); // far above end+margin
        assert!(!slot_is_plausible(500, 0, bounds)); // below base-margin
    }

    #[test]
    fn finalize_contested_and_exclusive() {
        let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();
        let (a, b) = (ip(1), ip(2)); // a < b

        // contested shred: A arrives at 1000, B at 1500 -> A wins by 500ns
        let mut psa = PerSourceArrival::default();
        psa.observe(a, 1000);
        psa.observe(b, 1500);
        let id = ShredId { slot: 100, fec_set_index: 0, index: 5, is_data: true };
        finalize_one(id, &psa, None, &mut acc);

        // exclusive shred: only A delivers
        let mut psa2 = PerSourceArrival::default();
        psa2.observe(a, 2000);
        let id2 = ShredId { slot: 101, fec_set_index: 0, index: 1, is_data: true };
        finalize_one(id2, &psa2, None, &mut acc);

        let lagg = acc.get(&(None, true)).unwrap();
        assert_eq!(lagg.total_shreds, 2);
        assert_eq!(lagg.contested_total, 1);

        let sa_a = lagg.per_source.get(&a).unwrap();
        assert_eq!(sa_a.delivered, 2);
        assert_eq!(sa_a.exclusive, 1);
        assert_eq!(sa_a.contested_delivered, 1);
        assert_eq!(sa_a.contested_firsts, 1);
        assert_eq!(sa_a.lead_samples_ns, vec![0]); // A won the contested race

        let sa_b = lagg.per_source.get(&b).unwrap();
        assert_eq!(sa_b.delivered, 1);
        assert_eq!(sa_b.exclusive, 0);
        assert_eq!(sa_b.contested_firsts, 0);
        assert_eq!(sa_b.lead_samples_ns, vec![500]); // B was 500ns behind

        // pair (a,b): delta = ts[b]-ts[a] = +500 -> a_faster
        let pa = lagg.per_pair.get(&(a, b)).unwrap();
        assert_eq!((pa.a_faster, pa.b_faster, pa.ties), (1, 0, 0));
        assert_eq!(pa.deltas, vec![500]);
    }

    #[test]
    fn coding_and_data_are_separate_series() {
        let mut acc: HashMap<AccKey, LeaderAgg> = HashMap::new();
        let a = ip(1);
        let mut d = PerSourceArrival::default();
        d.observe(a, 10);
        finalize_one(ShredId { slot: 1, fec_set_index: 0, index: 0, is_data: true }, &d, None, &mut acc);
        let mut c = PerSourceArrival::default();
        c.observe(a, 10);
        finalize_one(ShredId { slot: 1, fec_set_index: 0, index: 0, is_data: false }, &c, None, &mut acc);
        assert!(acc.contains_key(&(None, true)));
        assert!(acc.contains_key(&(None, false)));
    }
}

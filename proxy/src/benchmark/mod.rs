//! Validator-granularity shred-source benchmark.
//!
//! Measures how shred SOURCES (jito, blockrazor, ...) compare for each leader
//! VALIDATOR, by tapping the raw UDP receive path (before dedup), attributing
//! each packet to `(source_ip, slot, fec_set, index, data|coding, rx_ts)` with
//! zero reconstruction, matching the same shred across sources, and bucketing
//! arrival timing by the slot's leader.
//!
//! Design: see `docs/validator-shred-bench-design.md`. The tap is read-only over
//! packets and pushes onto a bounded channel (drop-on-full) consumed by a single
//! aggregator thread, so the production reconstruct -> shmem/gRPC path is never
//! blocked.

pub mod aggregator;
pub mod leader;
pub mod parse;
pub mod recv_timestamp;
pub mod sources;
pub mod stats;
pub mod validators;

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Sender;
use log::{info, warn};
use solana_perf::packet::{Packet, PacketBatch};

use self::{leader::LeaderScheduleHandle, parse::Observation, validators::ValidatorMap};

/// CLI / env configuration for the benchmark. Flattened into `CommonArgs`.
#[derive(clap::Args, Clone, Debug)]
pub struct BenchmarkArgs {
    /// Enable the validator-granularity shred-source benchmark tap.
    #[arg(long, env, default_value_t = false)]
    pub enable_benchmark: bool,

    /// Use per-packet kernel SO_TIMESTAMPNS timestamps (replaces the stock
    /// receiver with a timestamped recv path). Requires Linux for kernel
    /// timestamps; otherwise falls back to userspace time.
    #[arg(long, env, default_value_t = false)]
    pub benchmark_kernel_timestamps: bool,

    /// Include coding shreds in the headline stats (default: data shreds only).
    #[arg(long, env, default_value_t = false)]
    pub benchmark_include_coding: bool,

    /// Path to write a raw per-observation CSV dump (milestone M1). Optional.
    #[arg(long, env)]
    pub benchmark_csv_path: Option<PathBuf>,

    /// RPC URL used to fetch the leader schedule (getSlotLeaders). When unset,
    /// per-validator bucketing is disabled (stats still computed globally).
    #[arg(long, env)]
    pub rpc_url: Option<String>,

    /// Path to measured_validators_map.json for leader -> region/rtt labeling.
    #[arg(long, env)]
    pub validator_map_path: Option<PathBuf>,

    /// Node country for the in-region predicate (mirrors arb_bot).
    #[arg(long, env, default_value = "Germany")]
    pub node_country: String,

    /// In-region iff measured rtt < this (microseconds), else country match.
    #[arg(long, env, default_value_t = 5000u128)]
    pub region_max_rtt_us: u128,

    /// Finalize a shred once its slot is older than `current_max_slot - this`.
    #[arg(long, env, default_value_t = 64u64)]
    pub benchmark_window_slots: u64,

    /// How often to emit aggregated stats, in seconds.
    #[arg(long, env, default_value_t = 60u64)]
    pub benchmark_flush_secs: u64,

    /// Capacity of the observation channel (in batches, each up to ~64
    /// observations of ~48 bytes). Drops on overflow. Default 8192 ~= 25MB ceiling.
    #[arg(long, env, default_value_t = 8_192usize)]
    pub benchmark_channel_capacity: usize,

    /// Minimum shreds per leader before emitting that leader's stats.
    #[arg(long, env, default_value_t = 4u64)]
    pub benchmark_min_samples: u64,

    /// Pin the benchmark aggregator thread to this CPU core (Linux only). Keep it
    /// off the listen/forward/reconstruct cores to avoid tail-latency jitter.
    #[arg(long, env)]
    pub benchmark_aggregator_core_id: Option<usize>,
}

impl BenchmarkArgs {
    pub fn to_config(&self) -> BenchmarkConfig {
        BenchmarkConfig {
            enabled: self.enable_benchmark,
            kernel_timestamps: self.benchmark_kernel_timestamps,
            data_only: !self.benchmark_include_coding,
            csv_path: self.benchmark_csv_path.clone(),
            rpc_url: self.rpc_url.clone(),
            validator_map_path: self.validator_map_path.clone(),
            node_country: self.node_country.clone(),
            region_max_rtt_us: self.region_max_rtt_us,
            window_slots: self.benchmark_window_slots,
            flush_interval: Duration::from_secs(self.benchmark_flush_secs.max(1)),
            channel_capacity: self.benchmark_channel_capacity.max(1),
            min_samples: self.benchmark_min_samples,
            aggregator_core_id: self.benchmark_aggregator_core_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BenchmarkConfig {
    pub enabled: bool,
    pub kernel_timestamps: bool,
    pub data_only: bool,
    pub csv_path: Option<PathBuf>,
    pub rpc_url: Option<String>,
    pub validator_map_path: Option<PathBuf>,
    pub node_country: String,
    pub region_max_rtt_us: u128,
    pub window_slots: u64,
    pub flush_interval: Duration,
    pub channel_capacity: usize,
    pub min_samples: u64,
    pub aggregator_core_id: Option<usize>,
}

/// Cheap, cloneable handle the ingest path uses to push observations. The only
/// hot-path work is parsing ~5 header bytes per packet and one bounded
/// `try_send`; on overflow it drops and counts, never blocks.
#[derive(Clone)]
pub struct BenchmarkHandle {
    sender: Sender<Vec<Observation>>,
    dropped: Arc<AtomicU64>,
}

impl BenchmarkHandle {
    /// Tap an entire batch with a single (userspace) receive timestamp. Used by
    /// the send-thread tap when kernel timestamps are disabled.
    pub fn observe_batch(&self, batch: &PacketBatch, rx_ts_ns: i64) {
        let mut obs = Vec::with_capacity(batch.len());
        for pkt in batch.iter() {
            if let Some(data) = pkt.data(..) {
                if let Some(o) = parse::parse_observation(data, pkt.meta().addr, rx_ts_ns) {
                    obs.push(o);
                }
            }
        }
        self.push(obs);
    }

    /// Tap packets with per-packet kernel timestamps. Used by the timestamped
    /// recv thread. `ts[i]` corresponds to `packets[i]`.
    pub fn observe_packets(&self, packets: &[Packet], ts: &[i64]) {
        let mut obs = Vec::with_capacity(packets.len());
        for (pkt, &t) in packets.iter().zip(ts.iter()) {
            if let Some(data) = pkt.data(..) {
                if let Some(o) = parse::parse_observation(data, pkt.meta().addr, t) {
                    obs.push(o);
                }
            }
        }
        self.push(obs);
    }

    #[inline]
    fn push(&self, obs: Vec<Observation>) {
        if obs.is_empty() {
            return;
        }
        let n = obs.len() as u64;
        if self.sender.try_send(obs).is_err() {
            // Count dropped observations (not batches) so the metric reflects
            // true sample loss under overload.
            self.dropped.fetch_add(n, Ordering::Relaxed);
        }
    }
}

/// Pin the current thread to a CPU core (Linux). Mirrors how the `custom` branch
/// pins the reconstructor, so the aggregator can be kept off the hot cores.
#[cfg(target_os = "linux")]
fn pin_to_core(core_id: usize) {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core_id, &mut cpuset);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset) == 0 {
            info!("pinned benchmark aggregator to core {core_id}");
        } else {
            warn!(
                "failed to pin benchmark aggregator to core {core_id}: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_to_core(_core_id: usize) {}

/// Spawned benchmark subsystem: the handle the forwarder taps, plus background
/// thread handles and whether the timestamped recv path should be used.
pub struct BenchmarkRuntime {
    pub handle: BenchmarkHandle,
    pub kernel_timestamps: bool,
    pub join_handles: Vec<JoinHandle<()>>,
}

/// Start the benchmark subsystem if enabled. Loads the validator map, spawns the
/// leader-schedule poller (if an RPC URL is set) and the aggregator thread.
pub fn start(config: BenchmarkConfig, exit: Arc<AtomicBool>) -> Option<BenchmarkRuntime> {
    if !config.enabled {
        return None;
    }
    info!("benchmark enabled: {config:?}");
    if config.kernel_timestamps {
        warn!(
            "benchmark kernel timestamps ON: the listen thread is replaced with a \
             timestamped recv path that parses + allocates per batch upstream of the \
             reconstruct fork (benchmark-only perturbation of the recv path). Pin the \
             aggregator (--benchmark-aggregator-core-id) off the reconstruct cores."
        );
    }

    let (sender, receiver) = crossbeam_channel::bounded::<Vec<Observation>>(config.channel_capacity);
    let dropped = Arc::new(AtomicU64::new(0));

    // Validator map (optional).
    let validators: Option<Arc<ValidatorMap>> = match &config.validator_map_path {
        Some(path) => match ValidatorMap::load(path) {
            Ok(vm) => {
                info!("benchmark loaded validator map: {} validators from {}", vm.len(), path.display());
                Some(Arc::new(vm))
            }
            Err(e) => {
                warn!("benchmark failed to load validator map {}: {e}", path.display());
                None
            }
        },
        None => None,
    };

    let mut join_handles = Vec::new();

    // Leader schedule poller (optional; required for per-validator bucketing).
    let leader_handle: Option<LeaderScheduleHandle> = match &config.rpc_url {
        Some(url) => {
            let (h, join) = leader::spawn(url.clone(), exit.clone());
            join_handles.push(join);
            Some(h)
        }
        None => {
            warn!("benchmark: no RPC URL set, per-validator bucketing disabled (global stats only)");
            None
        }
    };

    // Aggregator thread.
    let agg_cfg = config.clone();
    let agg_dropped = dropped.clone();
    let agg_exit = exit.clone();
    let agg_validators = validators.clone();
    let agg_core = config.aggregator_core_id;
    let agg_join = std::thread::Builder::new()
        .name("ssBenchAgg".to_string())
        .spawn(move || {
            if let Some(core_id) = agg_core {
                pin_to_core(core_id);
            }
            aggregator::run(
                receiver,
                agg_cfg,
                leader_handle,
                agg_validators,
                agg_dropped,
                agg_exit,
            );
        })
        .unwrap();
    join_handles.push(agg_join);

    Some(BenchmarkRuntime {
        handle: BenchmarkHandle { sender, dropped },
        kernel_timestamps: config.kernel_timestamps,
        join_handles,
    })
}

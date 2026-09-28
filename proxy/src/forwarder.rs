use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, RecvError};
use dashmap::DashMap;
use itertools::Itertools;
use jito_protos::shredstream::{Entry as PbEntry, TraceShred};
use log::{debug, error, info, warn};
use prost::Message;
use solana_client::client_error::reqwest;
use solana_ledger::shred::ReedSolomonCache;
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_net_utils::SocketConfig;
use solana_perf::{
    deduper::Deduper,
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_sdk::clock::Slot;
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer::{self, StreamerReceiveStats},
};
use tokio::sync::broadcast::Sender;

use crate::{
    deshred, resolve_hostname_port, stream_stats::StreamStatsHandle, ShredstreamProxyError,
};

// values copied from https://github.com/solana-labs/solana/blob/33bde55bbdde13003acf45bb6afe6db4ab599ae4/core/src/sigverify_shreds.rs#L20
pub const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
pub const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
pub const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);

/// Bind to ports and start forwarding shreds
#[allow(clippy::too_many_arguments)]
pub fn start_forwarder_threads(
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>, /* sockets shared between endpoint discovery thread and forwarders */
    src_addr: IpAddr,
    src_port: u16,
    maybe_multicast_socket: Option<Vec<UdpSocket>>,
    num_threads: Option<usize>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    should_reconstruct_shreds: bool,
    entry_sender: Arc<Sender<PbEntry>>,
    debug_trace_shred: bool,
    use_discovery_service: bool,
    forward_stats: Arc<StreamerReceiveStats>,
    metrics: Arc<ShredMetrics>,
    shmem_ring_path: Option<std::path::PathBuf>,
    bench_handle: Option<crate::benchmark::BenchmarkHandle>,
    bench_kernel_timestamps: bool,
    pipeline_handle: Option<crate::benchmark::PipelineLatencyHandle>,
    lean_ingest: Option<crate::lean_ingest::LeanIngestConfig>,
    stream_stats: Option<StreamStatsHandle>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    // Lean ingest polls one unicast socket (SO_REUSEPORT would only spread one source's
    // flow-hashed traffic over idle sockets anyway).
    let num_threads = if lean_ingest.is_some() {
        1
    } else {
        num_threads
            .unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()).min(4))
    };

    // multi_bind_in_range returns (port, Vec<UdpSocket>)
    let (_port, sockets) = solana_net_utils::multi_bind_in_range_with_config(
        src_addr,
        (src_port, src_port + 1),
        SocketConfig::default().reuseport(true),
        num_threads,
    )
    .unwrap_or_else(|_| {
        panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
    });

    // The unicast listen sockets come first in the chain below; multicast sockets
    // (DoubleZero) are appended after them. Sockets at index >= this are the
    // DoubleZero multicast listeners, whose benchmark observations are attributed
    // to `SourceId::DoubleZero` (by ingress) rather than by packet source IP.
    let n_unicast_sockets = sockets.len();

    if let Some(config) = lean_ingest {
        return crate::lean_ingest::start_lean_ingest_threads(
            config,
            sockets
                .into_iter()
                .chain(maybe_multicast_socket.into_iter().flatten())
                .collect(),
            n_unicast_sockets,
            unioned_dest_sockets,
            deduper,
            should_reconstruct_shreds,
            entry_sender,
            debug_trace_shred,
            forward_stats,
            metrics,
            shmem_ring_path,
            bench_handle,
            bench_kernel_timestamps,
            pipeline_handle,
            stream_stats,
            exit,
        );
    }

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);
    let (reconstruct_tx, reconstruct_rx) = crossbeam_channel::bounded::<PacketBatch>(1_024);
    let mut thread_hdls = Vec::with_capacity(num_threads + 1);

    if should_reconstruct_shreds {
        let metrics = metrics.clone();
        let exit = exit.clone();
        // receives shreds from recv_from_channel_and_send_multiple_dest and calls deshred::reconstruct_shreds
        let hdl = std::thread::Builder::new()
            .name("shred_reconstructor".to_string())
            .spawn(move || {
                let mut reconstructor = Reconstructor::new(
                    shmem_ring_path.as_deref(),
                    entry_sender,
                    pipeline_handle,
                    metrics,
                    stream_stats,
                );
                while !exit.load(Ordering::Relaxed) {
                    match reconstruct_rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(pkt_batch) => {
                            reconstructor
                                .ingest_and_publish(pkt_batch.iter().filter_map(|p| p.data(..)));
                            reconstructor.finish();
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {} // do nothing
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .unwrap();
        thread_hdls.push(hdl);
    };

    sockets
        .into_iter()
        .chain(maybe_multicast_socket.into_iter().flatten())
        .enumerate()
        .flat_map(|(thread_id, incoming_shred_socket)| {
            let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
            // Sockets appended after the unicast ones are the DoubleZero multicast
            // listeners; attribute their observations to SourceId::DoubleZero.
            let bench_source_override = if thread_id >= n_unicast_sockets {
                Some(crate::benchmark::sources::DOUBLEZERO_SENTINEL)
            } else {
                None
            };
            // When kernel timestamps are enabled, use the timestamped recv path
            // which taps the benchmark in the listen thread (closest to receipt)
            // and forwards the PacketBatch downstream unchanged. Otherwise use the
            // stock streamer receiver and tap (if any) in the send thread.
            let listen_thread = match (bench_handle.as_ref(), bench_kernel_timestamps) {
                (Some(bh), true) => crate::benchmark::recv_timestamp::start_recv_and_tap_thread(
                    format!("ssListen{thread_id}"),
                    Arc::new(incoming_shred_socket),
                    exit.clone(),
                    packet_sender,
                    forward_stats.clone(),
                    bh.clone(),
                    bench_source_override,
                ),
                _ => streamer::receiver(
                    format!("ssListen{thread_id}"),
                    Arc::new(incoming_shred_socket),
                    exit.clone(),
                    packet_sender,
                    recycler.clone(),
                    forward_stats.clone(),
                    Duration::default(),
                    false,
                    None,
                    false,
                ),
            };

            let deduper = deduper.clone();
            let unioned_dest_sockets = unioned_dest_sockets.clone();
            let metrics = metrics.clone();
            let shutdown_receiver = shutdown_receiver.clone();
            let reconstruct_tx = reconstruct_tx.clone();
            let exit = exit.clone();
            // Send-thread tap only when NOT using the kernel-timestamp recv path
            // (avoids tapping the same packets twice).
            let send_bench_handle = if bench_kernel_timestamps {
                None
            } else {
                bench_handle.clone()
            };

            let send_thread = Builder::new()
                .name(format!("ssPxyTx_{thread_id}"))
                .spawn(move || {
                    let send_socket =
                        UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
                            .expect("to bind to udp port for forwarding");
                    let mut local_dest_sockets = unioned_dest_sockets.load();

                    let refresh_subscribers_tick = if use_discovery_service {
                        crossbeam_channel::tick(Duration::from_secs(30))
                    } else {
                        crossbeam_channel::tick(Duration::MAX)
                    };

                    while !exit.load(Ordering::Relaxed) {
                        crossbeam_channel::select! {
                            // forward packets
                            recv(packet_receiver) -> maybe_packet_batch => {
                                let res = recv_from_channel_and_send_multiple_dest(
                                    maybe_packet_batch,
                                    &deduper,
                                    &send_socket,
                                    &local_dest_sockets,
                                    should_reconstruct_shreds,
                                    &reconstruct_tx,
                                    debug_trace_shred,
                                    &metrics,
                                    send_bench_handle.as_ref(),
                                    bench_source_override,
                                );

                                // If the channel is closed or error, break out
                                if res.is_err() {
                                    break;
                                }
                            }

                            // refresh thread-local subscribers
                            recv(refresh_subscribers_tick) -> _ => {
                                local_dest_sockets = unioned_dest_sockets.load();
                            }

                            // handle shutdown (avoid using sleep since it can hang)
                            recv(shutdown_receiver) -> _ => {
                                break;
                            }
                        }
                    }
                    info!("Exiting forwarder thread {thread_id}.");
                })
                .unwrap();

            vec![listen_thread, send_thread]
        })
        .collect::<Vec<JoinHandle<()>>>()
}

/// Deshred state plus every output of reconstructed entries (shmem ring, pipeline-latency
/// events, gRPC broadcast). Owned by whichever thread runs reconstruction: the
/// `shred_reconstructor` thread, or the lean ingest thread (`lean_ingest.rs`).
pub(crate) struct Reconstructor {
    shmem_ring: Option<crate::shmem_ring::ShmemRingProducer>,
    /// Ring v2 (positions for agave's fast lane): enabled by `SHMEM_RING_V2_PATH`.
    shmem_ring_v2: Option<crate::shmem_ring_v2::ShmemRingV2Producer>,
    batch_offsets: crate::shmem_ring_v2::BatchOffsets,
    all_shreds: deshred::SlotShreds,
    slot_fec_indexes_to_iterate: Vec<(Slot, u32)>,
    deshredded_entries: Vec<(Slot, Vec<u8>)>,
    // Parallel to deshredded_entries: composing data-shred index range +
    // unknown_start, for pipeline-latency attribution.
    entry_ranges: Vec<(u32, u32, bool)>,
    highest_slot_seen: Slot,
    rs_cache: ReedSolomonCache,
    entry_sender: Arc<Sender<PbEntry>>,
    pipeline_handle: Option<crate::benchmark::PipelineLatencyHandle>,
    metrics: Arc<ShredMetrics>,
    /// `--stream-entries`: events of the current call, and where they go.
    stream: Option<(Vec<deshred::StreamEvent>, StreamStatsHandle)>,
}

impl Reconstructor {
    /// Creates (truncates) the shmem ring if configured; panics if that fails.
    /// `stream_stats`: `Some` turns on streaming entry emission.
    pub(crate) fn new(
        shmem_ring_path: Option<&std::path::Path>,
        entry_sender: Arc<Sender<PbEntry>>,
        pipeline_handle: Option<crate::benchmark::PipelineLatencyHandle>,
        metrics: Arc<ShredMetrics>,
        stream_stats: Option<StreamStatsHandle>,
    ) -> Self {
        Self {
            shmem_ring: shmem_ring_path.map(|path| {
                crate::shmem_ring::ShmemRingProducer::create(path)
                    .expect("Failed to create shmem ring buffer")
            }),
            shmem_ring_v2: std::env::var_os("SHMEM_RING_V2_PATH").map(|path| {
                let path = std::path::PathBuf::from(path);
                let ring = crate::shmem_ring_v2::ShmemRingV2Producer::create(&path)
                    .expect("Failed to create shmem ring v2");
                info!("Shmem ring v2 (positions) at {}", path.display());
                ring
            }),
            batch_offsets: crate::shmem_ring_v2::BatchOffsets::default(),
            all_shreds: deshred::SlotShreds::default(),
            slot_fec_indexes_to_iterate: Vec::new(),
            deshredded_entries: Vec::new(),
            entry_ranges: Vec::new(),
            highest_slot_seen: 0,
            rs_cache: ReedSolomonCache::default(),
            entry_sender,
            pipeline_handle,
            metrics,
            stream: stream_stats.map(|handle| (Vec::new(), handle)),
        }
    }

    /// Latency-critical half: deshred `payloads` and put every completed batch in the shmem
    /// ring. Call [`Self::finish`] afterwards.
    pub(crate) fn ingest_and_publish<'a>(&mut self, payloads: impl IntoIterator<Item = &'a [u8]>) {
        deshred::reconstruct_shred_payloads(
            payloads,
            &mut self.all_shreds,
            &mut self.slot_fec_indexes_to_iterate,
            &mut self.deshredded_entries,
            &mut self.entry_ranges,
            &mut self.highest_slot_seen,
            &self.rs_cache,
            &self.metrics,
            self.stream.as_mut().map(|(events, _)| events),
        );

        for ((slot, entries_bytes), &(start_index, end_index, unknown_start)) in
            self.deshredded_entries.iter().zip(self.entry_ranges.iter())
        {
            // Shmem write FIRST (lowest latency path) — unchanged. An empty payload is a
            // batch-complete marker for ring v2 only.
            if let Some(ring) = self.shmem_ring.as_mut() {
                if !entries_bytes.is_empty() {
                    ring.publish(*slot, entries_bytes);
                }
            }
            if let Some(ring) = self.shmem_ring_v2.as_mut() {
                let (parent, is_final, last_in_slot) = deshred::record_position(
                    &self.all_shreds,
                    *slot,
                    start_index,
                    end_index,
                );
                let entry_count = entries_bytes
                    .get(..8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()) as u32)
                    .unwrap_or(0);
                let entry_offset =
                    self.batch_offsets
                        .take(*slot, start_index, entry_count, is_final);
                let mut flags = 0;
                if is_final {
                    flags |= crate::shmem_ring_v2::FLAG_FINAL;
                }
                if last_in_slot {
                    flags |= crate::shmem_ring_v2::FLAG_LAST_IN_SLOT;
                }
                if unknown_start {
                    flags |= crate::shmem_ring_v2::FLAG_GUESSED_START;
                }
                ring.publish(
                    &crate::shmem_ring_v2::RecordMeta {
                        slot: *slot,
                        parent_slot: parent.unwrap_or(u64::MAX),
                        batch_start: start_index,
                        batch_end: end_index,
                        entry_offset,
                        entry_count,
                        flags,
                        t_publish_ns: SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_nanos() as u64)
                            .unwrap_or(0),
                    },
                    entries_bytes,
                );
            }
            if entries_bytes.is_empty() {
                continue;
            }

            // Pipeline latency: AFTER the shmem write (so it never
            // delays consumer visibility), capture the realtime
            // publish instant and hand a tiny event to the
            // aggregator. Non-blocking (drop-on-full); a single
            // Option branch when disabled.
            if let Some(ph) = self.pipeline_handle.as_ref() {
                let publish_ts_ns = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0);
                ph.record(crate::benchmark::aggregator::PublishEvent {
                    slot: *slot,
                    start_index,
                    end_index,
                    unknown_start,
                    publish_ts_ns,
                });
            }
        }
    }

    /// Off-path half, run once every batch of the packet batch is in the ring.
    pub(crate) fn finish(&mut self) {
        if self.shmem_ring_v2.is_some() {
            self.batch_offsets
                .prune(self.highest_slot_seen.saturating_sub(64));
        }
        for ((slot, entries_bytes), (start_index, end_index, unknown_start)) in self
            .deshredded_entries
            .drain(..)
            .zip(self.entry_ranges.drain(..))
        {
            if entries_bytes.is_empty() {
                // Ring v2 batch-complete marker: nothing to walk or broadcast.
                continue;
            }
            // Proven-start batches were published unchecked; walk them now, off the
            // publish path, for metrics and to make an unknown wire format loud.
            // Guessed starts were validated before they were emitted.
            if !unknown_start {
                deshred::observe_known_start_batch(
                    slot,
                    start_index,
                    end_index,
                    &entries_bytes,
                    &self.metrics,
                );
            }

            // Then gRPC broadcast (existing path). With --stream-entries it carries the
            // same records as the ring.
            let _ = self.entry_sender.send(PbEntry {
                slot,
                entries: entries_bytes,
            });
        }
        if let Some((events, stats)) = self.stream.as_mut() {
            for event in events.drain(..) {
                stats.record(event);
            }
        }
    }
}

/// Broadcasts the same packet to multiple recipients, parses it into a Shred if possible,
/// and stores that shred in `all_shreds`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn recv_from_channel_and_send_multiple_dest(
    maybe_packet_batch: Result<PacketBatch, RecvError>,
    deduper: &RwLock<Deduper<2, [u8]>>,
    send_socket: &UdpSocket,
    local_dest_sockets: &[SocketAddr],
    should_reconstruct_shreds: bool,
    reconstruct_tx: &crossbeam_channel::Sender<PacketBatch>,
    debug_trace_shred: bool,
    metrics: &ShredMetrics,
    bench: Option<&crate::benchmark::BenchmarkHandle>,
    bench_source_override: Option<IpAddr>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = maybe_packet_batch.map_err(ShredstreamProxyError::RecvError)?;
    let trace_shred_received_time = SystemTime::now();
    metrics
        .received
        .fetch_add(packet_batch.len() as u64, Ordering::Relaxed);
    debug!(
        "Got batch of {} packets, total size in bytes: {}",
        packet_batch.len(),
        packet_batch.iter().map(|x| x.meta().size).sum::<usize>()
    );

    // Hand off to the reconstructor FIRST so the benchmark tap never delays the
    // reconstruct -> shmem/gRPC path.
    if should_reconstruct_shreds {
        let _ = reconstruct_tx.try_send(packet_batch.clone());
    }

    // Benchmark tap (userspace timestamp path). Runs BEFORE dedup so every
    // source's copy of a shred is observed; read-only over the batch.
    if let Some(bench) = bench {
        let rx_ts_ns = trace_shred_received_time
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        bench.observe_batch_as(&packet_batch, rx_ts_ns, bench_source_override);
    }

    let mut packet_batch_vec = vec![packet_batch];

    let num_deduped = solana_perf::deduper::dedup_packets_and_count_discards(
        &deduper.read().unwrap(),
        &mut packet_batch_vec,
    );
    // Store stats for each Packet
    packet_batch_vec.iter().for_each(|batch| {
        batch.iter().for_each(|packet| {
            metrics
                .packets_received
                .entry(packet.meta().addr)
                .and_modify(|(discarded, not_discarded)| {
                    *discarded += packet.meta().discard() as u64;
                    *not_discarded += (!packet.meta().discard()) as u64;
                })
                .or_insert_with(|| {
                    (
                        packet.meta().discard() as u64,
                        (!packet.meta().discard()) as u64,
                    )
                });
        });
    });

    // send out to RPCs
    local_dest_sockets.iter().for_each(|outgoing_socketaddr| {
        let packets_with_dest = packet_batch_vec[0]
            .iter()
            .filter_map(|pkt| {
                let data = pkt.data(..)?;
                let addr = outgoing_socketaddr;
                Some((data, addr))
            })
            .collect::<Vec<(&[u8], &SocketAddr)>>();

        match batch_send(send_socket, &packets_with_dest) {
            Ok(_) => {
                metrics
                    .success_forward
                    .fetch_add(packets_with_dest.len() as u64, Ordering::Relaxed);
                metrics.duplicate.fetch_add(num_deduped, Ordering::Relaxed);
            }
            Err(SendPktsError::IoError(err, num_failed)) => {
                metrics
                    .fail_forward
                    .fetch_add(packets_with_dest.len() as u64, Ordering::Relaxed);
                metrics
                    .duplicate
                    .fetch_add(num_failed as u64, Ordering::Relaxed);
                error!(
                    "Failed to send batch of size {} to {outgoing_socketaddr:?}. \
                     {num_failed} packets failed. Error: {err}",
                    packets_with_dest.len()
                );
            }
        }
    });

    // Count TraceShred shreds
    if debug_trace_shred {
        packet_batch_vec[0]
            .iter()
            .filter_map(|p| TraceShred::decode(p.data(..)?).ok())
            .filter(|t| t.created_at.is_some())
            .for_each(|trace_shred| {
                let elapsed = trace_shred_received_time
                    .duration_since(SystemTime::try_from(trace_shred.created_at.unwrap()).unwrap())
                    .unwrap_or_default();

                datapoint_info!(
                    "shredstream_proxy-trace_shred_latency",
                    "trace_region" => trace_shred.region,
                    ("trace_seq_num", trace_shred.seq_num as i64, i64),
                    ("elapsed_micros", elapsed.as_micros(), i64),
                );
            });
    }

    Ok(())
}

/// Starts a thread that updates our destinations used by the forwarder threads
pub fn start_destination_refresh_thread(
    endpoint_discovery_url: String,
    discovered_endpoints_port: u16,
    static_dest_sockets: Vec<(SocketAddr, String)>,
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new().name("ssPxyDstRefresh".to_string()).spawn(move || {
        let fetch_socket_tick = crossbeam_channel::tick(Duration::from_secs(30));
        let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
        let mut socket_count = static_dest_sockets.len();
        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                    recv(fetch_socket_tick) -> _ => {
                        let fetched = fetch_unioned_destinations(
                            &endpoint_discovery_url,
                            discovered_endpoints_port,
                            &static_dest_sockets,
                        );
                        let new_sockets = match fetched {
                            Ok(s) => {
                                info!("Sending shreds to {} destinations: {s:?}", s.len());
                                s
                            }
                            Err(e) => {
                                warn!("Failed to fetch from discovery service, retrying. Error: {e}");
                                datapoint_warn!("shredstream_proxy-destination_refresh_error",
                                                ("prev_unioned_dest_count", socket_count, i64),
                                                ("errors", 1, i64),
                                                ("error_str", e.to_string(), String),
                                );
                                continue;
                            }
                        };
                        socket_count = new_sockets.len();
                        unioned_dest_sockets.store(Arc::new(new_sockets));
                    }
                    recv(metrics_tick) -> _ => {
                        datapoint_info!("shredstream_proxy-destination_refresh_stats",
                                        ("destination_count", socket_count, i64),
                        );
                    }
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
        }
    }).unwrap()
}

/// Returns dynamically discovered endpoints with CLI arg defined endpoints
fn fetch_unioned_destinations(
    endpoint_discovery_url: &str,
    discovered_endpoints_port: u16,
    static_dest_sockets: &[(SocketAddr, String)],
) -> Result<Vec<SocketAddr>, ShredstreamProxyError> {
    let bytes = reqwest::blocking::get(endpoint_discovery_url)?.bytes()?;

    let sockets_json = match serde_json::from_slice::<Vec<IpAddr>>(&bytes) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Failed to parse json from: {:?}",
                std::str::from_utf8(&bytes)
            );
            return Err(ShredstreamProxyError::from(e));
        }
    };

    // resolve again since ip address could change
    let static_dest_sockets = static_dest_sockets
        .iter()
        .filter_map(|(_socketaddr, hostname_port)| {
            Some(resolve_hostname_port(hostname_port).ok()?.0)
        })
        .collect::<Vec<_>>();

    let unioned_dest_sockets = sockets_json
        .into_iter()
        .map(|ip| SocketAddr::new(ip, discovered_endpoints_port))
        .chain(static_dest_sockets)
        .unique()
        .collect::<Vec<SocketAddr>>();
    Ok(unioned_dest_sockets)
}

/// Reset dedup + send metrics to influx
pub fn start_forwarder_accessory_thread(
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    metrics: Arc<ShredMetrics>,
    metrics_update_interval_ms: u64,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("ssPxyAccessory".to_string())
        .spawn(move || {
            let metrics_tick =
                crossbeam_channel::tick(Duration::from_millis(metrics_update_interval_ms));
            let deduper_reset_tick = crossbeam_channel::tick(Duration::from_secs(2));
            let mut rng = rand::thread_rng();
            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    // reset deduper to avoid false positives
                    recv(deduper_reset_tick) -> _ => {
                        deduper
                            .write()
                            .unwrap()
                            .maybe_reset(&mut rng, DEDUPER_FALSE_POSITIVE_RATE, DEDUPER_RESET_CYCLE);
                    }

                    // send metrics to influx
                    recv(metrics_tick) -> _ => {
                        metrics.report();
                        metrics.reset();
                    }

                    // handle SIGINT shutdown
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        })
        .unwrap()
}

pub struct ShredMetrics {
    // receive stats
    /// Total number of shreds received. Includes duplicates when receiving shreds from multiple regions
    pub received: AtomicU64,
    /// Total number of shreds successfully forwarded, accounting for all destinations
    pub success_forward: AtomicU64,
    /// Total number of shreds failed to forward, accounting for all destinations
    pub fail_forward: AtomicU64,
    /// Number of duplicate shreds received
    pub duplicate: AtomicU64,
    /// Packets not forwarded because the lean ingest forward queue was full
    pub forward_queue_dropped: AtomicU64,
    /// (discarded, not discarded, from other shredstream instances)
    pub packets_received: DashMap<IpAddr, (u64, u64)>,

    // service metrics
    pub enabled_grpc_service: bool,
    /// Number of data shreds recovered using coding shreds
    pub recovered_count: AtomicU64,
    /// Emitted batches that needed Reed-Solomon recovery to complete (recovery runs as
    /// soon as any 32 distinct shreds of an FEC set are present)
    pub recovered_batch_count: AtomicU64,
    /// Number of Solana entries decoded from shreds
    pub entry_count: AtomicU64,
    /// Number of transactions decoded from shreds
    pub txn_count: AtomicU64,
    /// Number of times we couldn't find the previous DATA_COMPLETE_SHRED flag
    pub unknown_start_position_count: AtomicU64,
    /// Number of FEC recovery errors
    pub fec_recovery_error_count: AtomicU64,
    /// Reed-Solomon recovery attempts that failed (`merkle::recover` error)
    pub fec_recovery_failed_count: AtomicU64,
    /// Recovery attempts on an FEC set holding shreds of more than one signed version
    /// (different leader signatures for one (slot, fec_set_index))
    pub fec_recovery_mixed_version_count: AtomicU64,
    /// Shreds left out of a recovery because their own Merkle proof does not lead to the
    /// root the rest of their version agrees on
    pub fec_recovery_bad_shred_count: AtomicU64,
    /// Recoveries that succeeded only after using one version and dropping bad shreds
    pub fec_recovery_regrouped_count: AtomicU64,
    /// Recovery attempts skipped because the set has not grown enough since it last failed
    pub fec_recovery_retry_skipped_count: AtomicU64,
    /// Complete batches held back because an FEC set in them mixes data shreds of two
    /// signed versions (released once recovery replaces the minority version)
    pub mixed_version_batch_held_count: AtomicU64,
    // --stream-entries
    /// Records published before their batch completed, and the entries/txs in them
    pub stream_records_early_count: AtomicU64,
    pub stream_entries_early_count: AtomicU64,
    pub stream_txs_early_count: AtomicU64,
    /// Entries/txs published when their batch completed
    pub stream_entries_completion_count: AtomicU64,
    pub stream_txs_completion_count: AtomicU64,
    /// Streamed batches completed, and those that had early records
    pub stream_batches_count: AtomicU64,
    pub stream_batches_split_count: AtomicU64,
    /// Streams stopped by a data shred of another signed version of the same FEC set
    pub stream_version_conflict_count: AtomicU64,
    /// Streamed batches whose prefix failed the entry walk (published whole instead)
    pub stream_malformed_count: AtomicU64,
    /// Number of bincode Entry deserialization errors
    pub bincode_deserialize_error_count: AtomicU64,
    /// Number of times we couldn't find the previous DATA_COMPLETE_SHRED flag but tried to deshred+deserialize, and failed
    pub unknown_start_position_error_count: AtomicU64,
    /// Guessed starts inside an FEC set, never emitted (batches start on FEC boundaries)
    pub unknown_start_mid_fec_count: AtomicU64,
    /// Repeats of an already rejected guess, skipped without deshredding
    pub unknown_start_retry_skipped_count: AtomicU64,
    /// Guessed starts whose payload passed the structural walk and were emitted
    pub unknown_start_validated_count: AtomicU64,
    /// Guessed starts whose payload failed the structural walk; shreds kept for a retry
    pub unknown_start_invalid_count: AtomicU64,
    /// Batches held back by a rejected guess and emitted later, once their start resolved
    pub held_batch_emitted_count: AtomicU64,
    /// Proven-start batches that do not parse (published anyway; see `deshred.rs`)
    pub known_start_invalid_count: AtomicU64,
    /// Alpenglow block markers and empty batches (entry count 0)
    pub block_marker_count: AtomicU64,
    /// Total time and count of structural batch walks (mean = ns / count)
    pub batch_walk_ns: AtomicU64,
    pub batch_walk_count: AtomicU64,

    // cumulative metrics (persist after reset)
    pub agg_received_cumulative: AtomicU64,
    pub agg_success_forward_cumulative: AtomicU64,
    pub agg_fail_forward_cumulative: AtomicU64,
    pub duplicate_cumulative: AtomicU64,
}

impl Default for ShredMetrics {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ShredMetrics {
    pub fn new(enabled_grpc_service: bool) -> Self {
        Self {
            enabled_grpc_service,
            received: Default::default(),
            success_forward: Default::default(),
            fail_forward: Default::default(),
            duplicate: Default::default(),
            forward_queue_dropped: Default::default(),
            packets_received: DashMap::with_capacity(10),
            recovered_count: Default::default(),
            recovered_batch_count: Default::default(),
            entry_count: Default::default(),
            txn_count: Default::default(),
            unknown_start_position_count: Default::default(),
            fec_recovery_error_count: Default::default(),
            fec_recovery_failed_count: Default::default(),
            fec_recovery_mixed_version_count: Default::default(),
            fec_recovery_bad_shred_count: Default::default(),
            fec_recovery_regrouped_count: Default::default(),
            fec_recovery_retry_skipped_count: Default::default(),
            mixed_version_batch_held_count: Default::default(),
            stream_records_early_count: Default::default(),
            stream_entries_early_count: Default::default(),
            stream_txs_early_count: Default::default(),
            stream_entries_completion_count: Default::default(),
            stream_txs_completion_count: Default::default(),
            stream_batches_count: Default::default(),
            stream_batches_split_count: Default::default(),
            stream_version_conflict_count: Default::default(),
            stream_malformed_count: Default::default(),
            bincode_deserialize_error_count: Default::default(),
            unknown_start_position_error_count: Default::default(),
            unknown_start_mid_fec_count: Default::default(),
            unknown_start_retry_skipped_count: Default::default(),
            unknown_start_validated_count: Default::default(),
            unknown_start_invalid_count: Default::default(),
            held_batch_emitted_count: Default::default(),
            known_start_invalid_count: Default::default(),
            block_marker_count: Default::default(),
            batch_walk_ns: Default::default(),
            batch_walk_count: Default::default(),
            agg_received_cumulative: Default::default(),
            agg_success_forward_cumulative: Default::default(),
            agg_fail_forward_cumulative: Default::default(),
            duplicate_cumulative: Default::default(),
        }
    }

    pub fn report(&self) {
        datapoint_info!(
            "shredstream_proxy-connection_metrics",
            ("received", self.received.load(Ordering::Relaxed), i64),
            (
                "success_forward",
                self.success_forward.load(Ordering::Relaxed),
                i64
            ),
            (
                "fail_forward",
                self.fail_forward.load(Ordering::Relaxed),
                i64
            ),
            ("duplicate", self.duplicate.load(Ordering::Relaxed), i64),
            (
                "forward_queue_dropped",
                self.forward_queue_dropped.swap(0, Ordering::Relaxed),
                i64
            ),
        );

        if self.enabled_grpc_service {
            datapoint_info!(
                "shredstream_proxy-service_metrics",
                (
                    "recovered_count",
                    self.recovered_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "recovered_batch_count",
                    self.recovered_batch_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "entry_count",
                    self.entry_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                ("txn_count", self.txn_count.swap(0, Ordering::Relaxed), i64),
                (
                    "unknown_start_position_count",
                    self.unknown_start_position_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_error_count",
                    self.fec_recovery_error_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_failed_count",
                    self.fec_recovery_failed_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_mixed_version_count",
                    self.fec_recovery_mixed_version_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_bad_shred_count",
                    self.fec_recovery_bad_shred_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_regrouped_count",
                    self.fec_recovery_regrouped_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_retry_skipped_count",
                    self.fec_recovery_retry_skipped_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "mixed_version_batch_held_count",
                    self.mixed_version_batch_held_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_records_early_count",
                    self.stream_records_early_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_entries_early_count",
                    self.stream_entries_early_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_txs_early_count",
                    self.stream_txs_early_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_entries_completion_count",
                    self.stream_entries_completion_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_txs_completion_count",
                    self.stream_txs_completion_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_batches_count",
                    self.stream_batches_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_batches_split_count",
                    self.stream_batches_split_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_version_conflict_count",
                    self.stream_version_conflict_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "stream_malformed_count",
                    self.stream_malformed_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "bincode_deserialize_error_count",
                    self.bincode_deserialize_error_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "unknown_start_position_error_count",
                    self.unknown_start_position_error_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "unknown_start_mid_fec_count",
                    self.unknown_start_mid_fec_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "unknown_start_retry_skipped_count",
                    self.unknown_start_retry_skipped_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "unknown_start_validated_count",
                    self.unknown_start_validated_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "unknown_start_invalid_count",
                    self.unknown_start_invalid_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "held_batch_emitted_count",
                    self.held_batch_emitted_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "known_start_invalid_count",
                    self.known_start_invalid_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "block_marker_count",
                    self.block_marker_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "batch_walk_ns",
                    self.batch_walk_ns.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "batch_walk_count",
                    self.batch_walk_count.swap(0, Ordering::Relaxed),
                    i64
                ),
            );
        }

        self.packets_received
            .retain(|addr, (discarded_packets, not_discarded_packets)| {
                datapoint_info!("shredstream_proxy-receiver_stats",
                    "addr" => addr.to_string(),
                    ("discarded_packets", *discarded_packets, i64),
                    ("not_discarded_packets", *not_discarded_packets, i64),
                );
                false
            });
    }

    /// resets current values, increments cumulative values
    pub fn reset(&self) {
        self.agg_received_cumulative
            .fetch_add(self.received.swap(0, Ordering::Relaxed), Ordering::Relaxed);
        self.agg_success_forward_cumulative.fetch_add(
            self.success_forward.swap(0, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.agg_fail_forward_cumulative.fetch_add(
            self.fail_forward.swap(0, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.duplicate_cumulative
            .fetch_add(self.duplicate.swap(0, Ordering::Relaxed), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        str::FromStr,
        sync::{Arc, Mutex, RwLock},
        thread,
        thread::sleep,
        time::Duration,
    };

    use solana_perf::{
        deduper::Deduper,
        packet::{Meta, Packet, PacketBatch},
    };
    use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};

    use crate::forwarder::{recv_from_channel_and_send_multiple_dest, ShredMetrics};

    fn listen_and_collect(listen_socket: UdpSocket, received_packets: Arc<Mutex<Vec<Vec<u8>>>>) {
        let mut buf = [0u8; PACKET_DATA_SIZE];
        loop {
            listen_socket.recv(&mut buf).unwrap();
            received_packets.lock().unwrap().push(Vec::from(buf));
        }
    }

    #[test]
    fn test_2shreds_3destinations() {
        let packet_batch = PacketBatch::new(vec![
            Packet::new(
                [1; PACKET_DATA_SIZE],
                Meta {
                    size: PACKET_DATA_SIZE,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 48289, // received on random port
                    flags: PacketFlags::empty(),
                },
            ),
            Packet::new(
                [2; PACKET_DATA_SIZE],
                Meta {
                    size: PACKET_DATA_SIZE,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 9999,
                    flags: PacketFlags::empty(),
                },
            ),
        ]);
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded::<PacketBatch>();
        packet_sender.send(packet_batch).unwrap();

        let dest_socketaddrs = vec![
            SocketAddr::from_str("0.0.0.0:32881").unwrap(),
            SocketAddr::from_str("0.0.0.0:33881").unwrap(),
            SocketAddr::from_str("0.0.0.0:34881").unwrap(),
        ];

        let test_listeners = dest_socketaddrs
            .iter()
            .map(|socketaddr| {
                (
                    UdpSocket::bind(socketaddr).unwrap(),
                    *socketaddr,
                    // store results in vec of packet, where packet is Vec<u8>
                    Arc::new(Mutex::new(vec![])),
                )
            })
            .collect::<Vec<_>>();

        let udp_sender = UdpSocket::bind("0.0.0.0:10000").unwrap();

        // spawn listeners
        test_listeners
            .iter()
            .for_each(|(listen_socket, _socketaddr, to_receive)| {
                let socket = listen_socket.try_clone().unwrap();
                let to_receive = to_receive.to_owned();
                thread::spawn(move || listen_and_collect(socket, to_receive));
            });

        let (reconstruct_tx, _reconstruct_rx) = crossbeam_channel::bounded(10_240);
        // send packets
        recv_from_channel_and_send_multiple_dest(
            packet_receiver.recv(),
            &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
                &mut rand::thread_rng(),
                crate::forwarder::DEDUPER_NUM_BITS,
            ))),
            &udp_sender,
            &Arc::new(dest_socketaddrs),
            true,
            &reconstruct_tx,
            false,
            &Arc::new(ShredMetrics::default()),
            None,
            None,
        )
        .unwrap();

        // allow packets to be received
        sleep(Duration::from_millis(500));

        let received = test_listeners
            .iter()
            .map(|(_, _, results)| results.clone())
            .collect::<Vec<_>>();

        // check results
        for received in received.iter() {
            let received = received.lock().unwrap();
            assert_eq!(received.len(), 2);
            assert!(received
                .iter()
                .all(|packet| packet.len() == PACKET_DATA_SIZE));
            assert_eq!(received[0], [1; PACKET_DATA_SIZE]);
            assert_eq!(received[1], [2; PACKET_DATA_SIZE]);
        }

        assert_eq!(
            received
                .iter()
                .fold(0, |acc, elem| acc + elem.lock().unwrap().len()),
            6
        );
    }
}

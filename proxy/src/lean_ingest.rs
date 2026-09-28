//! Lean ingest (`--lean-ingest`, off by default): receive, FEC recovery, deshred and shmem
//! publish run to completion on one busy-polling thread, with no channel hop and no thread
//! wake between a datagram's arrival and the publish of the batch it completes.
//!
//! The default path hands every packet batch across three threads (listener -> `ssPxyTx` ->
//! `shred_reconstructor`), a futex wake per hop. On FRA's shared cores that measured p50
//! 153 us / p99 1.3-1.7 ms from the kernel receive stamp of the last needed shred to the
//! shmem publish, against 33-49 us p50 for the same code on quieter hosts.
//!
//! Here the `ssLeanIngest` thread owns every listen socket (unicast, then DoubleZero
//! multicast), all non-blocking, and for each receive:
//! 1. copies the datagrams onto a bounded queue for the forwarding thread (no syscall);
//! 2. deshreds and publishes completed batches to the shmem ring (`Reconstructor`);
//! 3. wakes the forwarding thread (a futex only if it is parked);
//! 4. taps the benchmark with the receive timestamps, walks proven-start batches and
//!    broadcasts them to gRPC.
//!
//! `ssLeanFwd` dedups and `sendmmsg`s to the validator destinations exactly as an
//! `ssPxyTx` thread does, so loopback sends never delay a publish. Reconstruction still sees
//! every source's copy of a shred (before dedup), as on the default path, so a bloom-filter
//! false positive can only cost a forward, never a deshred.
//!
//! The ingest thread spins at 100% of a core: pin it to a dedicated core with
//! `--lean-ingest-core` (Linux). The shmem ring format and every metric are unchanged.

use std::{
    io,
    net::{IpAddr, Ipv6Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle, Thread},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};
use jito_protos::shredstream::Entry as PbEntry;
use log::{info, warn};
use solana_perf::{
    deduper::Deduper,
    packet::{Packet, PacketBatch, PACKETS_PER_BATCH},
};
use solana_streamer::streamer::StreamerReceiveStats;

use crate::{
    benchmark::{
        recv_timestamp::{enable_socket_timestamps, recv_mmsg_timestamped},
        sources::DOUBLEZERO_SENTINEL,
        BenchmarkHandle, PipelineLatencyHandle,
    },
    forwarder::{recv_from_channel_and_send_multiple_dest, Reconstructor, ShredMetrics},
};

/// Receive calls (not packets) the forwarding thread may lag behind before copies are
/// dropped from forwarding (counted in `forward_queue_dropped`). Deshred is unaffected.
/// Same bound as the default path's reconstruct channel: at most 1024 x 64 packets
/// (~85 MB) if the forwarder stalls during a burst, ~100 ms of backlog at typical rates.
const FORWARD_QUEUE_CAPACITY: usize = 1_024;
/// Longest the forwarding thread sleeps when the ingest thread has nothing for it; it is
/// woken explicitly as soon as there is.
const FORWARD_IDLE_PARK: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug, Default)]
pub struct LeanIngestConfig {
    /// Core to pin the busy-polling ingest thread to (Linux only).
    pub core: Option<usize>,
}

/// Start the ingest and forwarding threads. `sockets` holds the unicast listen sockets
/// first and the DoubleZero multicast sockets after them (`n_unicast_sockets` splits them).
#[allow(clippy::too_many_arguments)]
pub fn start_lean_ingest_threads(
    config: LeanIngestConfig,
    sockets: Vec<UdpSocket>,
    n_unicast_sockets: usize,
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    should_reconstruct_shreds: bool,
    entry_sender: Arc<tokio::sync::broadcast::Sender<PbEntry>>,
    debug_trace_shred: bool,
    forward_stats: Arc<StreamerReceiveStats>,
    metrics: Arc<ShredMetrics>,
    shmem_ring_path: Option<PathBuf>,
    bench_handle: Option<BenchmarkHandle>,
    bench_kernel_timestamps: bool,
    pipeline_handle: Option<PipelineLatencyHandle>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    for socket in &sockets {
        socket
            .set_nonblocking(true)
            .expect("set listen socket non-blocking");
        if bench_kernel_timestamps {
            if let Err(e) = enable_socket_timestamps(socket) {
                warn!("Failed to enable SO_TIMESTAMPNS: {e}");
            }
        }
    }
    let n_sockets = sockets.len();
    let (forward_tx, forward_rx) = crossbeam_channel::bounded(FORWARD_QUEUE_CAPACITY);

    let forward_hdl = {
        let metrics = metrics.clone();
        let exit = exit.clone();
        Builder::new()
            .name("ssLeanFwd".to_string())
            .spawn(move || {
                forward_loop(
                    forward_rx,
                    deduper,
                    unioned_dest_sockets,
                    debug_trace_shred,
                    metrics,
                    exit,
                )
            })
            .unwrap()
    };
    let forward_thread = forward_hdl.thread().clone();

    let ingest_hdl = Builder::new()
        .name("ssLeanIngest".to_string())
        .spawn(move || {
            if let Some(core) = config.core {
                pin_current_thread(core).unwrap_or_else(|e| {
                    panic!("--lean-ingest-core {core}: failed to pin the ingest thread: {e}")
                });
                info!("Lean ingest thread pinned to core {core}");
            }
            let reconstructor = should_reconstruct_shreds.then(|| {
                Reconstructor::new(
                    shmem_ring_path.as_deref(),
                    entry_sender,
                    pipeline_handle,
                    metrics.clone(),
                )
            });
            let source_overrides = (0..sockets.len())
                .map(|i| (i >= n_unicast_sockets).then_some(DOUBLEZERO_SENTINEL))
                .collect();
            ingest_loop(IngestLoop {
                sockets,
                source_overrides,
                reconstructor,
                forward_tx,
                forward_thread,
                bench_handle,
                bench_kernel_timestamps,
                forward_stats,
                metrics,
                exit,
            });
        })
        .unwrap();

    info!(
        "Lean ingest started on {n_sockets} socket(s) ({n_unicast_sockets} unicast){}",
        config
            .core
            .map(|c| format!(", pinned to core {c}"))
            .unwrap_or_default()
    );
    vec![ingest_hdl, forward_hdl]
}

struct IngestLoop {
    sockets: Vec<UdpSocket>,
    /// Per socket: `Some(DOUBLEZERO_SENTINEL)` for the multicast listeners.
    source_overrides: Vec<Option<IpAddr>>,
    reconstructor: Option<Reconstructor>,
    forward_tx: Sender<PacketBatch>,
    forward_thread: Thread,
    bench_handle: Option<BenchmarkHandle>,
    bench_kernel_timestamps: bool,
    forward_stats: Arc<StreamerReceiveStats>,
    metrics: Arc<ShredMetrics>,
    exit: Arc<AtomicBool>,
}

fn ingest_loop(mut state: IngestLoop) {
    let mut packets = vec![Packet::default(); PACKETS_PER_BATCH];
    let mut timestamps = vec![0i64; PACKETS_PER_BATCH];
    let mut last_error_log: Option<Instant> = None;
    while !state.exit.load(Ordering::Relaxed) {
        let mut idle = true;
        for (socket, source_override) in state.sockets.iter().zip(&state.source_overrides) {
            let count = match recv_mmsg_timestamped(socket, &mut packets, &mut timestamps) {
                Ok(count) => count,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) =>
                {
                    0
                }
                Err(e) => {
                    if last_error_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
                        warn!("lean ingest: receive failed: {e}");
                        last_error_log = Some(Instant::now());
                    }
                    0
                }
            };
            if count == 0 {
                continue;
            }
            idle = false;
            let received = &packets[..count];
            if !state.bench_kernel_timestamps {
                timestamps[..count].fill(unix_nanos());
            }
            let stats = &state.forward_stats;
            stats.packets_count.fetch_add(count, Ordering::Relaxed);
            stats.packet_batches_count.fetch_add(1, Ordering::Relaxed);
            if count == PACKETS_PER_BATCH {
                stats
                    .full_packet_batches_count
                    .fetch_add(1, Ordering::Relaxed);
            }

            // 1. Queue a copy for forwarding. No syscall: the forwarder never blocks on
            //    the channel, so a send never has a waiter to wake.
            if state
                .forward_tx
                .try_send(PacketBatch::new(received.to_vec()))
                .is_err()
            {
                state
                    .metrics
                    .forward_queue_dropped
                    .fetch_add(count as u64, Ordering::Relaxed);
            }

            // 2. Deshred and publish.
            if let Some(reconstructor) = state.reconstructor.as_mut() {
                reconstructor.ingest_and_publish(received.iter().filter_map(|p| p.data(..)));
            }

            // 3. Let the forwarder run (a futex wake only if it is parked).
            state.forward_thread.unpark();

            // 4. Everything that does not gate consumer visibility.
            if let Some(bench) = state.bench_handle.as_ref() {
                bench.observe_packets_as(received, &timestamps[..count], *source_override);
            }
            if let Some(reconstructor) = state.reconstructor.as_mut() {
                reconstructor.finish();
            }
        }
        if idle {
            std::hint::spin_loop();
        }
    }
    info!("Exiting lean ingest thread.");
}

fn forward_loop(
    forward_rx: Receiver<PacketBatch>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>,
    debug_trace_shred: bool,
    metrics: Arc<ShredMetrics>,
    exit: Arc<AtomicBool>,
) {
    let send_socket = UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
        .expect("to bind to udp port for forwarding");
    // The ingest thread already reconstructs; this sender only satisfies the signature.
    let (unused_reconstruct_tx, _unused_reconstruct_rx) = crossbeam_channel::bounded(1);
    while !exit.load(Ordering::Relaxed) {
        let mut forwarded = false;
        while let Ok(packet_batch) = forward_rx.try_recv() {
            forwarded = true;
            let dest_sockets = unioned_dest_sockets.load();
            let _ = recv_from_channel_and_send_multiple_dest(
                Ok(packet_batch),
                &deduper,
                &send_socket,
                &dest_sockets,
                false, // should_reconstruct_shreds
                &unused_reconstruct_tx,
                debug_trace_shred,
                &metrics,
                None, // benchmark tapped by the ingest thread
                None,
            );
        }
        if !forwarded {
            std::thread::park_timeout(FORWARD_IDLE_PARK);
        }
    }
    info!("Exiting lean forward thread.");
}

fn unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn pin_current_thread(core: usize) -> io::Result<()> {
    if core >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("core {core} >= CPU_SETSIZE"),
        ));
    }
    // SAFETY: cpu_set_t is a plain bitmask (all zero = empty set), `core` is in bounds, and
    // pid 0 means the calling thread.
    let rc = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_core: usize) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "core pinning is only implemented on Linux",
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        net::UdpSocket,
        sync::{atomic::AtomicBool, Arc, RwLock},
        time::{Duration, Instant},
    };

    use arc_swap::ArcSwap;
    use solana_perf::deduper::Deduper;
    use solana_streamer::streamer::StreamerReceiveStats;

    use super::{start_lean_ingest_threads, LeanIngestConfig};
    use crate::{
        deshred::validated_start_tests::{payload_of_len, Leader, SLOT},
        forwarder::{ShredMetrics, DEDUPER_NUM_BITS},
        shmem_ring::{PollResult, ShmemRingConsumer},
    };

    /// Every shred sent twice (two sources) reaches the shmem ring as whole batches in
    /// order, reaches gRPC, and is forwarded to the destination exactly once.
    #[test]
    fn lean_ingest_publishes_batches_and_forwards_deduped_shreds() {
        let mut leader = Leader::new();
        let set = leader.fec_set_payload_bytes();
        let payloads = [
            payload_of_len(set, &[]),
            payload_of_len(2 * set, &[]),
            payload_of_len(set, &[]),
        ];
        let shreds: Vec<Vec<u8>> = payloads
            .iter()
            .flat_map(|p| leader.batch(p))
            .map(|s| s.payload().as_ref().to_vec())
            .collect();
        assert_eq!(shreds.len(), 4 * 64);

        let listen = UdpSocket::bind("127.0.0.1:0").unwrap();
        let listen_addr = listen.local_addr().unwrap();
        // The forward socket is bound to [::] (as on the default path); macOS cannot send
        // from it to a plain IPv4 address, so the destination listens on IPv6 loopback.
        let destination = UdpSocket::bind("[::1]:0").unwrap();
        destination
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let ring_path = std::env::temp_dir().join(format!(
            "lean_ingest_test_{}_{:?}.ring",
            std::process::id(),
            std::thread::current().id()
        ));
        let entry_sender = Arc::new(tokio::sync::broadcast::Sender::new(100));
        let mut grpc = entry_sender.subscribe();
        let metrics = Arc::new(ShredMetrics::new(true));
        let exit = Arc::new(AtomicBool::new(false));
        let handles = start_lean_ingest_threads(
            LeanIngestConfig { core: None },
            vec![listen],
            1,
            Arc::new(ArcSwap::from_pointee(vec![destination
                .local_addr()
                .unwrap()])),
            Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
                &mut rand::thread_rng(),
                DEDUPER_NUM_BITS,
            ))),
            true,
            entry_sender,
            false,
            Arc::new(StreamerReceiveStats::new("lean_ingest_test")),
            metrics.clone(),
            Some(ring_path.clone()),
            None,
            false,
            None,
            exit.clone(),
        );

        // The ingest thread creates the ring at startup.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut consumer = loop {
            if let Ok(c) = ShmemRingConsumer::open(&ring_path) {
                break c;
            }
            assert!(Instant::now() < deadline, "ring never created");
            std::thread::sleep(Duration::from_millis(5));
        };

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        for _source in 0..2 {
            for chunk in shreds.chunks(16) {
                for shred in chunk {
                    sender.send_to(shred, listen_addr).unwrap();
                }
                std::thread::sleep(Duration::from_micros(200));
            }
        }

        let mut published = vec![];
        let deadline = Instant::now() + Duration::from_secs(5);
        while published.len() < payloads.len() && Instant::now() < deadline {
            match consumer.poll() {
                PollResult::Entry(e) => published.push((e.slot, e.entries_bytes.to_vec())),
                PollResult::Empty => std::thread::yield_now(),
                PollResult::Reset => panic!("ring reset"),
            }
        }
        let expected: Vec<_> = payloads.iter().map(|p| (SLOT, p.clone())).collect();
        assert_eq!(published, expected);

        let mut forwarded = vec![];
        let mut buf = [0u8; 2048];
        while let Ok(n) = destination.recv(&mut buf) {
            forwarded.push(buf[..n].to_vec());
        }
        let unique: HashSet<_> = forwarded.iter().cloned().collect();
        assert_eq!(unique.len(), shreds.len());
        assert_eq!(forwarded.len(), shreds.len(), "duplicates were forwarded");

        exit.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        for payload in &payloads {
            assert_eq!(&grpc.try_recv().unwrap().entries, payload);
        }
        let load = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(load(&metrics.forward_queue_dropped), 0);
        assert_eq!(load(&metrics.received), 2 * shreds.len() as u64);
        assert_eq!(load(&metrics.known_start_invalid_count), 0);
        assert_eq!(load(&metrics.batch_walk_count), 3);
        let _ = std::fs::remove_file(&ring_path);
    }
}

//! Timestamped UDP receive using SO_TIMESTAMPNS (milestone M4).
//!
//! Ported from the `custom` research branch and extended to capture a
//! **per-packet** kernel receive timestamp (the original only kept the first
//! packet's). When benchmark kernel-timestamps are enabled, the listen thread
//! uses this path instead of `solana_streamer::streamer::receiver`: it taps the
//! benchmark with `(source_ip, per-packet kernel rx ts, raw bytes)` and then
//! forwards the `PacketBatch` downstream UNCHANGED, so the production
//! forward/reconstruct path is identical to the stock receiver.

#![allow(clippy::needless_range_loop)]

#[cfg(target_os = "linux")]
use {
    libc::{
        c_void, cmsghdr, iovec, mmsghdr, msghdr, sockaddr_storage, socklen_t, timespec, AF_INET,
        AF_INET6, CMSG_DATA, CMSG_FIRSTHDR, CMSG_SPACE, MSG_WAITFORONE, SCM_TIMESTAMPNS, SOL_SOCKET,
        SO_TIMESTAMPNS,
    },
    std::{
        mem::{self, MaybeUninit},
        net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
        os::unix::io::AsRawFd,
    },
};

use {
    log::{info, warn},
    solana_perf::packet::{Packet, PacketBatch, NUM_RCVMMSGS, PACKETS_PER_BATCH},
    solana_streamer::streamer::StreamerReceiveStats,
    std::{
        cmp, io,
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

use super::BenchmarkHandle;

#[inline]
fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Enable SO_TIMESTAMPNS so the kernel records a timestamp when each packet
/// enters the socket receive buffer.
#[cfg(target_os = "linux")]
pub fn enable_socket_timestamps(socket: &UdpSocket) -> io::Result<()> {
    let val: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            SOL_SOCKET,
            SO_TIMESTAMPNS,
            &val as *const _ as *const c_void,
            mem::size_of::<libc::c_int>() as socklen_t,
        )
    };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn enable_socket_timestamps(_socket: &UdpSocket) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
const CMSG_BUF_SIZE: usize = unsafe { CMSG_SPACE(mem::size_of::<timespec>() as u32) as usize };

/// 8-byte-aligned backing for a control-message buffer. `cmsghdr`/`timespec`
/// require 8-byte alignment on 64-bit Linux; a bare `[u8; N]` (align 1) read as
/// those types is technically UB, so we force the alignment here.
#[cfg(target_os = "linux")]
#[repr(C, align(8))]
#[derive(Clone, Copy)]
struct CmsgBuf([u8; CMSG_BUF_SIZE]);

#[cfg(target_os = "linux")]
fn extract_timestamp(hdr: &msghdr) -> Option<i64> {
    if hdr.msg_controllen == 0 || hdr.msg_control.is_null() {
        return None;
    }
    unsafe {
        let cmsg: *mut cmsghdr = CMSG_FIRSTHDR(hdr);
        if !cmsg.is_null()
            && (*cmsg).cmsg_level == SOL_SOCKET
            && (*cmsg).cmsg_type == SCM_TIMESTAMPNS
        {
            let ts = &*(CMSG_DATA(cmsg) as *const timespec);
            return Some(ts.tv_sec as i64 * 1_000_000_000 + ts.tv_nsec as i64);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn cast_socket_addr(addr: &sockaddr_storage, hdr: &mmsghdr) -> Option<SocketAddr> {
    use libc::{sa_family_t, sockaddr_in, sockaddr_in6};
    const SOCKADDR_IN_SIZE: usize = mem::size_of::<sockaddr_in>();
    const SOCKADDR_IN6_SIZE: usize = mem::size_of::<sockaddr_in6>();

    if addr.ss_family == AF_INET as sa_family_t
        && hdr.msg_hdr.msg_namelen == SOCKADDR_IN_SIZE as socklen_t
    {
        let addr = unsafe { &*(addr as *const _ as *const sockaddr_in) };
        return Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
            u16::from_be(addr.sin_port),
        )));
    }
    if addr.ss_family == AF_INET6 as sa_family_t
        && hdr.msg_hdr.msg_namelen == SOCKADDR_IN6_SIZE as socklen_t
    {
        let addr = unsafe { &*(addr as *const _ as *const sockaddr_in6) };
        return Some(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(addr.sin6_addr.s6_addr),
            u16::from_be(addr.sin6_port),
            addr.sin6_flowinfo,
            addr.sin6_scope_id,
        )));
    }
    None
}

/// Like `recv_mmsg` but with per-message control buffers for SO_TIMESTAMPNS.
/// Fills `out_ts[i]` with each packet's kernel rx timestamp (falling back to
/// userspace now() if the cmsg is missing). Returns the number of packets.
#[cfg(target_os = "linux")]
pub fn recv_mmsg_timestamped(
    sock: &UdpSocket,
    packets: &mut [Packet],
    out_ts: &mut [i64],
) -> io::Result<usize> {
    // Compute count BEFORE touching any MaybeUninit. If a caller passes a
    // non-empty `packets` but a shorter/empty `out_ts`, count would be 0 and we
    // must NOT call assume_init_mut() on an uninitialized hdrs[0] (UB).
    let count = cmp::min(cmp::min(NUM_RCVMMSGS, packets.len()), out_ts.len());
    if count == 0 {
        return Ok(0);
    }
    const SOCKADDR_STORAGE_SIZE: socklen_t = mem::size_of::<sockaddr_storage>() as socklen_t;

    let mut iovs = [MaybeUninit::<iovec>::uninit(); NUM_RCVMMSGS];
    let mut addrs = [MaybeUninit::<sockaddr_storage>::zeroed(); NUM_RCVMMSGS];
    let mut hdrs = [MaybeUninit::<mmsghdr>::uninit(); NUM_RCVMMSGS];
    let mut cmsg_bufs = [CmsgBuf([0u8; CMSG_BUF_SIZE]); NUM_RCVMMSGS];

    let sock_fd = sock.as_raw_fd();

    for i in 0..count {
        let buffer = packets[i].buffer_mut();
        iovs[i].write(iovec {
            iov_base: buffer.as_mut_ptr() as *mut c_void,
            iov_len: buffer.len(),
        });

        let mut msg_hdr: msghdr = unsafe { mem::zeroed() };
        msg_hdr.msg_name = addrs[i].as_mut_ptr() as *mut _;
        msg_hdr.msg_namelen = SOCKADDR_STORAGE_SIZE;
        msg_hdr.msg_iov = iovs[i].as_mut_ptr();
        msg_hdr.msg_iovlen = 1;
        msg_hdr.msg_control = cmsg_bufs[i].0.as_mut_ptr() as *mut c_void;
        msg_hdr.msg_controllen = CMSG_BUF_SIZE as _;
        msg_hdr.msg_flags = 0;

        hdrs[i].write(mmsghdr { msg_len: 0, msg_hdr });
    }

    let mut ts = timespec {
        tv_sec: 1,
        tv_nsec: 0,
    };
    #[allow(clippy::useless_conversion)]
    let nrecv = unsafe {
        libc::recvmmsg(
            sock_fd,
            hdrs[0].assume_init_mut(),
            count as u32,
            MSG_WAITFORONE.try_into().unwrap(),
            &mut ts,
        )
    };
    let nrecv = if nrecv < 0 {
        // Clean up initialized entries before returning.
        for i in 0..count {
            unsafe {
                iovs[i].assume_init_drop();
                addrs[i].assume_init_drop();
                hdrs[i].assume_init_drop();
            }
        }
        return Err(io::Error::last_os_error());
    } else {
        usize::try_from(nrecv).unwrap()
    };

    let fallback = now_unix_nanos();
    for i in 0..nrecv {
        let hdr = unsafe { hdrs[i].assume_init_ref() };
        let addr = unsafe { addrs[i].assume_init_ref() };
        packets[i].meta_mut().size = hdr.msg_len as usize;
        if let Some(addr) = cast_socket_addr(addr, hdr) {
            packets[i].meta_mut().set_socket_addr(&addr);
        }
        out_ts[i] = extract_timestamp(&hdr.msg_hdr).unwrap_or(fallback);
    }

    for i in 0..count {
        unsafe {
            iovs[i].assume_init_drop();
            addrs[i].assume_init_drop();
            hdrs[i].assume_init_drop();
        }
    }

    Ok(nrecv)
}

#[cfg(not(target_os = "linux"))]
pub fn recv_mmsg_timestamped(
    sock: &UdpSocket,
    packets: &mut [Packet],
    out_ts: &mut [i64],
) -> io::Result<usize> {
    let mut i = 0;
    let count = cmp::min(cmp::min(NUM_RCVMMSGS, packets.len()), out_ts.len());
    for idx in 0..count {
        match sock.recv_from(packets[idx].buffer_mut()) {
            Err(_) if i > 0 => break,
            Err(e) => return Err(e),
            Ok((nrecv, from)) => {
                packets[idx].meta_mut().size = nrecv;
                packets[idx].meta_mut().set_socket_addr(&from);
                out_ts[idx] = now_unix_nanos();
                if i == 0 {
                    sock.set_nonblocking(true)?;
                }
            }
        }
        i += 1;
    }
    Ok(i)
}

/// Recv loop that captures per-packet kernel timestamps, taps the benchmark, and
/// forwards the `PacketBatch` downstream on `sender` (same item type as the
/// stock receiver, so the forward/reconstruct path is unchanged).
pub fn start_recv_and_tap_thread(
    thread_name: String,
    socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
    sender: crossbeam_channel::Sender<PacketBatch>,
    stats: Arc<StreamerReceiveStats>,
    bench: BenchmarkHandle,
) -> JoinHandle<()> {
    socket
        .set_read_timeout(Some(Duration::new(1, 0)))
        .expect("set_read_timeout");

    if let Err(e) = enable_socket_timestamps(&socket) {
        warn!("Failed to enable SO_TIMESTAMPNS: {e}");
    } else if let Ok(addr) = socket.local_addr() {
        info!("Enabled SO_TIMESTAMPNS on {addr}");
    }

    Builder::new()
        .name(thread_name)
        .spawn(move || {
            let mut ts_buf = vec![0i64; PACKETS_PER_BATCH];
            while !exit.load(Ordering::Relaxed) {
                let mut batch = PacketBatch::with_capacity(PACKETS_PER_BATCH);
                let mut total_packets = 0;

                socket.set_nonblocking(false).ok();
                let start = Instant::now();

                loop {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    batch.resize(
                        cmp::min(total_packets + NUM_RCVMMSGS, PACKETS_PER_BATCH),
                        Packet::default(),
                    );
                    let cap = batch.len();
                    match recv_mmsg_timestamped(
                        &socket,
                        &mut batch[total_packets..cap],
                        &mut ts_buf[total_packets..cap],
                    ) {
                        Ok(0) => continue,
                        Ok(npkts) => {
                            if total_packets == 0 {
                                socket.set_nonblocking(true).ok();
                            }
                            total_packets += npkts;
                            // Coalesce window is zero on purpose: this matches the
                            // stock streamer::receiver, which is invoked with
                            // Duration::default() (one recvmmsg per batch, lowest
                            // latency). Do NOT widen it without intent.
                            if total_packets >= PACKETS_PER_BATCH
                                || start.elapsed() > Duration::default()
                            {
                                break;
                            }
                        }
                        Err(_) if total_packets > 0 => break,
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock
                                || e.kind() == io::ErrorKind::TimedOut
                            {
                                continue;
                            }
                            break;
                        }
                    }
                }

                if total_packets > 0 {
                    batch.truncate(total_packets);

                    // Tap the benchmark with per-packet kernel timestamps BEFORE
                    // forwarding (closest to true receipt). Read-only over the batch.
                    bench.observe_packets(&batch[..total_packets], &ts_buf[..total_packets]);

                    stats
                        .packets_count
                        .fetch_add(total_packets, Ordering::Relaxed);
                    stats.packet_batches_count.fetch_add(1, Ordering::Relaxed);
                    if total_packets == PACKETS_PER_BATCH {
                        stats
                            .full_packet_batches_count
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    if sender.send(batch).is_err() {
                        break;
                    }
                }
            }
        })
        .unwrap()
}

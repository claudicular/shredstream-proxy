#![allow(unused_imports)]
//! Timestamped UDP receive using SO_TIMESTAMPNS.
//!
//! Provides a `recv_mmsg` variant that extracts the kernel-level socket
//! timestamp from each packet, and a recv loop thread that passes the
//! earliest timestamp alongside each PacketBatch.

#[cfg(target_os = "linux")]
use {
    libc::{
        c_void, cmsghdr, iovec, mmsghdr, msghdr, sockaddr_storage, socklen_t, timespec,
        AF_INET, AF_INET6, CMSG_DATA, CMSG_FIRSTHDR, CMSG_SPACE, MSG_WAITFORONE, SCM_TIMESTAMPNS,
        SOL_SOCKET, SO_TIMESTAMPNS,
    },
    std::{
        mem::{self, MaybeUninit},
        net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
        os::unix::io::AsRawFd,
    },
};

use {
    log::{info, warn},
    solana_perf::packet::{PacketBatch, PACKETS_PER_BATCH, NUM_RCVMMSGS},
    solana_streamer::{
        packet::{Meta, Packet},
        streamer::StreamerReceiveStats,
    },
    std::{
        cmp,
        io,
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

/// Enable SO_TIMESTAMPNS on a socket so the kernel records a timestamp
/// when each packet enters the socket receive buffer.
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

/// Size of the control message buffer needed for one SCM_TIMESTAMPNS.
#[cfg(target_os = "linux")]
const CMSG_BUF_SIZE: usize = unsafe { CMSG_SPACE(mem::size_of::<timespec>() as u32) as usize };

/// Extract SCM_TIMESTAMPNS from a msghdr's control message buffer.
/// Returns nanoseconds since UNIX epoch.
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
            return Some(ts.tv_sec * 1_000_000_000 + ts.tv_nsec);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn cast_socket_addr(
    addr: &sockaddr_storage,
    hdr: &mmsghdr,
) -> Option<SocketAddr> {
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

/// Like solana_streamer::recvmmsg::recv_mmsg but with control message buffers
/// for SO_TIMESTAMPNS extraction. Returns (num_packets, earliest_kernel_timestamp_nanos).
#[cfg(target_os = "linux")]
pub fn recv_mmsg_timestamped(
    sock: &UdpSocket,
    packets: &mut [Packet],
) -> io::Result<(usize, Option<i64>)> {
    if packets.is_empty() {
        return Ok((0, None));
    }
    debug_assert!(packets.iter().all(|pkt| pkt.meta() == &Meta::default()));
    const SOCKADDR_STORAGE_SIZE: socklen_t = mem::size_of::<sockaddr_storage>() as socklen_t;

    let mut iovs = [MaybeUninit::<iovec>::uninit(); NUM_RCVMMSGS];
    let mut addrs = [MaybeUninit::<sockaddr_storage>::zeroed(); NUM_RCVMMSGS];
    let mut hdrs = [MaybeUninit::<mmsghdr>::uninit(); NUM_RCVMMSGS];
    // Control message buffers for timestamp extraction
    let mut cmsg_bufs = [[0u8; CMSG_BUF_SIZE]; NUM_RCVMMSGS];

    let sock_fd = sock.as_raw_fd();
    let count = cmp::min(iovs.len(), packets.len());

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
        msg_hdr.msg_control = cmsg_bufs[i].as_mut_ptr() as *mut c_void;
        msg_hdr.msg_controllen = CMSG_BUF_SIZE as _;
        msg_hdr.msg_flags = 0;

        hdrs[i].write(mmsghdr {
            msg_len: 0,
            msg_hdr,
        });
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
        return Err(io::Error::last_os_error());
    } else {
        usize::try_from(nrecv).unwrap()
    };

    let mut earliest_ts: Option<i64> = None;

    for i in 0..nrecv {
        let hdr = unsafe { hdrs[i].assume_init_ref() };
        let addr = unsafe { addrs[i].assume_init_ref() };
        packets[i].meta_mut().size = hdr.msg_len as usize;
        if let Some(addr) = cast_socket_addr(addr, hdr) {
            packets[i].meta_mut().set_socket_addr(&addr);
        }

        // Extract kernel timestamp from the first packet
        if i == 0 {
            earliest_ts = extract_timestamp(&hdr.msg_hdr);
        }
    }

    // Clean up
    for i in 0..count {
        unsafe {
            iovs[i].assume_init_drop();
            addrs[i].assume_init_drop();
            hdrs[i].assume_init_drop();
        }
    }

    Ok((nrecv, earliest_ts))
}

#[cfg(not(target_os = "linux"))]
pub fn recv_mmsg_timestamped(
    sock: &UdpSocket,
    packets: &mut [Packet],
) -> io::Result<(usize, Option<i64>)> {
    // Fallback: no timestamps on non-Linux
    let mut i = 0;
    let count = cmp::min(NUM_RCVMMSGS, packets.len());
    for p in packets.iter_mut().take(count) {
        p.meta_mut().size = 0;
        match sock.recv_from(p.buffer_mut()) {
            Err(_) if i > 0 => break,
            Err(e) => return Err(e),
            Ok((nrecv, from)) => {
                p.meta_mut().size = nrecv;
                p.meta_mut().set_socket_addr(&from);
                if i == 0 {
                    sock.set_nonblocking(true)?;
                }
            }
        }
        i += 1;
    }
    Ok((i, None))
}

/// Recv loop that mirrors solana_streamer::streamer::recv_loop but captures
/// kernel timestamps and sends (kernel_timestamp_nanos, PacketBatch) on the channel.
pub fn start_recv_thread(
    thread_name: String,
    socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
    sender: crossbeam_channel::Sender<(i64, PacketBatch)>,
    stats: Arc<StreamerReceiveStats>,
) -> JoinHandle<()> {
    // Set socket read timeout (same as Solana's streamer)
    socket
        .set_read_timeout(Some(Duration::new(1, 0)))
        .expect("set_read_timeout");

    // Enable kernel timestamps
    if let Err(e) = enable_socket_timestamps(&socket) {
        warn!("Failed to enable SO_TIMESTAMPNS: {e}");
    } else {
        info!("Enabled SO_TIMESTAMPNS on {}", socket.local_addr().unwrap());
    }

    Builder::new()
        .name(thread_name)
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                let mut batch = PacketBatch::with_capacity(PACKETS_PER_BATCH);
                let mut total_packets = 0;
                let mut batch_kernel_ts: Option<i64> = None;

                // First call: blocking (wait for first packet)
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

                    match recv_mmsg_timestamped(&socket, &mut batch[total_packets..]) {
                        Ok((npkts, kernel_ts)) => {
                            if npkts == 0 {
                                continue;
                            }
                            if total_packets == 0 {
                                // First successful recv: capture timestamp, switch to non-blocking
                                batch_kernel_ts = kernel_ts;
                                socket.set_nonblocking(true).ok();
                            }
                            total_packets += npkts;

                            // With coalesce=0 (Duration::default()), break immediately
                            // Same behavior as Solana's recv_from with max_wait=0
                            if total_packets >= PACKETS_PER_BATCH || start.elapsed() > Duration::default() {
                                break;
                            }
                        }
                        Err(_) if total_packets > 0 => {
                            // Non-blocking recv failed after we got some packets — done
                            break;
                        }
                        Err(e) => {
                            // Blocking recv error (timeout, etc.)
                            if e.kind() == io::ErrorKind::WouldBlock
                                || e.kind() == io::ErrorKind::TimedOut
                            {
                                continue;
                            }
                            // Real error
                            break;
                        }
                    }
                }

                if total_packets > 0 {
                    batch.truncate(total_packets);

                    // Use kernel timestamp, or fall back to current time
                    let ts_nanos = batch_kernel_ts.unwrap_or_else(|| {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as i64
                    });

                    stats
                        .packets_count
                        .fetch_add(total_packets, Ordering::Relaxed);
                    stats
                        .packet_batches_count
                        .fetch_add(1, Ordering::Relaxed);
                    if total_packets == PACKETS_PER_BATCH {
                        stats
                            .full_packet_batches_count
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    let _ = sender.send((ts_nanos, batch));
                }
            }
        })
        .unwrap()
}

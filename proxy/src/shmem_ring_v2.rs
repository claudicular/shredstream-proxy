//! Shared-memory ring v2: entry records with their **ledger position**, for an executor
//! (agave's fast lane) that must run a slot's transactions in block order.
//!
//! v1 (`shmem_ring.rs`) records carry only `(seq, slot, data_len)`. With
//! `--stream-entries` a batch is published as several records (the new complete entries of
//! its growing prefix), and records of different batches of a slot are published in
//! completion order, so a v1 consumer cannot compute transaction ordinals or tell a partial
//! record from a whole batch. v2 adds, per record: the parent slot, the batch's first data
//! shred index, the current last shred index, the index of the record's first entry within
//! the batch, flags (FINAL / LAST_IN_SLOT / GUESSED_START) and the publish time. v1 is
//! written first and is unchanged.
//!
//! Layout (all little-endian, 8-byte aligned):
//! - file header, 128 B: magic "SHRINGv2", version, header_size, data_region_size,
//!   max_record, producer_pid, created_epoch_ns (generation), write_pos and write_seq on
//!   their own cache line;
//! - records, never straddling the end of the data region (the writer skips to the start;
//!   a reader that sees a non-matching seq at a position skips to the start as well):
//!
//! | off | field |
//! |---|---|
//! | 0 | `seq u64`, written last (Release) |
//! | 8 | `slot u64` |
//! | 16 | `parent_slot u64` (`u64::MAX` if unknown) |
//! | 24 | `batch_start u32`: first data shred index of the batch |
//! | 28 | `batch_end u32`: last data shred index covered by this record |
//! | 32 | `entry_offset u32`: entries of this batch published before this record |
//! | 36 | `entry_count u32`: entries in this record (0 for a FINAL-only marker) |
//! | 40 | `flags u16`: 1 FINAL, 2 LAST_IN_SLOT, 4 GUESSED_START |
//! | 42 | `reserved u16` |
//! | 44 | `data_len u32` |
//! | 48 | `t_publish_ns u64` (CLOCK_REALTIME) |
//! | 56 | `reserved u64` |
//! | 64 | payload: bincode `Vec<Entry>` (u64 count + entries), `data_len` bytes |
//!
//! Reader protocol (seqlock): read `write_pos` (Acquire); read the record's `seq`
//! (Acquire); copy header and payload; re-read `write_pos`; the copy is valid only if the
//! writer cannot have reached it: `write_pos_after + max_record <= record_pos + capacity`.
//! A lapped reader or a new generation resets to the head.

use std::{
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    ptr,
    sync::atomic::{AtomicU64, Ordering, fence},
    time::{SystemTime, UNIX_EPOCH},
};

/// Magic bytes: "SHRINGv2".
pub const MAGIC_V2: u64 = u64::from_le_bytes(*b"SHRINGv2");
pub const VERSION_V2: u32 = 2;
pub const HEADER_SIZE: usize = 128;
pub const RECORD_HEADER_SIZE: usize = 64;
pub const DEFAULT_DATA_REGION_SIZE: usize = 64 * 1024 * 1024;
pub const MAX_PAYLOAD: usize = 1024 * 1024;

pub const FLAG_FINAL: u16 = 1;
pub const FLAG_LAST_IN_SLOT: u16 = 2;
pub const FLAG_GUESSED_START: u16 = 4;

#[inline]
const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

#[repr(C)]
struct RingHeaderV2 {
    magic: u64,            // 0x00
    version: u32,          // 0x08
    header_size: u32,      // 0x0C
    data_region_size: u64, // 0x10
    max_record: u64,       // 0x18
    producer_pid: u64,     // 0x20
    created_epoch_ns: u64, // 0x28
    _pad0: [u8; 16],       // 0x30..0x3F
    write_pos: AtomicU64,  // 0x40
    write_seq: AtomicU64,  // 0x48
    _pad1: [u8; 48],       // 0x50..0x7F
}

const _: () = assert!(std::mem::size_of::<RingHeaderV2>() == HEADER_SIZE);

/// Position and flags of one record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordMeta {
    pub slot: u64,
    pub parent_slot: u64,
    pub batch_start: u32,
    pub batch_end: u32,
    pub entry_offset: u32,
    pub entry_count: u32,
    pub flags: u16,
    pub t_publish_ns: u64,
}

pub struct ShmemRingV2Producer {
    mmap_ptr: *mut u8,
    mmap_len: usize,
    data_region_size: usize,
    max_payload: usize,
    local_write_pos: u64,
    local_write_seq: u64,
}

// SAFETY: used from the single reconstructor thread (SPSC).
unsafe impl Send for ShmemRingV2Producer {}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl ShmemRingV2Producer {
    pub fn create(path: &Path) -> io::Result<Self> {
        Self::create_with_size(path, DEFAULT_DATA_REGION_SIZE)
    }

    /// Creates the ring under a temporary name and renames it over `path`, so a reader
    /// never maps a half-initialized file; readers detect the new generation.
    pub fn create_with_size(path: &Path, data_region_size: usize) -> io::Result<Self> {
        // A record may use at most a quarter of the region, so a reader lagging by less than
        // three quarters of the region is never lapped by an in-flight write.
        let max_record = (RECORD_HEADER_SIZE + MAX_PAYLOAD).min(data_region_size / 4) & !7;
        let max_payload = max_record - RECORD_HEADER_SIZE;
        let total_size = HEADER_SIZE + data_region_size;
        let tmp = path.with_extension("tmp");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.set_len(total_size as u64)?;
        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                total_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if mmap_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mmap_ptr = mmap_ptr as *mut u8;
        // Touch every page so the first publishes do not fault.
        unsafe { ptr::write_bytes(mmap_ptr, 0, total_size) };
        let header = unsafe { &mut *(mmap_ptr as *mut RingHeaderV2) };
        header.version = VERSION_V2;
        header.header_size = HEADER_SIZE as u32;
        header.data_region_size = data_region_size as u64;
        header.max_record = max_record as u64;
        header.producer_pid = std::process::id() as u64;
        header.created_epoch_ns = now_ns();
        header.write_pos.store(0, Ordering::Release);
        header.write_seq.store(0, Ordering::Release);
        fence(Ordering::Release);
        // Magic last: a reader that sees it sees an initialized header.
        unsafe { ptr::write_volatile(&mut header.magic, MAGIC_V2) };
        std::fs::rename(&tmp, path)?;
        Ok(Self {
            mmap_ptr,
            mmap_len: total_size,
            data_region_size,
            max_payload,
            local_write_pos: 0,
            local_write_seq: 0,
        })
    }

    /// Publish one record. Returns its seq, or 0 if the payload is too large.
    pub fn publish(&mut self, meta: &RecordMeta, payload: &[u8]) -> u64 {
        if payload.len() > self.max_payload {
            return 0;
        }
        let total = align8(RECORD_HEADER_SIZE + payload.len());
        let region_offset = (self.local_write_pos as usize) % self.data_region_size;
        if region_offset + total > self.data_region_size {
            self.local_write_pos += (self.data_region_size - region_offset) as u64;
        }
        let offset = (self.local_write_pos as usize) % self.data_region_size;
        let seq = self.local_write_seq + 1;
        unsafe {
            let p = self.mmap_ptr.add(HEADER_SIZE + offset);
            // Invalidate first, fill, then commit the seq.
            (*(p as *const AtomicU64)).store(0, Ordering::Relaxed);
            fence(Ordering::Release);
            ptr::write_unaligned(p.add(8) as *mut u64, meta.slot);
            ptr::write_unaligned(p.add(16) as *mut u64, meta.parent_slot);
            ptr::write_unaligned(p.add(24) as *mut u32, meta.batch_start);
            ptr::write_unaligned(p.add(28) as *mut u32, meta.batch_end);
            ptr::write_unaligned(p.add(32) as *mut u32, meta.entry_offset);
            ptr::write_unaligned(p.add(36) as *mut u32, meta.entry_count);
            ptr::write_unaligned(p.add(40) as *mut u16, meta.flags);
            ptr::write_unaligned(p.add(42) as *mut u16, 0);
            ptr::write_unaligned(p.add(44) as *mut u32, payload.len() as u32);
            ptr::write_unaligned(p.add(48) as *mut u64, meta.t_publish_ns);
            ptr::write_unaligned(p.add(56) as *mut u64, 0);
            ptr::copy_nonoverlapping(payload.as_ptr(), p.add(RECORD_HEADER_SIZE), payload.len());
            (*(p as *const AtomicU64)).store(seq, Ordering::Release);
        }
        self.local_write_seq = seq;
        self.local_write_pos += total as u64;
        let header = unsafe { &*(self.mmap_ptr as *const RingHeaderV2) };
        header.write_seq.store(seq, Ordering::Release);
        header.write_pos.store(self.local_write_pos, Ordering::Release);
        seq
    }
}

impl Drop for ShmemRingV2Producer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
        }
    }
}

/// Tracks entries already published per batch, to fill `entry_offset`.
#[derive(Default)]
pub struct BatchOffsets {
    published: ahash::HashMap<(u64, u32), u32>,
}

impl BatchOffsets {
    /// Offset for a record of `(slot, batch_start)` with `entry_count` entries; forgets
    /// the batch when `final_`.
    pub fn take(&mut self, slot: u64, batch_start: u32, entry_count: u32, final_: bool) -> u32 {
        let key = (slot, batch_start);
        let offset = self.published.get(&key).copied().unwrap_or(0);
        if final_ {
            self.published.remove(&key);
        } else {
            self.published
                .insert(key, offset.saturating_add(entry_count));
        }
        offset
    }

    /// Drop batches of slots older than `min_slot` (stalled streams).
    pub fn prune(&mut self, min_slot: u64) {
        self.published.retain(|(slot, _), _| *slot >= min_slot);
    }

    pub fn len(&self) -> usize {
        self.published.len()
    }

    pub fn is_empty(&self) -> bool {
        self.published.is_empty()
    }
}

/// Reference reader (the fast lane has its own copy; this one is for tests and tools).
pub struct ShmemRingV2Consumer {
    mmap_ptr: *const u8,
    mmap_len: usize,
    data_region_size: usize,
    max_record: u64,
    read_pos: u64,
    read_seq: u64,
    generation: u64,
}

unsafe impl Send for ShmemRingV2Consumer {}

pub enum PollV2 {
    Record(RecordMeta, Vec<u8>),
    Empty,
    /// Lapped or new generation: state reset to the head.
    Reset,
}

impl ShmemRingV2Consumer {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let len = file.metadata()?.len() as usize;
        if len < HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "too small"));
        }
        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if mmap_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mmap_ptr = mmap_ptr as *const u8;
        let header = unsafe { &*(mmap_ptr as *const RingHeaderV2) };
        let magic = unsafe { ptr::read_volatile(&header.magic) };
        if magic != MAGIC_V2 {
            unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, len) };
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
        }
        fence(Ordering::Acquire);
        let data_region_size = header.data_region_size as usize;
        let max_record = header.max_record;
        let generation = header.created_epoch_ns;
        let read_pos = header.write_pos.load(Ordering::Acquire);
        let read_seq = header.write_seq.load(Ordering::Acquire);
        Ok(Self {
            mmap_ptr,
            mmap_len: len,
            data_region_size,
            max_record,
            read_pos,
            read_seq,
            generation,
        })
    }

    fn header(&self) -> &'static RingHeaderV2 {
        // SAFETY: the mapping lives as long as `self`; callers never keep the reference
        // beyond the call that obtained it.
        unsafe { &*(self.mmap_ptr as *const RingHeaderV2) }
    }

    pub fn poll(&mut self) -> PollV2 {
        let header = self.header();
        if header.created_epoch_ns != self.generation {
            return PollV2::Reset;
        }
        let write_pos = header.write_pos.load(Ordering::Acquire);
        if self.read_pos >= write_pos {
            return PollV2::Empty;
        }
        if write_pos - self.read_pos > self.data_region_size as u64 {
            self.read_pos = write_pos;
            self.read_seq = header.write_seq.load(Ordering::Acquire);
            return PollV2::Reset;
        }
        for _ in 0..2 {
            let offset = (self.read_pos as usize) % self.data_region_size;
            if offset + RECORD_HEADER_SIZE > self.data_region_size {
                self.read_pos += (self.data_region_size - offset) as u64;
                continue;
            }
            let p = unsafe { self.mmap_ptr.add(HEADER_SIZE + offset) };
            let seq = unsafe { (*(p as *const AtomicU64)).load(Ordering::Acquire) };
            if seq != self.read_seq + 1 {
                // Wrap skip: the writer continued at the region start.
                self.read_pos += (self.data_region_size - offset) as u64;
                continue;
            }
            let (meta, data_len) = unsafe {
                (
                    RecordMeta {
                        slot: ptr::read_unaligned(p.add(8) as *const u64),
                        parent_slot: ptr::read_unaligned(p.add(16) as *const u64),
                        batch_start: ptr::read_unaligned(p.add(24) as *const u32),
                        batch_end: ptr::read_unaligned(p.add(28) as *const u32),
                        entry_offset: ptr::read_unaligned(p.add(32) as *const u32),
                        entry_count: ptr::read_unaligned(p.add(36) as *const u32),
                        flags: ptr::read_unaligned(p.add(40) as *const u16),
                        t_publish_ns: ptr::read_unaligned(p.add(48) as *const u64),
                    },
                    ptr::read_unaligned(p.add(44) as *const u32) as usize,
                )
            };
            if data_len > MAX_PAYLOAD || offset + RECORD_HEADER_SIZE + data_len > self.data_region_size
            {
                self.read_pos = header.write_pos.load(Ordering::Acquire);
                self.read_seq = header.write_seq.load(Ordering::Acquire);
                return PollV2::Reset;
            }
            let payload = unsafe {
                std::slice::from_raw_parts(p.add(RECORD_HEADER_SIZE), data_len).to_vec()
            };
            fence(Ordering::Acquire);
            let write_pos_after = header.write_pos.load(Ordering::Acquire);
            let seq_after = unsafe { (*(p as *const AtomicU64)).load(Ordering::Acquire) };
            if seq_after != seq
                || write_pos_after + self.max_record > self.read_pos + self.data_region_size as u64
            {
                self.read_pos = write_pos_after;
                self.read_seq = header.write_seq.load(Ordering::Acquire);
                return PollV2::Reset;
            }
            self.read_pos += align8(RECORD_HEADER_SIZE + data_len) as u64;
            self.read_seq = seq;
            return PollV2::Record(meta, payload);
        }
        PollV2::Empty
    }
}

impl Drop for ShmemRingV2Consumer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::sync::atomic::AtomicU32};

    static N: AtomicU32 = AtomicU32::new(0);

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "shmem_ring_v2_test_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn meta(slot: u64, start: u32, offset: u32, count: u32, flags: u16) -> RecordMeta {
        RecordMeta {
            slot,
            parent_slot: slot - 1,
            batch_start: start,
            batch_end: start + 3,
            entry_offset: offset,
            entry_count: count,
            flags,
            t_publish_ns: 42,
        }
    }

    #[test]
    fn roundtrip_and_wrap() {
        let path = temp_path();
        let mut producer = ShmemRingV2Producer::create_with_size(&path, 1 << 20).unwrap();
        let mut consumer = ShmemRingV2Consumer::open(&path).unwrap();
        assert!(matches!(consumer.poll(), PollV2::Empty));
        let payload = vec![7u8; 3000];
        let mut read = 0u64;
        for i in 0..2000u64 {
            let m = meta(100 + i, (i % 7) as u32, i as u32, 2, FLAG_FINAL);
            producer.publish(&m, &payload[..(i as usize % 3000)]);
            loop {
                match consumer.poll() {
                    PollV2::Record(got, data) => {
                        assert_eq!(got.slot, 100 + read);
                        assert_eq!(got.entry_offset, read as u32);
                        assert_eq!(data.len(), read as usize % 3000);
                        read += 1;
                    }
                    PollV2::Empty => break,
                    PollV2::Reset => panic!("unexpected reset"),
                }
            }
        }
        assert_eq!(read, 2000);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn lapped_reader_resets() {
        let path = temp_path();
        let mut producer = ShmemRingV2Producer::create_with_size(&path, 1 << 16).unwrap();
        let mut consumer = ShmemRingV2Consumer::open(&path).unwrap();
        for i in 0..1000u64 {
            producer.publish(&meta(i + 1, 0, 0, 1, 0), &[1u8; 500]);
        }
        assert!(matches!(consumer.poll(), PollV2::Reset));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn batch_offsets() {
        let mut o = BatchOffsets::default();
        assert_eq!(o.take(5, 0, 3, false), 0);
        assert_eq!(o.take(5, 0, 2, false), 3);
        assert_eq!(o.take(5, 0, 4, true), 5);
        assert_eq!(o.take(5, 0, 1, false), 0);
        o.prune(6);
        assert!(o.is_empty());
    }
}

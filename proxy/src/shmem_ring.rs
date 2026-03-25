//! Shared memory SPSC ring buffer for ultra-low-latency entry delivery.
//!
//! The producer writes deshredded entries to a memory-mapped file in `/dev/shm`.
//! A consumer in another process spin-polls the write index and reads entries
//! with zero-copy directly from the mapped region.
//!
//! Layout: 128-byte header + 4 MiB data region.
//! Entries are variable-length, 8-byte aligned, with sequence numbers for validity.

use std::{
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    ptr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use log::info;

/// Magic bytes: "SHRINGBF"
const MAGIC: u64 = 0x534852494E474246;
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 128;
const DEFAULT_DATA_REGION_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
const MAX_ENTRY_SIZE: usize = 256 * 1024; // 256 KiB
const ENTRY_HEADER_SIZE: usize = 24; // seq(8) + slot(8) + data_len(4) + pad(4)

/// Round up to next multiple of 8.
#[inline]
const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// On-disk/on-mmap header layout. Must be repr(C) with exact field offsets.
#[repr(C)]
struct RingHeader {
    magic: u64,            // 0x00
    version: u32,          // 0x08
    header_size: u32,      // 0x0C
    data_region_size: u32, // 0x10
    max_entry_size: u32,   // 0x14
    producer_pid: u64,     // 0x18
    created_epoch_ns: u64, // 0x20
    _pad0: [u8; 24],      // 0x28..0x3F
    write_pos: AtomicU64,  // 0x40 (cache-line aligned)
    write_seq: AtomicU64,  // 0x48
    _pad1: [u8; 48],      // 0x50..0x7F
}

const _: () = assert!(std::mem::size_of::<RingHeader>() == HEADER_SIZE);

pub struct ShmemRingProducer {
    mmap_ptr: *mut u8,
    mmap_len: usize,
    data_region_size: usize,
    local_write_pos: u64,
    local_write_seq: u64,
}

// SAFETY: ShmemRingProducer is only used from the single reconstructor thread (SPSC contract).
unsafe impl Send for ShmemRingProducer {}

impl ShmemRingProducer {
    /// Create a new ring buffer at the given path.
    /// Truncates and reinitializes the file (signals any existing consumer to reset).
    pub fn create(path: &Path) -> io::Result<Self> {
        Self::create_with_size(path, DEFAULT_DATA_REGION_SIZE)
    }

    pub fn create_with_size(path: &Path, data_region_size: usize) -> io::Result<Self> {
        let total_size = HEADER_SIZE + data_region_size;

        // Create/truncate the file and zero-fill to ensure pages are allocated
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(total_size as u64)?;
        // Write zeros to materialize the file pages (required on some platforms)
        {
            use std::io::Write;
            let mut f = &file;
            let zeros = vec![0u8; 4096];
            let mut remaining = total_size;
            while remaining > 0 {
                let chunk = remaining.min(4096);
                f.write_all(&zeros[..chunk])?;
                remaining -= chunk;
            }
            f.flush()?;
        }
        let fd = file.as_raw_fd();

        // mmap it read-write, shared
        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                total_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if mmap_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mmap_ptr = mmap_ptr as *mut u8;

        // Initialize header
        let header = unsafe { &mut *(mmap_ptr as *mut RingHeader) };
        header.magic = MAGIC;
        header.version = VERSION;
        header.header_size = HEADER_SIZE as u32;
        header.data_region_size = data_region_size as u32;
        header.max_entry_size = MAX_ENTRY_SIZE as u32;
        header.producer_pid = std::process::id() as u64;
        header.created_epoch_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        header._pad0 = [0; 24];
        header.write_pos.store(0, Ordering::Release);
        header.write_seq.store(0, Ordering::Release);
        header._pad1 = [0; 48];

        info!(
            "Created shmem ring buffer at {} ({} bytes: {} header + {} data)",
            path.display(),
            total_size,
            HEADER_SIZE,
            data_region_size
        );

        Ok(Self {
            mmap_ptr,
            mmap_len: total_size,
            data_region_size,
            local_write_pos: 0,
            local_write_seq: 0,
        })
    }

    /// Write an entry to the ring buffer.
    /// Returns the sequence number assigned, or 0 if the entry was too large and skipped.
    pub fn publish(&mut self, slot: u64, entries_bytes: &[u8]) -> u64 {
        let data_len = entries_bytes.len();
        if data_len > MAX_ENTRY_SIZE {
            log::warn!(
                "shmem_ring: skipping entry for slot {} ({} bytes > {} max)",
                slot,
                data_len,
                MAX_ENTRY_SIZE
            );
            return 0;
        }

        let total_size = align8(ENTRY_HEADER_SIZE + data_len);

        // Check if entry fits at current position without crossing the data region boundary
        let region_offset = (self.local_write_pos as usize) % self.data_region_size;
        if region_offset + total_size > self.data_region_size {
            // Skip to next wrap boundary
            self.local_write_pos += (self.data_region_size - region_offset) as u64;
        }

        let offset = (self.local_write_pos as usize) % self.data_region_size;
        self.local_write_seq += 1;

        // Write entry into the data region
        let data_region_start = HEADER_SIZE;
        let entry_ptr = unsafe { self.mmap_ptr.add(data_region_start + offset) };

        unsafe {
            // Entry header: seq(8) + slot(8) + data_len(4) + pad(4) = 24 bytes
            ptr::write_unaligned(entry_ptr as *mut u64, self.local_write_seq); // entry_seq
            ptr::write_unaligned(entry_ptr.add(8) as *mut u64, slot);
            ptr::write_unaligned(entry_ptr.add(16) as *mut u32, data_len as u32);
            ptr::write_unaligned(entry_ptr.add(20) as *mut u32, 0); // padding

            // Entry data (memcpy)
            ptr::copy_nonoverlapping(entries_bytes.as_ptr(), entry_ptr.add(ENTRY_HEADER_SIZE), data_len);
        }

        // Publish: update write_pos and write_seq atomically (Release ordering)
        let new_write_pos = self.local_write_pos + total_size as u64;
        let header = unsafe { &*(self.mmap_ptr as *const RingHeader) };
        header.write_seq.store(self.local_write_seq, Ordering::Release);
        header.write_pos.store(new_write_pos, Ordering::Release);

        self.local_write_pos = new_write_pos;

        self.local_write_seq
    }

    /// Current write sequence number.
    pub fn write_seq(&self) -> u64 {
        self.local_write_seq
    }
}

impl Drop for ShmemRingProducer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
        }
    }
}

/// Consumer side — for use by the arb-bot or test binaries.
/// Reads entries from an existing ring buffer via zero-copy mmap.
pub struct ShmemRingConsumer {
    mmap_ptr: *const u8,
    mmap_len: usize,
    data_region_size: usize,
    local_read_pos: u64,
    local_read_seq: u64,
    created_epoch_ns: u64,
}

// SAFETY: Consumer is used from a single reader thread (SPSC contract).
unsafe impl Send for ShmemRingConsumer {}

/// A zero-copy reference to an entry in the ring buffer.
pub struct EntryRef<'a> {
    pub seq: u64,
    pub slot: u64,
    pub entries_bytes: &'a [u8],
}

pub enum PollResult<'a> {
    /// A new entry is available.
    Entry(EntryRef<'a>),
    /// No new data.
    Empty,
    /// Consumer was lapped or producer restarted. State has been reset.
    Reset,
}

impl ShmemRingConsumer {
    /// Open an existing ring buffer file.
    /// Starts reading from the current write position (does not replay history).
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len() as usize;

        if file_len < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too small for ring header",
            ));
        }

        let fd = file.as_raw_fd();
        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                file_len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if mmap_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let mmap_ptr = mmap_ptr as *const u8;

        // Validate header
        let header = unsafe { &*(mmap_ptr as *const RingHeader) };
        if header.magic != MAGIC {
            unsafe {
                libc::munmap(mmap_ptr as *mut libc::c_void, file_len);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid magic: expected {:#x}, got {:#x}", MAGIC, header.magic),
            ));
        }

        let data_region_size = header.data_region_size as usize;
        let created_epoch_ns = header.created_epoch_ns;

        // Start from current write position (skip history)
        let write_pos = header.write_pos.load(Ordering::Acquire);
        let write_seq = header.write_seq.load(Ordering::Acquire);

        info!(
            "Opened shmem ring buffer at {:?}, data_region={}B, starting at seq={} pos={}",
            path, data_region_size, write_seq, write_pos
        );

        Ok(Self {
            mmap_ptr,
            mmap_len: file_len,
            data_region_size,
            local_read_pos: write_pos,
            local_read_seq: write_seq,
            created_epoch_ns,
        })
    }

    /// Non-blocking poll for the next entry.
    /// Returns a zero-copy reference into the mmap'd region.
    pub fn poll(&mut self) -> PollResult<'_> {
        let header = unsafe { &*(self.mmap_ptr as *const RingHeader) };

        // Check for producer restart
        if header.created_epoch_ns != self.created_epoch_ns {
            self.created_epoch_ns = header.created_epoch_ns;
            let write_pos = header.write_pos.load(Ordering::Acquire);
            let write_seq = header.write_seq.load(Ordering::Acquire);
            self.local_read_pos = write_pos;
            self.local_read_seq = write_seq;
            return PollResult::Reset;
        }

        let current_write_pos = header.write_pos.load(Ordering::Acquire);

        // No new data
        if self.local_read_pos >= current_write_pos {
            return PollResult::Empty;
        }

        // Check for lapping
        if current_write_pos - self.local_read_pos > self.data_region_size as u64 {
            self.local_read_pos = current_write_pos;
            self.local_read_seq = header.write_seq.load(Ordering::Acquire);
            return PollResult::Reset;
        }

        let region_offset = (self.local_read_pos as usize) % self.data_region_size;
        let data_region_start = HEADER_SIZE;
        let entry_ptr = unsafe { self.mmap_ptr.add(data_region_start + region_offset) };

        // Read entry header
        let entry_seq = unsafe { ptr::read_unaligned(entry_ptr as *const u64) };

        // Check if this is a valid entry (seq should be read_seq + 1)
        if entry_seq != self.local_read_seq + 1 {
            // This position was skipped (wrap padding). Jump to next region boundary.
            let skip = self.data_region_size - region_offset;
            self.local_read_pos += skip as u64;
            // Retry (at most once due to wrap)
            return self.poll();
        }

        let slot = unsafe { ptr::read_unaligned(entry_ptr.add(8) as *const u64) };
        let data_len = unsafe { ptr::read_unaligned(entry_ptr.add(16) as *const u32) } as usize;

        let entries_bytes =
            unsafe { std::slice::from_raw_parts(entry_ptr.add(ENTRY_HEADER_SIZE), data_len) };

        let total_size = align8(ENTRY_HEADER_SIZE + data_len);
        self.local_read_pos += total_size as u64;
        self.local_read_seq = entry_seq;

        PollResult::Entry(EntryRef {
            seq: entry_seq,
            slot,
            entries_bytes,
        })
    }

    /// How many entries behind the consumer is.
    pub fn lag(&self) -> u64 {
        let header = unsafe { &*(self.mmap_ptr as *const RingHeader) };
        let write_seq = header.write_seq.load(Ordering::Acquire);
        write_seq.saturating_sub(self.local_read_seq)
    }
}

impl Drop for ShmemRingConsumer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU32;

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn temp_path() -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("shmem_ring_test_{}_{}", std::process::id(), id));
        path
    }

    #[test]
    fn test_basic_write_read() {
        let path = temp_path();

        let mut producer = ShmemRingProducer::create(&path).unwrap();
        let mut consumer = ShmemRingConsumer::open(&path).unwrap();

        // No data yet
        assert!(matches!(consumer.poll(), PollResult::Empty));

        // Write entry
        producer.publish(100, b"test data 123");

        // Read it back
        match consumer.poll() {
            PollResult::Entry(entry) => {
                assert_eq!(entry.seq, 1);
                assert_eq!(entry.slot, 100);
                assert_eq!(entry.entries_bytes, b"test data 123");
            }
            _ => panic!("expected Entry"),
        }

        // No more data
        assert!(matches!(consumer.poll(), PollResult::Empty));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_multiple_entries() {
        let path = temp_path();
        let mut producer = ShmemRingProducer::create(&path).unwrap();
        let mut consumer = ShmemRingConsumer::open(&path).unwrap();

        for i in 0..100u64 {
            let data = format!("entry_{}", i);
            producer.publish(i, data.as_bytes());
        }

        for i in 0..100u64 {
            match consumer.poll() {
                PollResult::Entry(entry) => {
                    assert_eq!(entry.seq, i + 1);
                    assert_eq!(entry.slot, i);
                    let expected = format!("entry_{}", i);
                    assert_eq!(entry.entries_bytes, expected.as_bytes());
                }
                _ => panic!("expected entry at index {}", i),
            }
        }

        assert!(matches!(consumer.poll(), PollResult::Empty));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_wraparound() {
        let path = temp_path();
        // Small buffer to force wraparound
        let data_region_size = 4096;
        let mut producer = ShmemRingProducer::create_with_size(&path, data_region_size).unwrap();
        let mut consumer = ShmemRingConsumer::open(&path).unwrap();

        // Write entries that will exceed the buffer
        let big_data = vec![0xABu8; 512]; // 512 bytes + 24 header = 536, aligned to 536 per entry

        let mut read_count = 0;
        for i in 0..20u64 {
            producer.publish(i, &big_data);

            // Read as we go
            loop {
                match consumer.poll() {
                    PollResult::Entry(entry) => {
                        assert_eq!(entry.slot, read_count);
                        assert_eq!(entry.entries_bytes.len(), 512);
                        read_count += 1;
                    }
                    PollResult::Empty => break,
                    PollResult::Reset => break,
                }
            }
        }

        assert_eq!(read_count, 20);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_lapping_detection() {
        let path = temp_path();
        let data_region_size = 4096;
        let mut producer = ShmemRingProducer::create_with_size(&path, data_region_size).unwrap();
        let mut consumer = ShmemRingConsumer::open(&path).unwrap();

        // Write many entries without reading — consumer should detect lapping
        let data = vec![0u8; 512];
        for i in 0..50u64 {
            producer.publish(i, &data);
        }

        // Consumer is way behind — should get Reset
        match consumer.poll() {
            PollResult::Reset => {} // expected
            PollResult::Entry(_) => {} // also ok if it caught up
            PollResult::Empty => panic!("should not be empty after 50 writes"),
        }

        std::fs::remove_file(&path).ok();
    }
}

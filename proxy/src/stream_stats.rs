//! Window statistics for streaming entry emission (`--stream-entries`).
//!
//! The reconstruct thread hands one [`StreamEvent`] per early record (when its batch
//! completes) and one per completed streamed batch to this thread over a bounded channel
//! (drop on full, counted). Each window this thread logs one info line and, when the
//! benchmark InfluxDB settings are present, writes one `shredstream_bench-stream` point:
//! batch/entry/transaction counts split early vs at completion, and the gain
//! distribution. The gain of an early-published entry is its batch's completion time minus
//! the entry's publish time: how much sooner it reached the ring than whole-batch
//! emission would have delivered it. Percentiles are weighted per entry and per
//! transaction (reservoir-sampled).

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use log::info;
use rand::Rng;

use crate::{
    benchmark::{
        influx::{append_point, InfluxConfig, InfluxWriter},
        stats::quantiles,
    },
    deshred::StreamEvent,
};

const CHANNEL_CAPACITY: usize = 65_536;
const MAX_SAMPLES: usize = 16_384;
pub const MEASUREMENT: &str = "shredstream_bench-stream";

/// Cheap handle for the reconstruct thread: one non-blocking `try_send` per event.
#[derive(Clone)]
pub struct StreamStatsHandle {
    sender: Sender<StreamEvent>,
    dropped: Arc<AtomicU64>,
}

impl StreamStatsHandle {
    #[inline]
    pub fn record(&self, event: StreamEvent) {
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Start the stats thread. `influx` is the benchmark's InfluxDB config (used only if
/// complete); otherwise windows are only logged.
pub fn start(
    interval: Duration,
    influx: Option<InfluxConfig>,
    exit: Arc<AtomicBool>,
) -> (StreamStatsHandle, JoinHandle<()>) {
    let (sender, receiver) = crossbeam_channel::bounded(CHANNEL_CAPACITY);
    let dropped = Arc::new(AtomicU64::new(0));
    let thread_dropped = dropped.clone();
    let join = Builder::new()
        .name("ssStreamStats".to_string())
        .spawn(move || {
            let writer = influx.as_ref().and_then(InfluxWriter::new);
            run(receiver, interval, writer, thread_dropped, exit)
        })
        .unwrap();
    (StreamStatsHandle { sender, dropped }, join)
}

/// Reservoir sample of a weighted distribution (Algorithm R over weight units).
#[derive(Default)]
struct Weighted {
    n: u64,
    max: u64,
    samples: Vec<u64>,
}

impl Weighted {
    fn push(&mut self, value: u64, weight: u32, rng: &mut impl Rng) {
        self.max = self.max.max(value);
        for _ in 0..weight {
            self.n += 1;
            if self.samples.len() < MAX_SAMPLES {
                self.samples.push(value);
            } else {
                let j = rng.gen_range(0..self.n);
                if (j as usize) < MAX_SAMPLES {
                    self.samples[j as usize] = value;
                }
            }
        }
    }

    /// p50, p90, p99 in microseconds.
    fn quantiles_us(&self) -> [i64; 3] {
        let mut s: Vec<i64> = self.samples.iter().map(|v| *v as i64).collect();
        let q = quantiles(&mut s, &[0.5, 0.9, 0.99]);
        [q[0] / 1000, q[1] / 1000, q[2] / 1000]
    }
}

#[derive(Default)]
pub(crate) struct Window {
    batches: u64,
    split_batches: u64,
    records_early: u64,
    entries_early: u64,
    txs_early: u64,
    entries_completion: u64,
    txs_completion: u64,
    gain_per_entry: Weighted,
    gain_per_tx: Weighted,
}

impl Window {
    pub(crate) fn add(&mut self, event: StreamEvent, rng: &mut impl Rng) {
        match event {
            StreamEvent::Early {
                gain_ns,
                entries,
                transactions,
            } => {
                self.records_early += 1;
                self.entries_early += u64::from(entries);
                self.txs_early += u64::from(transactions);
                self.gain_per_entry.push(gain_ns, entries, rng);
                self.gain_per_tx.push(gain_ns, transactions, rng);
            }
            StreamEvent::Completed {
                split,
                entries,
                transactions,
            } => {
                self.batches += 1;
                self.split_batches += u64::from(split);
                self.entries_completion += u64::from(entries);
                self.txs_completion += u64::from(transactions);
            }
        }
    }

    fn pct(part: u64, whole: u64) -> f64 {
        if whole == 0 {
            0.0
        } else {
            100.0 * part as f64 / whole as f64
        }
    }

    pub(crate) fn summary(&self, dropped: u64) -> String {
        let e = self.gain_per_entry.quantiles_us();
        let t = self.gain_per_tx.quantiles_us();
        format!(
            "stream emission: {} batches ({:.1}% split), early records {}, entries early {} ({:.1}%), \
             txs early {} ({:.1}%); gain per early entry p50/p90/p99/max {}/{}/{}/{} us, \
             per early tx p50/p90/p99 {}/{}/{} us; events dropped {}",
            self.batches,
            Self::pct(self.split_batches, self.batches),
            self.records_early,
            self.entries_early,
            Self::pct(self.entries_early, self.entries_early + self.entries_completion),
            self.txs_early,
            Self::pct(self.txs_early, self.txs_early + self.txs_completion),
            e[0],
            e[1],
            e[2],
            self.gain_per_entry.max / 1000,
            t[0],
            t[1],
            t[2],
            dropped,
        )
    }

    fn line(&self, dropped: u64, ts_ns: u128) -> String {
        let e = self.gain_per_entry.quantiles_us();
        let t = self.gain_per_tx.quantiles_us();
        let mut buf = String::new();
        append_point(
            &mut buf,
            MEASUREMENT,
            &[("leader", "ALL")],
            &[
                ("batches", self.batches as i64),
                ("split_batches", self.split_batches as i64),
                ("records_early", self.records_early as i64),
                ("entries_early", self.entries_early as i64),
                ("txs_early", self.txs_early as i64),
                ("entries_completion", self.entries_completion as i64),
                ("txs_completion", self.txs_completion as i64),
                ("gain_entry_n", self.gain_per_entry.n as i64),
                ("gain_entry_p50_us", e[0]),
                ("gain_entry_p90_us", e[1]),
                ("gain_entry_p99_us", e[2]),
                ("gain_entry_max_us", (self.gain_per_entry.max / 1000) as i64),
                ("gain_tx_n", self.gain_per_tx.n as i64),
                ("gain_tx_p50_us", t[0]),
                ("gain_tx_p90_us", t[1]),
                ("gain_tx_p99_us", t[2]),
                ("events_dropped", dropped as i64),
            ],
            ts_ns,
        );
        buf
    }
}

fn run(
    receiver: Receiver<StreamEvent>,
    interval: Duration,
    writer: Option<InfluxWriter>,
    dropped: Arc<AtomicU64>,
    exit: Arc<AtomicBool>,
) {
    let mut rng = rand::thread_rng();
    let mut window = Window::default();
    let mut window_start = Instant::now();
    while !exit.load(Ordering::Relaxed) {
        match receiver.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => {
                window.add(event, &mut rng);
                for event in receiver.try_iter().take(CHANNEL_CAPACITY) {
                    window.add(event, &mut rng);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if window_start.elapsed() >= interval {
            let dropped_now = dropped.swap(0, Ordering::Relaxed);
            if window.batches > 0 || dropped_now > 0 {
                info!("{}", window.summary(dropped_now));
                if let Some(w) = writer.as_ref() {
                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0);
                    w.write(&window.line(dropped_now, ts));
                }
            }
            window = Window::default();
            window_start = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_weights_gain_per_entry_and_per_tx() {
        let mut rng = rand::thread_rng();
        let mut w = Window::default();
        w.add(
            StreamEvent::Early {
                gain_ns: 100_000,
                entries: 3,
                transactions: 1,
            },
            &mut rng,
        );
        w.add(
            StreamEvent::Early {
                gain_ns: 900_000,
                entries: 1,
                transactions: 9,
            },
            &mut rng,
        );
        w.add(
            StreamEvent::Completed {
                split: true,
                entries: 4,
                transactions: 10,
            },
            &mut rng,
        );
        w.add(
            StreamEvent::Completed {
                split: false,
                entries: 5,
                transactions: 20,
            },
            &mut rng,
        );
        assert_eq!(w.gain_per_entry.n, 4);
        assert_eq!(w.gain_per_tx.n, 10);
        // 3 of 4 entries gained 100 us; 9 of 10 txs gained 900 us.
        assert_eq!(w.gain_per_entry.quantiles_us()[0], 100);
        assert_eq!(w.gain_per_tx.quantiles_us()[0], 900);
        let line = w.line(0, 1);
        assert!(
            line.starts_with("shredstream_bench-stream,leader=ALL batches=2i,split_batches=1i,")
        );
        assert!(line
            .contains("entries_early=4i,txs_early=10i,entries_completion=9i,txs_completion=30i"));
        assert!(w.summary(0).contains("2 batches (50.0% split)"));
    }
}

//! On-demand, provider-independent races. Everything here runs on ssBenchAgg.
//! The receive tap sees only an atomic epoch; it never reads files, resolves
//! sources, acquires a lock, or waits for this controller.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use log::{info, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};

use super::{
    influx::{append_point, InfluxWriter},
    leader::LeaderScheduleHandle,
    parse::{Observation, ShredId},
    sources::{self, DOUBLEZERO_SENTINEL},
    stats::quantiles,
    BenchmarkConfig,
};

const SAMPLE_CAP: usize = 16_384;
const MATCH_CAP: usize = 500_000;
const CONTROL_LIMIT: u64 = 65_536;
const MAX_DELTA_NS: i64 = 10_000_000_000;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn default_duration() -> u64 {
    86_400
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogicalSource {
    Jito,
    Doublezero,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    pub name: String,
    #[serde(default)]
    pub ips: Vec<IpAddr>,
    #[serde(default)]
    pub logical: Option<LogicalSource>,
}

impl SourceSpec {
    fn matches(&self, ip: IpAddr) -> bool {
        match self.logical {
            Some(LogicalSource::Jito) => sources::is_jito(ip),
            Some(LogicalSource::Doublezero) => ip == DOUBLEZERO_SENTINEL,
            None => self.ips.contains(&ip),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if !safe_name(&self.name) {
            return Err(
                "source name must be 1..80 ASCII letters/digits, '.', '_', '-', or ':'".into(),
            );
        }
        if self.logical.is_some() != self.ips.is_empty() || self.ips.len() > 64 {
            return Err("source needs either one logical identity or 1..64 IPs".into());
        }
        if self.ips.contains(&DOUBLEZERO_SENTINEL) {
            return Err(
                "use logical=doublezero for multicast, not the internal sentinel IP".into(),
            );
        }
        Ok(())
    }
}

fn safe_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 80
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-:".contains(&b))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionSpec {
    pub id: String,
    pub baseline: SourceSpec,
    pub candidate: SourceSpec,
    #[serde(default = "default_duration")]
    pub max_duration_secs: u64,
    #[serde(default)]
    pub csv_max_rows: u64,
}

impl SessionSpec {
    fn validate(&self) -> Result<(), String> {
        if !safe_name(&self.id) || self.id == "." || self.id == ".." || self.id.contains(':') {
            return Err("session ID must be a safe, unique filename (letters/digits/._-)".into());
        }
        self.baseline.validate()?;
        self.candidate.validate()?;
        if self.baseline.name == self.candidate.name
            || self
                .baseline
                .ips
                .iter()
                .any(|ip| self.candidate.matches(*ip))
            || self
                .candidate
                .ips
                .iter()
                .any(|ip| self.baseline.matches(*ip))
            || (self.baseline.logical.is_some() && self.baseline.logical == self.candidate.logical)
        {
            return Err(
                "baseline and candidate must have distinct names and non-overlapping identities"
                    .into(),
            );
        }
        if !(1..=604_800).contains(&self.max_duration_secs) || self.csv_max_rows > 10_000_000 {
            return Err(
                "duration must be 1..604800 seconds; csv_max_rows must be <=10000000".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase", deny_unknown_fields)]
enum Command {
    Start { version: u32, session: SessionSpec },
    Stop { version: u32, session_id: String },
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct Counts {
    pub both_delivered: u64,
    pub contested: u64,
    pub candidate_faster: u64,
    pub baseline_faster: u64,
    pub ties: u64,
    pub baseline_only: u64,
    pub candidate_only: u64,
    pub clock_anomalies: u64,
    #[serde(skip)]
    delta_sum_ns: i128,
    #[serde(skip)]
    time_saved_sum_ns: i128,
}

impl Counts {
    fn observe(&mut self, arrival: &Arrival) -> Option<i64> {
        match (arrival.times[0], arrival.times[1]) {
            (Some(a), Some(b)) => {
                self.both_delivered += 1;
                let d = b as i128 - a as i128;
                if d.abs() > MAX_DELTA_NS as i128 {
                    self.clock_anomalies += 1;
                    return None;
                }
                let d = d as i64;
                self.contested += 1;
                self.delta_sum_ns += d as i128;
                self.time_saved_sum_ns += (-(d as i128)).max(0);
                match d.cmp(&0) {
                    std::cmp::Ordering::Less => self.candidate_faster += 1,
                    std::cmp::Ordering::Greater => self.baseline_faster += 1,
                    std::cmp::Ordering::Equal => self.ties += 1,
                }
                Some(d)
            }
            (Some(_), None) => {
                self.baseline_only += 1;
                None
            }
            (None, Some(_)) => {
                self.candidate_only += 1;
                None
            }
            (None, None) => None,
        }
    }
}

#[derive(Default)]
struct Distribution {
    counts: Counts,
    samples: Vec<i64>,
}

impl Distribution {
    fn observe(&mut self, arrival: &Arrival, rng: &mut impl Rng) {
        if let Some(d) = self.counts.observe(arrival) {
            // Uniform reservoir over the WHOLE window, independent of exact
            // counts and sums. Unlike the legacy pair's first-N sample cap.
            if self.samples.len() < SAMPLE_CAP {
                self.samples.push(d);
            } else {
                let j = rng.gen_range(0..self.counts.contested);
                if j < SAMPLE_CAP as u64 {
                    self.samples[j as usize] = d;
                }
            }
        }
    }
}

#[derive(Default, Clone)]
struct Arrival {
    times: [Option<i64>; 2],
}

impl Arrival {
    fn observe(&mut self, side: usize, ts: i64) {
        self.times[side] = Some(self.times[side].map_or(ts, |old| old.min(ts)));
    }
}

#[derive(Debug, Serialize)]
struct Row {
    session: String,
    baseline: String,
    candidate: String,
    leader: String,
    shred_type: String,
    timestamp_ns: i64,
    #[serde(flatten)]
    counts: Counts,
    delta_sum_us: i64,
    time_saved_sum_us: i64,
    sample_count: usize,
    delta_p50_us: Option<i64>,
    delta_p90_us: Option<i64>,
    delta_p99_us: Option<i64>,
}

impl Row {
    fn new(spec: &SessionSpec, leader: String, is_data: bool, mut dist: Distribution) -> Self {
        let q = quantiles(&mut dist.samples, &[0.5, 0.9, 0.99]);
        let present = !dist.samples.is_empty();
        Self {
            session: spec.id.clone(),
            baseline: spec.baseline.name.clone(),
            candidate: spec.candidate.name.clone(),
            leader,
            shred_type: if is_data { "data" } else { "code" }.into(),
            timestamp_ns: unix_ns(),
            delta_sum_us: to_us(dist.counts.delta_sum_ns),
            time_saved_sum_us: to_us(dist.counts.time_saved_sum_ns),
            sample_count: dist.samples.len(),
            delta_p50_us: present.then_some(q[0] / 1000),
            delta_p90_us: present.then_some(q[1] / 1000),
            delta_p99_us: present.then_some(q[2] / 1000),
            counts: dist.counts,
        }
    }

    fn append_influx(&self, buf: &mut String) {
        let c = &self.counts;
        let mut fields = vec![
            ("both_delivered", c.both_delivered as i64),
            ("contested", c.contested as i64),
            ("candidate_faster", c.candidate_faster as i64),
            ("baseline_faster", c.baseline_faster as i64),
            ("ties", c.ties as i64),
            ("baseline_only", c.baseline_only as i64),
            ("candidate_only", c.candidate_only as i64),
            ("clock_anomalies", c.clock_anomalies as i64),
            ("delta_sum_us", self.delta_sum_us),
            ("time_saved_sum_us", self.time_saved_sum_us),
            ("sample_count", self.sample_count as i64),
        ];
        if let Some(v) = self.delta_p50_us {
            fields.push(("delta_p50_us", v));
        }
        if let Some(v) = self.delta_p90_us {
            fields.push(("delta_p90_us", v));
        }
        if let Some(v) = self.delta_p99_us {
            fields.push(("delta_p99_us", v));
        }
        append_point(
            buf,
            "shredstream_bench-session-pair",
            &[
                ("session", &self.session),
                ("baseline", &self.baseline),
                ("candidate", &self.candidate),
                ("leader", &self.leader),
                ("shred_type", &self.shred_type),
            ],
            &fields,
            self.timestamp_ns as u128,
        );
    }
}

fn to_us(ns: i128) -> i64 {
    (ns / 1000).clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Pure matching state. No production packet is filtered or modified here.
struct Race {
    spec: SessionSpec,
    epoch: u64,
    map: ahash::HashMap<ShredId, Arrival>,
    windows: HashMap<(String, bool), Distribution>,
    totals: Counts,
    last_seen_ns: [Option<i64>; 2],
    max_slot: u64,
    start_slot: Option<u64>,
    stop_slot: Option<u64>,
    finalized_before: u64,
    late_observations: u64,
    capacity_drops: u64,
    observation_drops: u64,
    ts_missing: u64,
}

impl Race {
    fn new(spec: SessionSpec, epoch: u64) -> Self {
        Self {
            spec,
            epoch,
            map: Default::default(),
            windows: Default::default(),
            totals: Default::default(),
            last_seen_ns: [None, None],
            max_slot: 0,
            start_slot: None,
            stop_slot: None,
            finalized_before: 0,
            late_observations: 0,
            capacity_drops: 0,
            observation_drops: 0,
            ts_missing: 0,
        }
    }

    /// Returns the selected side only for an observation inside the trial.
    fn observe(&mut self, epoch: u64, obs: &Observation) -> Option<usize> {
        if epoch != self.epoch || obs.rx_ts_ns <= 0 {
            return None;
        }
        let side = if self.spec.baseline.matches(obs.source) {
            0
        } else if self.spec.candidate.matches(obs.source) {
            1
        } else {
            return None;
        };
        self.last_seen_ns[side] = Some(self.last_seen_ns[side].unwrap_or(0).max(obs.rx_ts_ns));
        self.max_slot = self.max_slot.max(obs.slot);
        // Arm only after both providers have appeared. Exclude the current
        // partial slot; no "missing" samples from before B was subscribed.
        let start = match self.start_slot {
            Some(s) => s,
            None => {
                if self.last_seen_ns.iter().all(Option::is_some) {
                    self.start_slot = Some(self.max_slot.saturating_add(1));
                }
                return None;
            }
        };
        if obs.slot < start || self.stop_slot.is_some_and(|s| obs.slot >= s) {
            return None;
        }
        if obs.slot < self.finalized_before {
            self.late_observations += 1;
            return None;
        }
        let id = obs.shred_id();
        if self.map.len() >= MATCH_CAP && !self.map.contains_key(&id) {
            self.capacity_drops += 1;
            return None;
        }
        self.map.entry(id).or_default().observe(side, obs.rx_ts_ns);
        Some(side)
    }

    fn stop(&mut self) {
        self.stop_slot.get_or_insert(self.max_slot);
        // The last observed slot may have been only partially measured.
        let stop = self.stop_slot.unwrap();
        self.map.retain(|id, _| id.slot < stop);
    }

    fn sweep(&mut self, horizon: u64, finish: bool, leaders: Option<&LeaderScheduleHandle>) {
        let threshold = if finish {
            self.stop_slot.unwrap_or(self.max_slot)
        } else {
            self.max_slot.saturating_sub(horizon)
        };
        self.finalized_before = self.finalized_before.max(threshold);
        let mut rng = rand::thread_rng();
        self.map.retain(|id, arrival| {
            if id.slot >= threshold {
                return true;
            }
            let leader = leaders
                .and_then(|l| l.leader_for_slot(id.slot))
                .map(|p| p.to_string())
                .unwrap_or_else(|| "unknown".into());
            self.windows
                .entry((leader, id.is_data))
                .or_default()
                .observe(arrival, &mut rng);
            self.windows
                .entry(("ALL".into(), id.is_data))
                .or_default()
                .observe(arrival, &mut rng);
            if id.is_data {
                self.totals.observe(arrival);
            }
            false
        });
    }
}

struct Recording {
    race: Race,
    dir: PathBuf,
    rows: BufWriter<File>,
    csv: Option<BufWriter<File>>,
    csv_rows: u64,
    output_error: Option<String>,
    started_ns: i64,
    started: Instant,
    draining: Option<Instant>,
    last_flush: Instant,
}

impl Recording {
    fn create(root: &Path, spec: SessionSpec, epoch: u64, cfg: &Settings) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let dir = root.join(&spec.id);
        // Never resume or overwrite a previous session, including after a proxy
        // restart with an old "start" command still in the control file.
        fs::create_dir(&dir)?;
        let started_ns = unix_ns();
        write_json(
            &dir.join("manifest.json"),
            &serde_json::json!({
                "version": 1, "session": spec, "started_at_ns": started_ns,
                "kernel_timestamps": cfg.kernel_timestamps, "data_only": cfg.data_only,
                "window_slots": cfg.window_slots, "delta_definition": "candidate_rx - baseline_rx",
            }),
        )?;
        let rows = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join("windows.jsonl"))?,
        );
        let csv = if spec.csv_max_rows > 0 {
            let mut w = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(dir.join("observations.csv"))?,
            );
            writeln!(w, "rx_ts_ns,source,provider,slot,fec_set_index,index,type")?;
            Some(w)
        } else {
            None
        };
        Ok(Self {
            race: Race::new(spec, epoch),
            dir,
            rows,
            csv,
            csv_rows: 0,
            output_error: None,
            started_ns,
            started: Instant::now(),
            draining: None,
            last_flush: Instant::now(),
        })
    }

    fn stop(&mut self) {
        self.race.stop();
        self.draining.get_or_insert_with(Instant::now);
    }

    fn observe(&mut self, epoch: u64, obs: &Observation) {
        let Some(side) = self.race.observe(epoch, obs) else {
            return;
        };
        if self.csv_rows >= self.race.spec.csv_max_rows {
            return;
        }
        if let Some(w) = self.csv.as_mut() {
            let name = if side == 0 {
                &self.race.spec.baseline.name
            } else {
                &self.race.spec.candidate.name
            };
            match writeln!(
                w,
                "{},{},{},{},{},{},{}",
                obs.rx_ts_ns,
                obs.source,
                name,
                obs.slot,
                obs.fec_set_index,
                obs.index,
                if obs.is_data { "data" } else { "code" }
            ) {
                Ok(()) => self.csv_rows += 1,
                Err(e) => {
                    self.output_error = Some(format!("CSV write failed: {e}"));
                    self.csv = None;
                }
            }
        }
    }

    fn flush(&mut self, influx: Option<&InfluxWriter>) {
        let mut lp = String::new();
        for ((leader, data), dist) in self.race.windows.drain() {
            let row = Row::new(&self.race.spec, leader, data, dist);
            let result = serde_json::to_writer(&mut self.rows, &row)
                .map_err(io::Error::other)
                .and_then(|_| self.rows.write_all(b"\n"));
            if let Err(e) = result {
                self.output_error = Some(format!("window write failed: {e}"));
            }
            row.append_influx(&mut lp);
        }
        if let Err(e) = self.rows.flush() {
            self.output_error = Some(format!("window flush failed: {e}"));
        }
        if let Some(w) = self.csv.as_mut() {
            if let Err(e) = w.flush() {
                self.output_error = Some(format!("CSV flush failed: {e}"));
            }
        }
        if let Some(w) = influx {
            w.write(&lp);
        }
        self.last_flush = Instant::now();
    }

    fn status(&self, state: &str) -> serde_json::Value {
        serde_json::json!({
            "session_id": self.race.spec.id, "state": state,
            "baseline": self.race.spec.baseline, "candidate": self.race.spec.candidate,
            "started_at_ns": self.started_ns, "elapsed_secs": self.started.elapsed().as_secs(),
            "baseline_last_seen_ns": self.race.last_seen_ns[0], "candidate_last_seen_ns": self.race.last_seen_ns[1],
            "start_slot": self.race.start_slot, "stop_slot_exclusive": self.race.stop_slot,
            "pending_shreds": self.race.map.len(), "finalized_data": self.race.totals,
            "delta_sum_us": to_us(self.race.totals.delta_sum_ns),
            "time_saved_sum_us": to_us(self.race.totals.time_saved_sum_ns),
            "observation_drops": self.race.observation_drops, "ts_missing": self.race.ts_missing,
            "capacity_drops": self.race.capacity_drops, "late_observations": self.race.late_observations,
            "csv_rows": self.csv_rows, "csv_max_rows": self.race.spec.csv_max_rows,
            "output_error": self.output_error, "session_dir": self.dir,
        })
    }
}

struct Settings {
    window_slots: u64,
    flush_interval: Duration,
    kernel_timestamps: bool,
    data_only: bool,
}

pub struct Controller {
    path: PathBuf,
    status_path: PathBuf,
    root: PathBuf,
    gate: Arc<AtomicU64>,
    next_epoch: u64,
    settings: Settings,
    recording: Option<Recording>,
    last_command: Option<Vec<u8>>,
    last_poll: Option<Instant>,
    last_result: serde_json::Value,
    error: Option<String>,
    health_cursor: [u64; 2],
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    s.into()
}

impl Controller {
    pub fn new(path: PathBuf, gate: Arc<AtomicU64>, cfg: &BenchmarkConfig) -> Self {
        Self {
            status_path: suffixed(&path, ".status.json"),
            root: suffixed(&path, ".sessions"),
            path,
            gate,
            next_epoch: 0,
            settings: Settings {
                window_slots: cfg.window_slots,
                flush_interval: cfg.flush_interval,
                kernel_timestamps: cfg.kernel_timestamps,
                data_only: cfg.data_only,
            },
            recording: None,
            last_command: None,
            last_poll: None,
            error: None,
            last_result: serde_json::json!({"state": "idle"}),
            health_cursor: [0, 0],
        }
    }

    pub fn health(&mut self, dropped: &AtomicU64, missing: &AtomicU64) {
        let now = [
            dropped.load(Ordering::Relaxed),
            missing.load(Ordering::Relaxed),
        ];
        self.record_health(now);
    }

    fn record_health(&mut self, now: [u64; 2]) {
        if let Some(r) = self.recording.as_mut() {
            r.race.observation_drops += now[0].saturating_sub(self.health_cursor[0]);
            r.race.ts_missing += now[1].saturating_sub(self.health_cursor[1]);
        }
        self.health_cursor = now;
    }

    /// Use the actual swapped values, not a pre-flush snapshot: drops can
    /// accumulate while a blocking pipeline/Influx write occupies ssBenchAgg.
    pub fn flushed_health(&mut self, dropped: u64, missing: u64) {
        self.record_health([dropped, missing]);
        self.health_cursor = [0, 0];
    }

    pub fn observe(&mut self, epoch: u64, obs: &Observation) {
        if let Some(r) = self.recording.as_mut() {
            r.observe(epoch, obs);
        }
    }

    pub fn accepts_epoch(&self, epoch: u64) -> bool {
        self.recording
            .as_ref()
            .is_some_and(|r| r.race.epoch == epoch)
    }

    fn apply(&mut self, command: Command) -> Result<(), String> {
        match command {
            Command::Start { version, session } => {
                if version != 1 {
                    return Err("unsupported control version".into());
                }
                session.validate()?;
                if let Some(r) = &self.recording {
                    return if r.race.spec == session && r.draining.is_none() {
                        Ok(())
                    } else {
                        Err("stop the current session and wait for completion before starting another".into())
                    };
                }
                self.next_epoch = self.next_epoch.checked_add(1).ok_or("epoch exhausted")?;
                let r = Recording::create(&self.root, session, self.next_epoch, &self.settings)
                    .map_err(|e| {
                        format!("cannot create fresh session (IDs cannot be reused): {e}")
                    })?;
                info!("benchmark session {} started", r.race.spec.id);
                self.recording = Some(r);
                self.gate.store(self.next_epoch, Ordering::Relaxed);
                Ok(())
            }
            Command::Stop {
                version,
                session_id,
            } => {
                if version != 1 {
                    return Err("unsupported control version".into());
                }
                match self.recording.as_mut() {
                    Some(r) if r.race.spec.id == session_id => {
                        r.stop();
                        Ok(())
                    }
                    None if self.last_result["session_id"] == session_id => Ok(()),
                    _ => Err("stop session_id does not match an active session".into()),
                }
            }
        }
    }

    pub fn tick(&mut self, leaders: Option<&LeaderScheduleHandle>, influx: Option<&InfluxWriter>) {
        if self.last_poll.is_some_and(|t| t.elapsed() < POLL_INTERVAL) {
            return;
        }
        self.last_poll = Some(Instant::now());
        let mut bytes = Vec::new();
        let read =
            File::open(&self.path).and_then(|f| f.take(CONTROL_LIMIT + 1).read_to_end(&mut bytes));
        match read {
            Ok(_) if self.last_command.as_ref() != Some(&bytes) => {
                let result = if bytes.len() as u64 > CONTROL_LIMIT {
                    Err("control file exceeds 64KiB".into())
                } else {
                    serde_json::from_slice(&bytes)
                        .map_err(|e| e.to_string())
                        .and_then(|c| self.apply(c))
                };
                self.error = result.err();
                self.last_command = Some(bytes);
                if let Some(e) = &self.error {
                    warn!("benchmark control rejected: {e}");
                }
            }
            Err(e) if e.kind() != io::ErrorKind::NotFound || self.recording.is_some() => {
                self.error = Some(format!("control read failed; keeping current session: {e}"));
            }
            _ => {}
        }
        let mut finish = false;
        if let Some(r) = self.recording.as_mut() {
            if r.started.elapsed().as_secs() >= r.race.spec.max_duration_secs {
                r.stop();
            }
            r.race.sweep(self.settings.window_slots, false, leaders);
            if r.last_flush.elapsed() >= self.settings.flush_interval {
                r.flush(influx);
            }
            // A wall-clock deadline also finishes if every feed disappears. The
            // grace is nominal slot horizon + 1s; coverage is always windowed.
            if let Some(t) = r.draining {
                let grace = Duration::from_millis(
                    self.settings
                        .window_slots
                        .saturating_mul(400)
                        .saturating_add(1000),
                );
                finish = r.race.start_slot.is_none()
                    || r.race
                        .max_slot
                        .saturating_sub(r.race.stop_slot.unwrap_or(0))
                        > self.settings.window_slots
                    || t.elapsed() >= grace;
            }
        }
        if finish {
            self.finish("complete", leaders, influx);
        }
        self.publish_status();
    }

    fn finish(
        &mut self,
        state: &str,
        leaders: Option<&LeaderScheduleHandle>,
        influx: Option<&InfluxWriter>,
    ) {
        self.gate.store(0, Ordering::Relaxed);
        if let Some(mut r) = self.recording.take() {
            r.race.sweep(self.settings.window_slots, true, leaders);
            r.flush(influx);
            self.last_result = r.status(state);
            self.last_result["ended_at_ns"] = unix_ns().into();
            if let Err(e) = write_json(&r.dir.join("status.json"), &self.last_result) {
                self.error = Some(format!("session status write failed: {e}"));
            }
            info!("benchmark session {} {state}", r.race.spec.id);
        }
    }

    fn publish_status(&mut self) {
        let mut status = match &self.recording {
            Some(r) => r.status(if r.draining.is_some() {
                "draining"
            } else if r.race.start_slot.is_none() {
                "waiting_for_sources"
            } else {
                "recording"
            }),
            None => self.last_result.clone(),
        };
        status["version"] = 1.into();
        status["updated_at_ns"] = unix_ns().into();
        status["pid"] = std::process::id().into();
        status["control_error"] = self.error.clone().into();
        if let Some(r) = &self.recording {
            if let Err(e) = write_json(&r.dir.join("status.json"), &status) {
                self.error = Some(format!("session status write failed: {e}"));
            }
        }
        if let Err(e) = write_json(&self.status_path, &status) {
            warn!("benchmark status write failed: {e}");
        }
    }

    pub fn shutdown(
        &mut self,
        leaders: Option<&LeaderScheduleHandle>,
        influx: Option<&InfluxWriter>,
    ) {
        if let Some(r) = self.recording.as_mut() {
            r.stop();
        }
        self.finish("interrupted", leaders, influx);
        self.publish_status();
    }
}

fn write_json(path: &Path, value: &serde_json::Value) -> io::Result<()> {
    // Same-directory rename keeps readers from seeing partial JSON. No fsync on
    // a production host: this is telemetry, not a durability barrier.
    let temp = suffixed(
        path,
        &format!(".{}-{}.tmp", std::process::id(), rand::random::<u64>()),
    );
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let result = (|| {
        serde_json::to_writer_pretty(&mut f, value).map_err(io::Error::other)?;
        f.write_all(b"\n")?;
        fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str) -> SessionSpec {
        SessionSpec {
            id: id.into(),
            baseline: SourceSpec {
                name: "owned".into(),
                ips: vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()],
                logical: None,
            },
            candidate: SourceSpec {
                name: "trial".into(),
                ips: vec!["10.0.0.3".parse().unwrap()],
                logical: None,
            },
            max_duration_secs: 60,
            csv_max_rows: 0,
        }
    }

    fn obs(ip: &str, slot: u64, index: u32, ts: i64) -> Observation {
        Observation {
            source: ip.parse().unwrap(),
            slot,
            index,
            fec_set_index: 0,
            is_data: true,
            rx_ts_ns: ts,
        }
    }

    fn armed() -> Race {
        let mut r = Race::new(spec("test"), 7);
        r.observe(7, &obs("10.0.0.1", 100, 0, 1_000_000));
        r.observe(7, &obs("10.0.0.3", 100, 0, 1_100_000));
        assert_eq!(r.start_slot, Some(101));
        assert!(r.map.is_empty());
        r
    }

    #[test]
    fn ip_group_minimum_signed_delta_and_additive_benefit() {
        let mut r = armed();
        r.observe(7, &obs("10.0.0.1", 101, 0, 1_000_000));
        r.observe(7, &obs("10.0.0.2", 101, 0, 800_000));
        r.observe(7, &obs("10.0.0.3", 101, 0, 900_000)); // candidate loses by 100us
        r.observe(7, &obs("10.0.0.1", 101, 1, 1_000_000));
        r.observe(7, &obs("10.0.0.3", 101, 1, 500_000)); // candidate saves 500us
        r.observe(7, &obs("10.0.0.1", 101, 2, 2_000_000)); // baseline-only
        r.observe(7, &obs("10.0.0.3", 101, 3, 2_000_000)); // candidate-only
        r.max_slot = 103;
        r.stop();
        r.sweep(64, true, None);
        assert_eq!(r.totals.contested, 2);
        assert_eq!(r.totals.candidate_faster, 1);
        assert_eq!(r.totals.baseline_faster, 1);
        assert_eq!(r.totals.baseline_only, 1);
        assert_eq!(r.totals.candidate_only, 1);
        assert_eq!(to_us(r.totals.delta_sum_ns), -400);
        assert_eq!(to_us(r.totals.time_saved_sum_ns), 500);
    }

    #[test]
    fn epoch_warmup_stop_and_late_copy_boundaries() {
        let mut r = Race::new(spec("boundaries"), 7);
        r.observe(6, &obs("10.0.0.1", 100, 0, 1));
        r.observe(7, &obs("10.0.0.9", 999, 0, 1));
        assert_eq!(r.max_slot, 0);
        assert_eq!(r.start_slot, None);
        r = armed();
        r.observe(7, &obs("10.0.0.1", 100, 0, 1)); // excluded partial first slot
        r.observe(7, &obs("10.0.0.1", 101, 0, 1000));
        r.observe(7, &obs("10.0.0.1", 102, 0, 1000)); // excluded partial last slot
        r.stop();
        r.observe(7, &obs("10.0.0.3", 101, 0, 2000)); // late match during drain
        r.observe(7, &obs("10.0.0.3", 102, 0, 2000));
        r.sweep(64, true, None);
        assert_eq!(r.totals.contested, 1);
        assert_eq!(r.totals.baseline_only, 0);
        r.observe(7, &obs("10.0.0.3", 101, 0, 1500)); // cannot reopen finalized key
        assert_eq!(r.late_observations, 1);
        assert!(r.map.is_empty());
    }

    #[test]
    fn unavailable_latency_is_null_and_clock_anomaly_is_counted() {
        let mut d = Distribution::default();
        d.observe(
            &Arrival {
                times: [Some(1), None],
            },
            &mut rand::thread_rng(),
        );
        d.observe(
            &Arrival {
                times: [Some(1), Some(MAX_DELTA_NS + 2)],
            },
            &mut rand::thread_rng(),
        );
        let row = Row::new(&spec("empty"), "ALL".into(), true, d);
        assert_eq!(row.counts.both_delivered, 1);
        assert_eq!(row.counts.contested, 0);
        assert_eq!(row.counts.clock_anomalies, 1);
        assert_eq!(row.delta_p50_us, None);
        let mut lp = String::new();
        row.append_influx(&mut lp);
        assert!(!lp.contains("delta_p50_us="));
        assert!(lp.contains("baseline_only=1i"));
    }

    #[test]
    fn exact_counts_survive_reservoir_cap() {
        let mut d = Distribution::default();
        let mut rng = rand::thread_rng();
        for _ in 0..(SAMPLE_CAP + 100) {
            d.observe(
                &Arrival {
                    times: [Some(2000), Some(1000)],
                },
                &mut rng,
            );
        }
        assert_eq!(d.samples.len(), SAMPLE_CAP);
        assert_eq!(d.counts.contested, SAMPLE_CAP as u64 + 100);
        assert_eq!(to_us(d.counts.time_saved_sum_ns), SAMPLE_CAP as i64 + 100);
    }

    #[test]
    fn rejects_ambiguous_sources_and_paths() {
        let mut s = spec("ok");
        assert!(s.validate().is_ok());
        s.id = "../escape".into();
        assert!(s.validate().is_err());
        s.id = "ok".into();
        s.candidate.ips = s.baseline.ips.clone();
        assert!(s.validate().is_err());
        s.candidate = SourceSpec {
            name: "jito".into(),
            ips: vec![],
            logical: Some(LogicalSource::Jito),
        };
        s.baseline.ips = vec!["64.130.40.21".parse().unwrap()];
        assert!(s.validate().is_err());
        s.baseline = SourceSpec {
            name: "dz".into(),
            ips: vec![],
            logical: Some(LogicalSource::Doublezero),
        };
        assert!(s.validate().is_ok());
        assert!(s.baseline.matches(DOUBLEZERO_SENTINEL));
    }

    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "ss-bench-session-test-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            fs::create_dir(&p).unwrap();
            Self(p)
        }
        fn controller(&self) -> Controller {
            let path = self.0.join("control.json");
            Controller {
                status_path: suffixed(&path, ".status.json"),
                root: suffixed(&path, ".sessions"),
                path,
                gate: Arc::new(AtomicU64::new(0)),
                next_epoch: 0,
                settings: Settings {
                    window_slots: 4,
                    flush_interval: Duration::from_secs(60),
                    kernel_timestamps: true,
                    data_only: true,
                },
                recording: None,
                last_command: None,
                last_poll: None,
                last_result: serde_json::json!({"state":"idle"}),
                error: None,
                health_cursor: [0, 0],
            }
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn control_rejection_does_not_mutate_active_session_and_ids_are_durable() {
        let temp = Scratch::new();
        let mut c = temp.controller();
        c.tick(None, None);
        assert_eq!(c.gate.load(Ordering::Relaxed), 0);
        c.apply(Command::Start {
            version: 1,
            session: spec("first"),
        })
        .unwrap();
        let epoch = c.gate.load(Ordering::Relaxed);
        assert!(epoch > 0);
        fs::write(&c.path, "{broken").unwrap();
        c.last_poll = None;
        c.tick(None, None);
        assert!(c.error.is_some());
        assert_eq!(c.gate.load(Ordering::Relaxed), epoch);
        assert!(c
            .apply(Command::Start {
                version: 1,
                session: spec("second")
            })
            .is_err());
        assert!(c
            .apply(Command::Stop {
                version: 1,
                session_id: "wrong".into()
            })
            .is_err());
        c.apply(Command::Stop {
            version: 1,
            session_id: "first".into(),
        })
        .unwrap();
        c.finish("complete", None, None);
        assert_eq!(c.gate.load(Ordering::Relaxed), 0);
        assert!(c
            .apply(Command::Start {
                version: 1,
                session: spec("first")
            })
            .is_err());
        c.apply(Command::Start {
            version: 1,
            session: spec("second"),
        })
        .unwrap();
        c.observe(epoch, &obs("10.0.0.1", 100, 0, 1000));
        assert_eq!(
            c.recording.as_ref().unwrap().race.last_seen_ns,
            [None, None]
        );
        c.shutdown(None, None);
    }

    #[test]
    fn bounded_csv_and_local_report_without_influx() {
        let temp = Scratch::new();
        let mut c = temp.controller();
        let mut s = spec("local");
        s.csv_max_rows = 1;
        c.apply(Command::Start {
            version: 1,
            session: s,
        })
        .unwrap();
        let epoch = c.gate.load(Ordering::Relaxed);
        for (slot, ip, ts) in [
            (100, "10.0.0.1", 1000),
            (100, "10.0.0.3", 1000),
            (101, "10.0.0.1", 2000),
            (101, "10.0.0.3", 1000),
            (102, "10.0.0.1", 2000),
        ] {
            c.observe(epoch, &obs(ip, slot, 0, ts));
        }
        c.apply(Command::Stop {
            version: 1,
            session_id: "local".into(),
        })
        .unwrap();
        c.finish("complete", None, None);
        let dir = c.root.join("local");
        assert_eq!(
            fs::read_to_string(dir.join("observations.csv"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        let rows = fs::read_to_string(dir.join("windows.jsonl")).unwrap();
        let all = rows
            .lines()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
            .find(|v| v["leader"] == "ALL")
            .unwrap();
        assert_eq!(all["contested"], 1);
        assert_eq!(all["delta_sum_us"], -1);
        assert_eq!(c.last_result["state"], "complete");
    }

    #[test]
    fn health_includes_drops_between_poll_and_flush_swap() {
        let temp = Scratch::new();
        let mut c = temp.controller();
        let dropped = AtomicU64::new(5);
        let missing = AtomicU64::new(1);
        c.health(&dropped, &missing); // idle losses are not part of the next session
        c.apply(Command::Start {
            version: 1,
            session: spec("health"),
        })
        .unwrap();
        dropped.store(7, Ordering::Relaxed);
        c.health(&dropped, &missing);
        c.flushed_health(10, 2); // more losses during an aggregator write
        dropped.store(3, Ordering::Relaxed);
        missing.store(1, Ordering::Relaxed);
        c.health(&dropped, &missing);
        let r = &c.recording.as_ref().unwrap().race;
        assert_eq!(r.observation_drops, 8);
        assert_eq!(r.ts_missing, 2);
    }

    #[test]
    fn runtime_control_tap_and_aggregator_end_to_end() {
        use solana_perf::packet::Packet;
        use std::sync::atomic::AtomicBool;

        let temp = Scratch::new();
        let control = temp.0.join("runtime.json");
        let status_path = suffixed(&control, ".status.json");
        let cfg = BenchmarkConfig {
            enabled: true,
            kernel_timestamps: true,
            data_only: true,
            csv_path: None,
            control_path: Some(control.clone()),
            rpc_url: None,
            validator_map_path: None,
            node_country: String::new(),
            region_max_rtt_us: 0,
            window_slots: 4,
            flush_interval: Duration::from_secs(1),
            channel_capacity: 1024,
            min_samples: 4,
            aggregator_core_id: None,
            influx: None,
            emit_source_pair: false,
            pipeline_latency: false,
        };
        let exit = Arc::new(AtomicBool::new(false));
        let rt = crate::benchmark::start(cfg, exit.clone()).unwrap();
        // Guarantee the test's worker exits even if an assertion panics.
        struct StopOnDrop(Arc<AtomicBool>);
        impl Drop for StopOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }
        let guard = StopOnDrop(exit);
        let wait_for = |state: &str| {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let status = fs::read(&status_path)
                    .ok()
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
                if status.as_ref().is_some_and(|s| s["state"] == state) {
                    return status.unwrap();
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for {state}: {status:?}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        wait_for("idle");
        write_json(
            &control,
            &serde_json::json!({"action":"start", "version":1, "session":spec("runtime")}),
        )
        .unwrap();
        wait_for("waiting_for_sources");
        let send = |ip: &str, slot: u64, ts: i64| {
            let mut p = Packet::default();
            p.meta_mut().size = 83;
            p.meta_mut().addr = ip.parse().unwrap();
            p.buffer_mut()[64] = 0xa5;
            p.buffer_mut()[65..73].copy_from_slice(&slot.to_le_bytes());
            rt.handle.observe_packets(&[p], &[ts]);
        };
        let now = unix_ns();
        for slot in 100..=102 {
            send("10.0.0.1", slot, now + 2000);
            send("10.0.0.3", slot, now + 1000);
        }
        wait_for("recording");
        write_json(
            &control,
            &serde_json::json!({"action":"stop", "version":1, "session_id":"runtime"}),
        )
        .unwrap();
        let status = wait_for("complete");
        assert_eq!(status["finalized_data"]["contested"], 1);
        assert_eq!(status["delta_sum_us"], -1);
        assert_eq!(status["time_saved_sum_us"], 1);
        assert_eq!(status["start_slot"], 101);
        assert_eq!(status["stop_slot_exclusive"], 102);

        // Simulate a monthly gap and an old producer finally queueing a batch.
        // It must not seed the no-RPC frontier and reject all fresh slots as a
        // MAX_SLOT_JUMP, nor contribute an arrival to the new trial.
        write_json(
            &control,
            &serde_json::json!({"action":"start", "version":1, "session":spec("next-month")}),
        )
        .unwrap();
        wait_for("waiting_for_sources");
        rt.handle
            .sender
            .try_send(crate::benchmark::ObservationBatch {
                epoch: 1,
                observations: vec![obs("10.0.0.1", 100, 0, now)],
            })
            .unwrap();
        for slot in 100_000..=100_002 {
            send("10.0.0.1", slot, now + 2000);
            send("10.0.0.3", slot, now + 1000);
        }
        wait_for("recording");
        write_json(
            &control,
            &serde_json::json!({"action":"stop", "version":1, "session_id":"next-month"}),
        )
        .unwrap();
        let next = wait_for("complete");
        assert_eq!(next["start_slot"], 100_001);
        assert_eq!(next["finalized_data"]["contested"], 1);
        drop(guard);
        for t in rt.join_handles {
            t.join().unwrap();
        }
    }
}

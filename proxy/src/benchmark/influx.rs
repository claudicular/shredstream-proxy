//! Minimal direct InfluxDB v2 line-protocol writer for the benchmark.
//!
//! Avoids solana-metrics entirely: we POST only the benchmark's own points to
//! the v2 write API (`/api/v2/write?org=&bucket=&precision=ns`, token auth) — the
//! same API the arb bot's influx already uses. Line protocol is identical on
//! influx v1/v2/v3, so this is forward-compatible.

use std::time::Duration;

use log::{info, warn};

/// Connection config for the benchmark's own influx writes.
#[derive(Clone)]
pub struct InfluxConfig {
    pub url: String, // base, e.g. http://127.0.0.1:8086
    pub org: String,
    pub bucket: String,
    pub token: String,
}

// Manual Debug so the token is never logged (BenchmarkConfig is printed at startup).
impl std::fmt::Debug for InfluxConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfluxConfig")
            .field("url", &self.url)
            .field("org", &self.org)
            .field("bucket", &self.bucket)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl InfluxConfig {
    /// All four fields must be non-empty to be usable.
    pub fn is_complete(&self) -> bool {
        !self.url.is_empty()
            && !self.org.is_empty()
            && !self.bucket.is_empty()
            && !self.token.is_empty()
    }
}

pub struct InfluxWriter {
    client: reqwest::blocking::Client,
    write_url: String,
    token: String,
}

impl InfluxWriter {
    pub fn new(cfg: &InfluxConfig) -> Option<Self> {
        if !cfg.is_complete() {
            warn!("benchmark influx config incomplete (need url/org/bucket/token); influx writes disabled");
            return None;
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| warn!("benchmark influx client build failed: {e}"))
            .ok()?;
        let base = cfg.url.trim_end_matches('/');
        let write_url = format!(
            "{base}/api/v2/write?org={}&bucket={}&precision=ns",
            encode(&cfg.org),
            encode(&cfg.bucket),
        );
        info!("benchmark influx writer -> {base}/api/v2/write (bucket={})", cfg.bucket);
        Some(Self {
            client,
            write_url,
            token: cfg.token.clone(),
        })
    }

    /// POST a line-protocol body. Logs and drops on error; never blocks the
    /// aggregator beyond the 5s client timeout.
    pub fn write(&self, body: &str) {
        if body.is_empty() {
            return;
        }
        let res = self
            .client
            .post(&self.write_url)
            .header("Authorization", format!("Token {}", self.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body.to_string())
            .send();
        match res {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().unwrap_or_default();
                warn!("benchmark influx write rejected: {status} {text}");
            }
            Err(e) => warn!("benchmark influx write error: {e}"),
        }
    }
}

/// Minimal percent-encoding for org/bucket query params (covers spaces and the
/// handful of reserved chars likely to appear).
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Escape an influx line-protocol tag value (comma, space, equals).
fn esc_tag(s: &str) -> String {
    if s.bytes().any(|b| matches!(b, b',' | b' ' | b'=')) {
        s.replace('\\', "\\\\")
            .replace(',', "\\,")
            .replace(' ', "\\ ")
            .replace('=', "\\=")
    } else {
        s.to_string()
    }
}

/// Append one line-protocol point to `buf`. All fields are i64 (written with the
/// `i` integer suffix). `ts_ns` is nanoseconds since the unix epoch.
pub fn append_point(
    buf: &mut String,
    measurement: &str,
    tags: &[(&str, &str)],
    fields: &[(&str, i64)],
    ts_ns: u128,
) {
    use std::fmt::Write;
    buf.push_str(measurement);
    for (k, v) in tags {
        let _ = write!(buf, ",{k}={}", esc_tag(v));
    }
    let mut first = true;
    for (k, v) in fields {
        let _ = write!(buf, "{}{k}={v}i", if first { ' ' } else { ',' });
        first = false;
    }
    let _ = writeln!(buf, " {ts_ns}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_protocol_format() {
        let mut buf = String::new();
        append_point(
            &mut buf,
            "shredstream_bench-vs-jito",
            &[("leader", "Fudp7uPD"), ("source", "1.2.3.4"), ("in_region", "true")],
            &[("beats", 410), ("contested", 640), ("delta_sum_us", -1200)],
            1782440000000000000u128,
        );
        assert_eq!(
            buf,
            "shredstream_bench-vs-jito,leader=Fudp7uPD,source=1.2.3.4,in_region=true beats=410i,contested=640i,delta_sum_us=-1200i 1782440000000000000\n"
        );
    }

    #[test]
    fn tag_escaping() {
        assert_eq!(esc_tag("DE"), "DE");
        assert_eq!(esc_tag("a b,c=d"), "a\\ b\\,c\\=d");
    }

    #[test]
    fn incomplete_config_disabled() {
        let cfg = InfluxConfig { url: "http://x:8086".into(), org: "o".into(), bucket: "".into(), token: "t".into() };
        assert!(!cfg.is_complete());
        assert!(InfluxWriter::new(&cfg).is_none());
    }
}

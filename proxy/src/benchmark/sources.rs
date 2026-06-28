//! Source identity for the benchmark.
//!
//! Jito's block-engine forwards shreds from a known, fixed set of server IPs
//! (one cluster per region). We hardcode those here so a source can be
//! recognized as **jito** purely by its packet source IP — jito is the latency
//! BASELINE every custom source is measured against. All jito PoPs collapse into
//! a single logical `SourceId::Jito` (so "jito's arrival time" for a shred is its
//! best PoP, the min across these IPs); every other source stays an anonymous IP
//! (MVP: no per-source naming yet).
//!
//! IP list sourced from the validator firewall allowlist (setup-validator.sh).
//! If jito changes/extends its egress IPs this list must be updated (recompile).

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::OnceLock,
};

/// (ip, jito region/city) for every known jito shred-source IP.
const JITO_SHRED_SOURCES: &[(&str, &str)] = &[
    // amsterdam
    ("74.118.140.240", "amsterdam"),
    ("202.8.8.174", "amsterdam"),
    ("64.130.42.228", "amsterdam"),
    ("64.130.43.92", "amsterdam"),
    ("64.130.55.26", "amsterdam"),
    ("64.130.42.227", "amsterdam"),
    ("64.130.43.19", "amsterdam"),
    ("64.130.55.28", "amsterdam"),
    // frankfurt
    ("64.130.50.14", "frankfurt"),
    ("198.13.137.137", "frankfurt"),
    ("64.130.40.25", "frankfurt"),
    ("64.130.47.93", "frankfurt"),
    ("64.130.57.46", "frankfurt"),
    ("64.130.57.99", "frankfurt"),
    ("64.130.57.171", "frankfurt"),
    ("64.130.40.23", "frankfurt"),
    ("64.130.40.22", "frankfurt"),
    ("64.130.40.21", "frankfurt"),
    ("64.130.40.26", "frankfurt"),
    ("64.130.40.24", "frankfurt"),
    // london
    ("142.91.127.175", "london"),
    ("88.211.250.116", "london"),
    ("88.211.250.140", "london"),
    ("88.211.250.172", "london"),
    ("88.211.250.108", "london"),
    ("88.211.250.76", "london"),
    ("88.211.251.36", "london"),
    // ny
    ("141.98.216.96", "ny"),
    ("64.130.48.56", "ny"),
    ("64.130.34.186", "ny"),
    ("64.130.34.143", "ny"),
    ("64.130.34.142", "ny"),
    ("64.130.34.189", "ny"),
    ("64.130.34.190", "ny"),
    ("64.130.34.141", "ny"),
    // slc
    ("64.130.53.8", "slc"),
    ("64.130.53.57", "slc"),
    ("64.130.53.81", "slc"),
    ("64.130.53.90", "slc"),
    ("64.130.53.82", "slc"),
    ("64.130.53.88", "slc"),
    ("64.130.33.181", "slc"),
    ("64.130.33.88", "slc"),
];

struct JitoTables {
    set: HashSet<IpAddr>,
    region: HashMap<IpAddr, &'static str>,
}

fn tables() -> &'static JitoTables {
    static T: OnceLock<JitoTables> = OnceLock::new();
    T.get_or_init(|| {
        let mut set = HashSet::new();
        let mut region = HashMap::new();
        for (ip, city) in JITO_SHRED_SOURCES {
            match ip.parse::<IpAddr>() {
                Ok(addr) => {
                    set.insert(addr);
                    region.insert(addr, *city);
                }
                Err(_) => debug_assert!(false, "invalid hardcoded jito IP: {ip}"),
            }
        }
        JitoTables { set, region }
    })
}

/// Number of distinct hardcoded jito source IPs (for startup logging).
pub fn jito_ip_count() -> usize {
    tables().set.len()
}

/// Is this source IP one of jito's known shred-source IPs?
pub fn is_jito(ip: IpAddr) -> bool {
    tables().set.contains(&ip)
}

/// The jito region/city for a known jito IP, else `None`.
pub fn jito_region(ip: IpAddr) -> Option<&'static str> {
    tables().region.get(&ip).copied()
}

/// Logical source identity used for matching/stats. All jito PoPs collapse into
/// `Jito` (the baseline); everything else is its raw IP.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SourceId {
    Jito,
    Ip(IpAddr),
}

impl SourceId {
    #[inline]
    pub fn is_jito(&self) -> bool {
        matches!(self, SourceId::Jito)
    }

    /// Tag/label form: "jito" or the dotted IP.
    pub fn label(&self) -> String {
        match self {
            SourceId::Jito => "jito".to_string(),
            SourceId::Ip(ip) => ip.to_string(),
        }
    }
}

/// Classify a packet's source IP into a `SourceId`.
#[inline]
pub fn classify(ip: IpAddr) -> SourceId {
    if is_jito(ip) {
        SourceId::Jito
    } else {
        SourceId::Ip(ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn known_jito_ips_classified() {
        // a few from different regions
        for ip in [
            "64.130.40.21", // frankfurt
            "74.118.140.240", // amsterdam
            "88.211.250.76", // london
            "141.98.216.96", // ny
            "64.130.33.88", // slc
        ] {
            let a: IpAddr = ip.parse().unwrap();
            assert!(is_jito(a), "{ip} should be jito");
            assert_eq!(classify(a), SourceId::Jito);
            assert!(jito_region(a).is_some());
        }
    }

    #[test]
    fn non_jito_ip_is_ip() {
        let a = IpAddr::V4(Ipv4Addr::new(45, 140, 1, 1)); // blockrazor-ish, not in list
        assert!(!is_jito(a));
        assert_eq!(classify(a), SourceId::Ip(a));
        assert_eq!(classify(a).label(), "45.140.1.1");
    }

    #[test]
    fn dedup_and_count() {
        // 64.130.50.14 appears twice in the source list; the set dedups it.
        assert!(jito_ip_count() >= 40 && jito_ip_count() <= 43);
        assert_eq!(SourceId::Jito.label(), "jito");
    }
}

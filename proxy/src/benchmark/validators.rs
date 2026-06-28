//! Loads `measured_validators_map.json` and joins a leader pubkey to its
//! geo/region and measured RTT. The map is keyed by base58 validator identity
//! pubkey; values carry `{ ip, rtt(microseconds), geo_info{...} }`.
//!
//! The region/relevance predicate mirrors arb_bot's `shredstream_thread`
//! (`location_relevant_validators::fetch`): a leader is "in region" iff its
//! measured RTT is below `region_max_rtt_us`, or (when no RTT was measured) its
//! geo country equals `node_country`.

use std::{collections::HashMap, path::Path};

use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GeoInfo {
    #[serde(default)]
    pub country: Option<String>,
    #[serde(rename = "countryCode", default)]
    pub country_code: Option<String>,
    #[serde(rename = "regionName", default)]
    pub region_name: Option<String>,
    #[serde(default)]
    pub city: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Validator {
    #[serde(default)]
    pub ip: Option<String>,
    /// Measured RTT in MICROSECONDS (matches arb_bot's units), or `None`.
    #[serde(default)]
    pub rtt: Option<u128>,
    #[serde(default)]
    pub geo_info: Option<GeoInfo>,
}

pub const UNKNOWN_REGION: &str = "unknown";

#[derive(Debug, Default)]
pub struct ValidatorMap {
    map: HashMap<Pubkey, Validator>,
}

impl ValidatorMap {
    /// Load and parse the JSON map. Entries whose key is not a valid base58
    /// pubkey are skipped.
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let data = std::fs::read_to_string(path)?;
        let string_map: HashMap<String, Validator> = serde_json::from_str(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let map = string_map
            .into_iter()
            .filter_map(|(k, v)| k.parse::<Pubkey>().ok().map(|pk| (pk, v)))
            .collect();
        Ok(Self { map })
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, leader: &Pubkey) -> Option<&Validator> {
        self.map.get(leader)
    }

    /// Region label for a leader: the geo country code, or `"unknown"` when the
    /// leader is absent from the map or has no geo data.
    pub fn region_label(&self, leader: &Pubkey) -> String {
        self.map
            .get(leader)
            .and_then(|v| v.geo_info.as_ref())
            .and_then(|g| g.country_code.clone())
            .unwrap_or_else(|| UNKNOWN_REGION.to_string())
    }

    /// Whether the leader is "in region" per the arb_bot proximity predicate.
    pub fn is_in_region(&self, leader: &Pubkey, node_country: &str, region_max_rtt_us: u128) -> bool {
        match self.map.get(leader) {
            None => false,
            Some(v) => match v.rtt {
                Some(rtt) => rtt < region_max_rtt_us,
                // Accept either the full country name ("Germany") or the code
                // ("DE") so --node-country works with either form.
                None => v
                    .geo_info
                    .as_ref()
                    .map(|g| {
                        g.country.as_deref() == Some(node_country)
                            || g.country_code.as_deref() == Some(node_country)
                    })
                    .unwrap_or(false),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_region() {
        let json = r#"{
            "2t53LvZfskcpXkdwLaBnfZLbNgyVHPu2BNFpcRBaEBhM": {
                "pubkey": [27,240,224,110,194,52,11,214,232,6,121,56,94,43,12,154,132,0,236,235,82,184,179,116,51,117,30,33,12,199,220,100],
                "ip": "45.139.132.99:8001",
                "rtt": 139718,
                "geo_info": {"status":"success","country":"Germany","countryCode":"DE","regionName":"Hesse","city":"Fechenheim","lat":50.1,"lon":8.7,"query":"45.139.132.99","message":null}
            },
            "close": {
                "ip": "1.2.3.4:8001",
                "rtt": 1200,
                "geo_info": {"country":"Germany","countryCode":"DE"}
            },
            "nortt_de": {
                "ip": "1.2.3.5:8001",
                "rtt": null,
                "geo_info": {"country":"Germany","countryCode":"DE"}
            }
        }"#;
        let string_map: HashMap<String, Validator> = serde_json::from_str(json).unwrap();
        let map: HashMap<Pubkey, Validator> = string_map
            .into_iter()
            .filter_map(|(k, v)| k.parse::<Pubkey>().ok().map(|pk| (pk, v)))
            .collect();
        let vm = ValidatorMap { map };
        // only the base58 key parses; "close"/"nortt_de" are dropped
        assert_eq!(vm.len(), 1);

        let pk: Pubkey = "2t53LvZfskcpXkdwLaBnfZLbNgyVHPu2BNFpcRBaEBhM"
            .parse()
            .unwrap();
        assert_eq!(vm.region_label(&pk), "DE");
        // rtt 139718us > 5000us, and country==Germany only matters when rtt is None
        assert!(!vm.is_in_region(&pk, "Germany", 5000));
        // a higher threshold makes it in-region
        assert!(vm.is_in_region(&pk, "Germany", 200_000));

        let missing = Pubkey::new_unique();
        assert_eq!(vm.region_label(&missing), "unknown");
        assert!(!vm.is_in_region(&missing, "Germany", 5000));
    }
}

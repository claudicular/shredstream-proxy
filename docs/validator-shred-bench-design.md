# Validator-Granularity Shred-Source Benchmark — Design

**Branch:** `validator-benching` (based on `proven-fixes`, the production branch).
**Status:** designed, not yet built. Build order is the phased plan in §11.
**Author:** research workflow + synthesis, 2026-06-25.

---

## 1. Goal

Measure how shred **sources** (jito block-engine, blockrazor, others) compare **at validator
granularity**, so we can learn which leader validators each source has a latency edge on (their
"backroom" peering). Concretely, answer:

1. Compared to jito, how does a given custom source perform **for a specific leader validator**?
2. Compared to other custom sources, how does a source perform **for a specific leader validator**?

Mechanism: point all sources at one UDP port; identify the source by the packet's **source IP**;
read the slot from the raw shred header; map slot → leader validator via the leader schedule; bucket
per-source arrival timing by leader.

---

## 2. Verdict on the original mental model

**Confirmed (by 3 independent agents):**
- A Solana shred's common header is a fixed 83-byte prefix; **slot is at bytes 65..73 (u64 LE)**,
  readable from the raw UDP payload with **zero reconstruction**. (`solana_ledger::shred::layout`,
  already a dep, already used at `proxy/src/deshred.rs:549`.)
- Source IP is captured per-packet on every branch at `packet.meta().addr`; production already keys
  metrics by it (`forwarder.rs:254`, `metrics.packets_received: DashMap<IpAddr,(u64,u64)>`).
- "All sources to one UDP port, identify by source IP" is exactly shred-stats' `--is-filter-addr`
  mode and is the right approach.

**Refuted / corrected:**
- **"Carry source identity forward until the slot is known"** — unnecessary. Slot and source IP are
  on the *same* raw packet at the *same* instant; attribute immediately at ingest.
- **"Dedup lets you match the same shred from multiple sources"** — only half-true. The global bloom
  `Deduper` (`main.rs:297`, `Arc<RwLock<>>`, keyed on full payload bytes) only flips a `discard`
  **boolean** on the 2nd byte-identical copy; it never records which source won or the delta. On the
  `custom` branch dedup runs **before** reconstruction, so `Packet::data()` returns `None` and the
  duplicate is *dropped from reconstruction*. → We must tap **pre-dedup** and build our own
  `ShredId → per-source first-arrival` map.
- **Downstream (shmem/gRPC) is a dead end.** Entries there are post-FEC, post-dedup aggregates of
  many shreds carrying slot only (`custom` adds two pipeline timestamps, still no source/fec/index).
  Per-shred source attribution is impossible there — it must live at UDP receive.
- **Today's kernel timestamp is per-batch, not per-packet** (`recv_timestamp.rs` reads the cmsg only
  for packet `i==0`) and is software not hardware. `proven-fixes` has *no* kernel RX timestamp at all.

---

## 3. Architecture decision: inline flag-gated tap (NOT a sidecar)

A standalone sidecar binding the same `host:port` with `SO_REUSEPORT` would **steal** a fraction of
production's datagrams (the kernel load-balances across the reuseport group; it does not broadcast).
Dual-sending requires reconfiguring sources we don't control. A passive `AF_PACKET`/`tc`/XDP sniffer
avoids theft but rebuilds the whole ingest layer.

**Decision:** a new module `proxy/src/benchmark.rs` + a `--benchmark` runtime flag, compiled into the
same binary, **off by default**, read-only w.r.t. `Packet`, feeding a dedicated aggregator thread over
its own bounded channel. Port `custom:proxy/src/recv_timestamp.rs` (SO_TIMESTAMPNS) — it is absent on
`proven-fixes`/`validator-benching` and is the benchmark clock.

---

## 4. Non-blocking guarantee for the prod reconstruct → shmem/gRPC path

The reconstruct path runs on its own thread (`shred_reconstructor`, `forwarder.rs:83-132`), fed a
clone of the batch at the fork; it is the thread that calls `ring.publish()` then `entry_sender.send()`.
It cannot be blocked by the tap **iff** the tap shares no lock, channel, or core with it. Rules:

1. **Reconstruct hand-off first.** In the forward thread (userspace path), order is:
   `reconstruct_tx.try_send(packet_batch.clone())` → `bench.observe_batch(&packet_batch, …)` → dedup →
   RPC fan-out (`forwarder.rs`). Nothing the benchmark does precedes the reconstructor getting its batch.
2. **Minimal work on the hot path.** The tap parses ~5 header bytes per packet into 24-byte
   `Observation`s and pushes them on a bounded channel. This is *intentionally* cheaper than sharing raw
   batches: sending parsed 24-byte observations beats copying/sharing 1.2KB×64 packet batches. All heavy
   work — cross-source matching, leader/region resolution, stats, CSV/influx I/O — happens on the
   aggregator thread. (Original "Arc-clone-only" idea was abandoned because the existing reconstruct path
   deep-clones the batch anyway and parsing tiny observations is the lower-memory-traffic option.)
3. **One small bounded allocation per batch.** `observe_batch`/`observe_packets` allocate a single
   `Vec<Observation>` (≤ ~1.5KB) per batch — negligible vs the per-packet firehose. No `PacketBatch`
   deep-copy. The aggregator is the place that must stay allocation-light (see rule 5).
4. **Dedicated bounded channel, drop-on-full.** Never piggyback on `reconstruct_tx` (it is
   `bounded(1_024)` and already `try_send`-drops under load). On overflow, increment the
   **dropped-observations** counter (counts samples, not batches); the hot path never blocks. Default
   channel capacity 8192 batches (~25MB ceiling), configurable.
5. **Pin the aggregator to an isolated core** via `--benchmark-aggregator-core-id` (Linux
   `sched_setaffinity`, mirroring how `custom` pins the reconstructor). Keep it off the
   listen/forward/reconstruct cores, else it steals cycles and injects tail-latency jitter (we already
   see 12–32 ms spikes). The aggregator caps per-series sample vectors (`MAX_SAMPLES_PER_SERIES`) and
   bounds the drain loop so it can't fight the reconstructor on the allocator or starve eviction.

**Kernel-timestamp (M4) mode caveat:** when `--benchmark-kernel-timestamps` is on, the listen thread is
replaced by the timestamped recv path, which does the parse+push in the listen thread (upstream of the
reconstruct fork). This is an opt-in, benchmark-only perturbation (~1µs/batch) and emits a startup
warning. The default (userspace) path keeps the tap after the reconstruct hand-off.

**Residual cost that is NOT zero (measure on M1):** per-packet kernel-timestamp extraction must happen
in the listen thread (the cmsg buffer is reused by the next `recvmmsg`), and the listen thread is
upstream of the reconstruct fork. Cost ≈ 64 × a few ns ≈ sub-µs per batch. Adopting `recv_timestamp.rs`
on `proven-fixes` (which uses the stock `streamer::receiver` with no timestamping) is itself the one
structural change to the production recv path — keep it behind the flag.

---

## 5. Ingest & source identity

- **Tap location (target):** the per-packet recvmmsg loop in ported `recv_timestamp.rs:191-201` —
  the only point holding, per packet, `addrs[i]` (source SocketAddr), the payload, and the cmsg.
  Change: extract `extract_timestamp(&hdrs[i].msg_hdr)` for **every** `i` (not just `i==0`).
- **Fallback tap (M1, no recv-path change):** top of `recv_from_channel_and_send_multiple_dest`
  (`forwarder.rs:218`), using `packet.meta().addr` + one `Instant::now()` per batch.
- **Source identity (MVP):** the source **IP** is the source key directly — no name mapping for MVP
  (user maintains IP→name later). Unknown IPs still bucket by IP.

---

## 6. Slot / leader attribution

Parse from raw payload, no reconstruction:
```rust
use solana_ledger::shred::layout;
let slot  = layout::get_slot(p)?;            // bytes 65..73 u64 LE
let index = layout::get_index(p)?;           // bytes 73..77 u32 LE
let stype = layout::get_shred_type(p).ok()?; // from variant byte 64
let fec   = u32::from_le_bytes(p[79..83].try_into().ok()?); // no helper; read directly
```
- **DATA vs CODING** from variant byte `b = p[64]`: data iff `b==0xA5` or `(b&0xF0) ∈ {0x80,0x90,0xb0}`;
  coding iff `b==0x5A` or `(b&0xF0) ∈ {0x40,0x60,0x70}`. `ShredType::from(variant)` does this.
  **Headline metric = DATA shreds only** (leader-signed, byte-identical across sources, every source
  must deliver them). Coding tracked only as a diagnostic.
- **Slot → leader pubkey:** rolling `slot → Pubkey` window via `RpcClient::get_slot_leaders(base, 5000)`
  (FRA RPC, see §9), refreshed ~1000 slots before exhaustion (arb_bot refreshes every 4000 of 5000,
  `leader_slot_cache.rs:30-80`). **Keep the `Vec<Pubkey>`** — arb_bot collapses it to a yes/no bitset
  and discards the identity; we need the identity. Absolute slots are continuous → no epoch-boundary
  special-casing.

---

## 7. Leader → region (matches `shredstream_thread`)

Load `data/measured_validators_map.json` into `HashMap<Pubkey, Validator>`
(`{ pubkey:[u8;32], ip, rtt:Option<u128 µs>, geo_info:{country,countryCode,regionName,city,lat,lon} }`).
Join is direct identity-pubkey → identity-pubkey.

**Region/relevance label = exactly what arb_bot's `is_slot_relevant` uses**
(`location_relevant_validators.rs:25-32`): a leader is **in-region** iff
`rtt.is_some() && rtt < REGION_MAX_RTT_US` **OR** `rtt.is_none() && geo.country == NODE_COUNTRY`.
Config: `NODE_COUNTRY="Germany"`, `REGION_MAX_RTT_US=5000` (5 ms in µs). Leaders missing from the map →
`unknown` bucket (optional `get_cluster_nodes` fallback later).

---

## 8. Cross-source matching & comparison engine

- **Match key:** `ShredId = (slot, fec_set_index, index, shred_type)`. (shred-stats' bare `slot:index`
  aliases data vs coding shreds; `deshred.rs:651-667` proves the fuller key is collision-free.)
- **Windowed map** `ShredId → { leader: Option<Pubkey>, first_ts_ns: [Option<i64>; N_SRC] }`, keep MIN
  per source (geyserbench `TransactionAccumulator` earliest-wins; shred-stats min-per-category).
- **Finalize by slot-age eviction:** keep `current_max_slot`; finalize+emit any `ShredId` with
  `slot < current_max_slot - SLOT_HORIZON` (≈64 slots ≈ 25 s, past Turbine spread + retransmits).
- **Per event:** `winner = argmin(first_ts)`; per-source lead time `= ts[src] - ts[winner]` (≥0);
  per-pair signed delta `= ts[b] - ts[a]`.
- **Do not disable the production deduper** — we tap pre-dedup and read the payload directly so we see
  every source's copy; forwarding/RPC dedup behavior is unchanged.

---

## 9. Configuration (`.env`, gitignored)

```
RPC_URL="http://64.130.41.179:8899"           # FRA node; getSlotLeaders tested OK 2026-06-25 (core 4.0.3)
# RPC_URL="https://mainnet.helius-rpc.com/?api-key=<key from arb_bot/.env SEND_RPC_URL>"  # fallback
VALIDATOR_MAP_PATH="data/measured_validators_map.json"  # refreshed from sol@64.130.41.179:/home/sol/
NODE_COUNTRY="Germany"
REGION_MAX_RTT_US=5000
```
Refresh map: `scp sol@64.130.41.179:/home/sol/measured_validators_map.json data/measured_validators_map.json`
(READ ONLY on the remote). Current copy: 824 validators (DE=209, 742 with RTT).

---

## 10. Stats & output

Reuse geyserbench's stats core nearly verbatim (`percentile`, `build_summary`, `compare_latency`,
`diff_ms`). Two grouping dimensions:
- per `(leader, source)` → **win-rate**, lead-time mean/p50/p90/p99, sample count, **coverage**
  (fraction of that leader's shreds the source delivered at all — catches backroom *exclusives*).
- per `(leader, source_a, source_b)` → signed pairwise delta quantiles (jito-vs-custom AND
  custom-vs-custom — symmetric N-way, not geyserbench's hardcoded target-vs-bases).
Roll up `leader → region`.

**Two corrections to the reference tools:**
1. Do NOT copy geyserbench's "seen by ALL sources" filter (`analysis.rs:75-78`) — it discards exactly
   the exclusive/backroom wins we want. Require ≥2 sources for a *delta*; count single-source
   exclusives in the coverage metric.
2. Do NOT copy shred-stats' descending-sort percentiles (`stats.go:130`) — its "P99" is inverted.

**Sink:** InfluxDB line protocol (already running at `http://64.130.41.179:8086/`), tags
`leader,region,source`/`source_pair`, fields `win_rate,p50_us,p99_us,n,coverage`; plus a raw per-ShredId
CSV dump for offline analysis (cheapest first milestone).

---

## 11. Phased plan

- **M0 (½d):** verify offsets on a real multi-source capture — `layout::get_slot/get_index/get_shred_type`
  + `p[79..83]` fec match the full `Shred` parse for data & coding, legacy & merkle. Gate all on this.
- **M1 (1–2d):** passive per-packet CSV logger at the fallback tap (`forwarder.rs:218`, `Instant::now()`),
  emit `(source_ip, slot, fec, index, type, ts)`. Proves source-IP + zero-recon parse live. No matching.
  Also measure the residual recv-path cost (§4).
- **M2 (2–3d):** cross-source matcher (ShredId map, dedicated channel, slot-age eviction, first-arrival
  wins, pairwise deltas) + global win-rate/delta quantiles. Reproduces shred-stats in-process.
- **M3 (2–3d):** leader schedule (FRA RPC `get_slot_leaders` rolling window, keep pubkeys) + load the
  validator map + add `leader`/`region` grouping. **This milestone answers Q1 & Q2.**
- **M4 (2–3d):** port `recv_timestamp.rs`, extend to per-packet SO_TIMESTAMPNS, move tap into the
  recvmmsg loop; InfluxDB sink; coverage/exclusivity metrics; optional TUI.

---

## 12. Resolved decisions (2026-06-25)

| # | Question | Decision |
|---|---|---|
| 1 | Source IP→name mapping | MVP: **IP only**; user maintains naming later |
| 2 | Timestamp precision | **µs / SO_TIMESTAMPNS** for now; monitor & iterate (may escalate to HW) |
| 3 | Byte-identical shreds across sources | **Confirm later** on a real capture (M1) |
| 4 | RPC for leader schedule | **FRA `http://64.130.41.179:8899`** (tested OK); Helius fallback |
| 5 | Validator map | **Refreshed** from live host → `data/measured_validators_map.json` (824) |
| 6 | Region granularity | **Same as `shredstream_thread`**: rtt<5ms OR country==Germany |
| 7 | Branch | **`validator-benching`** (off `proven-fixes`) — confirmed |

## 13. Still to confirm
- (#3) Do all sources forward byte-identical leader shreds (same signature/merkle proof)? If a source
  re-encodes, the global bloom won't collapse them (harmless — `ShredId` still matches) but coverage
  stats need this verified. Check on M1's real capture.
- Whether software SO_TIMESTAMPNS µs jitter is small enough vs the inter-source deltas (revisit after M2).

---

## 14. Build status & audit resolutions (2026-06-25)

**Implemented:** all of M0–M4 (TUI from M4 intentionally skipped as "optional"). Module
`proxy/src/benchmark/` (`mod.rs`, `parse.rs`, `stats.rs`, `validators.rs`, `leader.rs`, `aggregator.rs`,
`recv_timestamp.rs`) wired into `forwarder.rs` + `main.rs`, gated by `--enable-benchmark` (off by
default). Compiles clean, clippy clean, 10 unit tests pass (incl. the M0 offset proof against the real
shred fixtures, and slot-guard / contested-vs-exclusive aggregator tests).

A 6-agent audit (design-conformance, correctness, integration, unsafe-FFI, concurrency, design-risk)
was run; notable findings and their resolutions:

| Sev | Finding | Resolution |
|---|---|---|
| **Critical** | One stray/garbage UDP packet with a huge slot poisons `current_max_slot` → eviction finalizes every real shred single-source → matching permanently broken | `slot_is_plausible()` guard: absolute bound (`2^40`), leader-window gating when present, bounded forward-jump otherwise (`aggregator.rs`) |
| **High** | Unbounded drain loop starves `sweep_finalize`/flush under sustained load → map grows unbounded | `DRAIN_CAP` bounds batches per outer iteration so eviction always runs |
| **High** | `getSlotLeaders(current-256, 5000)` reaches ~4744 slots ahead → errors near epoch boundary → all "unknown" | `refresh()` clamps the request to the epoch end via `getEpochInfo` |
| **Medium** | Win-rate counted uncontested/exclusive deliveries as wins (walkover == real race) | Split: `win_rate_bps` over **contested** races; separate `coverage_bps` and `exclusive_bps` |
| **Medium** | `include_coding` blended coding into the data headline | Accumulators keyed by `(leader, is_data)`; `shred_type` tag on every row |
| **Medium** | Aggregator not core-pinned (tail jitter) | `--benchmark-aggregator-core-id` (`sched_setaffinity`) |
| **Medium** | Per-window sample Vecs unbounded | capped at `MAX_SAMPLES_PER_SERIES` (16384) |
| **Medium (UB)** | `recv_mmsg_timestamped` `count==0` → `assume_init_mut()` on uninit `hdrs[0]` | compute `count` first, guard `count==0` before any `MaybeUninit` |
| **Low** | cmsg buffer `[u8]` (align 1) read as `cmsghdr`/`timespec` (align 8) | `#[repr(C, align(8))] CmsgBuf` backing |
| **Low** | dropped counter counted batches not samples | counts `obs.len()` |
| **Low** | pairwise `a_win_rate` denominator included ties | denominator = decided comparisons; `ties` emitted separately |
| **Low** | lead-time mean (design §10) not emitted; `mean`/`QUANTILES` dead | `lead_mean_us` emitted; `QUANTILES` removed; `mean` rounds |
| **Low** | `--node-country DE` silently never matched (predicate used full name) | predicate accepts country name **or** code |
| **Low** | leader poller RPC could stall shutdown | `RpcClient::new_with_timeout(10s)` |

**Accepted / documented (not code-changed):** kernel-ts mode parses in the listen thread upstream of the
reconstruct fork (opt-in, ~1µs/batch, startup warning + §4 note); coverage is relative to the
cross-source *union*, not the leader's true output (no ground truth without full reconstruction);
default userspace path uses one batch-granular timestamp — headline per-validator conclusions should use
`--benchmark-kernel-timestamps` (§5/§13); leader pubkey as an influx tag is high-cardinality *by design*
(per-validator is the deliverable), bounded by `--benchmark-min-samples`.

---

## 15. Jito baseline + consuming the data per validator (2026-06-28)

**Jito is auto-identified by source IP.** `proxy/src/benchmark/sources.rs` hardcodes jito's
block-engine shred-source IPs (from the validator firewall allowlist, ~42 IPs across
amsterdam/frankfurt/london/ny/slc). `classify(ip)` maps any of them to a single `SourceId::Jito`
(all PoPs collapse — jito's time for a shred is its best PoP, the min across those IPs); every other
IP stays `SourceId::Ip(addr)` (MVP: anonymous). Updating jito's IPs requires editing that list +
recompiling. No config needed to pick the baseline.

**The headline series: `shredstream_bench-vs-jito`** — emitted per `(leader, source)` for every
non-jito source, oriented jito→source so the sign is unambiguous:
- tags: `leader, region, in_region, shred_type, source`
- fields: `contested` (both jito & source delivered), `beats` (source earlier than jito), `losses`,
  `ties`, `beat_rate_bps`, `coverage_vs_jito_bps` (= contested / jito_delivered), `source_excl`
  (shreds the source had that jito never did — the backroom signal), `delta_sum_us` (Σ(source−jito),
  negative ⇒ source faster), `delta_p50/p90/p99_us` (signed, per-window).

**Why these fields:** counts and `delta_sum_us` aggregate exactly across flush windows, so the *true*
long-horizon answer is a single grouped query; percentiles are per-window trends only (use the CSV for
exact distributions). Per-validator data is temporally sparse (a validator leads ~4 slots per rotation),
so always aggregate over hours and gate on a min `contested`.

**The per-validator query** (location-relevant validators, custom vs jito, last 24h):
```sql
SELECT sum(beats)/sum(contested)        AS beat_rate,
       sum(delta_sum_us)/sum(contested) AS mean_delta_us,   -- negative = source faster than jito
       sum(source_excl)                 AS exclusive,
       sum(contested)                   AS n
FROM "shredstream_bench-vs-jito"
WHERE in_region='true' AND time > now()-24h
GROUP BY leader, source
```
Filter `n` above a confidence threshold, rank per leader → the validator-by-validator routing intel.
The symmetric `shredstream_bench-pair` series remains for custom-vs-custom comparisons.

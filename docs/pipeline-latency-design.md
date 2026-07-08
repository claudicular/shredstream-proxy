# Pipeline-latency instrumentation — design (Design B, locked 2026-07-02)

Measures how long the proxy takes to turn arriving shreds into a published
`Vec<Entry>`, **internal to shredstream-proxy**, emitted to InfluxDB. This is the
missing half of the shred-source benchmark: that one measures the *source race to
the NIC*; this one measures *NIC arrival → shmem/gRPC publish*.

Backed by a 6-thread research pass (attachment points, `custom`-branch critique,
metric semantics, hot-path safety, downstream contract, external prior art).
Scope was deliberately narrowed from the original "embed timestamps downstream so
the bot measures true end-to-end" idea — see §7.

## 1. Goal & scope

- **In scope:** two internal latency series to InfluxDB — *pipeline debt* and
  *shred spread* (§3) — computed entirely inside the proxy, off the hot path.
- **Out of scope (deferred, §7):** embedding timestamps into the shmem record or
  gRPC `PbEntry`; any downstream/consumer change; per-stage decomposition;
  precise FEC-recovered attribution.
- **Non-negotiable:** the production reconstruct→shmem/gRPC path must not be
  slowed. Design B has **no structural hot-path pieces** to compile-gate (that was
  Design A's channel widening / tracker field). The only hot-path addition is one
  `now()` + one `try_send` per published entry-batch, so it is **runtime-gated**
  via `--enable-pipeline-latency` (requires `--enable-benchmark` +
  `--benchmark-kernel-timestamps`). When off, the residual cost is a single
  `Option::is_some()` branch per entry-batch (plus carrying two `u32`s in
  `deshredded_entries`) — negligible, and it lets you toggle in prod without a
  rebuild.

## 2. Terminology (precise)

All arrival timestamps are the **kernel** `SO_TIMESTAMPNS` value — `CLOCK_REALTIME`
nanoseconds, stamped by the kernel in `netif_receive_skb` (after NIC coalescing +
NAPI), read from the `SCM_TIMESTAMPNS` cmsg in `recv_mmsg_timestamped`
(`proxy/src/benchmark/recv_timestamp.rs`). This is *earlier* than when userspace
collects the packet into a batch; it is our ground-truth arrival clock.

For one published `Vec<Entry>` **B**, composed of the contiguous data-shred index
range `[s..=e]` (`deshred.rs` `to_deshred = &data_shreds[start..=end]`):

| Name | Definition | Clock |
|---|---|---|
| `rx(i)` | kernel receive timestamp of shred `i` | CLOCK_REALTIME |
| `avail(i)` | `rx(i)` if received; else FEC-completion time (coarse: "recovered", see §5) | CLOCK_REALTIME |
| `T_first(B)` | `min` over `i∈[s..=e]` of `rx(i)` — earliest composing shred arrived | CLOCK_REALTIME |
| `T_ready(B)` | `max` over `i∈[s..=e]` of `avail(i)` — last-needed shred arrived (batch decodable) | CLOCK_REALTIME |
| `T_publish(B)` | `SystemTime::now()` captured on the reconstruct thread **immediately after** `ring.publish` | CLOCK_REALTIME |
| `T_dequeue(i)` | *(deferred)* when `recvmmsg` returned shred `i` to userspace — splits ingress gap from reconstruct backlog | — |

`T_ready` is a **max over availability**, not "the last data shred" — the
fan-in-correct anchor (Google critical-path / Flink event-time watermark). The
"UDP → userspace collection" gap (`rx → T_dequeue`, ~30µs) is currently *folded
into* `T_publish − T_ready`; splitting it out is the `T_dequeue` deferral (§7).

Caveat: `rx(i)` is ~5–10µs *after* true wire arrival (coalescing/NAPI) and that
pre-stamp gap is unmeasurable on the Broadcom `bnxt_en` NIC (no HW timestamping).
`rx` is the earliest *measurable* arrival; the bias is near-constant.

## 3. Metrics

Per published `Vec<Entry>` B, two primary series (both derived, same clock):

- **`pipeline_debt = T_publish − T_ready`** — the proxy's controllable cost
  (reconstruct queue wait + RS recover + deshred + serialize + shmem write). This
  is the number that **grows under load / with more shred sources** (duplicate
  parse load backs up the single reconstruct thread) — the source-scaling signal.
- **`spread = T_ready − T_first`** — intra-batch shred arrival spread. *Not*
  proxy-controllable (leader cadence + network + sources); *shrinks* with more
  sources. Diagnostic only: it disambiguates "pipeline is slow" (high debt) from
  "shreds arrived spread out / straggler source" (high spread).

Aggregation:
- **Percentiles** p50/p90/p99/max (reuse `benchmark/stats.rs::quantiles`); never
  means. MEV value is in the tail.
- **Segment** by `class` ∈ {`clean`, `recovered`} (§5), reported globally
  (`leader="ALL"`) for the first cut. Per-leader and multi-FEC splits are
  deferred (§7): pipeline debt is *box-load*-driven, not leader-driven, so the
  global distribution captures the primary signal; per-leader `spread` is a later
  refinement.
- **Windowed** flush (reuse `--benchmark-flush-secs`).
- **Clock guard:** clamp samples that are negative or `> CAP` (~200ms, well under
  slot time) into a `clock_anomaly` counter — never into percentiles. Run chrony
  slew-only on the prod box. (`CLOCK_REALTIME` can step under NTP.)

InfluxDB measurement: `shredstream_bench-pipeline` (tags: `leader="ALL"`,
`class` ∈ {clean, recovered}; fields: `n`, `debt_p50/p90/p99/max_us`,
`spread_p50/p90/p99_us`, `clock_anomalies`, `dropped`).

## 4. Architecture — Design B (aggregator-join)

The reconstruct thread computes **nothing**; it only reports *that* it published
and *which* shreds composed the entry. The benchmark aggregator (`ssBenchAgg`,
which already taps every shred) computes `T_first`/`T_ready` by joining.

```
listen thread (kernel-ts)  ──tap──▶  Observation{slot,index,is_data,rx}  ──▶  ssBenchAgg
   (unchanged)                                                                 │  keeps windowed
   │                                                                           │  (slot,index)→earliest rx
   ▼                                                                           │  [~64 slots ≈ 25s]
reconstruct thread                                                            │
  … deshred B over data_shreds[s..=e] (unchanged) …                           │
  ring.publish(slot, entries)          ← shmem write FIRST, untouched         │
  t_pub = now(); try_send(PublishEvent{slot, s, e, t_pub})  ───────────────────▶ join:
       (only new lines; drop-on-full)                                           for i in s..=e: rx_i = map[(slot,i)]
                                                                                any missing → class=recovered
                                                                                else T_ready=max, T_first=min
                                                                                push debt=t_pub−T_ready, spread
                                                                                → percentiles → InfluxDB (off-thread)
```

**Why B over A (reconstruct-computes):** B adds nothing to the hot path beyond the
irreducible floor (§6); no channel widening, no per-shred store in
`ShredsStateTracker`, no extra RSS on the reconstruct thread. It reuses arrival
data the tap already streams, and the observation map holds the *true earliest
arrival across sources* (fixes "first-processed ≠ first-arrived" for free). The
cost is that FEC-recovered handling is *approximate* (§5) and it couples to the
benchmark being on (§6). A (thread per-shred `rx` through reconstruct, store in
the tracker, compute at deshred) is the migration target only if we later need
exact recovered attribution or pipeline-latency without the benchmark.

Aggregator additions (all off the hot path):
- A windowed secondary map `(slot, data_index) → earliest_rx`, populated from the
  Observation stream (data index is unique per slot, so `fec_set_index` is not
  needed in the key). Evict with the same slot-age window as the match map.
- A `PublishEvent` intake (new bounded drop-on-full channel, or a new variant on
  the existing observation channel) + the join + the two-series accumulators +
  emit. Reuse `influx.rs` `append_point` / `InfluxWriter`.

## 5. FEC-recovered shreds (coarse handling)

Recovery fills a data index from coding shreds; that index never arrived as a data
shred, so it has no `rx`. Because the tap observes **all** sources pre-dedup,
"no observation for `(slot, i, data)`" ⟺ "no source delivered it" ⟺ "recovered"
(modulo a rare late-arrival case, acceptable for the coarse cut).

Coarse rule: if **any** composing index in `[s..=e]` is missing from the
observation map → tag the batch `class=recovered` and route it to the separate
`recovered` series (compute `debt`/`spread` over the found indices, flagged
approximate). All-direct batches are the clean primary series.

The precise version — track the per-FEC-set coding-shred arrival that crossed the
recovery threshold (`recover_time(f) = a_f[k_f]`) so recovered batches get an exact
`T_ready` — needs reconstruct's ground truth (which shreds it recovered, the FEC
threshold `k`). That is a later upgrade and pushes toward Design A. Deferred.

## 6. Hot-path guarantees & gating

**Irreducible hot-path cost (per published entry-batch, not per shred):** one
`CLOCK_REALTIME` read (`now()`, ~20–30ns vDSO) + one non-blocking `try_send`,
placed **after** `ring.publish` so the shmem write / consumer visibility is never
delayed. Published entry-batches are ~10–100× rarer than shreds → a few µs/sec.

**Emission is off-thread and cannot backpressure:** bounded drop-on-full channel +
`dropped` counter (the existing benchmark pattern). Percentiles + the blocking
InfluxDB POST run only on `ssBenchAgg`, pinnable via
`--benchmark-aggregator-core-id`.

**Runtime gate `--enable-pipeline-latency`:** when off, `benchmark::start` does not
create the pipeline channel (`pipeline_tx = None`), so the reconstruct drain loop's
only cost is `if let Some(tx) = &pipeline_tx { … }` — one predictable branch per
entry-batch, no `now()`, no send. The aggregator side (`(slot,index)→rx` map, the
`PublishEvent` intake, the pipeline accumulators) is only wired when the flag is on.
Design B has no channel widening or per-shred storage to compile-gate, so a runtime
flag is sufficient and lets you toggle in prod without a rebuild.

**Coupling (accepted):** requires `--enable-benchmark` (needs the Observation
stream) and `--benchmark-kernel-timestamps` (needs per-shred `rx`). We already run
both in prod. Without the benchmark, pipeline-latency would need Design A.
`--enable-pipeline-latency` is ignored (with a warning) unless both are on.

**No eviction race:** observations are tapped at *arrival*; the `PublishEvent`
fires when reconstruction *finishes*. Even a multi-second reconstruct backlog (the
spike we want to catch) is far inside the ~25s observation window.

## 7. Deferred / explicitly out of scope

- **Downstream embedding** (shmem `VERSION`→2 32-byte record; gRPC
  `PbEntry.ts_ready_nanos` — the latter needs forking the pinned `jito_protos`
  submodule). Dropped because: the only extra it buys is the `T_publish →
  consumer_read` IPC segment, which was already minimized when shmem was
  introduced, and the full UDP→consumer number is polluted by the uncontrollable
  `spread`. Not worth the wire break + arb-bot lockstep. Revisit only if the IPC
  segment is ever suspected of regressing (then a one-off spot-check, not a
  permanent contract).
- **`T_dequeue` stage split** (separate socket-buffer/ingress gap from reconstruct
  backlog). Add one timestamp at `recvmmsg`-return when we want that granularity.
- **Precise FEC-recovered `T_ready`** (§5) and **per-stage timings**
  (RS/deshred/write) — add as needed.
- **Per-leader / multi-FEC segmentation** — pipeline debt is box-load-driven so the
  global distribution is the primary signal; per-leader `spread` is a later add.

## 8. Standalone fix (do regardless of this feature)

`recv_timestamp.rs:217` does `extract_timestamp(...).unwrap_or(now())` — any packet
with a missing cmsg records ~0 latency and **biases the existing benchmark
distribution down** (worse under load if cmsg-loss correlates with stress). Fix:
count cmsg-missing separately; never fold the fallback into a latency series.

## 9. Build order (when we implement)

1. The `recv_timestamp.rs:217` fallback-bias fix (independent, small).
2. Aggregator: windowed `(slot,index)→rx` map + `PublishEvent` intake + the two
   series + `shredstream_bench-pipeline` emit — wired only when
   `--enable-pipeline-latency` is on.
3. `deshred.rs` carries the composing `[start,end]` index range in
   `deshredded_entries`; reconstruct drain loop captures `t_pub` after
   `ring.publish` and `try_send`s the `PublishEvent` — gated on `pipeline_tx.is_some()`.
4. Verify off-thread decoupling + drop counter under load; confirm negligible cost
   when the flag is off.

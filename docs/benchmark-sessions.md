# On-demand shred-source trials

Session mode is opt-in. It starts idle and compares a selected baseline and
candidate when requested through `scripts/bench.py`. An existing production
provider is the baseline; no permanent second feed is required. The provider
definitions are snapshotted into each session. No Jito subscription is needed.

## One-time setup

Build the proxy with `cargo build --release --locked -p jito-shredstream-proxy`.
On the proxy host, choose a **persistent directory on disk**, writable by the
proxy user, for the control file and results. Avoid tmpfs if recording raw CSV.
For example, provision `/var/lib/shredstream` and set:

```dotenv
ENABLE_BENCHMARK=true
BENCHMARK_KERNEL_TIMESTAMPS=true
BENCHMARK_CONTROL_PATH=/var/lib/shredstream/benchmark.json
```

Keep the existing `RPC_URL` (for leader identities), Influx credentials, ports,
destinations, gRPC, shared-memory settings, and CPU affinity configuration.
`ENABLE_PIPELINE_LATENCY=true`, if already used, continues working while races
are idle. `BENCHMARK_AGGREGATOR_CORE_ID` should be away from receive/decode cores.

Deploy once using **`forward-only`** instead of `shredstream` when Jito is no
longer needed. Keep all common arguments, including `GRPC_SERVICE_PORT` when
decoding to gRPC/shared memory; only the Jito-specific auth/region arguments go
away. The helper does not restart the process or edit a service unit.

Without `BENCHMARK_CONTROL_PATH`, legacy continuous benchmarking behaves as
before. In session mode, `BENCHMARK_CSV_PATH` and `BENCHMARK_EMIT_SOURCE_PAIR` do
not control session output. The old raw CSV is not opened/truncated; sessions
have separate bounded recordings. Preserve old recordings before any restart
that still uses legacy mode.

## Monthly trial

Run the helper as the proxy user on the same host. It takes `--control` before
the subcommand, or reads `BENCHMARK_CONTROL_PATH` from the environment. The
helper does not load `.env` itself. Its default path is the example above.

Register the actual provider egress IPs (these are examples, not provider IPs):

```sh
python3 scripts/bench.py sources set owned --ip 198.51.100.10 --ip 198.51.100.11
python3 scripts/bench.py sources set trial --ip 203.0.113.10
python3 scripts/bench.py sources set dz --logical doublezero
python3 scripts/bench.py sources list
```

`jito`, `doublezero`, and a literal IP also work directly as source arguments.
Provider identities must be disjoint. The DoubleZero identity uses the existing
multicast socket attribution. New multicast interfaces/groups are still joined
only at process startup. Plain unicast feeds can be introduced at any time on
the existing `SRC_BIND_PORT`, subject to provider setup and the firewall.

Arrange for the candidate to deliver **raw shreds** to that port, then:

```sh
python3 scripts/bench.py start --baseline owned --candidate trial --duration 86400
python3 scripts/bench.py status
python3 scripts/bench.py stop --wait 40
```

The default automatic stop is 24 hours from session creation, including waiting
for sources; the maximum is seven days. Extend sampling by starting another
session if necessary. Judge sufficiency by observations of relevant leaders,
not a fixed wall time. Registration edits apply only to future sessions.

For an explicitly bounded raw recording, add `--csv-max-rows 1000000` to start
(default 0, maximum 10 million). It records admitted observations, including
duplicate provider packets, and stops writing at the limit. This is a prefix
capture, not a random sample of the full trial.

Start/stop waits for the proxy to acknowledge the command. Status includes
last-seen timestamps, finalized data counts, pending shreds, drop counters, and
output errors. A stale status means the helper cannot confirm a live controller.
The command file and provider registry are updated atomically, with helper
writes serialized by a lock. The proxy does not acquire that lock.

### Session boundaries

1. `waiting_for_sources`: observe both identities, then choose the following
   slot as the first eligible slot. The initial partial slot is excluded.
2. `recording`: compare earliest arrivals per provider, per shred; finalize once
   the slot is older than the configured horizon (default 64 slots).
3. `draining`: stop admitting the current partial slot and later slots; keep
   accepting delayed arrivals for earlier eligible slots. Complete when the
   frontier clears the horizon or after nominal horizon time + 1 second
   (default about 26.6 seconds), even if feeds have stopped.
4. `complete`: flush final eligible rows and return to idle. An interrupted
   proxy shutdown is marked `interrupted`, not a completed drain.

Each receive batch is stamped with an atomic session epoch. Queued observations
cannot be reassigned to a later session. Late copies of already-finalized shreds
cannot reopen them. Session IDs cannot be reused, including after a restart;
an old start file will be rejected instead of silently resuming/overwriting a
trial. Malformed or conflicting commands preserve the current valid session.

The status counts and race rows are finalized statistics, so they lag arrivals
by the matching horizon. Last-seen fields update from selected observations.
After both sources first appear, a later outage contributes to missing-delivery
counts: inspect presence/drop status and trial conditions when interpreting it.

## Results and reports

For a control file `CONTROL`, paths are:

```text
CONTROL.sources.json                  provider registry (helper-owned)
CONTROL.status.json                   current controller status
CONTROL.sessions/SESSION_ID/
    manifest.json                     immutable source/settings snapshot
    status.json                       live/final session status
    windows.jsonl                     additive per-window result rows
    observations.csv                  optional bounded raw recording
```

Results are local even without InfluxDB. Local output errors remain visible in
status; writes do not retry indefinitely or backpressure receive/decode.
Completed files remain on disk until the operator archives/removes them.

```sh
python3 scripts/bench.py report /var/lib/shredstream/benchmark.json.sessions/SESSION_ID
python3 scripts/bench.py report /var/lib/shredstream/benchmark.json.sessions/SESSION_ID --min-contested 1000 --votes votes.json --json
```

`votes.json` is a fresh `getVoteAccounts` JSON response. Stake-weighted results
use eligible observed validators and explicitly report how much network stake
they cover. Unobserved/low-sample validators are not treated as zero edge.
The legacy skill's `overlay.py --vsjito` remains for historical Jito data;
`bench.py report` is its provider-independent session replacement.

Session rows deliberately do not stamp the legacy startup-only region map.
Join `leader` to a relevant RTT snapshot for regional analysis rather than
trusting a stale `in_region` label. Reports separate data and coding shreds and
do not double-count `leader=ALL` and individual leader rows.

### Measurement schema

Influx measurement: `shredstream_bench-session-pair`.
Tags: `session`, `baseline`, `candidate`, `leader`, `shred_type` (`data`/`code`).
`leader=ALL` is an independent rollup; `unknown` means no leader could be resolved.

| Fields | Meaning |
|---|---|
| `both_delivered` | Same shred observed from both providers within the matching window |
| `contested` | Both-delivered observations with a plausible signed delta |
| `candidate_faster`, `baseline_faster`, `ties` | Exact counts; sum to `contested` |
| `baseline_only`, `candidate_only` | Observed on only one side within the window |
| `delta_sum_us` | Sum of candidate arrival minus baseline arrival; negative favors candidate |
| `time_saved_sum_us` | Sum of `max(0, baseline_rx - candidate_rx)` on contested shreds |
| `clock_anomalies` | Both delivered, but absolute delta exceeds 10 seconds |
| `sample_count` | Retained uniform reservoir size, at most 16384; not the contested count |
| `delta_p50_us`, `delta_p90_us`, `delta_p99_us` | Window percentiles; omitted/null with no contested samples |

Sum counts/sums across windows, then divide. `candidate_win_pct` includes ties
in its denominator. Sums accumulate in nanoseconds internally and truncate to
microseconds once per output row (<1 microsecond rounding per row). Percentiles
are not additive. Positive savings measure adding the candidate on shared
shreds, not replacing the baseline or the benefit on candidate-only shreds.
Coverage versus baseline is `both_delivered / (both_delivered + baseline_only)`;
it is not completeness versus all shreds the leader produced.

Import `docs/grafana-benchmark-sessions.json` into Grafana and choose the Influx
Flux datasource. Select the bucket, time range, and session. The dashboard
aggregates exact counts/sums and does not average window percentiles.

## Production-path contract

No changes to receive sockets, recv loop, dedup ordering, reconstruction,
shared-memory publishing, gRPC, or RPC forwarding. The existing benchmark tap
adds one relaxed atomic epoch read per batch. If idle and pipeline telemetry is
off, it returns before timestamp accounting, observation allocation, parsing,
or queueing. If pipeline telemetry is on, the shared observation stream continues
but race matching/CSV output stop. The timestamp-capable receiver remains
installed: idle mode does not eliminate all kernel-timestamp receive overhead.

Control polling, mapping, matching, reservoir sampling, local files, status,
and Influx writes execute on the benchmark aggregator thread. The existing
bounded, nonblocking observation queue remains in use. Session matching is
capped at 500,000 in-flight shred keys; capacity loss is reported. An Influx
request may occupy the aggregator for its existing 5-second timeout, causing
measurement drops under load, but cannot block a producer on that queue.

Candidate traffic still enters production reconstruction/forwarding exactly as
it does today. More incoming packets and telemetry work can consume shared CPU,
cache, memory, or disk bandwidth. Pin the aggregator appropriately and inspect
pipeline/drop metrics during a trial. This change does not claim zero physical
performance impact and does not implement benchmark-only candidate routing.

# CLAUDE.md

Guidance for working in this repo. Keep claims here grounded in the actual code —
if you change behavior, update this file.

## Purpose

`HelmsDeep` (HTTP Endpoint Load Measurement System, Determining Each Endpoint's
Performance) measures, **for each NCATS Translator service, the maximum
sustainable concurrency** — how many concurrent users the service can feasibly
handle before it violates a latency/error SLO. The Translator stack has three
classes of service (Retriever/**KPs**, Shepherd/**ARAs**, and the **ARS**), each
of which speaks a slightly different protocol and expects a different kind of
TRAPI query. This tool sends each component the query type it expects and reports
a single headline number per run: the *knee*.

> The architecture below is now **implemented**: all three layers (KPs/ARAs/ARS)
> are runnable through a real `helmsdeep` CLI, config is per-target,
> and the corpus is segmented per component. A few refinements remain — see
> **Status & what's left**. Check the **Current code map** for exactly what each
> file does before assuming behavior.

## The metric we produce: the "knee"

A run drives a **stepped ramp** of concurrent users (the per-target `stages`
table) and, for each stage, records RPS, mean/p50/p95/p99 latency, and error
rate. The **knee** is the highest stage where **both**:

- `p99 <= P99_SLO_MS` (per-target `p99_slo_ms`; e.g. 60 000 ms for KPs, larger
  for the slower ARA/ARS layers), **and**
- `error_rate <= MAX_ERROR_RATE` (shared, default 0.01 = 1%)

The knee's *effective concurrency* — computed via Little's Law,
`concurrency = rps * mean_latency_seconds` — is the deliverable: the max
sustainable concurrency for that service.

Implementation: `_stage_stats()` computes per-stage metrics and the Little's-Law
concurrency; the knee-selection loop lives in `on_test_stop()` in
`helmsdeep/trapi_loadtest.py`. If no stage meets the SLO, the service
saturated below the first stage (or the SLO is too strict).

## Translator component model

Each component class has its own (a) endpoint + protocol and (b) expected query
subset. The tool must route the correct corpus + protocol per target.

| Component | Role | Protocol it expects | Query it expects |
|-----------|------|---------------------|------------------|
| **Retriever / KP** | Knowledge Provider (one service: **Retriever**) | sync `POST /query` (TRAPI) | `lookup`-mode queries; set `parameters.tier` to 0 or 1 (see below). Cheapest layer. |
| **Shepherd / ARAs** | Reasoning agents | sync `POST /query` (or `/asyncquery`) | `inferred`/creative-mode queries. Expensive. |
| **ARS** | Autonomous Relay System | **async**: `POST /submit` → poll `GET /messages/{pk}` until `Done`/`Error` → fetch merged results | `inferred` query; very long-running (minutes–~1 hr). |

**Pathfinder** is an additional, heavier query class for the **ARA and ARS layers
only** (never KPs). It pins **two** endpoint entities and asks for connecting
paths via a `paths` map in the query_graph (not `edges`; no `knowledge_type`). It
gets its **own run types** — `aras_pathfinder` (sync, via the ARA `/query`) and
`ars_pathfinder` (async, via the ARS submit/poll/merge) — so the single-class
targets never mix it into the inferred corpus. Same protocols/endpoints as
`aras`/`ars`, just a different corpus + a gentler ramp / looser SLO.

The one place the classes *are* combined is the **mixed capacity profile**
(`aras_mixed`/`ars_mixed`): a deliberate 2/3 inferred + 1/3 Pathfinder blend that
asks whether the system holds a target concurrency under the workload mix
production actually sends, rather than characterizing one class in isolation.

### Retriever and the `tier` parameter

In the **current** system the KP layer is a single service, **Retriever** — not the
~40+ independent KP endpoints of the old architecture (those survive only in git
history; see **Reusable assets**). Retriever exposes a
`message["parameters"]["tier"]` field that selects the backend graph it queries:

- **Tier 0** — backend graph that can handle **arbitrary multi-hop** queries.
- **Tier 1** — backend graph that handles **mostly single-hop** queries.

So when characterizing Retriever, `tier` is a first-class load dimension: a Tier 0
multi-hop query is a heavier cost profile than a Tier 1 single-hop. A KP corpus
should set `tier` **deliberately per query** (and pair multi-hop shapes with Tier 0,
single-hop shapes with Tier 1) rather than sending one fixed value for everything.

### Layering rule — a run targets exactly ONE layer

The stack **cascades: ARS → ARAs → KPs.** A query to the ARS fans out to the ARAs,
which fan out to the KPs. So load-testing the ARS already exercises everything
downstream.

**Therefore the tool hits one component class per run, mutually exclusively —
never all layers at once.** Running an ARA test *and* an ARS test at the same time
would double-load the ARAs (and the KPs beneath them) and corrupt both
measurements. The operator picks the single layer they want to characterize; the
lower layers are loaded only as a side effect of that one run. There is no
"test everything simultaneously" mode, and there should never be one.

The ARS contract is **fundamentally different** from KP/ARA: it is asynchronous
(submit → poll → merge), not a single blocking `POST /query`. Any ARS support must
model that submit/poll loop rather than reuse the sync request path.

## Architecture

```
target registry   →  per-component dispatch  →  Locust step-load engine  →  per-service report
(config.TARGETS:     (TRAPIUser picks protocol   (stages ramp, knee         (stages.csv,
 endpoint/protocol/   + corpus for the chosen     detection — shared         by_qtype.csv,
 corpus/stages/SLO)   layer via LOADTEST_TARGET)   across layers)            summary.json[, ars_health])
```

The Locust step-load engine in `trapi_loadtest.py` is the **reusable core** and
is shared across layers. What varies by component is only (1) the request
protocol (sync `POST` vs ARS submit/poll/merge) and (2) which corpus subset is
sent (`lookup` for KPs, `inferred` for ARAs/ARS) — both selected from
`config.TARGETS` by the `LOADTEST_TARGET` the CLI sets.

## Current code map (what exists today)

Package `helmsdeep/`:

- **`config.py`** — the target registry. `TARGETS` maps each `--targets` value
  (`kps`/`aras`/`ars`, the Pathfinder run types `aras_pathfinder`/
  `ars_pathfinder`, and the mixed capacity profile `aras_mixed`/`ars_mixed`) to a
  component config: `label`, `endpoint`, `corpus`
  (key into the corpus module), `protocol` (`sync`/`async`), `implemented`, and
  per-target `stages` + `p99_slo_ms` + an optional `cooldown_s` (a quiet gap
  between stages — users ramp to 0 so slow in-flight queries drain into the stage
  that launched them instead of bleeding into the next; defaults to 0, set on the
  expensive ARA/ARS/Pathfinder targets). Async targets (`ars`, `ars_pathfinder`) add
  `messages_endpoint`, `poll_interval_s`, `max_poll_s`, and an optional
  `completion_max_poll_s` (>= `max_poll_s`; the extended cap for the completion
  sidecar — how long a background poller keeps watching a timed-out query to see if
  it *eventually* finishes; defaults to `max_poll_s` = off) and an optional
  `zero_result_is_failure` (whether a terminal `Done` carrying 0 results scores
  as a failure; defaults to `True`). The `*_pathfinder` targets
  reuse the ARA/ARS endpoints + protocols but point at the `pathfinder` corpus and
  ship a gentler ramp + looser SLO (pinned-two-endpoint path-finding is the
  heaviest query class). The `*_mixed` targets add two more optional fields:
  `request_timeout_s` (per-HTTP-call timeout, default 210; raised on `aras_mixed`
  so its 5-min p99 SLO is measurable instead of landing as client timeouts) and
  `checkpoints` — a list of pass/fail acceptance criteria (`users`, `goal`,
  `p99_slo_ms`, `max_error_rate`) evaluated against the stage that ran that many
  users. `MAX_ERROR_RATE` is shared; `DEFAULT_TARGET="kps"`.
  Per-target ramps/SLOs live here because cost profiles differ wildly by layer:
  `kps` holds each stage 5 minutes with a 60s cooldown because Retriever answers
  in seconds to tens of seconds, not the milliseconds the old ~40-KP layer did --
  at the 60s holds it used to carry, every stage tripped both measurement-quality
  flags (see `_stage_quality`).
  `build_timeline(stages, cooldown_s)` lays the ramp out on a wall clock as
  `(start, end, stage_idx_or_None)` windows (`None` = a cooldown gap); the
  `StepLoad` shape drives users from it, and the live console uses it only for
  the run's total duration (its stage identity comes from the collector -- see
  `console.Dashboard._phase`).
  `natural_duration_s()` and `time_scaled(cfg, budget_s)` implement
  `--time-budget`/`--quick`: `time_scaled` returns a `(compressed_cfg, scale)`
  pair whose *durations* (stage holds, `cooldown_s`, `request_timeout_s`,
  `max_poll_s`/`completion_max_poll_s`/`poll_interval_s`) are scaled to fit the
  budget, with per-knob floors (`MIN_HOLD_S` etc.) and spawn rates raised so a
  short stage still spends its time at load rather than climbing to it. The
  *shape* — users, SLOs, checkpoints — is never touched, and a budget above the
  natural duration is a no-op (scale 1.0): it only ever speeds a run up.
- **`cli.py`** — the `helmsdeep` entry point (registered in
  `setup.py` `console_scripts`). Parses `--targets` (required, one layer),
  `--host` (required), `--csv-prefix`, the mutually exclusive
  `--time-budget DURATION` / `--quick` (= `--time-budget 10m`), and the two
  output flags `--no-live` (sets `HELMSDEEP_LIVE=0`) / `--verbose`; rejects
  not-yet-`implemented` targets; sets `LOADTEST_TARGET` (+ `LOCUST_CSV_PREFIX`,
  + `HELMSDEEP_TIME_BUDGET_S` when a budget is given) and launches
  `python -m locust -f trapi_loadtest.py --headless --host …`. Unless
  `--verbose`, it adds `--only-summary --loglevel WARNING`: Locust's 2-second
  request table and its ramp narration are exactly the noise the live display
  replaces (Locust still prints its final tables at shutdown). `_print_plan()`
  shows the (possibly compressed) ramp and what was traded away before the run
  starts; `_duration()` parses `600`/`90s`/`10m`/`1h30m`.
- **`console.py`** — the live terminal display. Purely cosmetic and strictly
  read-only: every number comes from the run's `StageCollector` and its
  `_stage_stats`, so the screen can't drift from `stages.csv`, and an exception
  in the render loop is caught and logged rather than failing the run.
  - `Dashboard` — spawns a gevent greenlet that redraws twice a second.
    `_phase()` answers "stage N, holding or draining, how far in" **from the
    collector's `stage_idx`/`stage_started`/`stage_ended`**, never from elapsed
    time: the shape ticks off Locust's runner clock and the display off
    `test_start`, so a clock-derived stage disagreed with the measurements for a
    second or so at every boundary — exactly when the display has something to
    say. `_footer_lines()` renders the block (run progress, stage progress, live
    load/latency vs the SLO, plus an `ars` line for async targets);
    `_announce()`/`_recap()` write the scrolling record — a header when a stage
    starts, a `✓`/`✗` verdict line (the knee test on one stage) when it ends.
    Since a new stage can only appear after `mark_stage` closed the previous one,
    the previous stage's verdict is always final and always printed immediately
    before the next header: the transcript order is fixed, not a race.
  - `StickyFooter` — the bottom-of-screen block. `install()` wraps stdout/stderr
    (and re-points the logging handlers that captured them at setup time) in a
    proxy that erases the block before any other write, so prints and log lines
    scroll above it instead of colliding with it.
  - `sparkline(values, threshold, paint)` — the shape of a metric across the ramp,
    one character per stage, red where the stage broke `threshold`; the summary
    prints one for p99 and one for the error rate.
  - `live_enabled()` / `color_enabled()` — a non-TTY stdout (CI, a pipe) or
    `HELMSDEEP_LIVE=0` drops to plain mode: the same content as one status line
    every `PLAIN_EVERY_S`. `NO_COLOR` drops colour only.
- **`trapi_loadtest.py`** — the measurement engine (Locust). The component under
  test is chosen by `LOADTEST_TARGET`, and when `HELMSDEEP_TIME_BUDGET_S` is set
  the target config is passed through `config.time_scaled` at import (module-level
  `TIME_SCALE`, stamped into `summary.json` as `config.time_scale` and printed as
  a warning banner) so everything downstream reads the compressed values; `ENDPOINT`, `CORPUS`, `STAGES`,
  `P99_SLO_MS`, and the async knobs (`PROTOCOL`, `MESSAGES_PATH`,
  `POLL_INTERVAL_S`, `MAX_POLL_S`) plus `COOLDOWN_S` are all derived from
  `config.TARGETS[...]`.
  - `StepLoad(LoadTestShape)` — drives the `stages` ramp and marks the active stage.
    When `COOLDOWN_S` is set it inserts a drain gap between stages (`tick()`
    returns 0 users in the gap and calls `COLLECTOR.end_active_stage()` to freeze
    the just-finished stage's end time); an `@events.init` listener sets
    `stop_timeout = REQUEST_TIMEOUT` so a slow in-flight query finishes rather than
    being killed when users ramp to 0. The gap's spawn rate is the just-finished
    stage's **user count**, so Locust's dispatcher stops every user in one
    iteration: it removes `floor(spawn_rate)` users per iteration and blocks on
    each batch's `stop_timeout`, so a smaller rate walks the teardown down a few
    users a second while everyone not yet stopped keeps starting *new* queries.
    `_draining()` + the gate at the top of `TRAPIUser.query` close the remaining
    seam (Locust polls `tick()` only about once a second, and acts on it
    asynchronously): a user whose query returns inside the gap parks on
    `gevent.sleep(DRAIN_RECHECK_S)` instead of issuing another one. It reads the
    shape's own clock (`StepLoad.in_cooldown()`, via the module-level `SHAPE`),
    not the last tick, so the boundary is exact.
  - `StageCollector` / `COLLECTOR` — buckets every completed request into the
    stage active when it *finished* (per-stage, per-`qtype`). Also holds the two
    per-query ARS lists (`queries`, `completions`) and hands out the shared
    `new_query_id()` that joins them. Records each stage's
    wall-clock `stage_started` / `stage_ended` (the latter is `setdefault`, so a
    cooldown freeze isn't overwritten by the next `mark_stage`). `record()` also
    takes optional ARS signals: `status`, `result_count`, `response_bytes`.
    `begin_inflight()`/`end_inflight()` track logical queries currently running,
    for the live display only -- no report reads them.
  - `_stage_stats()` — per-stage RPS, percentiles, error rate, Little's-Law
    concurrency. `_ars_stage_health()` — per-stage ARS health row.
  - `TRAPIUser(HttpUser)` — dispatches on `PROTOCOL`: `_run_sync()` (KP/ARA: one
    blocking `POST`) or `_run_ars()` (submit → poll `/messages/{pk}` with
    `gevent.sleep` until `Done`/`Error`/timeout → `_fetch_merged()` counts
    `fields.data.message.results` and returns its HTTP status). One
    `COLLECTOR.record` per logical query, plus one `COLLECTOR.record_query` debug
    row (`_record_query`) carrying the `pk`, the submit/poll/merge HTTP codes, the
    poll count, the terminal ARS status, a `QueryIssues` tally of the
    intermediate (retried, non-fatal) errors hit along the way
    (`intermediate_error_count` + `intermediate_errors`), and a `message_url` —
    written on every terminal path including `SubmitError`/`NoPK`/`Timeout`. When a
    query blows `MAX_POLL_S` (already recorded as a Timeout failure — main stats
    unchanged), `_run_ars` spawns a detached `_extended_poll` greenlet that keeps
    polling to `COMPLETION_MAX_POLL_S` and appends one `COLLECTOR.record_completion`
    row (end-to-end time + whether it `finished`); queries that finish within
    `MAX_POLL_S` record their completion row inline.
  - `_stage_quality(row)` — the two conditions that make a stage's row untrust-
    worthy however correct its arithmetic: fewer than `MIN_P99_SAMPLES` (100)
    completed requests, so the p99 rests on the two slowest and moves with one
    outlier; and Little's-Law concurrency below `MIN_CONCURRENCY_RATIO` (0.5) of
    the stage's user count, meaning most users were blocked on queries that
    finished in the *next* stage (we bucket by finish time), inflating its tail
    and deflating this one. Each issue is `{kind, detail}` (`thin_samples` /
    `in_flight_bleed`) so the printed remedy names only the fix that applies and
    a script can branch without parsing prose. `on_test_stop` collects these into
    `stage_warnings` + `knee_unsupported` in the summary, a printed MEASUREMENT
    QUALITY block, and a caveat on the final headline when the knee rests on a
    flagged stage.
  - `_evaluate_checkpoints()` — for targets that configure `checkpoints`, judges
    each one against the stage matching its user count and returns a
    `PASS`/`FAIL`/`NO DATA` verdict (a checkpoint's `p99_slo_ms` defaults to the
    target's; explicit `None` means latency isn't judged — an overload probe where
    slowdown is expected but failures aren't).
  - `_start_dashboard()` (`@events.test_start`) / `DASHBOARD` — starts the
    `console.Dashboard` on the master/standalone node; `on_test_stop` recaps the
    final stage and lifts the footer before printing the summary.
    `_stash_headline()` + `_print_headline()` (`@events.quit`) repeat the knee
    (or the missed checkpoints) *after* Locust's own end-of-run tables, so the
    number the run exists to produce is the last thing on screen.
  - `on_test_stop()` — drains any in-flight completion greenlets (bounded), finds
    the knee, writes `stages.csv` (incl. a `stage_start` ISO-8601-UTC column) /
    `by_qtype.csv` / `summary.json`, (checkpointed targets only)
    `checkpoints.csv` + `checkpoints`/`checkpoints_passed` in the summary and a
    printed verdict block that sets `environment.process_exit_code = 1` on any
    miss, prints the stage table plus two `console.sparkline` shape rows (p99 and
    error rate across the ramp, red where a stage broke its bar), and (async only) `ars_health.csv`, the `ars_queries.csv` per-query
    debug log + a printed "FAILED QUERIES" block naming the first few pks/URLs,
    the `ars_completion.csv` sidecar, plus `red_flags` + a `completion` roll-up in
    the summary + printed block.
- **`trapi_corpus.py`** — the per-component query corpuses:
  - `_qg(nodes, edges, tier=None, bypass_cache=None)` — TRAPI envelope; adds
    scalar `parameters.tier` (KP-only) or top-level `bypass_cache` (ARA/ARS) only
    when supplied.
  - **`RETRIEVER_CORPUS`** — `lookup`-mode KP builders, each pinning its own
    `tier` (multi-hop→0, single-hop→1): `one_hop_lookup_pinned`,
    `one_hop_lookup_open`, `one_hop_no_predicate`, `two_hop_lookup`,
    `batch_lookup`, `malformed_query`.
  - **`SHEPHERD_CORPUS`** (also used as **`ARS_CORPUS`**) — `inferred` +
    `bypass_cache` creative queries, an even MVP1/MVP2 split (50/50), entity
    varied per request:
    - **MVP1** "what treats disease X?" (`chemical-[treats]->disease`), disease
      sampled from size-tiered pools via `mvp1_heavy`/`mvp1_medium`/`mvp1_light`
      (10/15/25 = the 50% MVP1 half, tiered 20/30/50 within it).
    - **MVP2** chemical `biolink:affects` gene, with object aspect/direction
      qualifiers on the gene. The edge is **always** oriented chemical(subject)→
      gene(object) (matching the Translator TestHarness `generate_query.py`); the
      two variants differ only by which endpoint is pinned —
      `mvp2_chem_affects_gene` (pinned gene, open chemical) /
      `mvp2_chem_affects_open_gene` (pinned chemical, open gene) (25/25). Aspect is
      the canonical `activity_or_abundance`; direction + entity sampled per request.
  - **`PATHFINDER_CORPUS`** — the Pathfinder run type (ARA/ARS only, selected by
    `aras_pathfinder`/`ars_pathfinder`). `pathfinder_drug_disease` pins **two**
    endpoints (a drug + a disease, sampled per request from `CHEM_DISEASE_PAIRS`)
    and asks for connecting paths via a `paths` map in the query_graph — built by
    `_pathfinder_qg(nodes, paths)` (`nodes` + empty `edges` + `paths`; no
    `knowledge_type`, no `tier`; `bypass_cache=True`). Most intensive query class.
  - **`MIXED_CORPUS`** — the mixed capacity profile (`aras_mixed`/`ars_mixed`).
    Not hand-written: `_mixed_corpus()` blends `SHEPHERD_CORPUS` and
    `PATHFINDER_CORPUS` at `INFERRED_PATHFINDER_RATIO = (2, 1)` — 2/3 inferred
    MVP1+MVP2, 1/3 Pathfinder — preserving each corpus's internal weights, so
    retuning the MVP1/MVP2 mix propagates here automatically.
  - Entity pools: `HEAVY_DISEASES` (curated hubs) + `LONG_TAIL_DISEASES` from
    **`curie_list.json`** (~1000 real MONDO CURIEs, shipped via `package_data`),
    a curated `GENES` pool (NCBIGene), and curated drug↔disease `CHEM_DISEASE_PAIRS`.
    `corpus_for(name)` returns the right list.

For KPs, cost is driven by query-graph **shape** (hops, mode, pinned vs open,
batch, predicate). For ARA/ARS creative queries, the dominant cost driver is the
**pinned disease's answer-set size**, which is why that corpus varies the entity.

## Status & what's left

Implemented: component awareness, the `helmsdeep` CLI + entry point,
per-target stages/SLO, segmented corpuses, scalar `parameters.tier` per KP query,
the ARS async submit/poll/merge user, ARS health metrics + red flags, the
tiered inferred disease mix, and the mixed capacity profile (2:1
inferred/Pathfinder blend + pass/fail acceptance checkpoints, ARA and ARS).

Remaining refinements (not yet done — don't assume these exist):

- **Medium vs light tiers aren't calibrated.** `inferred_medium` and
  `inferred_light` currently draw from the same `LONG_TAIL_DISEASES` pool; split
  it by measured answer-set size (a one-time profiling pass) for true separation.
- **No per-ARA child-result breakdown.** ARS health treats the merged message as
  a whole; the `trace=y` response exposes children, so per-agent health is possible.
- **No full multi-endpoint registry.** Retriever is a single service today; the
  old per-KP/ARA URL registries survive only in git history (see below).
- **Config is env/registry-driven, not file-driven.** Stages/SLO are edited in
  `config.py`; there's no external config-file or full CLI override yet.
- **No `results/` output convention** — outputs land in the working directory.

## Reusable assets in git history

The "Start the rewrite" commit (`9fe6cd0`) deleted the old per-service scripts, but
they contain assets worth recovering for the roadmap below. Retrieve with
`git show <commit>:<path>`:

- `git show b912968:kps.json` — registry of ~40+ KP endpoints (`*.ci.transltr.io`),
  some with per-KP `predicates` overrides.
- `git show b912968:aras.json` — registry of ARA endpoints (Aragorn, ARAX, BTE,
  mediKanren, CQS, imProving Agent).
- `git show b912968:ars_stress_test.py` — the ARS submit/poll/merge workflow
  (base URL `https://ars.ci.transltr.io/ars/api`).
- `git show b912968:generate_message.py` — TRAPI message builders, including batch
  handling (`set_interpretation: "BATCH"`) and `inferred` ARA queries.
- `git show b912968:curie_list.json` — ~1000 MONDO disease CURIEs. **Already
  restored** into the package as `helmsdeep/curie_list.json` (the
  long-tail disease pool for the inferred corpus).

## How to run

```bash
pip install -e .          # Python >= 3.12; installs locust

# One layer per run (kps | aras | ars); --host required.
helmsdeep --targets kps --host https://your-retriever.example.org --csv-prefix run1
helmsdeep --targets aras --host https://your-ara.example.org --csv-prefix run1
helmsdeep --targets ars  --host https://ars.ci.transltr.io/ars/api --csv-prefix run1

# Pathfinder is its own (heavier) run type, ARA/ARS only:
helmsdeep --targets aras_pathfinder --host https://your-ara.example.org --csv-prefix pf1
helmsdeep --targets ars_pathfinder  --host https://ars.ci.transltr.io/ars/api --csv-prefix pf1

# Mixed capacity profile (ARA/ARS only): 2/3 inferred MVP1+MVP2 + 1/3 Pathfinder,
# ramped to 30 -> 45 -> 60 concurrent and judged pass/fail per checkpoint.
helmsdeep --targets aras_mixed --host https://your-ara.example.org --csv-prefix mix1
helmsdeep --targets ars_mixed  --host https://ars.ci.transltr.io/ars/api --csv-prefix mix1
```

- The `LoadTestShape` (`StepLoad`) **drives users, spawn rate, and duration**, so
  there is no `-u` / `-r` / `-t`. Tune the ramp via the per-target `stages` in
  `config.py`.
- `--csv-prefix` is optional; it falls back to the `LOCUST_CSV_PREFIX` env var,
  then to `trapi_run`.
- You can also run the locustfile directly (`locust -f helmsdeep/
  trapi_loadtest.py --headless --host …`), selecting the layer with the
  `LOADTEST_TARGET` env var (defaults to `kps`). Note locust has no
  `--csv-prefix` flag — set `LOCUST_CSV_PREFIX` instead.
- A checkpointed run (`*_mixed`) **exits non-zero** when a checkpoint is missed,
  so it can gate a CI/acceptance job.
- `--quick` (= `--time-budget 10m`) or `--time-budget 30m` compresses any target
  to a wall clock; see the conventions note below for what that costs.
- Outputs (written by the master/standalone node only):
  `<prefix>_stages.csv`, `<prefix>_by_qtype.csv`, `<prefix>_summary.json`
  (+ `<prefix>_checkpoints.csv` for checkpointed targets) (+
  `<prefix>_ars_health.csv`, the `<prefix>_ars_queries.csv` per-query debug log,
  the `<prefix>_ars_completion.csv` sidecar, and a `red_flags` list +
  `completion` roll-up for the `ars` target), plus a printed summary table with
  the knee.

## Conventions & gotchas

- **The shape owns concurrency.** Tune load by editing the per-target `stages`
  in `config.py`, not CLI flags.
- **Two kinds of run: knee-finding vs acceptance.** Every target reports the knee
  ("how far can we go?"). A target that also defines `checkpoints` answers a
  pass/fail question at named concurrency levels ("does 30 hold?") and exits
  non-zero on a miss. `aras_mixed`/`ars_mixed` are the acceptance profile: a 2:1
  inferred/Pathfinder blend checked at 30 (peak) / 45 (headroom) / 60 (overload,
  error-rate only). Checkpoints are generic, not special-cased to that profile --
  a target without them behaves exactly as before.
- **Cooldown drains, it doesn't bleed.** With `cooldown_s` set, the gap between
  stages ramps users to 0; the just-finished stage's end time is frozen so its
  `duration_s`/RPS reflect the active window, and a slow query still running
  drains into *that* stage (via `stop_timeout`), keeping the next stage clean.
  A drain is **only** in-flight queries finishing — **no new query starts once
  the gap begins**. Locust won't give you that for free: its teardown is
  rate-limited and its shape poll is ~1 s coarse, so the gap both stops every
  user in a single dispatch iteration (spawn rate = the stage's user count) and
  gates `TRAPIUser.query` on `_draining()`. Without the gate the frozen stage
  keeps collecting requests it never launched under load, inflating its RPS.
- **Closed-loop load.** `TRAPIUser.wait_time = constant(0)` — no think time; users
  hammer the endpoint as fast as responses return.
- **A compressed run is not a measurement.** `--time-budget`/`--quick` scale
  durations only (holds, cooldowns, poll/timeout caps) — the ramp, SLOs, and
  checkpoints are identical, so the run asks the same questions of the same load
  levels. But it answers them from far fewer samples (a p99 over a handful of
  queries is noise), and the shrunken per-query caps *change what counts as a
  failure*: a query that would finish in 4 minutes is a timeout when the cap is 2.
  Compressed runs are for exercising a host/corpus/config end to end, not for
  quoting a knee or gating CI. `config.time_scale` in `summary.json` (< 1.0) is
  how you tell after the fact.
- **The live display never measures anything.** `console.py` reads the same
  `StageCollector` and `_stage_stats()` the CSVs are written from; it owns no
  tally of its own, and its greenlet swallows its own exceptions. Changing what
  the footer says is a rendering change, never a measurement change.
- **The collector owns "which stage are we on" -- for the display too.** The
  display derives the stage, its progress, and its verdict from
  `COLLECTOR.stage_idx`/`stage_started`/`stage_ended`, not from its own elapsed
  clock. Two clocks meant the screen and the CSV could name different stages at a
  boundary, and stage verdicts landed on either side of the next stage's header
  depending on which won the race. If you add anything that reports where a run
  is, read it from the collector.
- **A stage's row can be right and still not mean anything.** `_stage_quality`
  flags the two ways that happens -- a p99 over too few samples, and effective
  concurrency far below the user count (in-flight queries bleeding into the next
  stage). Both are ramp-shape problems, fixed with longer holds and a
  `cooldown_s` in `config.py`, never by adjusting the numbers. If you retune a
  target's stages, check a real run for these flags rather than assuming the old
  holds still fit the service's latency.
- **Don't trust Locust's blended aggregate during a ramp.** We bucket per stage in
  `StageCollector` precisely because an aggregate p99 would mix easy early stages
  with saturated late ones.
- **`malformed_query` 4xx is success.** A 4xx on the malformed query is treated as
  a valid measurement of the error path; only 5xx counts as a failure. See the
  `TRAPIUser.query` handling.
- **Long timeouts on purpose.** `REQUEST_TIMEOUT` defaults to 210 s (per-target
  override: `request_timeout_s`) because TRAPI queries are slow; ARS runs are far longer still (minutes–~1 hr) and need the async model.
- **gevent concurrency.** Locust uses gevent green-threads; avoid blocking calls in
  the user path. The ARS poll loop uses `gevent.sleep`, **never** `time.sleep`.
- **ARS submit/poll/merge is one logical measurement.** `_run_ars` issues several
  HTTP calls (named `ars_submit`/`ars_poll`/`ars_merge` — they show in Locust's
  own table as per-step diagnostics) but records exactly one `COLLECTOR.record`
  per logical query, with latency = wall-clock submit→terminal. It also fires a
  single synthetic `ars_query` Locust request event carrying that same full
  wall-clock, so Locust's native stats table shows the true per-query time
  (otherwise the only ARS rows would be the individual sub-calls — e.g. the
  `ars_merge` GET, which times just the final merge fetch, not the whole query).
  A `Done` that returns **0 results counts as a failure** by default (and raises
  a red flag) — see `zero_result_is_failure` below; a non-terminal status past
  `max_poll_s` is a `Timeout` failure.
- **`zero_result_is_failure` decides what a 0-result `Done` means.** Default
  `True`: an empty answer set under load usually means a downstream agent silently
  dropped out, so it scores as a failure and counts against the knee. Set it
  `False` on a target to score only transport/protocol outcomes (submit error,
  `Error` status, `Timeout`) as failures — the zero-result query's latency then
  also joins the percentile pool instead of being discarded (failed requests
  contribute no latency samples), so it shifts mean/p99/concurrency, not just the
  error rate. Either way the query is still tallied in `ars_health`
  (`zero_result_done`) and still raises a red flag, the per-query debug log still
  carries the `Done with 0 results` note (its `failed` column follows the policy),
  and the summary JSON + printed ARS health block record which policy was in force.
- **Intermediate errors are the trouble a query survived.** The poll loop retries
  through non-200 polls, unparseable poll bodies and a bad merge fetch, so none of
  them change `ars_status` — which means a query that fought through thirty 502s
  lands in the log as a clean `Done`. `QueryIssues` tallies them per query into
  `intermediate_error_count` (0 = clean) + `intermediate_errors` (`poll HTTP 502
  x3; merge HTTP 500`), and `on_test_stop` prints a roll-up separate from the
  FAILED QUERIES block precisely because most queries carrying them succeeded.
- **The per-query debug log is the bridge from a number to a query.** The
  aggregates say 3% failed; `<prefix>_ars_queries.csv` says which pks, with the
  HTTP status of each step and a `message_url` to pull one up. One row per logical
  query on every terminal path (including `SubmitError`/`NoPK`, which have no pk
  at all), attributed to the stage active when it reached its outcome. It joins to
  `ars_completion.csv` on `query`; a timed-out query whose extended poll was still
  in flight at shutdown is in the debug log but *not* the sidecar.
- **Completion tracking is a sidecar, not a metric change.** `max_poll_s` stays the
  failure threshold for the main stats and the knee — a query not terminal by then
  is a `Timeout` failure, exactly as before. Separately, `completion_max_poll_s`
  (>= `max_poll_s`, default 10 min on `ars`/`ars_pathfinder`) lets a **detached
  background greenlet** keep polling that same query to see if it *eventually*
  finishes; the outcome (end-to-end time + `finished`/`within_slo`/`status`) goes
  only to `<prefix>_ars_completion.csv` and the `completion` summary roll-up. It
  never feeds the per-stage stats, the `ars_query` event, or the knee, so existing
  measurements are unchanged. The greenlets are drained (bounded by the extra
  budget) in `on_test_stop`; queries still unfinished at shutdown are simply absent
  from the sidecar. This separates *slow* (finished after the SLO) from *broken*
  (never finished).
- **Inferred corpus mixes MVP1 + MVP2 and varies entities per request.** MVP1
  (`mvp1_heavy/medium/light`, treats-disease) samples a tiered disease; MVP2
  (`mvp2_chem_affects_gene`/`mvp2_chem_affects_open_gene`, a chemical→gene
  `affects` edge with the gene-pinned and chemical-pinned variants) samples the
  pinned entity + direction. The edge orientation is always chemical(subject)→
  gene(object) and the aspect qualifier is always `activity_or_abundance`. The
  per-request variation covers the real cost surface and avoids warming caches.
  MVP1 medium and light share the long-tail pool until calibrated (see Status &
  what's left).
- **Environments & TRAPI versions vary per service.** Endpoints live across
  `*.ci.transltr.io`, `*.test.transltr.io`, and prod, and individual services pin
  different TRAPI versions in their URL paths. Target deliberately.
- **Swap the CURIEs.** The KP corpus uses a few real MONDO/CHEBI entities and the
  inferred corpus draws diseases from `curie_list.json`; replace/extend them with
  entities the target service actually knows about, or queries return empty and
  won't reflect real cost.

## Roadmap (broader repo, next phases)

Done in earlier phases: per-component adapter + `--targets` CLI entry point
(one layer per run — there is intentionally no "all" mode, which would
double-load shared downstream services per the layering rule), the ARS async
submit→poll→merge user, per-target config, and a README for human onboarding.

Remaining, ordered so a future session can pick up where this leaves off:

a. **Calibrate the inferred tiers.** Profile each disease once (sort by merged
   result count) and split `LONG_TAIL_DISEASES` into real medium/light pools.
b. **Per-ARA health breakdown** for ARS, parsing the `trace=y` children so a
   red flag can name *which* downstream agent dropped answers.
c. **Restore the full endpoint registry as config** (per-KP/ARA URLs +
   predicate/query overrides) from the git-history assets above, if/when the
   stack returns to multiple independent KP/ARA endpoints.
d. **Make config file-driven** (external config file / full CLI overrides for
   stages, SLOs, poll knobs) instead of editing `config.py`.
e. **Adopt a `results/` output convention** so each service's reports land in a
   predictable, per-service location.

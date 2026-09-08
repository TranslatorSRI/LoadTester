# HelmsDeep

**HelmsDeep** — *HTTP Endpoint Load Measurement System, Determining Each
Endpoint's Performance* — drives a stepped ramp of concurrent users against a
single NCATS Translator component and reports the **max sustainable
concurrency** (the "knee") — the highest load where the service still meets a
latency/error SLO.

The Translator stack cascades **ARS → ARAs → KPs**, so a run targets exactly
**one** layer at a time (testing a higher layer already loads everything beneath
it). All three workflows are wired up: **Retriever (KP)**, **Shepherd (ARA)**,
and the asynchronous **ARS**.

> **In plain language:** HelmsDeep slowly turns up the number of simultaneous
> users hitting a service and watches when it starts getting too slow or too
> error-prone. The single headline number it reports — the **knee**, or "max
> sustainable concurrency" — is *the largest number of simultaneous users the
> service handled while still staying fast enough and erroring rarely enough.*
> Above that load, the service is overwhelmed. If you only read one thing,
> read the **["How to read the results"](#how-to-read-the-results)** section.

## Install

```bash
pip install -e .          # Python >= 3.11; installs locust
```

3.11 rather than 3.12 because HelmsDeep is also installed as a dependency of the
[Translator TestHarness](https://github.com/TranslatorSRI/TestHarness), which
runs on a 3.11 base image and drives this CLI as its performance runner. Nothing
here uses 3.12-only syntax; keep it that way.

## Run a workflow

```bash
# Retriever (KP) — sync lookup queries, scalar parameters.tier per query
helmsdeep --targets kps \
    --host https://your-retriever-service.example.org \
    --csv-prefix run1

# Shepherd (ARA) — sync creative-mode (inferred) queries, cache bypassed
helmsdeep --targets aras \
    --host https://your-ara-service.example.org \
    --csv-prefix run1

# ARS — async submit/poll/merge of inferred queries (host = the service root;
# the /ars/api/... paths come from the target config, don't put them in --host)
helmsdeep --targets ars \
    --host https://ars.ci.transltr.io \
    --csv-prefix run1

# Pathfinder — its own heavier run type (ARA/ARS only); pins two endpoints and
# asks for connecting paths. Sync via the ARA, async via the ARS.
helmsdeep --targets aras_pathfinder \
    --host https://your-ara-service.example.org \
    --csv-prefix pf1
helmsdeep --targets ars_pathfinder \
    --host https://ars.ci.transltr.io \
    --csv-prefix pf1

# Mixed capacity profile (ARA/ARS only) — 2/3 inferred MVP1+MVP2, 1/3 Pathfinder
# in one blended workload, ramped to 30 → 45 → 60 concurrent users and judged
# against pass/fail acceptance checkpoints.
helmsdeep --targets aras_mixed \
    --host https://your-ara-service.example.org \
    --csv-prefix mix1
helmsdeep --targets ars_mixed \
    --host https://ars.ci.transltr.io \
    --csv-prefix mix1
```

- `--targets` selects the layer: `kps` (Retriever), `aras` (Shepherd), `ars`, the
  Pathfinder run types `aras_pathfinder` / `ars_pathfinder` (ARA/ARS only), or the
  mixed capacity profile `aras_mixed` / `ars_mixed` (ARA/ARS only — see
  ["Mixed capacity profile"](#mixed-capacity-profile-aras_mixed--ars_mixed)).
- `--host` is **required** — the **service root**, with no path of its own. Each
  target's `endpoint` (and, for async targets, `messages_endpoint`) is appended
  to it: `kps`/`aras` append `/query`, and `ars` appends `/ars/api/submit` then
  `/ars/api/messages/{pk}`. So the ARS host is `https://ars.ci.transltr.io`, not
  `https://ars.ci.transltr.io/ars/api` — including the API path there sends every
  request to `/ars/api/ars/api/...`.
- `--csv-prefix` is optional; it falls back to the `LOCUST_CSV_PREFIX` env var,
  then to `trapi_run`.
- `--time-budget` / `--quick` compress a run to fit a wall clock — see
  ["Shorter runs"](#shorter-runs---time-budget----quick) below.
- `--no-live` / `--verbose` control the terminal output — see
  ["Watching a run"](#watching-a-run) below.

The load profile (users, spawn rate, duration) is driven by the `StepLoad` shape,
**not** by CLI flags — so there is intentionally no `-u/-r/-t`. The ramp and knee
threshold are **per target** in `helmsdeep/config.py` (`stages` and
`p99_slo_ms`), since cost profiles differ wildly by layer; edit them there.

An optional per-target `cooldown_s` inserts a quiet gap **between** stages: users
ramp to 0 so slow in-flight queries drain (counted under the stage that launched
them) before the next stage starts clean, instead of bleeding into it. It defaults
to 0 (cheap KP lookups don't bleed) and is set on the expensive ARA/ARS/Pathfinder
targets.

## Watching a run

A run takes minutes to hours, so the terminal is the instrument panel. While it
works, HelmsDeep pins a live status block to the bottom of the screen:

```
── HelmsDeep · ARS (ars) ─────────────────────────────────────────────────
 run    ━━━━━━━━────────────────  33%  11m20s in · 22m40s left · ends ~18:19
 stage  2/4  30u  HOLD  ━━━━━━━━━━────  72%  1m40s left
 load   143 done · 20 in flight · 2 failed (1.4%) · 0.71 rps · conc 46.3
 latency p50 64.8s · p95 96.0s · p99 98.8s / SLO 240.0s
 ars    Done 139 · Error 1 · Timeout 1 · 0-result 1 · oldest 3m20s
──────────────────────────────────────────────────────────────────────────
```

- **run** — progress through the whole ramp, and when it ends.
- **stage** — which stage you are on, its user count, and whether it is holding
  under load (`HOLD`) or in a cooldown gap draining slow queries (`DRAIN`).
- **load / latency** — the *current stage's* numbers, live: throughput, errors
  against the shared 1% cap, and p99 against this target's SLO. These are the
  same figures the stage's row in `stages.csv` will carry.
- **ars** — async runs only: terminal statuses so far, zero-result `Done`s, and
  how long the oldest in-flight query has been polling.

Everything else scrolls above the block, including a header when a stage starts
and a verdict line when it ends:

```
▶ stage 3/4  45 users  7m00s hold  (spawn 5/s)
  ✓ stage 2 · 30u · 6m00s · n=216 · 0.71 rps · p99 98.8s ✓ · err 0.40% ✓ · conc 21.3
```

That verdict line is the knee test applied to one stage: a `✓` means the stage
met **both** the p99 SLO and the error cap, so it is a knee candidate. It always
appears directly under the header of the stage it describes (after the cooldown
note, where there is one), and it always carries that stage's final numbers —
the ones its row in `stages.csv` will carry.

The end-of-run summary table adds two **shape rows** — one character per stage,
scaled against that metric's bar and painted red where a stage went over it — so
you can see where the latency curve turns up and where errors start without
reading the numbers:

```
    p99  ▁▁▁▂▄█   (SLO 60.0s; red = over)
    err  ▁▁▁▁▂█   (cap 1.0%; red = over)
```

Because the bars are measured against the SLO (not against the tallest stage), a
run that stayed comfortably inside its budget stays visibly low rather than being
stretched to full height.

And the headline number is repeated one final time *after* Locust's own
end-of-run tables, so the answer is the last thing on screen rather than the
middle of the scrollback.

Two flags adjust this:

- `--no-live` — no redrawn block; the same status is printed as a single line
  every 30 seconds. This is also the automatic behavior when stdout is not a
  terminal (CI, a pipe, `tee` to a log file), so piped output stays readable.
  `HELMSDEEP_LIVE=0` does the same thing via the environment; `NO_COLOR` keeps
  the layout but drops the ANSI colors.
- `--verbose` — restores Locust's own output: its full request table every two
  seconds plus its INFO log. It is off by default because the table scrolls the
  status out of view within seconds, and its totals are *blended across the whole
  ramp* — the one number this tool deliberately refuses to quote. Locust's final
  tables print at the end either way.

## Shorter runs (`--time-budget` / `--quick`)

A full run is long on purpose: slow queries need long holds to accumulate enough
samples for a trustworthy percentile (`ars_mixed` takes ~70 minutes). When you
don't need that — checking a host, corpus, or config change end to end, or taking
a rough read — compress it:

```bash
# Smoke run: the whole ramp in about 10 minutes.
helmsdeep --targets ars_mixed --host https://ars.ci.transltr.io --quick

# Or name your own budget: 30m, 1h, 90s, or plain seconds.
helmsdeep --targets aras_mixed --host https://your-ara.example.org --time-budget 30m
```

`--quick` is shorthand for `--time-budget 10m`. Both scale the **durations** —
stage holds, cooldowns, and the poll/timeout caps that gate how long one query may
run — by a single factor. The **shape is untouched**: same user counts, same
stages, same SLOs, same checkpoints. So `--targets ars_mixed --time-budget 30m`
still ramps 10 → 30 → 45 → 60 users and still judges all three checkpoints; each
stage just gets ~6 minutes instead of ~15.

The CLI prints the compressed plan before it starts, so you know what you're
committing to:

```
Compressed run: 1h10m -> 30m02s (time scale 0.43), budget 30m00s
  Stages: 10u x 4m17s -> 30u x 6m26s -> 45u x 6m26s -> 60u x 6m26s, 2m09s cooldown between
  Per-query cap cut to 4m17s -- a query slower than that is recorded as a
  timeout failure, so error rates are not comparable to a full run's.
```

**What you give up.** Two things, and they matter:

1. **Sample count, and with it the tail.** A p99 over a handful of queries is
   noise. The mean and p50 hold up far better than p95/p99.
2. **What counts as a failure.** The per-query caps shrink with everything else,
   so a query that would have finished in 4 minutes is a *timeout* when the cap is
   2. Error rates — and therefore checkpoint verdicts — are not comparable to a
   full run's.

Every output is stamped with `config.time_scale` (1.0 = a full run), and the
printed summary leads with a warning banner. **Don't quote a compressed run's knee
as a capacity result, and don't gate CI on its checkpoints.**

Compression only ever speeds a run up: a budget above the target's natural
duration is ignored (a 2-hour budget leaves every target alone). Stages won't
shrink below a 30-second floor, so a budget too small to fit
them simply overruns — the CLI says so and prints the real projected duration.

> **Want each stage to get a specific amount of time?** The budget is for the
> whole run, including cooldowns, so divide accordingly — or edit the per-target
> `stages` in `config.py` directly, which is the only way to change the *shape*
> (dropping the baseline stage from a mixed run, say, to give the three
> checkpoints 10 minutes each).

## Outputs

Written to the working directory by the standalone/master node:

- `<prefix>_stages.csv` — one row per stage (overall metrics), including a
  `stage_start` column with the stage's wall-clock start time (ISO 8601 UTC)
- `<prefix>_by_qtype.csv` — one row per (stage, query type)
- `<prefix>_summary.json` — config (including which `target` was measured and the
  `time_scale` it ran at), all stages, the chosen knee, and `stage_warnings` /
  `knee_unsupported` (see
  ["Measurement quality warnings"](#measurement-quality-warnings)) (plus
  `checkpoints` for targets that define them)
- `<prefix>_checkpoints.csv` — **targets with acceptance criteria** (`aras_mixed`
  / `ars_mixed`): one row per checkpoint with its `PASS` / `FAIL` verdict
- `<prefix>_ars_health.csv` — **ARS only**: per-stage health signals (see below)
- `<prefix>_ars_queries.csv` — **ARS only**: one row per logical query with its
  **pk**, the HTTP status of each step, the terminal ARS status, any intermediate
  (retried, non-fatal) errors it hit along the way, and a ready-made URL for
  pulling that query up (see below)
- `<prefix>_ars_completion.csv` — **ARS only**: one row per logical query with its
  end-to-end response time and whether it *eventually* finished — polled past the
  `max_poll_s` failure threshold up to `completion_max_poll_s` (see below)
- `<prefix>_report.html` — Locust's native, self-contained **HTML report**
  (latency-over-time charts, request/failure tables). Open it in any browser. See
  the caveat under ["How to read the results"](#how-to-read-the-results): its
  totals are *blended across the whole run*, so the authoritative ceiling is the
  knee in `summary.json`, not the HTML aggregate.
- a printed summary table ending in the headline **max sustainable concurrency**
  (repeated once more after Locust's own end-of-run tables, so it is the last
  thing in the terminal)

For a guide to interpreting every one of these — written for non-specialists —
see ["How to read the results"](#how-to-read-the-results) below.

### Mixed capacity profile (`aras_mixed` / `ars_mixed`)

Every other target characterizes **one** query class in isolation to *find* the
knee. The mixed profile answers a different, operational question: **can the
system hold a target concurrency when both query classes arrive together, the way
they do in production?**

- **Corpus** — one blended workload: **2/3 inferred MVP1+MVP2** creative queries
  and **1/3 Pathfinder**. MVP1/MVP2 keep their relative `SHEPHERD_CORPUS` weights
  (so retuning that mix carries over); only the 2:1 split is imposed.
  `by_qtype.csv` still breaks the two classes out separately, so you can see
  which one drives the tail.
- **Ramp** — a short low-load baseline stage, then the three concurrency levels
  the profile exists to judge: **30 → 45 → 60** simultaneous users. The baseline
  exists so a failure at 30 can be read against a healthy reference (is 30 slow,
  or is *everything* slow?).
- **Verdict** — each level carries pass/fail **acceptance checkpoints** instead of
  only feeding the knee:

  | Users | Goal | Criteria |
  |-------|------|----------|
  | **30** | sustain peak load | p99 ≤ 300 000 ms **and** errors ≤ 1% |
  | **45** | headroom above peak | p99 ≤ 300 000 ms **and** errors ≤ 1% |
  | **60** | no substantial failures under overload | errors ≤ 5% (latency **not** judged — slowdown is expected here, failures are not) |

The verdicts land in `<prefix>_checkpoints.csv`, in `summary.json` (`checkpoints`
plus a single `checkpoints_passed` boolean), and in the printed report:

```
ACCEPTANCE CHECKPOINTS
 usr      p99      bar   err%    bar  verdict  goal
  30   184203   300000    0.4    1.0     PASS  sustain peak load
  45   291560   300000    0.9    1.0     PASS  headroom above peak
  60   402118      n/a    3.1    5.0     PASS  no substantial failures under overload
RESULT: all 3 checkpoints met.
```

A missed checkpoint prints why (`-> p99 331402ms > 300000ms`) and makes the run
**exit non-zero**, so it can gate a CI/acceptance job. A checkpoint whose stage
recorded no completed requests is reported as `NO DATA` rather than a silent pass.
The knee is still computed and reported alongside, unchanged.

Checkpoints are a generic per-target feature (`checkpoints` in `config.py`), not
special-cased to this profile: any target can define them, and targets that don't
behave exactly as before. Edit the users, goals, and bars there — the ARA and ARS
variants carry the same three checkpoints so the two layers are directly
comparable.

> **Layering still applies:** `ars_mixed` fans out to the ARAs, so run one or the
> other — never both at once.

### ARS async workflow and health signals

The `ars` target is asynchronous: each logical query is `POST /submit` → poll
`GET /messages/{pk}?trace=y` (every `poll_interval_s`, capped at `max_poll_s`,
default 15 min) until `status` is `Done`/`Error` → fetch `GET /messages/{merged_pk}`
and count `fields.data.message.results`. Latency is the wall-clock submit→terminal
time; one measurement is recorded per logical query. A single `ars_query` Locust
request event carries that full wall-clock, so Locust's native stats table shows
the true per-query time — the individual `ars_submit`/`ars_poll`/`ars_merge` calls
also appear there as per-step diagnostics (e.g. `ars_merge` times only the final
merge fetch, not the whole query), but aren't double-counted as the query time.

Because the ARS is what real users hit, the run also captures health signals to
flag silent downstream breakage, written to `<prefix>_ars_health.csv` and
`summary.json` (`ars_health` + a human-readable `red_flags` list):

- **result-count variation** (min/mean/max + coefficient of variation) across
  identical queries;
- **zero-result `Done`** count — by default a `Done` with 0 results is treated
  as a **failure** (counts against the error rate and the knee) and flagged; the
  per-target `zero_result_is_failure` flag can score it as a success instead (see
  ["Is an empty answer a failure?"](#is-an-empty-answer-a-failure));
- **response size** (merged-message bytes, mean/max);
- **result drop under load** — flagged when the mean result count falls sharply
  as concurrency rises across stages.

### Is an empty answer a failure?

A terminal `Done` that carries **0 results** is scored as a **failure** by
default: under load an empty answer set usually means a downstream agent silently
dropped out, which is exactly what the knee should catch. But that conflates a
silent break with a query whose answer set is legitimately empty, so it is a
per-target switch in `config.py`:

```python
"zero_result_is_failure": True,   # the default; set False to score only
                                  # transport/protocol outcomes as failures
```

With `False`, only submit errors, an `Error` status, and `Timeout` count as
failures. Note this shifts **more than the error rate**: a failed request
contributes no latency sample, so a zero-result query that stops being a failure
starts feeding the percentile pool — mean, p99, and therefore the Little's-Law
concurrency all move too. The two policies give two different knees, which is the
point: run both to see how much of your ceiling is empty answers versus broken
transport.

Whichever policy is in force, the zero-result query is still counted in
`ars_health.csv` (`zero_result_done`), still raises a red flag, and still carries
the `Done with 0 results` note in the per-query debug log. The policy itself is
recorded in `summary.json` (`config.zero_result_is_failure`) and printed above the
ARS health table, so a run's numbers can't be read under the wrong assumption:

```
ARS HEALTH (per stage)
  0-result 'Done' scored as: FAILURE (zero_result_is_failure=True)
```

### Per-query debug log (`ars_queries.csv`)

The aggregate reports tell you *that* 3% of queries failed at 45 users. This file
tells you **which ones**, and hands you what you need to go look at them: one row
per logical ARS query, written for **every** query — successful, failed, or timed
out.

| Column | What it's for |
|--------|---------------|
| `query` | Query id. **Joins to `ars_completion.csv`** on the same column. |
| `pk` | The ARS primary key. Empty only when the submit never returned one. |
| `stage`, `users` | Which stage the query is attributed to (the stage active when it reached its outcome — the same rule the per-stage stats use). |
| `qtype` | Which corpus query it was (`mvp1_heavy`, `pathfinder_drug_disease`, …). |
| `submit_start` | When it was submitted (ISO 8601 UTC) — line it up against service logs. |
| `latency_s` | Wall clock from submit to terminal status. |
| `ars_status` | `Done` / `Error` / `Timeout` / `SubmitError` / `NoPK`. |
| `failed` | Whether it counted against the error rate. A **`Done` with 0 results is `failed=True`** under the default `zero_result_is_failure` policy — see [below](#is-an-empty-answer-a-failure). |
| `submit_http`, `poll_http`, `merge_http` | HTTP status of the submit, the last poll, and the merged fetch. Separates "the ARS said no" from "the HTTP call broke". |
| `polls` | How many status polls it took — a large number on a slow query says it was genuinely working, not stuck. |
| `intermediate_error_count` | How many **non-fatal** errors the query hit on the way to its outcome (`0` = it ran clean). The poll loop retries through these, so they never decide `ars_status` — but they say a query only limped to `Done`. |
| `result_count`, `response_bytes` | Size of the merged answer. |
| `intermediate_errors` | Those same errors in words, tallied: `poll HTTP 502 x3; merge HTTP 500`. Covers polls that came back non-200 (`poll request failed` when there was no HTTP response at all), poll bodies that wouldn't parse, and a merged fetch that 500'd, went missing (`Done without merged_version`) or wouldn't parse. Empty on a clean query. |
| `error` | Why it failed, in words — the **terminal** reason, empty on success (as opposed to `intermediate_errors`, which is the trouble on the way there). Also carries `Done with 0 results` when a target scores that as a success — the note is what makes the pk worth opening either way. |
| `message_url` | Full URL to that query's message. Paste it into a browser or `curl` it. |

The printed summary also points straight at the failures, so you don't have to
open the file to start:

```
FAILED QUERIES (12 of 380; first 5 shown, all in run1_ars_queries.csv)
  stage 2 pathfinder_drug_disease [Timeout] pk=8f3c… -- no terminal status within 600s (last: Running)
    https://ars.ci.transltr.io/ars/api/messages/8f3c…?trace=y
```

Intermediate errors get their own line, because most of the queries carrying them
**succeeded** — they never appear in the failed block, and a run that only limped
to green otherwise reads as clean:

```
INTERMEDIATE ERRORS: 34 of 380 queries hit retried, non-fatal errors (intermediate_errors column in run1_ars_queries.csv)
  stage 4 mvp1_heavy [Done] pk=a01f… -- poll HTTP 502 x3
```

Useful slices, once you have the file:

```bash
# Every failed query, as a table
awk -F, 'NR==1 || $9=="True"' run1_ars_queries.csv | column -s, -t

# The pks of everything that timed out at the top stage
awk -F, '$8=="Timeout" && $4==60 {print $2}' run1_ars_queries.csv

# Slowest queries, with their URLs
sort -t, -k7 -gr run1_ars_queries.csv | head -5 | cut -d, -f2,7,8,19

# Queries that succeeded but hit non-fatal errors on the way
awk -F, 'NR==1 || ($14+0 > 0 && $9=="False")' run1_ars_queries.csv | column -s, -t
```

**On the relationship to `ars_completion.csv`:** this file records what happened
*within* the measurement window (a query not terminal by `max_poll_s` is a
`Timeout` row here, which is exactly how the knee scores it). The completion
sidecar separately answers whether that same query *eventually* finished. Join
them on `query`. A timed-out query whose extended poll was still running at
shutdown appears **here but not there** — which is precisely the query you most
want the pk of.

### ARS completion tracking (`ars_completion.csv`)

`max_poll_s` (default 6 min for `ars`) is the **failure threshold**: a query that
hasn't reached a terminal status by then is recorded as a `Timeout` failure in the
main stats and the knee — unchanged. But a timed-out query isn't necessarily
*stuck*; the ARS may still finish it, just slowly. To capture that, once a query
blows `max_poll_s` a **background poller keeps watching it** up to
`completion_max_poll_s` (default 10 min) and records the outcome to
`<prefix>_ars_completion.csv` — one row per logical query:

- `total_response_s` — true end-to-end submit→terminal time (up to
  `completion_max_poll_s`);
- `finished` — whether it ever reached a terminal status (`Done` or `Error`);
- `within_slo` — whether it finished inside `max_poll_s` (i.e. was **not** a
  Timeout failure in the main stats); a slow-but-eventually-done query is
  `finished=True, within_slo=False`;
- `status` — the terminal status (`Done`/`Error`/`Timeout`/`SubmitError`/`NoPK`).

`summary.json` carries a `completion` roll-up (finished, finished-within-SLO,
finished-after-SLO, never-finished, submit-failed), and two red flags surface the
key distinctions: queries that **exceeded the SLO but did finish** (slow, not
stuck) and queries that **never finished** even within `completion_max_poll_s`.
This separates *slow* from *broken* — a service that eventually answers every
query is degraded differently than one that drops them. Set
`completion_max_poll_s` equal to (or omit it, defaulting to) `max_poll_s` to turn
extended tracking off; the sidecar then just marks each timed-out query
`finished=False`. Any query still being polled when the test ends is drained for a
bounded window at shutdown; ones still unfinished after that are absent from the
file.

## How to read the results

This section is for anyone — technical or not — who has a run's output files in
hand and wants to answer one question: **how much load can this service take?**

### Plain-language key

A few terms show up everywhere in the outputs. Here's what each one means,
without the jargon:

| Term | What it means |
|------|---------------|
| **Stage** | One step of the ramp. Each stage pins a fixed number of simultaneous users for a fixed time, then the next stage adds more. The run climbs stage by stage until the service struggles. |
| **Users / concurrency** | How many simultaneous users are hammering the service. "Users" is what we *asked for* in a stage; **concurrency** is the effective number actually in flight, computed from the measured throughput and speed. |
| **RPS** | Requests Per Second — how many queries the service actually completed each second during that stage. Higher is faster. |
| **Latency** | How long one query took, in **milliseconds** (1000 ms = 1 second). |
| **mean / p50 / p95 / p99** | Different ways to summarize latency. *mean* is the average. *p50* (median) is the typical query. *p95* / *p99* are the slow tail: "95% (or 99%) of queries were at least this fast." p99 is the one we hold to a standard, because it captures the bad experiences, not just the average. |
| **Error rate** | The fraction of queries that failed (e.g. `0.02` = 2%). For the ARS, a query that finishes but returns **zero answers** also counts as a failure by default — a per-target flag can change that, and the run says which policy applied. |
| **SLO** | Service Level Objective — the line in the sand for "acceptable." Here it's a p99 latency cap (e.g. `60000` ms = 60 s) plus a max error rate (default **1%**). A stage "passes" only if it stays under both. |
| **Checkpoint** | A pass/fail question asked of one specific load level ("does 30 simultaneous users still work?"), with its own latency/error bars. Only the mixed capacity profile defines these; other runs just report the knee. |
| **Knee** | The highest-load stage that still passes the SLO. It's the headline result: the most simultaneous users the service handled while staying fast enough and reliable enough. Past the knee, things fall apart. |

### Start here: `summary.json`

Open `<prefix>_summary.json` first. Two fields tell you almost everything:

- **`max_sustainable_concurrency`** — the headline number. This is the answer to
  "how much can this service take?"
- **`knee`** — the stage that number came from (its users, latency, error rate,
  etc.). This is the *last healthy stage* of the ramp.

If **`knee` is `null`** (and `max_sustainable_concurrency` is empty), the service
**failed even at the lightest load** — it was already too slow or too error-prone
in the very first stage. That's a strong signal something is wrong (or the SLO is
set tighter than the service can ever meet).

The same file also echoes the **`config`** that produced the run (which target,
which endpoint, the SLO, the ramp stages) so a result is self-documenting.

On a run with acceptance criteria (`aras_mixed` / `ars_mixed`), two more fields
answer the pass/fail question directly: **`checkpoints_passed`** (one boolean for
the whole run) and **`checkpoints`** (per-level verdicts and why). See
["Mixed capacity profile"](#mixed-capacity-profile-aras_mixed--ars_mixed).

### Reading `stages.csv` — the story of the ramp

`<prefix>_stages.csv` has one row per stage and shows the service degrading as
load climbs. Here's an illustrative example (SLO: p99 ≤ 60 000 ms, errors ≤ 1%):

| stage | users | rps  | mean_ms | p99_ms | error_rate | concurrency |
|-------|-------|------|---------|--------|------------|-------------|
| 1     | 5     | 4.8  | 1 040   | 2 100  | 0.000      | 5.0         |
| 2     | 10    | 9.1  | 1 090   | 3 400  | 0.000      | 9.9         |
| 3     | 20    | 17.0 | 1 170   | 9 800  | 0.004      | 19.9        |
| 4     | 40    | 22.0 | 1 800   | 41 000 | 0.008      | **39.6** ← knee |
| 5     | 80    | 19.0 | 4 200   | 78 000 | 0.140      | —           |

How to read it:

- **Top to bottom = more load.** Each stage adds users.
- **Watch two columns: `p99_ms` and `error_rate`.** As long as both stay under
  the SLO (here 60 000 ms and 0.01), the service is coping.
- **The knee is the last "green" row** — stage 4 above. Its `concurrency` (≈ 40)
  is the headline number.
- **The row after the knee shows the cliff:** at stage 5, p99 latency blew past
  the 60 s cap *and* the error rate jumped to 14%. Notice RPS actually *dropped*
  (19 vs 22) even though we added users — a classic sign the service is
  saturated and thrashing, not going faster.

A healthy run shows latency rising gently and errors near zero until a clear
knee, then a sharp cliff. If the very first row already breaks the SLO, you get
no knee (see above).

> **Why not just trust Locust's own totals?** During a ramp, an overall average
> would blend the easy early stages with the saturated late ones and hide the
> knee. That's why HelmsDeep reports **per stage**. (It's also why the HTML
> report's blended totals aren't the official number — see below.)

### `by_qtype.csv` — which query is the bottleneck

`<prefix>_by_qtype.csv` breaks each stage down by *type* of query (e.g. simple
single-hop lookups vs. heavier multi-hop or "what treats this disease?"
queries). Use it to answer "what's slowing things down?" — often one query
shape saturates long before the others, and that's where to focus.

### `ars_health.csv` and `red_flags` (ARS runs only)

The ARS is what real users actually hit, and it can fail *silently* — a query
can come back "successful" but with **fewer answers than it should**, or zero.
`<prefix>_ars_health.csv` and the **`red_flags`** list in `summary.json` exist to
catch exactly this. In plain terms they flag things like:

- **Answers quietly disappearing under load** — the average number of results per
  query drops sharply as concurrency rises (something downstream is dropping out).
- **"Done" but empty** — queries that finished but returned zero answers (counted
  as failures).
- **Wild result-count swings** — identical queries returning very different
  amounts, a sign of instability.

If `red_flags` is non-empty, the service may be degrading in a way the raw
latency/error numbers alone wouldn't reveal — read those flags.

### Measurement quality warnings

A stage's row can be arithmetically correct and still not mean what it looks
like. The summary flags two conditions the table itself cannot show, and repeats
the flag on the headline when the knee rests on a flagged stage:

```
MEASUREMENT QUALITY (3 of 7 stage(s) flagged)
  stage 0 (5u, n=15): p99 rests on the 2 slowest of 15 request(s)
                      effective concurrency 0.9 is only 18% of 5 users -- most
                      queries did not finish inside the stage, so they landed in
                      the next one
```

- **Too few requests for the percentile.** At `n` samples the p99 interpolates
  around index `(n-1) * 0.99`, so below ~100 completed requests it rests on the
  two slowest requests in the stage and moves with a single outlier. A stage that
  reports `p99 103.5s` off 16 requests is reporting one query.
- **Effective concurrency far below the user count.** Users are closed-loop with
  no think time (`wait_time = constant(0)`), so Little's Law concurrency should
  track the stage's user count. When it is under half of it, most users spent the
  stage blocked on queries that never finished inside it — and since requests are
  bucketed by *finish* time, those completions land in the **next** stage. That
  contaminates both: the giveaway is a later stage looking *faster* than an
  earlier one with fewer users.

The remedies live in the target's entry in `config.py`: **longer stage holds**
(more samples per stage) for the first, a **`cooldown_s` drain gap** (so slow
queries finish inside the stage that launched them) for the second. The printed
block names whichever applies.

`summary.json` carries the same information as `stage_warnings` and
`knee_unsupported`, with each issue tagged `thin_samples` or `in_flight_bleed`,
so a script can gate on it without parsing prose.

> This is why the `kps` target holds each stage for **5 minutes** with a 60s
> cooldown, rather than the 60s holds it carried when the KP layer was ~40
> independent services answering in milliseconds. Retriever answers in seconds to
> tens of seconds; at 60s holds every stage tripped both flags.

### The HTML report (`<prefix>_report.html`)

This is Locust's built-in report. **Just double-click it to open in a browser.**
It gives you nice visuals: response-time and request-rate charts over the whole
run, a table of every request type, and a list of any failures. It's great for a
quick visual feel and for sharing a screenshot.

> **Important caveat:** the HTML report's headline totals are **blended across
> the entire run** — it averages the gentle early stages together with the
> overwhelmed late ones. That makes its single "average response time" / "total
> RPS" numbers *not* the right ones to quote. **For the official ceiling, always
> read the knee in `summary.json` (and the per-stage story in `stages.csv`).**
> Think of the HTML as the pictures, and `summary.json`/`stages.csv` as the
> verdict.

## Tuning notes

- **Swap the CURIEs.** The corpus in `helmsdeep/trapi_corpus.py`
  uses a few real MONDO/CHEBI entities; replace them with entities your target
  service actually knows about, or queries return empty and won't reflect real
  cost.
- **Tier is per query (Retriever only).** Retriever exposes `parameters.tier`
  (0 or 1) to pick its backend graph. `RETRIEVER_CORPUS` pairs multi-hop shapes
  with tier 0 and single-hop shapes with tier 1. ARA queries carry no tier.
- **Shepherd/ARS send inferred + bypass_cache, mixing MVP1 and MVP2.**
  `SHEPHERD_CORPUS` (also used for `ars`) holds creative-mode queries
  (`knowledge_type: "inferred"`, `bypass_cache: true`) split evenly between two
  Translator templates, with entities varied per request to spread load and avoid
  cache-warming. `by_qtype.csv` breaks out latency per template/tier.
  - **MVP1 — "what treats disease X?"** (`chemical -[treats]-> disease`): the
    pinned disease is sampled from size-tiered pools (heavy/medium/light), so
    cost tracks answer-set size. Heavy is a curated list of common disease hubs;
    the long-tail pool is ~1000 real MONDO CURIEs in `curie_list.json`.
  - **MVP2 — chemical⇄gene "affects"** (`biolink:affects`, `inferred`, with
    `object_aspect`/`object_direction` qualifiers): both edge directions
    (chemical→gene and gene→chemical), with the gene (curated `GENES` pool) and
    qualifier combo varied per request.

  These are far heavier than KP lookups, so the `aras` target ships a gentler
  ramp and a looser `p99_slo_ms` (see `config.py`). Tune the per-template weights
  and entity pools (`HEAVY_DISEASES`, `GENES`, the tiers) to your real traffic.
- **ARS reuses the Shepherd corpus** (`ARS_CORPUS = SHEPHERD_CORPUS`) — the same
  inferred query the ARS fans out to its ARAs. Its poll cadence and per-query
  timeout (`poll_interval_s`, `max_poll_s`) are tunable in `config.py`.
- **Pathfinder is its own run type** (`aras_pathfinder` / `ars_pathfinder`, ARA/ARS
  only). `PATHFINDER_CORPUS` sends a single drug↔disease shape that pins **two**
  endpoints and asks for connecting paths (a `paths` map in the query_graph, not
  `edges`); the `(chemical, disease)` pair varies per request from a curated
  `CHEM_DISEASE_PAIRS` list — **swap these** for pairs your service knows, and keep
  them plausibly connected so paths come back non-empty (on ARS a zero-result
  `Done` is a failure by default). It's the heaviest query class, so its targets ship the
  gentlest ramps and loosest SLOs (and, for `ars_pathfinder`, the longest
  `max_poll_s`); tune them in `config.py`.
- **The mixed profile blends the two ARA/ARS classes** (`aras_mixed` /
  `ars_mixed`). `MIXED_CORPUS` is *computed* from `SHEPHERD_CORPUS` +
  `PATHFINDER_CORPUS` at the `INFERRED_PATHFINDER_RATIO` (2:1), so it tracks any
  retuning of the inferred mix. Change the blend by editing that ratio; change
  what counts as a pass by editing the per-target `checkpoints` in `config.py`.
  Because 1/3 of the mix is Pathfinder — the heaviest class — and p99 is a tail
  statistic, both mixed targets inherit the looser Pathfinder SLO. `aras_mixed`
  also raises `request_timeout_s` above the default 210 s: a sync target whose p99
  SLO sits near the HTTP timeout records slow queries as client timeouts (errors)
  instead of latencies.
- **Adjust corpus weights** in `RETRIEVER_CORPUS` / `SHEPHERD_CORPUS` to match
  your traffic mix.

## Running the engine directly

You can also invoke the locustfile without the CLI (defaults to the `kps`
target):

```bash
LOCUST_CSV_PREFIX=run1 \
locust -f helmsdeep/trapi_loadtest.py --headless \
    --host https://your-retriever-service.example.org \
    --html run1_report.html
```

The output-file prefix comes from the `LOCUST_CSV_PREFIX` env var (locust has no
`--csv-prefix` flag). Set `LOADTEST_TARGET` to choose a different (implemented)
layer. Add `--html <name>.html` yourself to get the HTML report (the `helmsdeep`
CLI passes this for you automatically, as `<prefix>_report.html`).

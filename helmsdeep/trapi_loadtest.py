"""
TRAPI step-load test for NCATS Translator services.

Runs a STEPPED ramp of concurrent users. For each stage it captures:
  - RPS, mean / p50 / p95 / p99 latency, error rate
  - effective concurrency (Little's Law: RPS * mean_latency_seconds)
  - the same metrics broken out per query type (qtype)

It then determines the "knee" = the highest stage where BOTH
  p99 <= P99_SLO_MS  AND  error_rate <= MAX_ERROR_RATE
hold. That stage's effective concurrency is your max sustainable concurrency.

The component under test is chosen by the LOADTEST_TARGET env var (see config.py).
Sync targets (KP/ARA) POST once; the async ARS target submits then polls
/messages/{pk} until terminal and fetches the merged message, additionally
capturing health signals (result-count variation, zero-result "Done" responses,
response size, result-drop-under-load) and surfacing them as red flags.

A target may also configure acceptance ``checkpoints`` (config.py): pass/fail
criteria at named concurrency levels, judged against the stage that ran them.
That turns a run from "find the knee" into "does the system hold N concurrent
queries?" -- the knee is still computed. A missed checkpoint exits non-zero.

Outputs:
  - <prefix>_stages.csv         one row per stage (overall)
  - <prefix>_by_qtype.csv       one row per (stage, qtype)
  - <prefix>_checkpoints.csv    targets with acceptance criteria: one row per
                                checkpoint with its PASS/FAIL verdict
  - <prefix>_ars_health.csv     ARS only: per-stage health signals
  - <prefix>_ars_queries.csv    ARS only: one row per logical query -- pk, the
                                HTTP status of each step, terminal ARS status,
                                any intermediate (retried, non-fatal) errors hit
                                along the way, and a URL to pull that query up
                                for debugging
  - <prefix>_ars_completion.csv ARS only: per-query end-to-end response time +
                                whether it eventually finished (polled past the
                                max_poll_s failure threshold up to
                                completion_max_poll_s)
  - <prefix>_summary.json       config, all stages, the knee (+ checkpoints,
                                ars_health/completion/red_flags)

Usage (headless, recommended for reproducible numbers):

  locust -f trapi_loadtest.py --headless \
      --host https://your-trapi-service.example.org \
      --csv-prefix run1            # optional; falls back to LOCUST_CSV_PREFIX env

The LoadTestShape drives users/duration, so you do NOT pass -u / -r / -t.
Per-target load/SLO and the ARS poll knobs live in config.py.
"""

import itertools
import json
import os
import statistics
import time
from collections import defaultdict

import gevent
from locust import HttpUser, task, constant, events
from locust import LoadTestShape
from locust.runners import MasterRunner, WorkerRunner

import config
import console
from trapi_corpus import corpus_for

# ----------------------------------------------------------------------------
# Configuration. Per-target load + SLO live in config.py (cost profiles differ
# wildly by layer); edit them there. REQUEST_TIMEOUT is shared.
# ----------------------------------------------------------------------------
# Which Translator layer this run targets (one layer per run; see CLAUDE.md).
# Set by the helmsdeep CLI; defaults so `locust -f` works directly.
TARGET = os.environ.get("LOADTEST_TARGET", config.DEFAULT_TARGET)
_TGT = config.TARGETS[TARGET]

# Optional wall-clock budget (--time-budget / --quick on the CLI). Compresses the
# stage holds, cooldowns, and poll/timeout caps to fit; the ramp itself -- user
# counts, SLOs, checkpoints -- is untouched. TIME_SCALE is 1.0 for a full run and
# is stamped on the outputs, because a compressed run trades away the sample
# count its percentiles and error rates rest on (see config.time_scaled).
_BUDGET_S = os.environ.get("HELMSDEEP_TIME_BUDGET_S")
TIME_SCALE = 1.0
if _BUDGET_S:
    _TGT, TIME_SCALE = config.time_scaled(_TGT, float(_BUDGET_S))

ENDPOINT = _TGT["endpoint"]    # request path for this component
CORPUS = corpus_for(_TGT["corpus"])   # query subset for this component
STAGES = _TGT["stages"]               # per-target step-load ramp
P99_SLO_MS = _TGT["p99_slo_ms"]       # per-target knee threshold
MAX_ERROR_RATE = config.MAX_ERROR_RATE   # shared error-rate cap
# Per individual HTTP call. Default 210s; a target whose p99 SLO sits near or
# above that raises it (otherwise slow queries land as client timeouts, i.e.
# errors, and the latency they would have reported is never measured).
REQUEST_TIMEOUT = _TGT.get("request_timeout_s", 210)

# Optional pass/fail acceptance criteria at named concurrency levels (see
# config.py). Empty for the plain find-the-knee targets.
CHECKPOINTS = _TGT.get("checkpoints", [])

# ARS is asynchronous (submit -> poll /messages/{pk} -> fetch merged). These are
# unused by the sync (KP/ARA) path.
PROTOCOL = _TGT.get("protocol", "sync")
MESSAGES_PATH = _TGT.get("messages_endpoint", "/messages")
POLL_INTERVAL_S = _TGT.get("poll_interval_s", 10)
MAX_POLL_S = _TGT.get("max_poll_s", 900)
# Extended cap (>= MAX_POLL_S) for the completion sidecar: after a query blows
# MAX_POLL_S (already a Timeout failure in the main stats), a background greenlet
# keeps polling up to here to record whether it *eventually* finishes. Defaults
# to MAX_POLL_S, i.e. no extended tracking.
COMPLETION_MAX_POLL_S = max(_TGT.get("completion_max_poll_s", MAX_POLL_S), MAX_POLL_S)
# Whether a terminal "Done" carrying 0 results scores as a failure (and so counts
# against the knee). Defaults True: an empty answer set under load usually means a
# downstream agent silently dropped out. When False, only transport/protocol
# outcomes (submit error, Error status, Timeout) fail, and the zero-result query's
# latency joins the percentile pool instead of being discarded -- but it is still
# tallied in ars_health and still raises a red flag.
ZERO_RESULT_IS_FAILURE = _TGT.get("zero_result_is_failure", False)

# Detached greenlets still polling timed-out queries for the completion sidecar;
# on_test_stop drains them (bounded) so their rows make it into the file.
_COMPLETION_GREENLETS = []

# Optional quiet gap between stages (users ramp to 0) so slow in-flight queries
# drain into the stage that launched them instead of bleeding into the next one.
COOLDOWN_S = _TGT.get("cooldown_s", 0)

CSV_PREFIX = os.environ.get("LOCUST_CSV_PREFIX", "trapi_run")

# Weighted, flattened corpus for O(1)-ish random selection.
import random
_FLAT = []
for qtype, builder, weight in CORPUS:
    _FLAT.extend([(qtype, builder)] * weight)


# ----------------------------------------------------------------------------
# Per-query intermediate errors (ARS only).
# ----------------------------------------------------------------------------
class QueryIssues:
    """Ordered tally of the SURVIVABLE problems hit while running one logical ARS
    query: polls that came back non-200 (or with a body we could not parse) and
    a merge fetch that misbehaved. The poll loop retries through all of these, so
    none of them decide the query's outcome -- but a "Done" that needed thirty
    retried 502s along the way is not the same measurement as a clean one, and
    the per-query debug log is where you want to see that.
    """

    def __init__(self):
        self._counts = {}   # label -> times seen, in first-seen order

    def add(self, label):
        self._counts[label] = self._counts.get(label, 0) + 1

    @property
    def count(self):
        """Total intermediate-error events (0 == the query ran clean)."""
        return sum(self._counts.values())

    def summary(self):
        """Compact one-cell rendering, e.g. 'poll HTTP 502 x3; merge HTTP 500'."""
        return "; ".join(
            label if n == 1 else f"{label} x{n}"
            for label, n in self._counts.items()
        )


# ----------------------------------------------------------------------------
# Per-stage metric collection.
#
# We can't trust Locust's single aggregate run stats because we ramp load --
# an aggregate p99 would blend easy early stages with saturated late ones.
# So we bucket every completed request into the stage that was active when it
# *finished*, keyed by the shape's stage index.
# ----------------------------------------------------------------------------
class StageCollector:
    def __init__(self):
        # stage_idx -> qtype -> list of latencies (ms); qtype "__all__" = overall
        self.samples = defaultdict(lambda: defaultdict(list))
        self.errors = defaultdict(lambda: defaultdict(int))
        self.requests = defaultdict(lambda: defaultdict(int))
        self.stage_idx = 0
        self.stage_started = {}   # stage_idx -> wall-clock start
        self.stage_ended = {}
        # ARS-only health signals (populated by the async path).
        self.result_counts = defaultdict(lambda: defaultdict(list))   # answers per query
        self.response_bytes = defaultdict(lambda: defaultdict(list))  # merged-msg size
        self.statuses = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # status -> n
        # ARS completion sidecar: one row per logical query -- did it eventually
        # finish (polled up to COMPLETION_MAX_POLL_S, beyond the max_poll_s
        # failure threshold) and its true end-to-end response time.
        self.completions = []
        # ARS per-query debug log: one row per logical query, carrying the pk and
        # the status codes needed to pull that exact query up afterwards.
        self.queries = []
        # Ids are handed out at submit time and shared by both per-query files,
        # so a row in one joins to its row in the other.
        self._next_query_id = itertools.count()
        # Logical queries currently in flight: token -> (start_ts, qtype). Read
        # only by the live terminal display ("20 in flight, oldest 3m20s"); no
        # report or metric depends on it.
        self.inflight = {}
        self._next_inflight = itertools.count()

    def begin_inflight(self, qtype):
        token = next(self._next_inflight)
        self.inflight[token] = (time.time(), qtype)
        return token

    def end_inflight(self, token):
        self.inflight.pop(token, None)

    def new_query_id(self):
        return next(self._next_query_id)

    def record_query(self, row):
        """Append one ARS per-query debug row (pk + status codes + outcome)."""
        self.queries.append(row)

    def mark_stage(self, idx):
        if idx != self.stage_idx:
            # setdefault (not =) so a cooldown that already froze this stage's end
            # time isn't overwritten when the next stage begins.
            self.stage_ended.setdefault(self.stage_idx, time.time())
        self.stage_idx = idx
        self.stage_started.setdefault(idx, time.time())

    def end_active_stage(self):
        # Called at cooldown start: freeze the just-finished stage's end time so
        # its duration reflects the active-load window, not the drain period.
        self.stage_ended.setdefault(self.stage_idx, time.time())

    def record(self, qtype, latency_ms, failed, *,
               status=None, result_count=None, response_bytes=None):
        s = self.stage_idx
        self.requests[s][qtype] += 1
        self.requests[s]["__all__"] += 1
        if failed:
            self.errors[s][qtype] += 1
            self.errors[s]["__all__"] += 1
        else:
            self.samples[s][qtype].append(latency_ms)
            self.samples[s]["__all__"].append(latency_ms)
        # Optional ARS health signals (recorded regardless of pass/fail so a
        # zero-result "Done" still shows up in the distributions).
        if status is not None:
            self.statuses[s][qtype][status] += 1
            self.statuses[s]["__all__"][status] += 1
        if result_count is not None:
            self.result_counts[s][qtype].append(result_count)
            self.result_counts[s]["__all__"].append(result_count)
        if response_bytes is not None:
            self.response_bytes[s][qtype].append(response_bytes)
            self.response_bytes[s]["__all__"].append(response_bytes)

    def record_completion(self, *, query_id, stage, qtype, total_ms, finished,
                          status, start_ts):
        """Record one ARS completion-sidecar row. `finished` == the query reached
        a terminal status (Done or Error); the `status` column preserves which.
        Purely a sidecar -- never feeds the per-stage stats or the knee.
        """
        self.completions.append({
            # Same id as this query's row in the per-query debug log, so the two
            # files join (a detached extended poll lands here out of order).
            "query": query_id,
            "stage": stage,
            "qtype": qtype,
            "submit_start": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                          time.gmtime(start_ts)),
            "total_response_s": round(total_ms / 1000.0, 2),
            "finished": finished,
            # Whether it finished inside the max_poll_s SLO window (i.e. was NOT a
            # Timeout in the main stats); slow-but-eventually-done => False.
            "within_slo": bool(finished and total_ms <= MAX_POLL_S * 1000.0),
            "status": status,
        })


COLLECTOR = StageCollector()


def _pct(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _stage_stats(idx, qtype):
    lat = COLLECTOR.samples[idx][qtype]
    n_req = COLLECTOR.requests[idx][qtype]
    n_err = COLLECTOR.errors[idx][qtype]
    started = COLLECTOR.stage_started.get(idx)
    ended = COLLECTOR.stage_ended.get(idx, time.time())
    dur = max(ended - started, 1e-9) if started else 1e-9
    rps = n_req / dur
    mean = sum(lat) / len(lat) if lat else 0.0
    # Wall-clock stage start, ISO 8601 UTC (e.g. 2026-06-10T14:30:05Z).
    stage_start = (
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)) if started else ""
    )
    return {
        "stage": idx,
        "qtype": qtype,
        "users": STAGES[idx][0] if idx < len(STAGES) else None,
        "stage_start": stage_start,
        "requests": n_req,
        "errors": n_err,
        "error_rate": (n_err / n_req) if n_req else 0.0,
        "duration_s": round(dur, 2),
        "rps": round(rps, 3),
        "mean_ms": round(mean, 2),
        "p50_ms": round(_pct(lat, 50), 2),
        "p95_ms": round(_pct(lat, 95), 2),
        "p99_ms": round(_pct(lat, 99), 2),
        # Little's Law: concurrency = throughput * latency(seconds)
        "concurrency": round(rps * (mean / 1000.0), 2),
    }


def _ars_stage_health(idx, qtype="__all__"):
    """ARS-only per-stage health signals derived from the collector.

    Result counts/bytes are recorded for every query that reached a merged
    message (including zero-result Done), so the distributions capture answers
    thinning out or sizes drifting under load -- signals that something
    downstream broke even when the request itself "succeeded".
    """
    counts = COLLECTOR.result_counts[idx][qtype]
    nbytes = COLLECTOR.response_bytes[idx][qtype]
    statuses = COLLECTOR.statuses[idx][qtype]
    mean = (sum(counts) / len(counts)) if counts else 0.0
    cv = (statistics.pstdev(counts) / mean) if (counts and mean) else 0.0
    return {
        "stage": idx,
        "requests": COLLECTOR.requests[idx][qtype],
        "done": statuses.get("Done", 0),
        "errored": statuses.get("Error", 0),
        "timed_out": statuses.get("Timeout", 0),
        "submit_failed": statuses.get("SubmitError", 0) + statuses.get("NoPK", 0),
        "zero_result_done": sum(1 for c in counts if c == 0),
        "results_min": min(counts) if counts else 0,
        "results_mean": round(mean, 2),
        "results_max": max(counts) if counts else 0,
        "results_cv": round(cv, 3),
        "bytes_mean": round(sum(nbytes) / len(nbytes), 1) if nbytes else 0,
        "bytes_max": max(nbytes) if nbytes else 0,
    }


# A stage's numbers can be arithmetically correct and still not mean what they
# look like. Two conditions make a row untrustworthy, and both are invisible in
# the table itself:
#
#  - Too few completed requests for the percentile we print. At n samples the
#    p99 interpolates around index (n-1)*0.99, so below ~100 requests it rests
#    on the two slowest requests in the stage and moves with a single outlier.
#  - Little's Law concurrency far below the stage's user count. Users are
#    closed-loop with no think time, so effective concurrency should track the
#    user count; when it doesn't, most users spent the stage blocked on queries
#    that never finished inside it. Those completions are bucketed into the NEXT
#    stage (we bucket by finish time), which inflates its tail and deflates
#    this one -- the classic symptom is a later stage looking *faster* than an
#    earlier one. A cooldown_s drain gap is the fix; longer holds help too.
MIN_P99_SAMPLES = 100
MIN_CONCURRENCY_RATIO = 0.5


def _stage_quality(row):
    """Reasons this stage's row should not be read at face value (may be empty).

    Each issue is ``{"kind": ..., "detail": ...}``; the kind is what decides
    which remedy the summary recommends (and lets a script branch on it without
    parsing prose).
    """
    issues = []
    n = row["requests"]
    if n < MIN_P99_SAMPLES:
        # How many completed requests actually sit at or above the p99 index.
        top = n - int((n - 1) * 0.99) if n else 0
        issues.append({
            "kind": "thin_samples",
            "detail": (f"p99 rests on the {top} slowest of {n} request(s)" if n
                       else "no completed requests"),
        })
    users = row["users"]
    if users and n and row["concurrency"] < users * MIN_CONCURRENCY_RATIO:
        issues.append({
            "kind": "in_flight_bleed",
            "detail": (f"effective concurrency {row['concurrency']:.1f} is only "
                       f"{row['concurrency'] / users:.0%} of {users} users -- "
                       f"most queries did not finish inside the stage, so they "
                       f"landed in the next one"),
        })
    return issues


def _evaluate_checkpoints(overall_rows):
    """Judge each configured checkpoint against the stage that ran its user count.

    A checkpoint asks a pass/fail question ("does 30 concurrent hold?") rather
    than the knee's open one ("how far can we go?"), so each one carries its own
    bars: ``p99_slo_ms`` (omitted = the target's, ``None`` = latency not judged,
    for an overload probe where slowdown is expected but failures are not) and
    ``max_error_rate`` (omitted = the shared cap).
    """
    # Match on user count; if two stages ran the same count, the later one wins.
    by_users = {r["users"]: r for r in overall_rows if r["users"] is not None}
    results = []
    for cp in CHECKPOINTS:
        users = cp["users"]
        p99_bar = cp.get("p99_slo_ms", P99_SLO_MS)
        err_bar = cp.get("max_error_rate", MAX_ERROR_RATE)
        row = by_users.get(users)
        base = {
            "users": users,
            "goal": cp.get("goal", ""),
            "p99_slo_ms": p99_bar,
            "max_error_rate": err_bar,
        }
        if row is None or not row["requests"]:
            results.append({**base, "stage": row["stage"] if row else None,
                            "requests": row["requests"] if row else 0,
                            "p99_ms": None, "error_rate": None, "concurrency": None,
                            "verdict": "NO DATA",
                            "detail": f"no completed requests at {users} users"})
            continue
        misses = []
        if p99_bar is not None and row["p99_ms"] > p99_bar:
            misses.append(f"p99 {row['p99_ms']:.0f}ms > {p99_bar}ms")
        if row["error_rate"] > err_bar:
            misses.append(f"errors {row['error_rate'] * 100:.2f}% > "
                          f"{err_bar * 100:.2f}%")
        results.append({
            **base,
            "stage": row["stage"],
            "requests": row["requests"],
            "p99_ms": row["p99_ms"],
            "error_rate": row["error_rate"],
            "concurrency": row["concurrency"],
            "verdict": "FAIL" if misses else "PASS",
            "detail": "; ".join(misses) if misses else "within limits",
        })
    return results


# ----------------------------------------------------------------------------
# The user: picks a weighted TRAPI query, POSTs it, records by qtype.
# Sync targets (KP/ARA) POST once; the ARS target submits then polls/merges.
# ----------------------------------------------------------------------------
class TRAPIUser(HttpUser):
    wait_time = constant(0)   # closed-loop; shape controls concurrency

    @task
    def query(self):
        # A cooldown is a DRAIN, not a quiet stage: the users still alive are
        # only waiting to be stopped, so none of them may start new work. Locust
        # cannot enforce that on its own -- it polls the shape about once a
        # second and its stop lands asynchronously after that -- so without this
        # gate a user whose query returns inside the gap immediately fires off
        # another one, which lands in the drain (inflating the RPS of the stage
        # whose window was already frozen) or bleeds into the next stage.
        if _draining():
            gevent.sleep(DRAIN_RECHECK_S)
            return
        qtype, builder = random.choice(_FLAT)
        payload = builder()
        token = COLLECTOR.begin_inflight(qtype)
        try:
            if PROTOCOL == "async":
                self._run_ars(qtype, payload)
            else:
                self._run_sync(qtype, payload)
        finally:
            COLLECTOR.end_inflight(token)

    def _run_sync(self, qtype, payload):
        """KP/ARA: a single blocking POST; success == HTTP 200."""
        # name= groups all requests of a qtype under one label in Locust's UI
        with self.client.post(
            ENDPOINT,
            json=payload,
            name=qtype,
            timeout=REQUEST_TIMEOUT,
            catch_response=True,
        ) as resp:
            failed = False
            # malformed_query is expected to 4xx -- treat that as a successful
            # measurement of the error path, not a load-test failure.
            if qtype == "malformed_query":
                if resp.status_code < 500:
                    resp.success()
                else:
                    failed = True
                    resp.failure(f"server error {resp.status_code}")
            else:
                if resp.status_code == 200:
                    resp.success()
                else:
                    failed = True
                    resp.failure(f"status {resp.status_code}")
            latency_ms = resp.request_meta["response_time"] or 0.0
            COLLECTOR.record(qtype, latency_ms, failed)

    def _record_ars(self, qtype, latency_ms, failed, exc_msg=None, **health):
        """Record one logical ARS query: into the per-stage COLLECTOR (drives
        HelmsDeep's own reports) AND as a single Locust request event named
        'ars_query', so the native stats table shows the full submit->terminal
        wall-clock -- not just the final ars_merge GET.
        """
        COLLECTOR.record(qtype, latency_ms, failed, **health)
        self.environment.events.request.fire(
            request_type="ARS",
            name="ars_query",
            response_time=latency_ms,
            response_length=health.get("response_bytes") or 0,
            exception=(exc_msg if failed else None),
            context={"qtype": qtype},
        )

    def _record_completion(self, query_id, qtype, start, finished, status, stage):
        """Append one completion-sidecar row for a logical ARS query."""
        COLLECTOR.record_completion(
            query_id=query_id, stage=stage, qtype=qtype,
            total_ms=(time.time() - start) * 1000.0,
            finished=finished, status=status, start_ts=start,
        )

    def _message_url(self, pk):
        """Full URL for pulling one ARS query up by hand (curl / browser)."""
        return f"{self.host.rstrip('/')}{MESSAGES_PATH}/{pk}?trace=y" if pk else ""

    def _record_query(self, query_id, qtype, pk, start, ars_status, *,
                      failed, error=None, submit_http=None, poll_http=None,
                      merge_http=None, polls=0, result_count=None,
                      response_bytes=None, stage=None, issues=None):
        """Append one row to the ARS per-query debug log.

        Written for EVERY logical query, terminal or not -- the failures are the
        rows you actually want when debugging, and they are exactly the ones the
        completion sidecar can be missing.
        """
        # Attributed to the stage active when the query REACHED ITS OUTCOME, the
        # same rule the per-stage stats bucket by -- so a slow query's row lines
        # up with the stage whose numbers it moved.
        stage = COLLECTOR.stage_idx if stage is None else stage
        COLLECTOR.record_query({
            "query": query_id,
            "pk": pk or "",
            "stage": stage,
            "users": STAGES[stage][0] if stage < len(STAGES) else "",
            "qtype": qtype,
            "submit_start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start)),
            "latency_s": round(time.time() - start, 2),
            "ars_status": ars_status,
            "failed": failed,
            "submit_http": submit_http if submit_http is not None else "",
            "poll_http": poll_http if poll_http is not None else "",
            "merge_http": merge_http if merge_http is not None else "",
            "polls": polls,
            # Intermediate = non-fatal and retried past: the query still reached
            # whatever `ars_status` says. `error` is the terminal reason (empty on
            # success); these two are the trouble on the way there, so a clean
            # Done and a Done that fought through poll 502s are distinguishable.
            "intermediate_error_count": issues.count if issues else 0,
            "result_count": result_count if result_count is not None else "",
            "response_bytes": response_bytes if response_bytes is not None else "",
            "intermediate_errors": issues.summary() if issues else "",
            "error": error or "",
            "message_url": self._message_url(pk),
        })

    def _extended_poll(self, query_id, qtype, pk, start, stage):
        """Background greenlet: a query already blew MAX_POLL_S (and is already a
        Timeout failure in the main stats). Keep polling up to
        COMPLETION_MAX_POLL_S to record, in the sidecar only, whether it
        *eventually* reaches a terminal status and its true end-to-end time.
        Never touches the per-stage stats, the ars_query event, or the knee.
        """
        deadline = start + COMPLETION_MAX_POLL_S
        status = None
        try:
            while time.time() < deadline:
                gevent.sleep(POLL_INTERVAL_S)
                try:
                    with self.client.get(
                        f"{MESSAGES_PATH}/{pk}?trace=y", name="ars_poll",
                        timeout=REQUEST_TIMEOUT, catch_response=True,
                    ) as resp:
                        if resp.status_code != 200:
                            resp.failure(f"poll status {resp.status_code}")
                            continue
                        resp.success()
                        status = (resp.json() or {}).get("status")
                        if status in ("Done", "Error"):
                            break
                except Exception:
                    continue   # transient; keep polling until the deadline
        finally:
            self._record_completion(
                query_id, qtype, start,
                finished=(status in ("Done", "Error")),
                status=(status if status in ("Done", "Error") else "Timeout"),
                stage=stage,
            )

    def _run_ars(self, qtype, payload):
        """ARS: submit -> poll /messages/{pk} until terminal -> fetch merged.

        Latency is wall-clock submit->terminal. One COLLECTOR.record per logical
        query, plus one synthetic 'ars_query' Locust request event carrying that
        same full wall-clock (the submit/poll/merge HTTP calls also show
        separately in Locust's own table for diagnostics, but are not
        double-counted as the query time). A Done with 0 results is a failure
        unless the target sets zero_result_is_failure=False.
        """
        start = time.time()
        query_id = COLLECTOR.new_query_id()
        issues = QueryIssues()   # non-fatal trouble on the way to the outcome

        def _elapsed_ms():
            return (time.time() - start) * 1000.0

        # 1) Submit -> pk.
        with self.client.post(
            ENDPOINT, json=payload, name="ars_submit",
            timeout=REQUEST_TIMEOUT, catch_response=True,
        ) as resp:
            submit_http = resp.status_code
            if submit_http != 201:
                resp.failure(f"submit status {submit_http}")
                self._record_ars(qtype, _elapsed_ms(), True,
                                 f"submit status {submit_http}",
                                 status="SubmitError")
                self._record_query(query_id, qtype, None, start, "SubmitError",
                                   failed=True, submit_http=submit_http,
                                   error=f"submit status {submit_http}")
                self._record_completion(query_id, qtype, start, False,
                                        "SubmitError", COLLECTOR.stage_idx)
                return
            try:
                pk = (resp.json() or {}).get("pk")
            except Exception:
                pk = None
            if not pk:
                resp.failure("no pk in submit response")
                self._record_ars(qtype, _elapsed_ms(), True,
                                 "no pk in submit response", status="NoPK")
                self._record_query(query_id, qtype, None, start, "NoPK",
                                   failed=True, submit_http=submit_http,
                                   error="no pk in submit response")
                self._record_completion(query_id, qtype, start, False, "NoPK",
                                        COLLECTOR.stage_idx)
                return
            resp.success()

        # 2) Poll until Done/Error or the per-query cap.
        status = None
        merged_pk = None
        poll_http = None      # last poll HTTP code seen -- for the debug log
        polls = 0
        deadline = start + MAX_POLL_S
        while time.time() < deadline:
            gevent.sleep(POLL_INTERVAL_S)   # cooperative; never time.sleep
            polls += 1
            with self.client.get(
                f"{MESSAGES_PATH}/{pk}?trace=y", name="ars_poll",
                timeout=REQUEST_TIMEOUT, catch_response=True,
            ) as resp:
                poll_http = resp.status_code
                if poll_http != 200:
                    resp.failure(f"poll status {poll_http}")
                    # status_code 0 == locust caught a connection error/timeout,
                    # i.e. no HTTP response at all.
                    issues.add(f"poll HTTP {poll_http}" if poll_http
                               else "poll request failed")
                    continue   # transient; keep polling until the deadline
                resp.success()
                try:
                    body = resp.json() or {}
                except Exception:
                    issues.add("poll body not JSON")
                    continue
                status = body.get("status")
                merged_pk = body.get("merged_version") or merged_pk
                if status in ("Done", "Error"):
                    break

        # 3) Terminal handling. The stage active now is what the main measurement
        # is attributed to; the completion sidecar row uses the same stage.
        stage = COLLECTOR.stage_idx
        if status == "Done":
            result_count, nbytes, merge_http = self._fetch_merged(merged_pk, issues)
            zero_result = result_count == 0
            # Whether an empty answer set scores against the error rate (and so
            # the knee) is a per-target policy -- see ZERO_RESULT_IS_FAILURE.
            failed = zero_result and ZERO_RESULT_IS_FAILURE
            self._record_ars(
                qtype, _elapsed_ms(), failed=failed,
                exc_msg=("Done with 0 results" if zero_result else None),
                status="Done", result_count=result_count, response_bytes=nbytes,
            )
            self._record_query(query_id, qtype, pk, start, "Done",
                               # `failed` follows the policy, so the debug log
                               # agrees with how the query was actually scored;
                               # the note is recorded either way, since an empty
                               # answer set is still why you'd open this pk.
                               failed=failed,
                               error=("Done with 0 results" if zero_result else None),
                               submit_http=submit_http, poll_http=poll_http,
                               merge_http=merge_http, polls=polls,
                               result_count=result_count, response_bytes=nbytes,
                               stage=stage, issues=issues)
            self._record_completion(query_id, qtype, start, True, "Done", stage)
        elif status == "Error":
            self._record_ars(qtype, _elapsed_ms(), True, "ARS Error status",
                             status="Error")
            self._record_query(query_id, qtype, pk, start, "Error", failed=True,
                               error="ARS Error status", submit_http=submit_http,
                               poll_http=poll_http, polls=polls, stage=stage,
                               issues=issues)
            self._record_completion(query_id, qtype, start, True, "Error", stage)
        else:
            # Not terminal within MAX_POLL_S: fail the main measurement now
            # (unchanged). Then, if an extended cap is configured, keep polling in
            # the background up to COMPLETION_MAX_POLL_S to record whether it ever
            # finishes -- purely for the completion sidecar.
            self._record_ars(qtype, _elapsed_ms(), True,
                             f"no terminal status within {MAX_POLL_S}s",
                             status="Timeout")
            # The pk is the point of this row: a timed-out query is the one you
            # most want to pull up by hand afterwards. `status` here is the last
            # non-terminal status the ARS reported (e.g. Running), if any.
            self._record_query(query_id, qtype, pk, start, "Timeout", failed=True,
                               error=(f"no terminal status within {MAX_POLL_S}s"
                                      + (f" (last: {status})" if status else "")),
                               submit_http=submit_http, poll_http=poll_http,
                               polls=polls, stage=stage, issues=issues)
            if COMPLETION_MAX_POLL_S > MAX_POLL_S:
                g = gevent.spawn(self._extended_poll, query_id, qtype, pk, start,
                                 stage)
                _COMPLETION_GREENLETS.append(g)
            else:
                self._record_completion(query_id, qtype, start, False, "Timeout",
                                        stage)

    def _fetch_merged(self, merged_pk, issues=None):
        """Fetch the merged message; return (result_count, response_bytes, http).

        Anything that goes wrong here is recorded in `issues` (the per-query
        intermediate-error log) rather than raised: a merged message we could not
        fetch or parse still leaves the query itself terminal, it just makes the
        0 we report for result_count mean something different.
        """
        if not merged_pk:
            if issues is not None:
                issues.add("Done without merged_version")
            return 0, 0, None
        with self.client.get(
            f"{MESSAGES_PATH}/{merged_pk}", name="ars_merge",
            timeout=REQUEST_TIMEOUT, catch_response=True,
        ) as resp:
            nbytes = len(resp.content or b"")
            if resp.status_code != 200:
                resp.failure(f"merge status {resp.status_code}")
                if issues is not None:
                    issues.add(f"merge HTTP {resp.status_code}"
                               if resp.status_code else "merge request failed")
                return 0, nbytes, resp.status_code
            resp.success()
            try:
                body = resp.json() or {}
                results = (((body.get("fields") or {}).get("data") or {})
                           .get("message") or {}).get("results") or []
                return len(results), nbytes, resp.status_code
            except Exception:
                if issues is not None:
                    issues.add("merged message not parseable")
                return 0, nbytes, resp.status_code


# ----------------------------------------------------------------------------
# Live terminal display (console.py). A sticky footer carries the one thing that
# used to scroll away -- which stage we are on and how it is doing -- while stage
# headers/verdicts and everything else scroll above it. Purely cosmetic: it reads
# the same COLLECTOR and _stage_stats the reports are written from, and a failure
# inside it can never fail the run.
# ----------------------------------------------------------------------------
DASHBOARD = None


@events.test_start.add_listener
def _start_dashboard(environment, **_kw):
    global DASHBOARD
    if isinstance(environment.runner, WorkerRunner):
        return   # workers have no console of their own
    DASHBOARD = console.Dashboard(
        label=_TGT["label"], target=TARGET,
        host=(environment.host or ""), endpoint=ENDPOINT,
        stages=STAGES, cooldown_s=COOLDOWN_S,
        p99_slo_ms=P99_SLO_MS, max_error_rate=MAX_ERROR_RATE,
        protocol=PROTOCOL, time_scale=TIME_SCALE,
        collector=COLLECTOR, stage_stats=_stage_stats,
    ).start()


# When cooldown is enabled, stages ramp down to 0 users between holds. Set a
# stop_timeout so a user finishing its (possibly very slow) current query is
# allowed to complete and be counted, rather than killed mid-request.
@events.init.add_listener
def _set_stop_timeout(environment, **_kw):
    if COOLDOWN_S:
        environment.stop_timeout = REQUEST_TIMEOUT


# ----------------------------------------------------------------------------
# Step-load shape. tick() returns (user_count, spawn_rate) or None to stop.
# It also tells the collector which stage is active.
# ----------------------------------------------------------------------------
SHAPE = None            # the live StepLoad instance, set in StepLoad.__init__
DRAIN_RECHECK_S = 0.2   # how often a parked user re-asks whether the drain is over


def _draining():
    """True while the ramp sits in a cooldown gap, i.e. no user may start a query.

    Read off the shape's own clock rather than off its last tick: Locust polls
    tick() about once a second and acts on it asynchronously, so a user asking
    "may I start another query?" needs a finer answer than the tick can give.
    """
    return bool(COOLDOWN_S) and SHAPE is not None and SHAPE.in_cooldown()


class StepLoad(LoadTestShape):
    def __init__(self):
        super().__init__()
        # One layout, shared with the live display, so the users being driven
        # and the stage being reported can never disagree.
        windows, self._total = config.build_timeline(STAGES, COOLDOWN_S)
        self._bounds = [w for w in windows if w[2] is not None]
        # Each cooldown carries the stage it follows, so the drain can be told to
        # stop exactly the users that stage was running (see tick()).
        self._cooldowns = []
        prev_idx = 0
        for start, end, idx in windows:
            if idx is None:
                self._cooldowns.append((start, end, prev_idx))
            else:
                prev_idx = idx
        global SHAPE
        SHAPE = self   # the user path asks us whether it is inside a drain

    def in_cooldown(self):
        """Is the run, right now, inside a cooldown gap?"""
        run_time = self.get_run_time()
        return any(start <= run_time < end for start, end, _ in self._cooldowns)

    def tick(self):
        run_time = self.get_run_time()
        if run_time > self._total:
            return None
        for start, end, idx in self._bounds:
            if start <= run_time < end:
                COLLECTOR.mark_stage(idx)
                users, rate, _hold = STAGES[idx]
                return (users, rate)
        # In a cooldown gap: ramp users to 0 so slow in-flight queries drain into
        # the just-finished stage (its end time is frozen here) rather than the
        # next one. stop_timeout (set at init) lets those queries finish.
        #
        # The spawn rate here is the just-finished stage's user count, because
        # Locust's dispatcher removes floor(spawn_rate) users per iteration and
        # blocks on each batch's stop_timeout: at a lower rate the teardown walks
        # down a few users a second (or slower), and every user not yet told to
        # stop keeps launching NEW queries the whole way down -- the opposite of
        # a drain. One rate, one iteration, every user stopped at once; each then
        # finishes only the query already in its hands. Held constant through the
        # gap (not read off the live user count) so the tick value doesn't change
        # under Locust and restart the teardown mid-drain.
        for start, end, prev_idx in self._cooldowns:
            if start <= run_time < end:
                COLLECTOR.end_active_stage()
                return (0, max(1, STAGES[prev_idx][0]))
        return None


# ----------------------------------------------------------------------------
# On test stop, compute per-stage stats, find the knee, write outputs.
# Only the master/standalone node writes files.
# ----------------------------------------------------------------------------
@events.test_stop.add_listener
def on_test_stop(environment, **_kw):
    if isinstance(environment.runner, WorkerRunner):
        return

    COLLECTOR.stage_ended.setdefault(COLLECTOR.stage_idx, time.time())

    # Close out the live display: the final stage gets the same verdict line the
    # earlier ones got, then the footer is lifted so the summary below prints
    # into a clean terminal.
    if DASHBOARD is not None:
        DASHBOARD.recap_final()
        DASHBOARD.stop()

    # Drain any in-flight completion-sidecar polls (queries that blew MAX_POLL_S
    # and are still being watched for eventual completion), bounded by the extra
    # budget, so their rows land in the sidecar. Queries still unfinished after
    # this are simply absent from the file.
    pending = [g for g in _COMPLETION_GREENLETS if not g.dead]
    if pending:
        extra = max(0, COMPLETION_MAX_POLL_S - MAX_POLL_S)
        print(f"Waiting up to {extra}s for {len(pending)} ARS completion "
              f"poll(s) still in flight...")
        gevent.joinall(pending, timeout=extra)

    overall_rows = []
    qtype_rows = []
    seen_stages = sorted(COLLECTOR.requests.keys())
    qtypes = [c[0] for c in CORPUS]

    for idx in seen_stages:
        overall_rows.append(_stage_stats(idx, "__all__"))
        for qt in qtypes:
            if COLLECTOR.requests[idx].get(qt):
                qtype_rows.append(_stage_stats(idx, qt))

    # Knee = highest stage meeting BOTH p99 SLO and error-rate cap.
    knee = None
    for row in overall_rows:
        if row["p99_ms"] <= P99_SLO_MS and row["error_rate"] <= MAX_ERROR_RATE:
            if knee is None or row["concurrency"] > knee["concurrency"]:
                knee = row

    # Which stages can't carry the numbers printed for them (see _stage_quality).
    stage_warnings = []
    for row in overall_rows:
        issues = _stage_quality(row)
        if issues:
            stage_warnings.append({
                "stage": row["stage"], "users": row["users"],
                "requests": row["requests"], "issues": issues,
            })
    knee_unsupported = bool(
        knee and any(w["stage"] == knee["stage"] for w in stage_warnings))

    # Pass/fail acceptance checkpoints (targets that configure them).
    checkpoints = _evaluate_checkpoints(overall_rows)

    # ARS health signals + red flags (async target only).
    ars_health = []
    red_flags = []
    completions = COLLECTOR.completions if PROTOCOL == "async" else []
    completion_summary = None
    if PROTOCOL == "async":
        finished = sum(1 for r in completions if r["finished"])
        within = sum(1 for r in completions if r["within_slo"])
        late = finished - within   # blew MAX_POLL_S but done by COMPLETION_MAX_POLL_S
        never = sum(1 for r in completions
                    if not r["finished"] and r["status"] == "Timeout")
        submit_failed = sum(1 for r in completions
                            if r["status"] in ("SubmitError", "NoPK"))
        completion_summary = {
            "total_queries": len(completions),
            "finished": finished,
            "finished_within_slo": within,
            "finished_after_slo": late,
            "never_finished": never,
            "submit_failed": submit_failed,
            "max_poll_s": MAX_POLL_S,
            "completion_max_poll_s": COMPLETION_MAX_POLL_S,
        }
        ars_health = [_ars_stage_health(idx) for idx in seen_stages]
        prev = None
        for h in ars_health:
            i = h["stage"]
            if h["zero_result_done"]:
                scored = ("counted as failures" if ZERO_RESULT_IS_FAILURE
                          else "NOT counted as failures per zero_result_is_failure=False")
                red_flags.append(
                    f"Stage {i}: {h['zero_result_done']} 'Done' response(s) returned "
                    f"0 results ({scored}; possible silent downstream break).")
            if h["errored"]:
                red_flags.append(f"Stage {i}: {h['errored']} query/queries returned Error status.")
            if h["timed_out"]:
                red_flags.append(
                    f"Stage {i}: {h['timed_out']} query/queries did not reach a terminal "
                    f"status within {MAX_POLL_S}s.")
            if h["submit_failed"]:
                red_flags.append(f"Stage {i}: {h['submit_failed']} submit failure(s) (no pk / bad status).")
            if h["requests"] >= 3 and h["results_mean"] > 0 and h["results_cv"] > 0.5:
                red_flags.append(
                    f"Stage {i}: high result-count variability (CV={h['results_cv']}, "
                    f"min={h['results_min']}, max={h['results_max']}) across identical queries.")
            if h["bytes_mean"] and h["bytes_max"] > 3 * h["bytes_mean"]:
                red_flags.append(
                    f"Stage {i}: abnormal response-size spread "
                    f"(max={h['bytes_max']}B vs mean={h['bytes_mean']}B).")
            # Result counts thinning out as concurrency rises.
            if (prev and prev["results_mean"] > 0 and h["results_mean"] > 0
                    and h["results_mean"] < 0.5 * prev["results_mean"]):
                red_flags.append(
                    f"Stage {i}: mean result count dropped under load "
                    f"({prev['results_mean']} -> {h['results_mean']} vs stage {prev['stage']}); "
                    f"the system may be shedding answers as concurrency rises.")
            prev = h

        # Completion-tracking red flags (from the sidecar).
        if completion_summary and completion_summary["finished_after_slo"]:
            red_flags.append(
                f"{completion_summary['finished_after_slo']} query/queries exceeded "
                f"the {MAX_POLL_S}s SLO but did finish within "
                f"{COMPLETION_MAX_POLL_S}s (slow, not stuck).")
        if completion_summary and completion_summary["never_finished"]:
            red_flags.append(
                f"{completion_summary['never_finished']} query/queries never reached "
                f"a terminal status even within {COMPLETION_MAX_POLL_S}s.")

    # Write overall CSV.
    fields = ["stage", "users", "stage_start", "requests", "errors", "error_rate",
              "duration_s", "rps", "mean_ms", "p50_ms", "p95_ms", "p99_ms",
              "concurrency"]
    with open(f"{CSV_PREFIX}_stages.csv", "w") as f:
        f.write(",".join(fields) + "\n")
        for r in overall_rows:
            f.write(",".join(str(r[k]) for k in fields) + "\n")

    # Write per-qtype CSV.
    qfields = ["stage", "qtype", "users", "requests", "errors", "error_rate",
               "rps", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "concurrency"]
    with open(f"{CSV_PREFIX}_by_qtype.csv", "w") as f:
        f.write(",".join(qfields) + "\n")
        for r in qtype_rows:
            f.write(",".join(str(r[k]) for k in qfields) + "\n")

    # Write the checkpoints CSV (targets that configure acceptance criteria).
    kfields = ["users", "goal", "stage", "requests", "p99_ms", "p99_slo_ms",
               "error_rate", "max_error_rate", "concurrency", "verdict", "detail"]
    if checkpoints:
        with open(f"{CSV_PREFIX}_checkpoints.csv", "w") as f:
            f.write(",".join(kfields) + "\n")
            for r in checkpoints:
                # Free-text columns are semicolon-separated; strip stray commas
                # so the naive CSV writer can't produce a ragged row.
                f.write(",".join(str(r[k]).replace(",", ";") for k in kfields) + "\n")

    # Write ARS health CSV (async target only).
    hfields = ["stage", "requests", "done", "errored", "timed_out",
               "submit_failed", "zero_result_done", "results_min",
               "results_mean", "results_max", "results_cv", "bytes_mean",
               "bytes_max"]
    if ars_health:
        with open(f"{CSV_PREFIX}_ars_health.csv", "w") as f:
            f.write(",".join(hfields) + "\n")
            for r in ars_health:
                f.write(",".join(str(r[k]) for k in hfields) + "\n")

    # Write the ARS per-query debug log (async target only): one row per logical
    # query with its pk, the HTTP status of each step, the terminal ARS status,
    # and a ready-made URL -- so a specific query can be pulled up afterwards.
    queries = COLLECTOR.queries
    qlfields = ["query", "pk", "stage", "users", "qtype", "submit_start",
                "latency_s", "ars_status", "failed", "submit_http", "poll_http",
                "merge_http", "polls", "intermediate_error_count",
                "result_count", "response_bytes", "intermediate_errors", "error",
                "message_url"]
    if queries:
        with open(f"{CSV_PREFIX}_ars_queries.csv", "w") as f:
            f.write(",".join(qlfields) + "\n")
            for r in queries:
                # `error`/`intermediate_errors` are free text; keep the naive
                # writer's rows rectangular.
                f.write(",".join(str(r[k]).replace(",", ";") for k in qlfields)
                        + "\n")

    # Write ARS completion sidecar (async target only): one row per logical query
    # -- end-to-end response time + whether it eventually finished (polled up to
    # COMPLETION_MAX_POLL_S, beyond the max_poll_s failure threshold).
    cfields = ["query", "stage", "qtype", "submit_start", "total_response_s",
               "finished", "within_slo", "status"]
    if completions:
        with open(f"{CSV_PREFIX}_ars_completion.csv", "w") as f:
            f.write(",".join(cfields) + "\n")
            for r in completions:
                f.write(",".join(str(r[k]) for k in cfields) + "\n")

    summary = {
        "config": {
            "target": TARGET,
            # < 1.0 means the run was compressed to a wall-clock budget: same
            # ramp, far fewer samples per stage, tighter poll/timeout caps.
            "time_scale": round(TIME_SCALE, 4),
            "component": _TGT["label"],
            "endpoint": ENDPOINT,
            "protocol": PROTOCOL,
            "p99_slo_ms": P99_SLO_MS,
            "max_error_rate": MAX_ERROR_RATE,
            "stages": STAGES,
            "corpus_weights": {c[0]: c[2] for c in CORPUS},
        },
        "stages": overall_rows,
        "knee": knee,
        "max_sustainable_concurrency": knee["concurrency"] if knee else None,
        # Stages whose row is arithmetically right but not trustworthy, and
        # whether the headline number rests on one of them.
        "stage_warnings": stage_warnings,
        "knee_unsupported": knee_unsupported,
    }
    if checkpoints:
        summary["config"]["checkpoints"] = CHECKPOINTS
        summary["checkpoints"] = checkpoints
        summary["checkpoints_passed"] = all(c["verdict"] == "PASS" for c in checkpoints)
    if PROTOCOL == "async":
        summary["config"]["zero_result_is_failure"] = ZERO_RESULT_IS_FAILURE
        summary["ars_health"] = ars_health
        summary["completion"] = completion_summary
        summary["red_flags"] = red_flags
    with open(f"{CSV_PREFIX}_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n" + "=" * 64)
    print("TRAPI LOAD TEST SUMMARY")
    print("=" * 64)
    if TIME_SCALE < 1.0:
        print(f"[!] COMPRESSED RUN (time scale {TIME_SCALE:.2f}). Stage holds and "
              f"poll/timeout caps\n    were shortened to fit a wall-clock budget. "
              f"Percentiles rest on far fewer\n    samples and the tighter caps "
              f"turn slow queries into timeouts, so treat these\n    numbers -- and "
              f"any checkpoint verdict -- as indicative, not a measurement.")
        print("-" * 64)
    hdr = f"{'stg':>3} {'usr':>4} {'rps':>7} {'p50':>7} {'p95':>8} {'p99':>8} {'err%':>6} {'conc':>7}"
    print(hdr)
    for r in overall_rows:
        print(f"{r['stage']:>3} {str(r['users']):>4} {r['rps']:>7.1f} "
              f"{r['p50_ms']:>7.0f} {r['p95_ms']:>8.0f} {r['p99_ms']:>8.0f} "
              f"{r['error_rate']*100:>5.1f}% {r['concurrency']:>7.1f}")
    # The table has the numbers; these two rows have the shape -- where the
    # latency curve turns up and where errors start, in one glance.
    if len(overall_rows) > 1:
        paint = console.painter()
        p99_spark = console.sparkline(
            [r["p99_ms"] for r in overall_rows], P99_SLO_MS, paint)
        err_spark = console.sparkline(
            [r["error_rate"] for r in overall_rows], MAX_ERROR_RATE, paint)
        print(f"    p99  {p99_spark}   "
              f"(SLO {console.fmt_ms(P99_SLO_MS)}; red = over)")
        print(f"    err  {err_spark}   "
              f"(cap {MAX_ERROR_RATE * 100:.1f}%; red = over)")
    print("-" * 64)
    if knee:
        print(f"Max sustainable concurrency: {knee['concurrency']} "
              f"(stage {knee['stage']}, {knee['users']} users, "
              f"p99={knee['p99_ms']:.0f}ms, err={knee['error_rate']*100:.1f}%)")
    else:
        print("No stage met the SLO -- service saturated below the first stage, "
              "or SLO is too strict. Lower the first stage or relax P99_SLO_MS.")

    if stage_warnings:
        print("-" * 64)
        print(f"MEASUREMENT QUALITY ({len(stage_warnings)} of "
              f"{len(overall_rows)} stage(s) flagged)")
        kinds = {i["kind"] for w in stage_warnings for i in w["issues"]}
        for w in stage_warnings:
            head = f"  stage {w['stage']} ({w['users']}u, n={w['requests']}): "
            for i, issue in enumerate(w["issues"]):
                print((head if i == 0 else " " * len(head)) + issue["detail"])
        if knee_unsupported:
            print(f"  [!] The knee (stage {knee['stage']}) is one of them: the "
                  f"headline number rests on a\n      stage that cannot support "
                  f"it. Treat it as indicative, not a measurement.")
        # Name only the remedy that applies -- both live in this target's entry.
        remedies = []
        if "thin_samples" in kinds:
            remedies.append("longer stage holds, for more samples per stage")
        if "in_flight_bleed" in kinds:
            remedies.append(
                f"a cooldown_s longer than the current {COOLDOWN_S}s"
                if COOLDOWN_S else
                "a cooldown_s drain gap, so slow queries finish inside the "
                "stage that launched them")
        print(f"  Fix in config.py under '{TARGET}': " + "; and ".join(remedies)
              + ".")

    if checkpoints:
        print("-" * 64)
        print("ACCEPTANCE CHECKPOINTS")
        print(f"{'usr':>4} {'p99':>8} {'bar':>8} {'err%':>6} {'bar':>6} "
              f"{'verdict':>8}  goal")
        for c in checkpoints:
            p99 = f"{c['p99_ms']:.0f}" if c["p99_ms"] is not None else "-"
            bar = f"{c['p99_slo_ms']:.0f}" if c["p99_slo_ms"] is not None else "n/a"
            err = f"{c['error_rate'] * 100:.1f}" if c["error_rate"] is not None else "-"
            print(f"{c['users']:>4} {p99:>8} {bar:>8} {err:>6} "
                  f"{c['max_error_rate'] * 100:>6.1f} {c['verdict']:>8}  {c['goal']}")
            if c["verdict"] != "PASS":
                print(f"     -> {c['detail']}")
        failed = [c for c in checkpoints if c["verdict"] != "PASS"]
        if failed:
            print(f"RESULT: {len(failed)} of {len(checkpoints)} checkpoint(s) not met.")
            # Non-zero exit so a CI/acceptance run fails on its own criteria.
            environment.process_exit_code = 1
        else:
            print(f"RESULT: all {len(checkpoints)} checkpoints met.")

    wrote = [f"{CSV_PREFIX}_stages.csv", f"{CSV_PREFIX}_by_qtype.csv"]
    if checkpoints:
        wrote.append(f"{CSV_PREFIX}_checkpoints.csv")
    if ars_health:
        print("-" * 64)
        print("ARS HEALTH (per stage)")
        print(f"  0-result 'Done' scored as: "
              f"{'FAILURE' if ZERO_RESULT_IS_FAILURE else 'success'} "
              f"(zero_result_is_failure={ZERO_RESULT_IS_FAILURE})")
        hh = (f"{'stg':>3} {'req':>4} {'done':>5} {'err':>4} {'t/o':>4} "
              f"{'0res':>5} {'res_mean':>9} {'res_cv':>7} {'bytes_max':>10}")
        print(hh)
        for h in ars_health:
            print(f"{h['stage']:>3} {h['requests']:>4} {h['done']:>5} "
                  f"{h['errored']:>4} {h['timed_out']:>4} {h['zero_result_done']:>5} "
                  f"{h['results_mean']:>9} {h['results_cv']:>7} {h['bytes_max']:>10}")
        if completion_summary:
            c = completion_summary
            print("-" * 64)
            print(f"COMPLETION TRACKING (polled to {COMPLETION_MAX_POLL_S}s; "
                  f"{MAX_POLL_S}s = failure threshold)")
            print(f"  {c['total_queries']} queries: {c['finished']} finished "
                  f"({c['finished_within_slo']} within {MAX_POLL_S}s, "
                  f"{c['finished_after_slo']} after), "
                  f"{c['never_finished']} never finished, "
                  f"{c['submit_failed']} submit-failed")
        failed_queries = [q for q in queries if q["failed"]]
        if failed_queries:
            print("-" * 64)
            shown = failed_queries[:5]
            print(f"FAILED QUERIES ({len(failed_queries)} of {len(queries)}; "
                  f"first {len(shown)} shown, all in "
                  f"{CSV_PREFIX}_ars_queries.csv)")
            for q in shown:
                print(f"  stage {q['stage']} {q['qtype']} [{q['ars_status']}] "
                      f"pk={q['pk'] or '(none)'} -- {q['error']}")
                if q["message_url"]:
                    print(f"    {q['message_url']}")
        # Intermediate errors are non-fatal, so most of the queries carrying them
        # are SUCCESSES -- they never show in the failed block above. Flag the
        # count here so a run that only limped to green doesn't read as clean.
        rough = [q for q in queries if q["intermediate_error_count"]]
        if rough:
            print("-" * 64)
            print(f"INTERMEDIATE ERRORS: {len(rough)} of {len(queries)} queries "
                  f"hit retried, non-fatal errors "
                  f"(intermediate_errors column in "
                  f"{CSV_PREFIX}_ars_queries.csv)")
            for q in rough[:5]:
                print(f"  stage {q['stage']} {q['qtype']} [{q['ars_status']}] "
                      f"pk={q['pk'] or '(none)'} -- {q['intermediate_errors']}")
        print("-" * 64)
        if red_flags:
            print(f"RED FLAGS ({len(red_flags)}):")
            for msg in red_flags:
                print(f"  [!] {msg}")
        else:
            print("RED FLAGS: none detected.")
        wrote.append(f"{CSV_PREFIX}_ars_health.csv")
        if queries:
            wrote.append(f"{CSV_PREFIX}_ars_queries.csv")
        if completions:
            wrote.append(f"{CSV_PREFIX}_ars_completion.csv")
    wrote.append(f"{CSV_PREFIX}_summary.json")
    print(f"Wrote: {', '.join(wrote)}")
    print("=" * 64 + "\n")

    # Locust prints its own (long) request/percentile/error tables AFTER this
    # listener returns, which would bury the number the run exists to produce.
    # Stash the headline and repeat it on quit, so it is the last thing on screen.
    _stash_headline(knee, checkpoints, red_flags if PROTOCOL == "async" else [],
                    knee_unsupported=knee_unsupported,
                    flagged_stages=len(stage_warnings))


# ----------------------------------------------------------------------------
# Final headline: printed after Locust's own end-of-run tables, so the answer is
# the last line in the terminal rather than the middle of the scrollback.
# ----------------------------------------------------------------------------
_HEADLINE = []


def _stash_headline(knee, checkpoints, red_flags, *, knee_unsupported=False,
                    flagged_stages=0):
    paint = console.painter()
    lines = [paint("─" * 64, "grey")]
    if knee:
        # A knee drawn from a stage that can't support it is worse than no
        # number, because it looks like one. Colour it accordingly.
        tone = "yellow" if knee_unsupported else "green"
        lines.append(
            paint("MAX SUSTAINABLE CONCURRENCY: ", "bold")
            + paint(f"{knee['concurrency']}", "bold", tone)
            + paint(f"   ({_TGT['label']}, stage {knee['stage']}, "
                    f"{knee['users']} users)", "grey"))
        if knee_unsupported:
            lines.append(paint(
                "  [!] indicative only -- that stage is flagged under "
                "MEASUREMENT QUALITY above", "yellow"))
    elif flagged_stages:
        lines.append(paint("NO STAGE MET THE SLO", "bold", "red")
                     + paint(f"   ({_TGT['label']}: but {flagged_stages} stage(s) "
                             f"are flagged -- see MEASUREMENT QUALITY above)",
                             "grey"))
    else:
        lines.append(paint("NO STAGE MET THE SLO", "bold", "red")
                     + paint(f"   ({_TGT['label']}: saturated below the first "
                             f"stage, or the SLO is too strict)", "grey"))
    if checkpoints:
        missed = [c for c in checkpoints if c["verdict"] != "PASS"]
        if missed:
            lines.append(paint(f"CHECKPOINTS: {len(missed)} of "
                               f"{len(checkpoints)} not met", "bold", "red"))
            for c in missed:
                lines.append(paint(f"  {c['verdict']} at {c['users']} users -- "
                                   f"{c['detail']}", "red"))
        else:
            lines.append(paint(f"CHECKPOINTS: all {len(checkpoints)} met",
                               "bold", "green"))
    if red_flags:
        lines.append(paint(f"RED FLAGS: {len(red_flags)} "
                           f"(see the ARS block above)", "yellow"))
    lines.append(paint(f"Reports: {CSV_PREFIX}_summary.json "
                       f"and {CSV_PREFIX}_stages.csv", "grey"))
    lines.append(paint("─" * 64, "grey"))
    _HEADLINE.extend(lines)


@events.quit.add_listener
def _print_headline(**_kw):
    # Belt and braces: on an abnormal exit (Ctrl-C before a stage completed, a
    # startup failure) on_test_stop may never have run, so the footer and the
    # stream proxies would still be installed. stop() is idempotent.
    if DASHBOARD is not None:
        DASHBOARD.stop()
    for line in _HEADLINE:
        print(line)

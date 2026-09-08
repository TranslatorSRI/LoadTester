"""
Component (target) registry for HelmsDeep.

The Translator stack cascades ARS -> ARAs -> KPs, so a run targets exactly ONE
layer at a time (see CLAUDE.md "Layering rule"). Each layer differs only in
(1) the request protocol and (2) which corpus subset is sent; the step-load
engine in ``trapi_loadtest.py`` is shared across all of them.

``--targets`` on the CLI selects one of these keys. ``kps`` (Retriever),
``aras`` (Shepherd), and ``ars`` (the async submit/poll/merge pipeline) are all
wired up.

Per-target load + SLO tuning lives here because cost profiles differ wildly by
layer: a KP lookup is cheap, an ARA creative query is expensive, and an ARS run
takes minutes to ~an hour. So each target carries its own step-load ramp and
p99 knee threshold. The error-rate cap is shared (``MAX_ERROR_RATE``).

Fields:
  label        human name for the component
  endpoint     request path appended to --host
  corpus       key into trapi_corpus.CORPUS_BY_NAME (which query subset to send)
  protocol     "sync" (blocking POST) or "async" (ARS submit/poll/merge)
  implemented  whether the pipeline is runnable yet
  stages       step-load ramp: list of (users, spawn_rate, hold_seconds). Each
               stage should hold long enough for a stable measurement.
  p99_slo_ms   knee requires this layer's stage p99 <= this (ms)
  cooldown_s   optional quiet gap BETWEEN stages (default 0). Users ramp to 0 so
               slow in-flight queries drain and are counted under the stage that
               launched them, rather than bleeding into the next stage. Set it on
               the expensive layers (ARA/ARS/Pathfinder); cheap KP lookups don't
               bleed, so they leave it at 0.
  request_timeout_s
               optional per-HTTP-call timeout (default 210). Raise it on sync
               targets whose p99_slo_ms sits near or above the default, or every
               slow query is recorded as a client timeout instead of a latency.
  checkpoints  optional list of pass/fail acceptance criteria, evaluated against
               the stage running that many users. Turns a run from "find the
               knee" into "does the system hold N concurrent queries?" -- the
               knee is still computed either way. Each entry:
                 users           which stage to judge (matched on user count)
                 goal            short human label for what the checkpoint proves
                 p99_slo_ms      latency bar; omit to inherit the target's
                                 p99_slo_ms, or set None for no latency check
                                 (an overload probe where slowdown is expected
                                 but failures are not)
                 max_error_rate  error-rate bar; omit to inherit MAX_ERROR_RATE
               Results land in <prefix>_checkpoints.csv, the summary JSON, and
               the printed report; any failed checkpoint sets a non-zero exit code.

Async (``ars``) targets add:
  messages_endpoint       base path for polling/merge: /messages/{pk}
  poll_interval_s         seconds between status polls (gevent.sleep)
  max_poll_s              per-query poll cap; exceeding it marks a Timeout failure
  completion_max_poll_s   optional extended cap (>= max_poll_s) for the completion
                          sidecar: after a query blows max_poll_s (already a
                          Timeout failure), a background greenlet keeps polling
                          up to this many seconds to record whether it
                          *eventually* finishes and its true end-to-end time.
                          Defaults to max_poll_s (no extended tracking).
  zero_result_is_failure  whether a terminal "Done" that carries 0 results counts
                          as a failure in the per-stage error rate (and therefore
                          against the knee). Defaults to True: a Done with no
                          answers usually means a downstream agent silently
                          dropped out, which is exactly what we want the knee to
                          catch. Set it False to score only transport/protocol
                          outcomes (submit errors, Error status, Timeout) as
                          failures and treat an empty answer set as a legitimate
                          response -- the query's latency then also joins the
                          percentile pool instead of being discarded. Either way
                          zero-result Dones are still counted in ars_health and
                          still raise a red flag.
"""

# Shared across all targets: a stage also needs error_rate <= this to be a knee.
MAX_ERROR_RATE = 0.01   # 1%

TARGETS = {
    "kps": {
        "label": "Retriever",
        "endpoint": "/query",
        "corpus": "retriever",
        "protocol": "sync",
        "implemented": True,
        # Ramp high to find saturation. The holds are 5 minutes, not the 60s
        # this target carried when the KP layer was ~40 independent services
        # answering in milliseconds: Retriever answers a lookup in seconds to
        # tens of seconds, and a 60s hold at that latency yielded 15-100
        # completed requests per stage -- too few to put a p99 on (the printed
        # value was one of the two slowest requests) and short enough that most
        # queries were still in flight when the stage ended, landing in the next
        # stage's bucket and making a later stage look faster than an earlier
        # one. Five minutes gives the low stages ~75-150 samples and the high
        # ones several hundred.
        "stages": [
            (5,   5, 300),
            (10,  5, 300),
            (20,  5, 300),
            (40, 10, 300),
            (80, 10, 300),
            (120, 20, 300),
            (160, 20, 300),
        ],
        "p99_slo_ms": 210000,
        # Matches the p99 SLO: a query that finishes inside its latency budget
        # drains into the stage that launched it rather than contaminating the
        # next one. (Cheap lookups didn't bleed; Retriever does.)
        "cooldown_s": 60,
    },
    "aras": {
        "label": "Shepherd",
        "endpoint": "/query",
        "corpus": "shepherd",
        "protocol": "sync",
        "implemented": True,
        # Creative (inferred) reasoning is far heavier -- gentler ramp, longer
        # holds (slow queries need time to accumulate samples), looser p99.
        "stages": [
            # (2,  1, 300),  # 5 mins
            # (3,  1, 300),  # 5 mins
            (5,  2, 330),  # 5.5 mins
            (30, 2, 360),  # 6 mins
            (45, 5, 420),  # 7 mins
            (60, 5, 600),  # 10 mins
        ],
        "p99_slo_ms": 300000,
        "cooldown_s": 120,                 # drain slow queries between stages
        "request_timeout_s": 300,
    },
    "ars": {
        "label": "ARS",
        "endpoint": "/ars/api/submit",            # submit path (appended to --host)
        "messages_endpoint": "/ars/api/messages", # poll/merge base: /messages/{pk}
        "corpus": "ars",
        "protocol": "async",
        "implemented": True,
        "poll_interval_s": 10,            # gevent.sleep between status polls
        "max_poll_s": 360,                # 6-min cap; exceeding it = Timeout
        "completion_max_poll_s": 600,     # keep polling (sidecar) up to 10 min to
                                          # see if a timed-out query ever finishes
        # A "Done" with 0 results is a silent downstream break -> counts against
        # the knee. Flip to False to score only transport failures.
        "zero_result_is_failure": False,
        # Runs take minutes -- very low concurrency, long holds.
        "stages": [
            (2,  1, 300),  # 5 mins
            (5,  1, 300),  # 5 mins
            (10, 2, 330),  # 5.5 mins
            (30, 2, 360),  # 6 mins
            (45, 5, 420),  # 7 mins
            (60, 5, 600),  # 10 mins
        ],
        "p99_slo_ms": 360000,             # 6-min knee target (< max_poll_s)
        "cooldown_s": 240,                 # drain slow queries between stages
    },
    # Pathfinder is its own run type (ARA + ARS only): it pins two endpoints and
    # asks for connecting paths -- the most intensive query class. Same endpoints
    # as aras/ars; only the corpus, ramp, and SLO differ (gentler + looser).
    "aras_pathfinder": {
        "label": "Shepherd (Pathfinder)",
        "endpoint": "/query",
        "corpus": "pathfinder",
        "protocol": "sync",
        "implemented": True,
        # Heavier than `aras`: lower concurrency, longer holds, looser p99.
        "stages": [
            (2,  1, 300),  # 5 mins
            (3,  1, 300),  # 5 mins
            (5,  2, 330),  # 5.5 mins
            (10, 2, 360),  # 6 mins
            (20, 5, 420),  # 7 mins
            (40, 5, 600),  # 10 mins
        ],
        "p99_slo_ms": 300000,             # 5-min knee target (vs 2 min for aras)
        "cooldown_s": 60,                 # drain slow queries between stages
    },
    # Mixed profile (ARA + ARS): 2/3 inferred MVP1+MVP2, 1/3 Pathfinder, run as
    # one blended workload. Unlike the other targets -- which characterize a
    # single query class to FIND the knee -- this one answers a pass/fail
    # capacity question at three named concurrency checkpoints (see
    # ``checkpoints`` below). The knee is still computed and reported.
    "aras_mixed": {
        "label": "Shepherd (Mixed 2:1 inferred/Pathfinder)",
        "endpoint": "/query",
        "corpus": "mixed",
        "protocol": "sync",
        "implemented": True,
        # The three checkpoints the profile exists to answer, plus a short
        # low-load stage first so a checkpoint failure can be read against a
        # healthy baseline (is 30 slow, or is everything slow?).
        "stages": [
            (10,  2, 300),  # 5 mins  -- baseline
            (30,  5, 900),  # 15 mins -- target peak load
            (45,  5, 900),  # 15 mins -- expected headroom
            (60, 10, 900),  # 15 mins -- overload probe
        ],
        # 1/3 of the mix is Pathfinder, the heaviest query class, and p99 is a
        # tail statistic -- so this profile inherits the looser Pathfinder SLO
        # rather than the tighter inferred-only one.
        "p99_slo_ms": 300000,             # 5-min knee target
        "request_timeout_s": 400,         # must exceed p99_slo_ms to measure it
        "cooldown_s": 120,                # drain slow queries between stages
        "checkpoints": [
            {"users": 30, "goal": "sustain peak load",
             "p99_slo_ms": 300000, "max_error_rate": 0.01},
            {"users": 45, "goal": "headroom above peak",
             "p99_slo_ms": 300000, "max_error_rate": 0.01},
            # Overload probe: latency is expected to degrade here, so only the
            # failure criterion applies (p99_slo_ms: None = no latency check).
            {"users": 60, "goal": "no substantial failures under overload",
             "p99_slo_ms": None, "max_error_rate": 0.05},
        ],
    },
    "ars_mixed": {
        "label": "ARS (Mixed 2:1 inferred/Pathfinder)",
        "endpoint": "/ars/api/submit",
        "messages_endpoint": "/ars/api/messages",
        "corpus": "mixed",
        "protocol": "async",
        "implemented": True,
        "poll_interval_s": 10,
        # Longer than the single-class ARS targets: a Pathfinder query fanned out
        # across every ARA is the slowest thing the stack does, and max_poll_s has
        # to sit above the p99 SLO or every checkpoint failure looks like a
        # timeout.
        "max_poll_s": 600,                # 10-min cap; exceeding it = Timeout
        "completion_max_poll_s": 900,     # sidecar: poll to 15 min for eventual finish
        "zero_result_is_failure": True,   # 0-result Done = silent downstream break
        "stages": [
            (10,  2, 600),  # 10 mins -- baseline
            (30,  5, 900),  # 15 mins -- target peak load
            (45,  5, 900),  # 15 mins -- expected headroom
            (60, 10, 900),  # 15 mins -- overload probe
        ],
        "p99_slo_ms": 300000,             # 5-min knee target (< max_poll_s)
        "cooldown_s": 300,                # drain slow queries between stages
        "checkpoints": [
            {"users": 30, "goal": "sustain peak load",
             "p99_slo_ms": 300000, "max_error_rate": 0.01},
            {"users": 45, "goal": "headroom above peak",
             "p99_slo_ms": 300000, "max_error_rate": 0.01},
            {"users": 60, "goal": "no substantial failures under overload",
             "p99_slo_ms": None, "max_error_rate": 0.05},
        ],
    },
    "ars_pathfinder": {
        "label": "ARS (Pathfinder)",
        "endpoint": "/ars/api/submit",
        "messages_endpoint": "/ars/api/messages",
        "corpus": "pathfinder",
        "protocol": "async",
        "implemented": True,
        "poll_interval_s": 10,
        "max_poll_s": 360,               # 6-min cap
        "completion_max_poll_s": 600,    # sidecar: poll up to 10 min for eventual finish
        "zero_result_is_failure": True,  # 0-result Done = silent downstream break
        "stages": [
            (2,  1, 300),  # 5 mins
            (3,  1, 300),  # 5 mins
            (5,  2, 330),  # 5.5 mins
            (10, 2, 360),  # 6 mins
            (20, 5, 420),  # 7 mins
            (40, 5, 600),  # 10 mins
        ],
        "p99_slo_ms": 300000,            # 5-min knee target (< max_poll_s)
        "cooldown_s": 60,                # drain slow queries between stages
    },
}

DEFAULT_TARGET = "kps"


# ---------------------------------------------------------------------------
# Time-budget compression (--time-budget / --quick).
#
# A target's natural run is long on purpose: slow queries need long holds to
# accumulate enough samples for a trustworthy percentile. But sometimes you want
# a fast pass -- to check a host/corpus/config end to end, or to get a rough read
# in a coffee break. Compression scales the *durations* (stage holds, cooldowns,
# and the poll/timeout caps that gate how long one query may run) by a single
# factor, leaving the shape of the ramp -- user counts, SLOs, checkpoints -- alone.
#
# What you give up is sample count, and with it the tail: a p99 over a handful of
# queries is noise. Compressed poll/timeout caps also *change what counts as a
# failure* -- a query that would have finished in 4 minutes is a timeout when the
# cap is 2 -- so a compressed run's error rates and checkpoint verdicts are
# indicative only. The engine stamps every output with the scale it used.
# ---------------------------------------------------------------------------

# Floors: below these a stage stops exercising anything, so compression stops
# rather than shrinking further (a budget too small for the floors simply
# overruns -- the CLI reports the honest projected duration).
MIN_HOLD_S = 30            # per stage
MIN_COOLDOWN_S = 10        # only where the target sets a cooldown at all
MIN_MAX_POLL_S = 120       # ARS per-query poll cap
MIN_REQUEST_TIMEOUT_S = 60  # per HTTP call
MIN_POLL_INTERVAL_S = 5    # ARS status polls; the floor keeps the extra polling
                           # load on a real service modest


def build_timeline(stages, cooldown_s=0):
    """Lay the ramp out on a wall clock, shared by the load shape and the console.

    Returns ``(windows, total_s)`` where each window is
    ``(start_s, end_s, stage_idx_or_None)`` -- ``stage_idx`` is ``None`` for a
    cooldown gap. Both the ``StepLoad`` shape (which drives users) and the live
    terminal display (which reports where the run is) read the ramp from here,
    so "what stage are we in?" can never disagree between them.
    """
    windows = []
    t = 0
    n = len(stages)
    for i, (_users, _rate, hold) in enumerate(stages):
        windows.append((t, t + hold, i))
        t += hold
        if cooldown_s and i < n - 1:   # gap BETWEEN stages, not after the last
            windows.append((t, t + cooldown_s, None))
            t += cooldown_s
    return windows, t


def natural_duration_s(cfg):
    """Wall-clock seconds the target's stages + cooldowns take as configured."""
    stages = cfg["stages"]
    cooldown = cfg.get("cooldown_s", 0)
    return sum(hold for _, _, hold in stages) + cooldown * max(0, len(stages) - 1)


def time_scaled(cfg, budget_s):
    """Return ``(compressed_cfg, scale)`` fitting roughly ``budget_s`` of wall clock.

    Durations shrink; the ramp does not. User counts, spawn rates, SLOs, and
    checkpoints are untouched, so a compressed run asks the same questions of the
    same load levels -- just with far fewer samples behind each answer.

    A budget at or above the natural duration is a no-op (scale 1.0): this only
    ever speeds a run up, never pads it out.
    """
    natural = natural_duration_s(cfg)
    if not natural or budget_s >= natural:
        return dict(cfg), 1.0
    scale = budget_s / natural

    def _scaled(value, floor):
        return max(floor, int(round(value * scale)))

    out = dict(cfg)
    stages = []
    for users, rate, hold in cfg["stages"]:
        hold = _scaled(hold, MIN_HOLD_S)
        # Raise the spawn rate where a compressed hold is too short to ramp up in
        # (reaching 60 users at 10/s takes 6s -- nothing at a 900s hold, a fifth
        # of a 30s one), so each stage still spends its time at the load it is
        # measuring rather than climbing towards it.
        rate = max(rate, -(-users // max(1, int(hold * 0.2))))
        stages.append((users, rate, hold))
    out["stages"] = stages
    if cfg.get("cooldown_s"):
        out["cooldown_s"] = _scaled(cfg["cooldown_s"], MIN_COOLDOWN_S)
    if cfg.get("request_timeout_s"):
        out["request_timeout_s"] = _scaled(cfg["request_timeout_s"],
                                           MIN_REQUEST_TIMEOUT_S)
    if cfg.get("max_poll_s"):
        out["max_poll_s"] = _scaled(cfg["max_poll_s"], MIN_MAX_POLL_S)
        # Poll cadence sets the resolution of an ARS latency: a 10s interval
        # against a compressed 120s cap quantizes every measurement to 10s.
        # Shrink it alongside the cap, floored so a compressed run doesn't
        # hammer the status endpoint far harder than the run it stands in for.
        if cfg.get("poll_interval_s"):
            out["poll_interval_s"] = _scaled(cfg["poll_interval_s"],
                                             MIN_POLL_INTERVAL_S)
        # Keep the sidecar's extra headroom proportional to the compressed cap,
        # so on_test_stop's bounded drain shrinks with the run instead of adding
        # the original 5 minutes back onto a 10-minute budget.
        if cfg.get("completion_max_poll_s"):
            ratio = cfg["completion_max_poll_s"] / cfg["max_poll_s"]
            out["completion_max_poll_s"] = int(round(out["max_poll_s"] * ratio))
    return out, scale

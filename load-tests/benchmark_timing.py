"""Clock evidence for benchmarks; rejects measurements that cannot support throughput.

The default acceptance budget is 250 ms plus 0.1% of the measured duration.
This is a declared measurement-quality policy, not an assertion about OS accuracy:
250 ms accommodates clock sampling / timestamp resolution in short local runs,
and 0.1% bounds relative clock-rate disagreement in longer runs. Sampling itself
must fit the 250 ms budget. Database intervals must fit inside the client interval;
a constant client/database clock offset therefore does not invalidate a run.
"""
from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import threading
import time

from flowforge_client import timestamp

ABSOLUTE_TOLERANCE_SECONDS = 0.250
RELATIVE_TOLERANCE_FRACTION = 0.001


@dataclass(frozen=True)
class ClockSample:
    wall_ns: int
    monotonic_ns: int
    capture_span_ns: int = 0

    def as_dict(self):
        return {
            "wallTime": dt.datetime.fromtimestamp(self.wall_ns / 1e9, dt.timezone.utc).isoformat(),
            "wallTimeUnixNs": self.wall_ns,
            "monotonicNs": self.monotonic_ns,
            "captureSpanSeconds": self.capture_span_ns / 1e9,
        }


def sample_clocks():
    # Bracket the wall read so a scheduling pause during sampling is visible.
    before = time.monotonic_ns()
    wall = time.time_ns()
    after = time.monotonic_ns()
    return ClockSample(wall, (before + after) // 2, after - before)


class ExecutionClock:
    """Thread-safe dual-clock elapsed time, with conservative deadline accounting.

    At each observation, add max(wall increment, monotonic increment, 0). A host
    suspend counts even if monotonic pauses. Backward wall adjustments never
    subtract elapsed time. Forward clock jumps may conservatively expire a run;
    that run also fails clock validation. No system clock is modified.
    """
    def __init__(self, sampler=sample_clocks):
        self._sampler = sampler
        self._lock = threading.Lock()
        self.start = self.end = sampler()
        self.deadline_elapsed_seconds = 0.0
        self.accumulated_disagreement_seconds = 0.0
        self.max_capture_span_seconds = self.start.capture_span_ns / 1e9
        self.backward_wall_observations = 0
        self.backward_monotonic_observations = 0
        self.observations = 1
        self._stopped = False

    def observe(self):
        with self._lock:
            if not self._stopped:
                current = self._sampler()
                wall = (current.wall_ns - self.end.wall_ns) / 1e9
                monotonic = (current.monotonic_ns - self.end.monotonic_ns) / 1e9
                self.deadline_elapsed_seconds += max(wall, monotonic, 0)
                self.accumulated_disagreement_seconds += abs(wall - monotonic)
                self.max_capture_span_seconds = max(self.max_capture_span_seconds, current.capture_span_ns / 1e9)
                self.backward_wall_observations += wall < 0
                self.backward_monotonic_observations += monotonic < 0
                self.end = current
                self.observations += 1
            return self.deadline_elapsed_seconds

    def stop(self):
        self.observe()
        with self._lock:
            self._stopped = True
        return self.end

    @property
    def monotonic_duration_seconds(self):
        return (self.end.monotonic_ns - self.start.monotonic_ns) / 1e9

    def report(self, workflows, tasks, attempts):
        wall = (self.end.wall_ns - self.start.wall_ns) / 1e9
        monotonic = self.monotonic_duration_seconds
        tolerance = ABSOLUTE_TOLERANCE_SECONDS + RELATIVE_TOLERANCE_FRACTION * max(wall, monotonic, 0)
        database, database_issues = database_spans(workflows, tasks, attempts)
        issues = list(database_issues)
        if wall <= 0 or monotonic <= 0:
            issues.append("Client execution duration must be positive on both clocks")
        if self.accumulated_disagreement_seconds > tolerance:
            issues.append("Accumulated wall/monotonic clock disagreement exceeds the measurement budget")
        if self.backward_wall_observations or self.backward_monotonic_observations:
            issues.append("A sampled clock moved backwards during execution")
        if self.max_capture_span_seconds > ABSOLUTE_TOLERANCE_SECONDS:
            issues.append("Clock sampling was interrupted beyond the absolute measurement budget")
        combined = database["combined"]["spanSeconds"]
        if combined is None:
            issues.append("No persisted timestamp span is available for cross-checking")
        elif combined > min(wall, monotonic) + tolerance:
            issues.append("Persisted database timestamp span exceeds the client execution duration")
        return {
            "consistent": not issues,
            "executionStart": self.start.as_dict(),
            "executionEnd": self.end.as_dict(),
            "wallDurationSeconds": wall,
            "monotonicDurationSeconds": monotonic,
            "deadlineElapsedSeconds": self.deadline_elapsed_seconds,
            "deadlineDerivation": "sum(max(forward wall increment, forward monotonic increment, 0)) across clock observations",
            "executionDurationDerivation": "executionEnd.monotonicNs - executionStart.monotonicNs, divided by 1e9; from before submission until terminal polling ends, excluding setup and verification",
            "absoluteToleranceSeconds": ABSOLUTE_TOLERANCE_SECONDS,
            "relativeToleranceFraction": RELATIVE_TOLERANCE_FRACTION,
            "allowedDiscrepancySeconds": tolerance,
            "toleranceRationale": "Declared measurement-quality budget: 250 ms absolute sampling/timestamp allowance plus 0.1% relative clock-rate disagreement; not a measured OS guarantee",
            "wallMonotonicDisagreementSeconds": abs(wall - monotonic),
            "accumulatedClockDisagreementSeconds": self.accumulated_disagreement_seconds,
            "maxCaptureSpanSeconds": self.max_capture_span_seconds,
            "backwardWallObservations": self.backward_wall_observations,
            "backwardMonotonicObservations": self.backward_monotonic_observations,
            "clockObservations": self.observations,
            "database": database,
            "issues": issues,
        }


def database_spans(workflows, tasks, attempts):
    """Use persisted API timestamps only, without assuming host/server clock sync."""
    groups = {"workflows": workflows, "tasks": tasks, "attempts": attempts}
    summaries, all_points, issues = {}, [], []
    for name, records in groups.items():
        points = []
        for record in records:
            dates = {key: timestamp(record[key]) for key in ("createdAt", "startedAt", "finishedAt") if record.get(key)}
            for earlier, later in (("createdAt", "startedAt"), ("createdAt", "finishedAt"), ("startedAt", "finishedAt")):
                if earlier in dates and later in dates and dates[later] < dates[earlier]:
                    issues.append(f"Persisted {name} {record.get('id', record.get('taskId'))} has {later} before {earlier}")
            points.extend((value, record[key]) for key, value in dates.items())
        summaries[name] = summarize_span(points)
        all_points.extend(points)
    summaries["combined"] = summarize_span(all_points)
    return summaries, issues


def summarize_span(points):
    if not points:
        return {"firstTimestamp": None, "lastTimestamp": None, "spanSeconds": None, "timestampCount": 0}
    first, last = min(points), max(points)
    return {"firstTimestamp": first[1], "lastTimestamp": last[1], "spanSeconds": last[0] - first[0], "timestampCount": len(points)}

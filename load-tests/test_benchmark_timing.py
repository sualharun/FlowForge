"""Deterministic clock/deadline and benchmark-result tests; no running stack required."""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import io
import itertools
import subprocess
import unittest
from unittest.mock import MagicMock, patch

import benchmark
from benchmark_timing import ClockSample, ExecutionClock, database_spans

EPOCH = 1_700_000_000


def sample(wall, monotonic, capture_span=0):
    return ClockSample(round((EPOCH + wall) * 1e9), round(monotonic * 1e9), round(capture_span * 1e9))


def date(seconds):
    return dt.datetime.fromtimestamp(EPOCH + seconds, dt.timezone.utc).isoformat()


def completed_record(start, finish):
    return {"id": "one", "createdAt": date(start), "startedAt": date(start), "finishedAt": date(finish)}


def clock_for(*pairs):
    points = iter(sample(*pair) for pair in pairs)
    return ExecutionClock(lambda: next(points))


class ClockValidationTests(unittest.TestCase):
    def test_steady_clocks_validate_and_preserve_raw_boundaries(self):
        clock = clock_for((0, 0), (3, 3), (10, 10))
        self.assertEqual(clock.observe(), 3)
        clock.stop()
        report = clock.report([completed_record(1, 9)], [], [])
        self.assertTrue(report["consistent"])
        self.assertEqual(report["monotonicDurationSeconds"], 10)
        self.assertEqual(report["wallDurationSeconds"], 10)
        self.assertEqual(report["executionStart"]["wallTimeUnixNs"], EPOCH * 1_000_000_000)
        self.assertEqual(report["executionEnd"]["monotonicNs"], 10_000_000_000)
        self.assertAlmostEqual(report["allowedDiscrepancySeconds"], .260)

    def test_sleep_divergence_counts_toward_deadline_and_invalidates_timing(self):
        clock = clock_for((0, 0), (100, 2))
        clock.stop()
        self.assertEqual(clock.deadline_elapsed_seconds, 100)
        report = clock.report([completed_record(1, 99)], [], [])
        self.assertFalse(report["consistent"])
        self.assertEqual(report["wallMonotonicDisagreementSeconds"], 98)

    def test_forward_wall_jump_cannot_extend_deadline(self):
        clock = clock_for((0, 0), (1000, 1), (1001, 2))
        self.assertEqual(clock.observe(), 1000)
        clock.stop()
        self.assertEqual(clock.deadline_elapsed_seconds, 1001)
        self.assertFalse(clock.report([completed_record(0, 1)], [], [])["consistent"])

    def test_backward_wall_jump_cannot_reduce_deadline(self):
        clock = clock_for((0, 0), (5, 5), (-5, 6), (-1, 10))
        self.assertEqual(clock.observe(), 5)
        self.assertEqual(clock.observe(), 6)
        clock.stop()
        self.assertEqual(clock.deadline_elapsed_seconds, 10)
        self.assertFalse(clock.report([completed_record(0, 1)], [], [])["consistent"])

    def test_cancelled_clock_jumps_are_detected_despite_matching_endpoints(self):
        clock = clock_for((0, 0), (10, 1), (2, 2), (20, 20))
        clock.observe()
        clock.observe()
        clock.stop()
        report = clock.report([completed_record(1, 19)], [], [])
        self.assertEqual(report["wallMonotonicDisagreementSeconds"], 0)
        self.assertEqual(report["accumulatedClockDisagreementSeconds"], 18)
        self.assertFalse(report["consistent"])

    def test_database_span_larger_than_client_duration_is_rejected(self):
        clock = clock_for((0, 0), (10, 10))
        clock.stop()
        report = clock.report([completed_record(0, 20)], [], [])
        self.assertFalse(report["consistent"])
        self.assertIn("Persisted database timestamp span exceeds", " ".join(report["issues"]))

    def test_constant_database_clock_offset_does_not_invalidate_span(self):
        clock = clock_for((0, 0), (10, 10))
        clock.stop()
        report = clock.report([completed_record(1001, 1009)], [], [])
        self.assertTrue(report["consistent"])

    def test_long_clock_sampling_pause_invalidates_measurement(self):
        clock = clock_for((0, 0), (10, 10, .5))
        clock.stop()
        report = clock.report([completed_record(1, 9)], [], [])
        self.assertFalse(report["consistent"])
        self.assertIn("Clock sampling was interrupted", " ".join(report["issues"]))

    def test_stopping_twice_does_not_include_verification_time(self):
        clock = clock_for((0, 0), (10, 10))
        clock.stop()
        clock.stop()  # No third sample exists: the second stop must not sample.
        self.assertEqual(clock.observe(), 10)
        self.assertEqual(clock.monotonic_duration_seconds, 10)

    def test_database_groups_cover_workflows_tasks_and_attempts(self):
        result, issues = database_spans([completed_record(0, 10)], [completed_record(2, 9)], [completed_record(4, 8)])
        self.assertFalse(issues)
        self.assertEqual(result["combined"]["spanSeconds"], 10)
        self.assertEqual(result["workflows"]["spanSeconds"], 10)
        self.assertEqual(result["tasks"]["spanSeconds"], 7)
        self.assertEqual(result["attempts"]["spanSeconds"], 4)
        self.assertEqual(result["combined"]["timestampCount"], 9)

    def test_negative_persisted_duration_is_rejected(self):
        clock = clock_for((0, 0), (10, 10))
        clock.stop()
        self.assertFalse(clock.report([completed_record(9, 1)], [], [])["consistent"])

    def test_missing_persisted_evidence_is_rejected(self):
        clock = clock_for((0, 0), (10, 10))
        clock.stop()
        self.assertFalse(clock.report([], [], [])["consistent"])


class BenchmarkResultTests(unittest.TestCase):
    def run_benchmark(self, database_finish):
        args = argparse.Namespace(base_url="http://unused", scenario="normal", workflows=1,
            submit_concurrency=1, poll_concurrency=1, width=1, layers=1, delay_ms=0,
            timeout=100, poll_interval=.01, output="unused.json", skip_payment_verification=False)
        workflow = {**completed_record(0, database_finish), "status": "COMPLETED", "totalTasks": 1}
        task = {**completed_record(0, database_finish), "status": "COMPLETED"}
        attempt = {**task, "attemptNumber": 1}
        details = {"workflowId": "one", "tasks": [task], "attempts": [attempt],
                   "dependencyEdgesChecked": 0, "retryDelays": [], "duplicateSideEffectCount": 0}
        client = MagicMock()
        client.submit.return_value = workflow
        client.request.side_effect = lambda path: workflow if path.endswith("/one") else {}
        steps = itertools.count()
        timer = ExecutionClock(lambda: sample((n := next(steps)), n))
        # benchmark.subprocess is the shared module object, so patching run() also
        # intercepts the subprocess call platform.platform() makes internally when
        # its cache is cold. Stub the module benchmark uses instead of relying on
        # that cache being warm, which made this test order-dependent.
        with patch.object(benchmark, "parse_args", return_value=args), \
             patch.object(benchmark, "Client", return_value=client), \
             patch.object(benchmark, "ExecutionClock", return_value=timer), \
             patch.object(benchmark, "verify_workflow", return_value=details), \
             patch.object(benchmark, "platform", MagicMock(
                 platform=lambda: "test-platform", python_version=lambda: "3.13.0")), \
             patch.object(benchmark.subprocess, "run", side_effect=[
                 subprocess.CompletedProcess([], 0, "test-commit\n", ""),
                 subprocess.CompletedProcess([], 0, " M load-tests/benchmark.py\n", "")]), \
             patch.object(benchmark, "write_result") as write, contextlib.redirect_stdout(io.StringIO()):
            status = benchmark.main()
        return status, write.call_args.args[1]

    def test_invalid_timing_keeps_correctness_but_nulls_throughput_and_fails_cli(self):
        status, result = self.run_benchmark(database_finish=1000)
        self.assertEqual(status, 1)
        self.assertTrue(result["correctnessVerified"])
        self.assertFalse(result["timingVerified"])
        self.assertFalse(result["verified"])
        self.assertIsNone(result["summary"]["workflowsPerSecond"])
        self.assertIsNone(result["summary"]["tasksPerSecond"])
        self.assertTrue(any(error["phase"] == "timing" for error in result["errors"]))

    def test_valid_measurement_reports_capacity_and_worktree_provenance(self):
        status, result = self.run_benchmark(database_finish=1)
        self.assertEqual(status, 0)
        self.assertEqual(result["schemaVersion"], 2)
        self.assertTrue(result["correctnessVerified"])
        self.assertTrue(result["timingVerified"])
        self.assertTrue(result["verified"])
        self.assertGreater(result["summary"]["workflowsPerSecond"], 0)
        self.assertEqual(result["environment"]["gitCommit"], "test-commit")
        self.assertTrue(result["environment"]["gitDirty"])
        self.assertEqual(len(result["environment"]["harnessSourceSha256"]["benchmark.py"]), 64)


if __name__ == "__main__":
    unittest.main()

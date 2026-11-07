"""Needs a saved trace:
    TRACY_TEST_TRACE=/path/to.tracy python -m pytest extra/mcp/test_thread_zone_stats.py
"""
import os
import time

import pytest

import tracy_mcp

TRACE = os.environ.get("TRACY_TEST_TRACE")
pytestmark = pytest.mark.skipif(not TRACE, reason="TRACY_TEST_TRACE not set")


@pytest.fixture(scope="module")
def worker():
    server = tracy_mcp.tracy_server
    w = server.create_worker_from_file(server.open_file(TRACE))
    while not w.is_background_done():
        time.sleep(0.1)
    yield w
    w.shutdown()


def test_threads_partition_the_global_stats(worker):
    counts, totals = {}, {}
    for thread in worker.get_threads():
        for name, stats in worker.get_thread_zone_stats(thread["id"]).items():
            counts[name] = counts.get(name, 0) + stats.count
            totals[name] = totals.get(name, 0) + stats.total
    for name, stats in worker.get_all_zone_stats().items():
        assert counts.get(name, 0) == stats.count
        assert totals.get(name, 0) == stats.total


def test_unknown_thread_is_empty(worker):
    assert worker.get_thread_zone_stats(2**40) == {}

def test_global_stats_match_all_occurrences(worker):
    import math

    limit = worker.get_zone_count() + 1
    for name, stats in worker.get_all_zone_stats().items():
        durations = worker.get_zone_durations(name, limit)
        assert stats.count == len(durations), name
        assert stats.total == sum(durations), name
        assert stats.min == min(durations), name
        assert stats.max == max(durations), name
        assert stats.avg == pytest.approx(sum(durations) / len(durations)), name
        assert stats.sum_sq == pytest.approx(math.fsum(float(d) * d for d in durations)), name

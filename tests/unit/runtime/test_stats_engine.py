"""Tests for StatsAndTelemetryEngine."""
import uuid

from elephantbroker.runtime.stats.engine import StatsAndTelemetryEngine
from elephantbroker.runtime.trace.ledger import TraceLedger


class TestStatsEngine:
    def _make(self):
        return StatsAndTelemetryEngine(TraceLedger())

    async def test_record_injection(self):
        engine = self._make()
        await engine.record_injection(uuid.uuid4(), uuid.uuid4(), 100)
        stats = await engine.get_stats_by_profile("any")
        assert stats["total_injections"] == 1.0

    async def test_record_use(self):
        engine = self._make()
        sid, fid = uuid.uuid4(), uuid.uuid4()
        await engine.record_injection(sid, fid, 50)
        await engine.record_use(sid, fid, True)
        stats = await engine.get_stats_by_profile("any")
        assert stats["useful_count"] == 1.0

    async def test_usefulness_rate(self):
        engine = self._make()
        sid = uuid.uuid4()
        await engine.record_injection(sid, uuid.uuid4(), 10)
        await engine.record_injection(sid, uuid.uuid4(), 20)
        stats = await engine.get_stats_by_profile("any")
        assert stats["usefulness_rate"] == 0.0

    async def test_empty_stats(self):
        engine = self._make()
        stats = await engine.get_stats_by_profile("any")
        assert stats["total_injections"] == 0.0

    async def test_record_use_with_was_useful_false(self):
        engine = self._make()
        sid, fid = uuid.uuid4(), uuid.uuid4()
        await engine.record_injection(sid, fid, 50)
        await engine.record_use(sid, fid, False)
        stats = await engine.get_stats_by_profile("any")
        assert stats["useful_count"] == 0.0

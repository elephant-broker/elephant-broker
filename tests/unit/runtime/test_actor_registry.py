"""Tests for ActorRegistry."""
import uuid
from unittest.mock import AsyncMock

from elephantbroker.runtime.actors.registry import ActorRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from tests.fixtures.factories import make_actor_ref


class TestActorRegistry:
    def _make(self):
        graph = AsyncMock()
        ledger = TraceLedger()
        reg = ActorRegistry(graph, ledger, dataset_name="test_ds")
        return reg, graph, ledger

    async def test_register_actor(self, monkeypatch, mock_add_data_points, mock_cognee):
        reg, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)
        actor = make_actor_ref()
        result = await reg.register_actor(actor)
        assert result.id == actor.id
        assert len(mock_add_data_points.calls) == 1

    async def test_resolve_existing_actor(self):
        reg, graph, _ = self._make()
        actor = make_actor_ref()
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(actor.id), "actor_type": actor.type.value,
            "display_name": actor.display_name, "authority_level": 0,
            "handles": [], "trust_level": 0.5, "tags": [],
        })
        result = await reg.resolve_actor(actor.id)
        assert result is not None
        assert result.display_name == actor.display_name

    async def test_resolve_missing_returns_none(self):
        reg, graph, _ = self._make()
        graph.get_entity = AsyncMock(return_value=None)
        result = await reg.resolve_actor(uuid.uuid4())
        assert result is None

    async def test_get_authority_chain(self):
        reg, graph, _ = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        chain = await reg.get_authority_chain(uuid.uuid4())
        assert chain == []

    async def test_get_relationships(self):
        reg, graph, _ = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        rels = await reg.get_relationships(uuid.uuid4())
        assert rels == []

    async def test_register_emits_trace_event(self, monkeypatch, mock_add_data_points, mock_cognee):
        reg, graph, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)
        await reg.register_actor(make_actor_ref())
        events = await ledger.query_trace(
            __import__("elephantbroker.schemas.trace", fromlist=["TraceQuery"]).TraceQuery()
        )
        assert len(events) == 1

    async def test_register_calls_add_data_points(self, monkeypatch, mock_add_data_points, mock_cognee):
        """add_data_points is called with ActorDataPoint."""
        reg, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)
        actor = make_actor_ref()
        await reg.register_actor(actor)
        assert len(mock_add_data_points.calls) == 1
        dp = mock_add_data_points.calls[0]["data_points"][0]
        assert dp.eb_id == str(actor.id)

    async def test_register_calls_cognee_add_with_actor_text(self, monkeypatch, mock_add_data_points, mock_cognee):
        """cognee.add() is called with actor description text."""
        reg, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)
        actor = make_actor_ref()
        await reg.register_actor(actor)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        assert actor.display_name in text

    async def test_register_cognee_text_includes_handles(self, monkeypatch, mock_add_data_points, mock_cognee):
        """When actor has handles, cognee.add() text includes them."""
        reg, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)
        actor = make_actor_ref(handles=["@user", "#channel"])
        await reg.register_actor(actor)
        text = mock_cognee.add.call_args[0][0]
        assert "@user" in text

    async def test_register_cognee_uses_correct_dataset(self, monkeypatch, mock_add_data_points, mock_cognee):
        """cognee.add() uses the configured dataset_name."""
        reg, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)
        await reg.register_actor(make_actor_ref())
        _, kwargs = mock_cognee.add.call_args
        assert kwargs["dataset_name"] == "test_ds"

    async def test_get_authority_chain_returns_supervisors(self):
        reg, graph, _ = self._make()
        sup_id = uuid.uuid4()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(sup_id), "actor_type": "manager_agent",
                "display_name": "manager", "authority_level": 5,
                "handles": [], "trust_level": 0.9, "tags": [],
            }
        }])
        chain = await reg.get_authority_chain(uuid.uuid4())
        assert len(chain) == 1
        assert chain[0].display_name == "manager"

    async def test_get_relationships_returns_typed(self):
        reg, graph, _ = self._make()
        source_id = uuid.uuid4()
        target_id = uuid.uuid4()
        graph.query_cypher = AsyncMock(return_value=[{
            "source": str(source_id), "target": str(target_id),
            "rel_type": "SUPERVISES", "props": {},
        }])
        rels = await reg.get_relationships(source_id)
        assert len(rels) == 1
        assert rels[0].relationship_type.value == "supervises"

    async def test_get_relationships_skips_unknown_type(self):
        reg, graph, _ = self._make()
        graph.query_cypher = AsyncMock(return_value=[{
            "source": str(uuid.uuid4()), "target": str(uuid.uuid4()),
            "rel_type": "UNKNOWN_REL", "props": {},
        }])
        rels = await reg.get_relationships(uuid.uuid4())
        assert rels == []

"""Tests for MetricsContext gateway-scoped metrics helper."""
from elephantbroker.runtime.metrics import MetricsContext, METRICS_AVAILABLE


def test_metrics_context_stores_gateway_id():
    ctx = MetricsContext("gw-test")
    assert ctx._gw == "gw-test"


def test_metrics_context_methods_exist():
    """Meta-test: ensure all expected helper methods are present."""
    ctx = MetricsContext("gw-test")
    expected_methods = [
        "inc_store", "inc_dedup", "inc_edge", "inc_gdpr_delete",
        "inc_pipeline", "inc_cognify", "inc_buffer_flush",
        "inc_working_set_build", "inc_rerank_call", "inc_rerank_fallback",
        "inc_embedding_cache", "inc_goal_hint",
        "inc_procedure_qualified", "inc_procedure_activated",
        "inc_procedure_step_completed", "inc_procedure_proof",
        "inc_facts_stored", "inc_facts_superseded",
        "inc_retrieval", "inc_llm_call",
        "observe_store_duration", "observe_retrieval_duration",
        "observe_pipeline_duration", "observe_llm_duration",
        "observe_working_set_duration", "observe_candidates",
        "observe_selected", "observe_tokens_used",
    ]
    for method in expected_methods:
        assert hasattr(ctx, method), f"Missing method: {method}"


def test_metrics_context_inc_store_does_not_raise():
    """Calling inc_store should not raise regardless of prometheus availability."""
    ctx = MetricsContext("gw-test")
    # Should not raise even if prometheus not available
    ctx.inc_store("store", "success")


def test_different_gateways_are_independent():
    ctx_a = MetricsContext("gw-a")
    ctx_b = MetricsContext("gw-b")
    assert ctx_a._gw != ctx_b._gw

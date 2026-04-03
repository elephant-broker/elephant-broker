"""Tier 3: Decision Domain Auto-Discovery — analyze guard history for UNCATEGORIZED patterns.

No LLM calls. Pure frequency analysis + embedding similarity.
Runs as part of consolidation pipeline after Stage 9.
"""
from __future__ import annotations

import json
import logging
import math
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import DomainSuggestion

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.runtime.redis_keys import RedisKeyBuilder

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.domain_discovery")

# DecisionDomain enum values and descriptions for embedding comparison
_EXISTING_DOMAINS = {
    "financial": "Financial transactions, payments, money transfers, billing",
    "data_access": "Database queries, file access, data exports, API calls",
    "code_change": "Code modifications, deployments, configuration changes",
    "communication": "Sending messages, emails, notifications to external parties",
    "system_admin": "System administration, user management, permission changes",
    "info_share": "Sharing information externally, data publishing",
    "resource_allocation": "Resource provisioning, scaling, cost commitments",
    "safety_critical": "Safety-critical operations, irreversible actions",
    "content_generation": "Content creation, document generation",
    "uncategorized": "Actions not matching any known domain",
}

_MIN_OCCURRENCES = 5


def _cosine_sim(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na < 1e-10 or nb < 1e-10:
        return 0.0
    return dot / (na * nb)


class DomainDiscoveryTask:
    """Analyze guard history for UNCATEGORIZED patterns.

    Algorithm (BS-9b):
    1. SCAN ALL guard_history Redis keys for this gateway
    2. Parse GuardEvent JSON entries, extract action_target + decision_domain
    3. Group by action_target where decision_domain == "uncategorized"
    4. For action_targets with 5+ occurrences:
       Embed and compare to existing domain descriptions via cosine similarity
    5. Return DomainSuggestion list
    """

    def __init__(
        self,
        embedding_service: CachedEmbeddingService,
        redis,
        redis_keys: RedisKeyBuilder,
    ) -> None:
        self._embeddings = embedding_service
        self._redis = redis
        self._keys = redis_keys

    @traced
    async def run(self, gateway_id: str) -> list[DomainSuggestion]:
        if not self._redis:
            return []

        # 1. Scan guard_history keys for this gateway (BS-9b)
        pattern = f"eb:{gateway_id}:guard_history:*"
        uncategorized_actions: dict[str, int] = {}

        try:
            cursor = 0
            while True:
                cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)
                for key in keys:
                    events_raw = await self._redis.lrange(key, 0, -1)
                    for ev_raw in events_raw:
                        try:
                            ev = json.loads(ev_raw) if isinstance(ev_raw, str) else ev_raw
                            domain = ev.get("decision_domain", "")
                            target = ev.get("action_target", "")
                            if domain == "uncategorized" and target:
                                uncategorized_actions[target] = uncategorized_actions.get(target, 0) + 1
                        except (json.JSONDecodeError, TypeError):
                            continue
                if cursor == 0:
                    break
        except Exception:
            logger.warning("Failed to scan guard history for domain discovery", exc_info=True)
            return []

        # 2. Filter by minimum occurrences
        candidates = {
            target: count for target, count in uncategorized_actions.items()
            if count >= _MIN_OCCURRENCES
        }
        if not candidates:
            return []

        # 3. Embed candidates + domain descriptions
        candidate_texts = list(candidates.keys())
        domain_texts = list(_EXISTING_DOMAINS.values())
        domain_names = list(_EXISTING_DOMAINS.keys())

        try:
            all_embeddings = await self._embeddings.embed_batch(candidate_texts + domain_texts)
        except Exception:
            logger.warning("Embedding failed for domain discovery", exc_info=True)
            return []

        candidate_embs = all_embeddings[: len(candidate_texts)]
        domain_embs = all_embeddings[len(candidate_texts) :]

        # 4. Find best matching domain for each candidate
        suggestions: list[DomainSuggestion] = []
        for i, target in enumerate(candidate_texts):
            best_sim = 0.0
            best_domain = "uncategorized"
            for j, d_emb in enumerate(domain_embs):
                sim = _cosine_sim(candidate_embs[i], d_emb)
                if sim > best_sim:
                    best_sim = sim
                    best_domain = domain_names[j]

            suggestions.append(DomainSuggestion(
                action_target=target,
                suggested_domain=best_domain,
                occurrences=candidates[target],
                similarity_to_existing=best_sim,
                gateway_id=gateway_id,
            ))

        logger.info(
            "Tier 3: %d uncategorized actions analyzed, %d suggestions (gateway=%s)",
            len(uncategorized_actions), len(suggestions), gateway_id,
        )
        return suggestions

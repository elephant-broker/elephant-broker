"""Semantic guard index — BM25 + embedding similarity for red-line detection (Phase 7 — §7.5)."""
from __future__ import annotations

import logging
import math
import re

from elephantbroker.schemas.guards import SemanticMatch

logger = logging.getLogger(__name__)


class SemanticGuardIndex:
    """BM25 + embedding-based semantic guard matching."""

    def __init__(self, embedding_service=None) -> None:
        self._embedding_service = embedding_service
        self._exemplar_texts: list[str] = []
        self._exemplar_embeddings: list[list[float]] = []
        # BM25 index state
        self._bm25_docs: list[list[str]] = []
        self._bm25_df: dict[str, int] = {}
        self._bm25_avgdl: float = 0.0
        self._bm25_N: int = 0

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Simple tokenization: lowercase, strip punctuation, split."""
        cleaned = re.sub(r'[^\w\s]', '', text.lower())
        return cleaned.split()

    async def build_index(self, redline_exemplars: list[str]) -> None:
        """Build BM25 index and optionally pre-compute embeddings."""
        self._exemplar_texts = list(redline_exemplars)
        self._exemplar_embeddings = []

        # BM25 index
        self._bm25_docs = [self._tokenize(text) for text in redline_exemplars]
        self._bm25_N = len(self._bm25_docs)
        self._bm25_df = {}
        total_len = 0
        for doc in self._bm25_docs:
            total_len += len(doc)
            seen = set()
            for token in doc:
                if token not in seen:
                    self._bm25_df[token] = self._bm25_df.get(token, 0) + 1
                    seen.add(token)
        self._bm25_avgdl = total_len / self._bm25_N if self._bm25_N > 0 else 0.0

    def score_bm25(self, action_text: str, k1: float = 1.5, b: float = 0.75) -> list[tuple[str, float]]:
        """Score action_text against all exemplars using BM25. Returns sorted (text, score) pairs."""
        if not self._bm25_docs or self._bm25_avgdl == 0:
            return []

        query_tokens = self._tokenize(action_text)
        if not query_tokens:
            return []

        n_docs = self._bm25_N
        scores: list[tuple[str, float]] = []

        for i, doc in enumerate(self._bm25_docs):
            doc_len = len(doc)
            score = 0.0
            # Count term frequencies in this doc
            tf_map: dict[str, int] = {}
            for token in doc:
                tf_map[token] = tf_map.get(token, 0) + 1

            for qt in query_tokens:
                df = self._bm25_df.get(qt, 0)
                if df == 0:
                    continue
                idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1)
                tf = tf_map.get(qt, 0)
                numerator = tf * (k1 + 1)
                denominator = tf + k1 * (1 - b + b * doc_len / self._bm25_avgdl)
                score += idf * numerator / denominator

            if score > 0:
                scores.append((self._exemplar_texts[i], score))

        if not scores:
            return []

        # Normalize by max score
        max_score = max(s for _, s in scores)
        if max_score > 0:
            scores = [(text, s / max_score) for text, s in scores]

        scores.sort(key=lambda x: x[1], reverse=True)
        return scores

    async def check_similarity(self, action_text: str, threshold: float = 0.80) -> list[SemanticMatch]:
        """Check semantic similarity using embeddings. Returns matches above threshold."""
        if not self._exemplar_texts or not self._embedding_service:
            return []

        try:
            action_embedding = await self._embedding_service.embed_text(action_text)
        except Exception as exc:
            logger.warning("Embedding failed for guard similarity check: %s", exc)
            return []

        # Compute embeddings for exemplars if not cached
        if not self._exemplar_embeddings:
            try:
                for text in self._exemplar_texts:
                    emb = await self._embedding_service.embed_text(text)
                    self._exemplar_embeddings.append(emb)
            except Exception as exc:
                logger.warning("Exemplar embedding failed: %s", exc)
                return []

        matches: list[SemanticMatch] = []
        for i, exemplar_emb in enumerate(self._exemplar_embeddings):
            sim = self._cosine_similarity(action_embedding, exemplar_emb)
            if sim >= threshold:
                matches.append(SemanticMatch(
                    exemplar_text=self._exemplar_texts[i],
                    similarity=sim,
                ))

        matches.sort(key=lambda m: m.similarity, reverse=True)
        return matches

    @staticmethod
    def _cosine_similarity(a: list[float], b: list[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if len(a) != len(b) or not a:
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def clear(self) -> None:
        """Clear all index state."""
        self._exemplar_texts = []
        self._exemplar_embeddings = []
        self._bm25_docs = []
        self._bm25_df = {}
        self._bm25_avgdl = 0.0
        self._bm25_N = 0

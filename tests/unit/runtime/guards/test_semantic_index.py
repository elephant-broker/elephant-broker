"""Tests for SemanticGuardIndex (Phase 7 — §7.5)."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.guards.semantic_index import SemanticGuardIndex


class TestBM25:
    @pytest.mark.asyncio
    async def test_build_index(self):
        idx = SemanticGuardIndex()
        await idx.build_index(["delete production database", "exfiltrate all data"])
        assert len(idx._exemplar_texts) == 2
        assert idx._bm25_N == 2

    def test_score_bm25_exact_match(self):
        idx = SemanticGuardIndex()
        idx._exemplar_texts = ["delete production database"]
        idx._bm25_docs = [idx._tokenize("delete production database")]
        idx._bm25_N = 1
        idx._bm25_df = {"delete": 1, "production": 1, "database": 1}
        idx._bm25_avgdl = 3.0
        scores = idx.score_bm25("delete production database")
        assert len(scores) >= 1
        assert scores[0][1] == 1.0  # Normalized to 1.0

    def test_score_bm25_no_match(self):
        idx = SemanticGuardIndex()
        idx._exemplar_texts = ["delete production database"]
        idx._bm25_docs = [idx._tokenize("delete production database")]
        idx._bm25_N = 1
        idx._bm25_df = {"delete": 1, "production": 1, "database": 1}
        idx._bm25_avgdl = 3.0
        scores = idx.score_bm25("hello world")
        assert len(scores) == 0

    def test_score_bm25_empty_index(self):
        idx = SemanticGuardIndex()
        scores = idx.score_bm25("anything")
        assert scores == []

    def test_score_bm25_empty_query(self):
        idx = SemanticGuardIndex()
        idx._exemplar_texts = ["test"]
        idx._bm25_docs = [["test"]]
        idx._bm25_N = 1
        idx._bm25_df = {"test": 1}
        idx._bm25_avgdl = 1.0
        scores = idx.score_bm25("")
        assert scores == []

    @pytest.mark.asyncio
    async def test_build_index_and_score(self):
        idx = SemanticGuardIndex()
        await idx.build_index(["delete production database", "send confidential report"])
        scores = idx.score_bm25("delete the production database now")
        assert len(scores) >= 1
        assert scores[0][0] == "delete production database"
        assert scores[0][1] > 0.5

    def test_tokenize_strips_punctuation(self):
        tokens = SemanticGuardIndex._tokenize("Hello, World! This is a test.")
        assert "hello" in tokens
        assert "world" in tokens
        assert "," not in tokens
        assert "!" not in tokens


class TestSemanticSimilarity:
    @pytest.mark.asyncio
    async def test_check_similarity_no_exemplars(self):
        idx = SemanticGuardIndex(embedding_service=AsyncMock())
        matches = await idx.check_similarity("test")
        assert matches == []

    @pytest.mark.asyncio
    async def test_check_similarity_above_threshold(self):
        embed = AsyncMock()
        embed.embed_text = AsyncMock(side_effect=[
            [1.0, 0.0, 0.0],  # action
            [0.99, 0.1, 0.0],  # exemplar
        ])
        idx = SemanticGuardIndex(embedding_service=embed)
        idx._exemplar_texts = ["dangerous action"]
        matches = await idx.check_similarity("dangerous action", threshold=0.9)
        assert len(matches) >= 1

    @pytest.mark.asyncio
    async def test_check_similarity_below_threshold(self):
        embed = AsyncMock()
        embed.embed_text = AsyncMock(side_effect=[
            [1.0, 0.0, 0.0],  # action
            [0.0, 1.0, 0.0],  # exemplar (orthogonal)
        ])
        idx = SemanticGuardIndex(embedding_service=embed)
        idx._exemplar_texts = ["unrelated"]
        matches = await idx.check_similarity("test", threshold=0.8)
        assert len(matches) == 0

    @pytest.mark.asyncio
    async def test_check_similarity_embedding_error(self):
        embed = AsyncMock()
        embed.embed_text = AsyncMock(side_effect=Exception("API error"))
        idx = SemanticGuardIndex(embedding_service=embed)
        idx._exemplar_texts = ["test"]
        matches = await idx.check_similarity("test")
        assert matches == []


class TestCosine:
    def test_identical_vectors(self):
        assert SemanticGuardIndex._cosine_similarity([1, 0, 0], [1, 0, 0]) == 1.0

    def test_orthogonal_vectors(self):
        assert SemanticGuardIndex._cosine_similarity([1, 0, 0], [0, 1, 0]) == 0.0

    def test_empty_vectors(self):
        assert SemanticGuardIndex._cosine_similarity([], []) == 0.0

    def test_zero_vector(self):
        assert SemanticGuardIndex._cosine_similarity([0, 0, 0], [1, 1, 1]) == 0.0


class TestClear:
    @pytest.mark.asyncio
    async def test_clear_resets_state(self):
        idx = SemanticGuardIndex()
        await idx.build_index(["test"])
        assert len(idx._exemplar_texts) == 1
        idx.clear()
        assert len(idx._exemplar_texts) == 0
        assert idx._bm25_N == 0


# --- Amendment 7.2 additional tests ---


class TestBM25Extended:
    def test_bm25_partial_keyword_match(self):
        """BM25 matches on partial keyword overlap."""
        idx = SemanticGuardIndex()
        idx._exemplar_texts = ["delete production database", "exfiltrate confidential data"]
        idx._bm25_docs = [idx._tokenize(t) for t in idx._exemplar_texts]
        idx._bm25_N = 2
        idx._bm25_df = {}
        total_len = 0
        for doc in idx._bm25_docs:
            total_len += len(doc)
            seen = set()
            for token in doc:
                if token not in seen:
                    idx._bm25_df[token] = idx._bm25_df.get(token, 0) + 1
                    seen.add(token)
        idx._bm25_avgdl = total_len / idx._bm25_N
        scores = idx.score_bm25("delete some files")
        assert len(scores) >= 1
        # "delete production database" should match because of shared "delete" token
        assert scores[0][0] == "delete production database"

    def test_bm25_multiple_exemplars_sorted_by_score(self):
        """Multiple exemplars are returned sorted by descending BM25 score."""
        idx = SemanticGuardIndex()
        idx._exemplar_texts = ["delete production database", "drop production table",
                                "unrelated topic about cooking"]
        idx._bm25_docs = [idx._tokenize(t) for t in idx._exemplar_texts]
        idx._bm25_N = 3
        idx._bm25_df = {}
        total_len = 0
        for doc in idx._bm25_docs:
            total_len += len(doc)
            seen = set()
            for token in doc:
                if token not in seen:
                    idx._bm25_df[token] = idx._bm25_df.get(token, 0) + 1
                    seen.add(token)
        idx._bm25_avgdl = total_len / idx._bm25_N
        scores = idx.score_bm25("delete production database immediately")
        assert len(scores) >= 2
        # Scores should be descending
        for i in range(len(scores) - 1):
            assert scores[i][1] >= scores[i + 1][1]

    def test_bm25_duplicate_tokens_in_query(self):
        """Duplicate tokens in query boost TF component."""
        idx = SemanticGuardIndex()
        idx._exemplar_texts = ["delete everything"]
        idx._bm25_docs = [idx._tokenize("delete everything")]
        idx._bm25_N = 1
        idx._bm25_df = {"delete": 1, "everything": 1}
        idx._bm25_avgdl = 2.0
        scores_single = idx.score_bm25("delete")
        scores_double = idx.score_bm25("delete delete")
        # Both should match; double query has more tokens contributing
        assert len(scores_single) >= 1
        assert len(scores_double) >= 1

    @pytest.mark.asyncio
    async def test_build_index_single_exemplar(self):
        idx = SemanticGuardIndex()
        await idx.build_index(["only one exemplar"])
        assert idx._bm25_N == 1
        assert len(idx._exemplar_texts) == 1
        assert idx._bm25_avgdl == 3.0  # "only", "one", "exemplar"

    @pytest.mark.asyncio
    async def test_build_index_empty_list(self):
        idx = SemanticGuardIndex()
        await idx.build_index([])
        assert idx._bm25_N == 0
        assert idx._exemplar_texts == []
        assert idx._bm25_avgdl == 0.0

    @pytest.mark.asyncio
    async def test_build_index_duplicate_exemplars(self):
        idx = SemanticGuardIndex()
        await idx.build_index(["same text", "same text"])
        assert idx._bm25_N == 2
        # Both are indexed (no dedup at build time)
        scores = idx.score_bm25("same text")
        assert len(scores) == 2


class TestSemanticSimilarityExtended:
    @pytest.mark.asyncio
    async def test_check_similarity_multiple_exemplars_above_threshold(self):
        """Multiple exemplars above threshold are all returned."""
        embed = AsyncMock()
        embed.embed_text = AsyncMock(side_effect=[
            [1.0, 0.0, 0.0],  # action
            [0.99, 0.1, 0.0],  # exemplar 1 (high sim)
            [0.95, 0.2, 0.0],  # exemplar 2 (also high sim)
        ])
        idx = SemanticGuardIndex(embedding_service=embed)
        idx._exemplar_texts = ["dangerous action 1", "dangerous action 2"]
        matches = await idx.check_similarity("dangerous", threshold=0.8)
        assert len(matches) == 2
        # Sorted by similarity descending
        assert matches[0].similarity >= matches[1].similarity

    @pytest.mark.asyncio
    async def test_check_similarity_at_exact_threshold_boundary(self):
        """Similarity exactly at threshold should be included (>= check)."""
        embed = AsyncMock()
        # Create vectors that produce cosine similarity of exactly 0.8
        # Use [0.8, 0.6, 0] and [1, 0, 0]: cos = 0.8 / (1.0 * 1.0) = 0.8
        embed.embed_text = AsyncMock(side_effect=[
            [1.0, 0.0, 0.0],    # action
            [0.8, 0.6, 0.0],    # exemplar
        ])
        idx = SemanticGuardIndex(embedding_service=embed)
        idx._exemplar_texts = ["borderline action"]
        matches = await idx.check_similarity("test", threshold=0.8)
        assert len(matches) == 1
        assert matches[0].similarity == pytest.approx(0.8, abs=0.01)


class TestCosineExtended:
    def test_cosine_negative_values(self):
        """Cosine similarity works with negative values."""
        sim = SemanticGuardIndex._cosine_similarity([1.0, -1.0], [-1.0, 1.0])
        assert sim == pytest.approx(-1.0, abs=0.001)

    def test_cosine_very_large_values(self):
        """Cosine similarity is scale-invariant."""
        sim_small = SemanticGuardIndex._cosine_similarity([1.0, 2.0], [3.0, 4.0])
        sim_large = SemanticGuardIndex._cosine_similarity([1000.0, 2000.0], [3000.0, 4000.0])
        assert sim_small == pytest.approx(sim_large, abs=0.001)


class TestTokenizeExtended:
    def test_tokenize_unicode_characters(self):
        """Tokenizer handles unicode characters."""
        tokens = SemanticGuardIndex._tokenize("cafe\u0301 re\u0301sume\u0301")
        assert len(tokens) >= 1
        # Should be lowercased
        for token in tokens:
            assert token == token.lower()

    def test_tokenize_numbers_preserved(self):
        """Numbers are preserved by tokenizer."""
        tokens = SemanticGuardIndex._tokenize("version 42 release 2024")
        assert "42" in tokens
        assert "2024" in tokens
        assert "version" in tokens

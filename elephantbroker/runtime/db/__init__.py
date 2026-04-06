"""PostgreSQL base store — async connection pool + query helpers.

All 7 SQLite audit stores subclass this. Table DDL is owned exclusively by
Alembic (elephantbroker/db/). This class never runs CREATE TABLE.

Usage::

    class MyStore(PostgresStore):
        async def my_method(self) -> list[dict]:
            return await self.fetch("SELECT * FROM my_table WHERE id = $1", some_id)
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PostgresStore:
    """Thin async base class wrapping an asyncpg connection pool.

    Subclasses receive the shared pool via ``init(pool)`` called from
    RuntimeContainer.from_config(). They must NOT create tables — that is
    exclusively Alembic's responsibility.
    """

    def __init__(self) -> None:
        self._pool: Any | None = None  # asyncpg.Pool at runtime

    async def init(self, pool: Any) -> None:
        """Store the shared pool reference.  Called once at startup."""
        self._pool = pool

    @property
    def _ready(self) -> bool:
        return self._pool is not None

    async def execute(self, sql: str, *args: Any) -> str:
        """Execute a DML statement (INSERT/UPDATE/DELETE). Returns status string."""
        if not self._pool:
            raise RuntimeError(f"{type(self).__name__} not initialised — pool is None")
        async with self._pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def executemany(self, sql: str, args_list: list[tuple[Any, ...]]) -> None:
        """Execute a DML statement for each args tuple in args_list."""
        if not self._pool:
            raise RuntimeError(f"{type(self).__name__} not initialised — pool is None")
        async with self._pool.acquire() as conn:
            await conn.executemany(sql, args_list)

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        """Execute a SELECT and return rows as list of dicts."""
        if not self._pool:
            raise RuntimeError(f"{type(self).__name__} not initialised — pool is None")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [dict(row) for row in rows]

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        """Execute a SELECT and return a single row as dict, or None."""
        if not self._pool:
            raise RuntimeError(f"{type(self).__name__} not initialised — pool is None")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            return dict(row) if row is not None else None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        """Execute a SELECT and return a single scalar value."""
        if not self._pool:
            raise RuntimeError(f"{type(self).__name__} not initialised — pool is None")
        async with self._pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def close(self) -> None:
        """Release the pool reference. The pool itself is closed by RuntimeContainer."""
        self._pool = None

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.core.settings import settings

try:
    import redis.asyncio as redis_asyncio
except Exception:  # pragma: no cover - optional dependency fallback
    redis_asyncio = None


@dataclass(slots=True)
class RedisTempBodyStore:
    """Temporary storage for raw email payloads.

    The project prefers Redis for short-lived storage, but the code keeps a
    lightweight in-memory fallback so the rest of the pipeline remains usable
    when Redis client support is unavailable in the runtime environment.
    """

    _client: Any | None = None
    _memory_store: dict[str, str] | None = None

    async def _get_client(self) -> Any | None:
        if self._client is not None:
            return self._client

        if redis_asyncio is None:
            self._memory_store = {}
            return None

        self._client = redis_asyncio.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        return self._client

    async def set_json(self, key: str, value: dict[str, Any], ttl_seconds: int) -> None:
        client = await self._get_client()
        payload = json.dumps(value, ensure_ascii=False, default=str)
        if client is not None:
            await client.set(key, payload, ex=ttl_seconds)
            return
        assert self._memory_store is not None
        self._memory_store[key] = payload

    async def delete(self, key: str) -> None:
        client = await self._get_client()
        if client is not None:
            await client.delete(key)
            return
        if self._memory_store is not None:
            self._memory_store.pop(key, None)


redis_temp_body_store = RedisTempBodyStore()

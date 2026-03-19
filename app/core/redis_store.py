from __future__ import annotations

import json
from typing import Any

import redis.asyncio as redis_asyncio

from app.core.settings import settings


class RedisTempBodyStore:
    """Temporary storage for raw email payloads backed by Redis."""

    def __init__(self) -> None:
        self._client: redis_asyncio.Redis[str] | None = None

    def _get_client(self) -> redis_asyncio.Redis[str]:
        if self._client is None:
            self._client = redis_asyncio.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
        return self._client

    async def ping(self) -> bool:
        client = self._get_client()
        return bool(await client.ping())

    async def set_json(self, key: str, value: dict[str, Any], ttl_seconds: int) -> None:
        client = self._get_client()
        payload = json.dumps(value, ensure_ascii=False, default=str)
        await client.set(key, payload, ex=ttl_seconds)

    async def get_json(self, key: str) -> dict[str, Any] | None:
        client = self._get_client()
        value = await client.get(key)
        if value is None:
            return None
        return json.loads(value)

    async def delete(self, key: str) -> None:
        client = self._get_client()
        await client.delete(key)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


redis_temp_body_store = RedisTempBodyStore()

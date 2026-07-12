"""
redix — ARC provider plugin: Redis/Valkey — cache, distributed locks,
pub/sub, and rate-limit counters ("one dependency, four jobs" per the
Architecture tech-stack table).

Exports `arc.redix`. Nothing in Phase 1 hard-requires it — not every ARC
project wants caching/locks/pubsub/rate-limiting, so it stays a plain
standalone plugin like any other. It only becomes a hard `requires` for
whoever installs `authn` (Phase 4, token store) — that is a fact about
authn's manifest, not about redix itself; redix's own manifest never
changes because of it.

Same lifecycle note as psqldb: register() only constructs the provider;
`await arc.redix.open()` / `await arc.redix.close()` are the application's
job at startup/shutdown until a real lifecycle hook design exists.
"""

from __future__ import annotations

from typing import Any

import redis.asyncio as redis

CAPABILITY = "redix"
URL_KEY = "redix_url"


class RedixProvider:
    def __init__(self, url: str) -> None:
        self.url = url
        self._client: redis.Redis | None = None

    async def open(self) -> None:
        if self._client is None:
            self._client = redis.from_url(self.url)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _client_or_raise(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError(
                "redix client is not open — call `await arc.redix.open()` "
                "during your application's startup first."
            )
        return self._client

    # ---- cache -------------------------------------------------------- #
    async def get(self, key: str) -> Any:
        return await self._client_or_raise().get(key)

    async def set(self, key: str, value: Any, ex: int | None = None) -> None:
        await self._client_or_raise().set(key, value, ex=ex)

    async def delete(self, key: str) -> None:
        await self._client_or_raise().delete(key)

    # ---- distributed lock ---------------------------------------------- #
    def lock(self, name: str, timeout: float = 10.0):
        """`async with arc.redix.lock("job:123"):` ..."""
        return self._client_or_raise().lock(name, timeout=timeout)

    # ---- pub/sub -------------------------------------------------------- #
    async def publish(self, channel: str, message: Any) -> None:
        await self._client_or_raise().publish(channel, message)

    def subscribe(self, channel: str):
        """Returns a pubsub object: `await ps.subscribe(channel)`, then
        `async for msg in ps.listen(): ...`"""
        return self._client_or_raise().pubsub()

    # ---- rate limiting (fixed window) ------------------------------------ #
    async def rate_limit(self, key: str, limit: int, window_seconds: int) -> bool:
        """Returns True if this call is within the limit for the window."""
        client = self._client_or_raise()
        counter_key = f"ratelimit:{key}"
        current = await client.incr(counter_key)
        if current == 1:
            await client.expire(counter_key, window_seconds)
        return current <= limit

    async def health(self) -> dict:
        try:
            pong = await self._client_or_raise().ping()
            return {"ok": bool(pong)}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}


def register(kernel: Any) -> None:
    kernel.settings.declare(URL_KEY, secret=True)

    url = kernel.settings.get(URL_KEY, reveal=True)
    if url is None:
        raise RuntimeError(
            f"'{URL_KEY}' is not set. Run: "
            f"arc settings set {URL_KEY} redis://host:6379/0 --secret"
        )

    provider = RedixProvider(url)
    kernel.export(CAPABILITY, provider, requires=[], optional_requires=[])
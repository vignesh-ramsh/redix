"""
redix — ARC provider plugin: Redis/Valkey — cache, distributed locks,
pub/sub, and rate-limit counters ("one dependency, four jobs" per the
Architecture tech-stack table).

Exports `arc.redix`. Nothing hard-requires it — not every ARC project wants
caching/locks/pubsub/rate-limiting, so it stays a plain standalone plugin
like any other, including for `authn` (Phase 4, docs/arc.MD §3.13): rate
limiting genuinely upgrades when redix is installed, but account lockout
is Postgres-backed and works identically without it — redix stays an
`optional_requires` there too, never a hard dependency. redix's own
manifest never changes based on what any other plugin decides.

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

    async def subscribe(self, channel: str):
        """Returns a pubsub object already subscribed to `channel`:
        `ps = await arc.redix.subscribe("events")`, then
        `async for msg in ps.listen(): ...`. (Previously returned an
        UNsubscribed pubsub and silently ignored `channel` — every caller
        had to subscribe again themselves, or got nothing.)"""
        ps = self._client_or_raise().pubsub()
        await ps.subscribe(channel)
        return ps

    # ---- pattern-based bulk delete ---------------------------------------- #
    async def scan_delete(self, pattern: str) -> int:
        """Deletes every key matching `pattern` (glob-style, e.g. "cache:*")
        using SCAN — never KEYS, which blocks the whole server while it
        walks the entire keyspace in one shot. Generic, not cache-specific:
        `arc clear-cache` (the kernel's own CLI) is what calls this with a
        handful of well-known prefixes; this method itself has no opinion
        about what any prefix means."""
        client = self._client_or_raise()
        deleted = 0
        batch: list[str] = []
        async for key in client.scan_iter(match=pattern, count=500):
            batch.append(key)
            if len(batch) >= 500:
                deleted += await client.delete(*batch)
                batch.clear()
        if batch:
            deleted += await client.delete(*batch)
        return deleted

    # ---- rate limiting (fixed window) ------------------------------------ #
    # INCR + EXPIRE as ONE atomic Lua script — the two-step version had a
    # real failure mode: a crash (or dropped connection) between INCR and
    # EXPIRE left the counter with NO TTL, permanently rate-limiting that
    # key once it crossed the limit (and `arc clear-cache` deliberately
    # never touches ratelimit:* keys, so there was no recovery path short
    # of a manual DEL). EXPIRE also refreshes only when the counter is
    # fresh, preserving the fixed-window semantics exactly.
    _RATE_LIMIT_LUA = """
    local current = redis.call('INCR', KEYS[1])
    if current == 1 then
        redis.call('EXPIRE', KEYS[1], ARGV[1])
    end
    return current
    """

    async def rate_limit(self, key: str, limit: int, window_seconds: int) -> bool:
        """Returns True if this call is within the limit for the window."""
        client = self._client_or_raise()
        counter_key = f"ratelimit:{key}"
        current = await client.eval(self._RATE_LIMIT_LUA, 1, counter_key, window_seconds)
        return int(current) <= limit

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
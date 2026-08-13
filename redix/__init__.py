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

import asyncio
import contextlib
import logging
from typing import Any

import redis.asyncio as redis
from redis.exceptions import LockError

CAPABILITY = "redix"
URL_KEY = "redix_url"
SOCKET_TIMEOUT_MS_KEY = "redix_socket_timeout_ms"

_logger = logging.getLogger("redix")


class RedixProvider:
    def __init__(self, url: str, socket_timeout_ms: int = 5_000) -> None:
        self.url = url
        # 0 disables it (redis-py's own default: no timeout, wait
        # indefinitely). A real gap found by this project's own failure-
        # mode audit: redis.from_url() with no timeout means a Redis that
        # goes silently unresponsive (not a clean connection-refused —
        # a network partition, an overloaded server that stops replying,
        # a black-holed connection) hangs a read/write for as long as the
        # OS's own TCP-level timeout, which on Linux is commonly measured
        # in MINUTES — a request-handling path that expects a cache
        # lookup to cost single-digit milliseconds, and that already
        # catches Exception to degrade gracefully to the DB (see authn's
        # own _cache_get_session/_cache_set_session), was never actually
        # protected against THIS failure mode: `except Exception` can't
        # catch a call that hasn't raised yet because it's still blocked.
        self.socket_timeout_ms = socket_timeout_ms
        self._client: redis.Redis | None = None

    async def open(self) -> None:
        if self._client is None:
            timeout = self.socket_timeout_ms / 1000 if self.socket_timeout_ms > 0 else None
            self._client = redis.from_url(
                self.url, socket_timeout=timeout, socket_connect_timeout=timeout
            )

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
    def lock(self, name: str, timeout: float = 10.0, *, renew_interval: float | None = None):
        """`async with arc.redix.lock("job:123") as fencing_token:` — a
        distributed lock that renews itself in the background for as long
        as it's held (so a critical section that happens to run longer
        than `timeout` doesn't silently lose the lock out from under it —
        the single biggest footgun with a plain TTL-based lock), and hands
        back a fencing token: an integer that only ever goes up, once per
        successful acquire of THIS name, forever (Kleppmann's "How to do
        distributed locking" — the fencing-tokens pattern). Pass it to
        whatever you're protecting (a write to psqldb, a call to some other
        service) so that side can reject any write carrying a token lower
        than the highest one it has already seen — the one thing renewal
        can reduce the odds of but can never fully rule out (a long GC/
        scheduler pause between acquiring and your first write can still
        let the TTL lapse before the first renewal even runs).

        Single Redis instance/primary only — this is a SET NX PX lock plus
        a best-effort background heartbeat, not Redlock's multi-node
        quorum. Good enough for "only one of my N workers does this job
        right now"; not a substitute for a real consensus system if you
        need correctness under a Redis primary failover.
        """
        return _RenewingLock(
            self._client_or_raise(), name, timeout=timeout, renew_interval=renew_interval
        )

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


class _RenewingLock:
    """`async with` wrapper around a redis-py Lock: acquires it, hands back
    a fencing token, keeps the TTL alive with a background heartbeat for as
    long as the `async with` block runs, and always cancels the heartbeat
    and releases on exit — even if the block raised. See
    RedixProvider.lock()'s own docstring for the full design rationale.
    """

    def __init__(
        self,
        client: redis.Redis,
        name: str,
        *,
        timeout: float,
        renew_interval: float | None,
    ) -> None:
        self._name = name
        self._timeout = timeout
        # A third of the TTL by default — the same margin Redlock's own
        # writeup uses: enough headroom that one slow/dropped renewal
        # doesn't cost the lock outright, without renewing so often it
        # meaningfully adds load.
        self._renew_interval = renew_interval if renew_interval is not None else timeout / 3
        self._lock = client.lock(name, timeout=timeout)
        self._client = client
        self._renew_task: asyncio.Task[None] | None = None
        self.fencing_token: int | None = None

    async def __aenter__(self) -> int:
        await self._lock.acquire()
        # A separate, TTL-less counter key — deliberately never expires,
        # so the sequence keeps climbing across every future acquire of
        # this same name forever. If this counter itself expired and reset
        # to 0, a new holder could hand out a LOWER token than one a
        # previous (possibly still-zombie) holder already has, defeating
        # the entire point of a fencing token.
        self.fencing_token = int(await self._client.incr(f"{self._name}:fencing"))
        self._renew_task = asyncio.create_task(self._renew_loop())
        return self.fencing_token

    async def _renew_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._renew_interval)
                try:
                    await self._lock.extend(self._timeout, replace_ttl=True)
                except LockError:
                    # We no longer hold it — someone else's TTL is running
                    # now. Nothing to do here: the fencing token already
                    # handed to the caller is exactly what protects
                    # whatever they're doing from this moment on, not this
                    # loop. Stop trying rather than spin forever.
                    _logger.warning(
                        "redix lock '%s': lost during renewal — the critical "
                        "section is no longer protected; relying on its "
                        "fencing token to reject any now-stale writes.",
                        self._name,
                    )
                    return
        except asyncio.CancelledError:
            pass

    async def __aexit__(self, *exc_info: object) -> None:
        if self._renew_task is not None:
            self._renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._renew_task
        with contextlib.suppress(LockError):
            await self._lock.release()


def register(kernel: Any) -> None:
    kernel.settings.declare(URL_KEY, secret=True)
    kernel.settings.declare(
        SOCKET_TIMEOUT_MS_KEY,
        type=int,
        default=5_000,
        doc="Socket read/write and connect timeout for every Redis operation, in "
        "milliseconds — bounds how long a call can block if Redis becomes "
        "unresponsive (not just cleanly unreachable) before raising instead of "
        "hanging. 0 disables it (redis-py's own default: wait indefinitely).",
    )

    url = kernel.settings.get(URL_KEY, reveal=True)
    if url is None:
        raise RuntimeError(
            f"'{URL_KEY}' is not set. Run: arc settings set {URL_KEY} redis://host:6379/0 --secret"
        )

    socket_timeout_ms = kernel.settings.get(SOCKET_TIMEOUT_MS_KEY)
    provider = RedixProvider(url, socket_timeout_ms=socket_timeout_ms)
    kernel.export(CAPABILITY, provider, requires=[], optional_requires=[])

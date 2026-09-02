"""
redix — ARC provider plugin: cache, distributed locks, pub/sub, and
rate-limit counters ("one dependency, four jobs" per the Architecture
tech-stack table).

Backend: any server speaking the Redis wire protocol (RESP2/RESP3) — a real
Redis, or Valkey (the BSD-licensed, wire-compatible fork). redix talks to
it entirely through `redis-py` (the `redis` package on PyPI), which is
just a RESP client and doesn't care which of the two is on the other end
of the socket; nothing in this file, or in `redix_url`'s value, picks one
over the other — point it at whichever server you're actually running.
Every mention of "Redis" below describing a general backend behavior
(timeouts, failover, instance topology) applies identically to Valkey; the
Lua/EVAL script further down calls the `redis.call(...)` Lua API
specifically because that's the literal function name both Redis' and
Valkey's own Lua interpreters expose — not a reference to which server
you're running.

Exports `arc.redix`. Nothing hard-requires it — not every ARC project wants
caching/locks/pubsub/rate-limiting, so it stays a plain standalone plugin
like any other, including for `authn` (Phase 4, docs/arc.MD §3.13): rate
limiting genuinely upgrades when redix is installed, but account lockout
is Postgres-backed and works identically without it — redix stays an
`optional_requires` there too, never a hard dependency. redix's own
manifest never changes based on what any other plugin decides.

Same lifecycle note as pgdb: register() only constructs the provider;
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
        # mode audit: redis.from_url() with no timeout means a server
        # (Redis or Valkey — the timeout is a socket-level setting, it
        # applies identically to either) that goes silently unresponsive
        # (not a clean connection-refused — a network partition, an
        # overloaded server that stops replying, a black-holed connection)
        # hangs a read/write for as long as the OS's own TCP-level timeout,
        # which on Linux is commonly measured in MINUTES — a request-
        # handling path that expects a cache lookup to cost single-digit
        # milliseconds, and that already catches Exception to degrade
        # gracefully to the DB (see authn's own _cache_get_session/
        # _cache_set_session), was never actually protected against THIS
        # failure mode: `except Exception` can't catch a call that hasn't
        # raised yet because it's still blocked.
        self.socket_timeout_ms = socket_timeout_ms
        self._client: redis.Redis | None = None
        # Registered once in open(), below — see rate_limit()'s own
        # docstring for why (EVALSHA instead of sending the full Lua
        # source on every call, on the hottest path in the system).
        self._rate_limit_script: Any | None = None

    async def open(self) -> None:
        if self._client is None:
            timeout = self.socket_timeout_ms / 1000 if self.socket_timeout_ms > 0 else None
            self._client = redis.from_url(
                self.url, socket_timeout=timeout, socket_connect_timeout=timeout
            )
            self._rate_limit_script = self._client.register_script(self._RATE_LIMIT_LUA)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            self._rate_limit_script = None

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
    def lock(
        self,
        name: str,
        timeout: float = 10.0,
        *,
        renew_interval: float | None = None,
        blocking_timeout: float | None = None,
    ):
        """`async with arc.redix.lock("job:123") as fencing_token:` — a
        distributed lock that renews itself in the background for as long
        as it's held (so a critical section that happens to run longer
        than `timeout` doesn't silently lose the lock out from under it —
        the single biggest footgun with a plain TTL-based lock), and hands
        back a fencing token: an integer that only ever goes up, once per
        successful acquire of THIS name, forever (Kleppmann's "How to do
        distributed locking" — the fencing-tokens pattern). Pass it to
        whatever you're protecting (a write to pgdb, a call to some other
        service) so that side can reject any write carrying a token lower
        than the highest one it has already seen — the one thing renewal
        can reduce the odds of but can never fully rule out (a long GC/
        scheduler pause between acquiring and your first write can still
        let the TTL lapse before the first renewal even runs).

        `blocking_timeout` bounds how long `__aenter__` will WAIT to
        acquire a lock someone else already holds — defaults to `timeout`
        itself (one lease-length is a reasonable bound: a healthy holder
        renews well within that, and a dead one's own lease will already
        have lapsed by then). redis-py's own default is
        `blocking_timeout=None` — wait forever — which turns one slow
        holder (or a Redis hiccup) into an unbounded pile-up of every
        OTHER caller waiting on that same name; a login-session lock keyed
        per-user (authn's own `login:sessions:{user_id}`) or a save()
        match_on lock keyed by caller-supplied DATA VALUES both make the
        set of lock names something a caller can influence, so "wait
        forever" was never a bound anyone chose on purpose. Pass
        `blocking_timeout=0` for redis-py's own non-blocking "fail
        immediately if held" behavior instead.

        Single Redis/Valkey instance/primary only — this is a SET NX PX
        lock plus a best-effort background heartbeat, not Redlock's
        multi-node quorum (Redlock the algorithm, coined against Redis
        originally, applies identically to a Valkey deployment — it's
        about running multiple independent instances of ANY server
        speaking this protocol, not a Redis-specific mechanism). Good
        enough for "only one of my N workers does this job right now";
        not a substitute for a real consensus system if you need
        correctness under a primary failover.
        """
        return _RenewingLock(
            self._client_or_raise(),
            name,
            timeout=timeout,
            renew_interval=renew_interval,
            blocking_timeout=blocking_timeout if blocking_timeout is not None else timeout,
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
        """Returns True if this call is within the limit for the window.

        FIXED window, not sliding: the counter resets wholesale when the
        window expires rather than ageing out individual hits. The
        consequence worth knowing is that a caller can spend its full
        budget at the very end of one window and again at the start of the
        next, so the true short-term ceiling is up to 2x `limit` across a
        window boundary. That's the standard trade-off for a
        one-round-trip counter (a sliding window needs a sorted set and
        more round trips), and it's fine for the volume-bounding job this
        does — but callers sizing a limit against a hard guarantee should
        size for 2x, and anything needing a real guarantee (account
        lockout) shouldn't be built on this at all.

        This is the hottest path in the whole system — every request that
        reaches rate_limit_middleware, plus authn's own login/forgot-
        password rate limits, calls this. `eval()` used to send the FULL
        Lua source over the wire on every single call; register_script()
        (open(), below) uploads it to the server exactly ONCE and hands
        back an AsyncScript that calls EVALSHA (just the script's hash)
        from here on — redis-py's own AsyncScript.__call__ already
        catches NoScriptError and transparently reloads-then-retries via
        EVALSHA if the server's script cache was ever flushed (a `SCRIPT
        FLUSH`, or a failover to a replica that never got it), so this
        gets the EVAL fallback for free without reimplementing it."""
        client = self._client_or_raise()
        counter_key = f"ratelimit:{key}"
        current = await self._rate_limit_script(keys=[counter_key], args=[window_seconds], client=client)
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

    #: How long a lock name's own fencing counter key is kept alive PAST
    #: its last increment — refreshed on every acquire, so any name still
    #: actually in use never loses its monotonic sequence. Long enough
    #: that no realistic gap between two legitimate acquires of the same
    #: name ever crosses it, short enough that a lock name that will never
    #: be used again (e.g. save()'s own match_on locks, keyed by
    #: caller-supplied DATA VALUES — one distinct key per distinct value
    #: ever saved, forever, under the old permanently-TTL-less shape)
    #: eventually gets reclaimed by Redis instead of leaking permanently.
    _FENCING_COUNTER_TTL_SECONDS = 30 * 24 * 3600  # 30 days

    def __init__(
        self,
        client: redis.Redis,
        name: str,
        *,
        timeout: float,
        renew_interval: float | None,
        blocking_timeout: float | None = None,
    ) -> None:
        self._name = name
        self._timeout = timeout
        self._blocking_timeout = blocking_timeout
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
        # Bounded, never indefinite by default — redis-py's own
        # acquire() defaults to blocking_timeout=None (wait forever),
        # which turns one slow/dead holder into an unbounded pile-up of
        # every other caller waiting on this exact name. See
        # RedixProvider.lock()'s own docstring for why "forever" was
        # never a bound anyone actually chose.
        acquired = await self._lock.acquire(blocking_timeout=self._blocking_timeout)
        if not acquired:
            raise LockError(
                f"could not acquire redix lock {self._name!r} within "
                f"{self._blocking_timeout}s"
            )
        # A counter key with a long, refreshed-on-every-acquire TTL (not
        # permanently TTL-less) — the sequence keeps climbing across every
        # ACTIVELY-used acquire of this same name, which is the only thing
        # the fencing guarantee actually needs: if this counter reset to 0
        # WHILE a previous (possibly still-zombie) holder's token was still
        # meaningful, a new holder could hand out a LOWER token than one
        # already in circulation, defeating the entire point. A name that
        # hasn't been acquired in _FENCING_COUNTER_TTL_SECONDS has no such
        # token anywhere left to defeat.
        pipe = self._client.pipeline()
        pipe.incr(f"{self._name}:fencing")
        pipe.expire(f"{self._name}:fencing", self._FENCING_COUNTER_TTL_SECONDS)
        fencing_value, _ = await pipe.execute()
        self.fencing_token = int(fencing_value)
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
    kernel.settings.declare(
        URL_KEY,
        secret=True,
        doc="Connection URL for redix's backing store — any server speaking the "
        "Redis wire protocol works: a real Redis, or Valkey (the wire-compatible, "
        "BSD-licensed fork). Still a redis://host:port/db URL either way — "
        "redis-py (the client library) doesn't distinguish the two.",
    )
    kernel.settings.declare(
        SOCKET_TIMEOUT_MS_KEY,
        type=int,
        default=5_000,
        doc="Socket read/write and connect timeout for every Redis/Valkey operation, "
        "in milliseconds — bounds how long a call can block if the server becomes "
        "unresponsive (not just cleanly unreachable) before raising instead of "
        "hanging. 0 disables it (redis-py's own default: wait indefinitely).",
    )

    url = kernel.settings.get(URL_KEY, reveal=True)
    if url is None:
        raise RuntimeError(
            f"'{URL_KEY}' is not set. Run: arc settings set {URL_KEY} redis://host:6379/0 "
            f"--secret (works for a Redis or a Valkey server — same redis:// URL either way)"
        )

    socket_timeout_ms = kernel.settings.get(SOCKET_TIMEOUT_MS_KEY)
    provider = RedixProvider(url, socket_timeout_ms=socket_timeout_ms)
    kernel.export(CAPABILITY, provider, requires=[], optional_requires=[])

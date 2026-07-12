"""
redix.cli — `arc redix ...` commands.

Mirrors psqldb.cli exactly: mounted via the `arc.plugins.cli` entry point,
independent of arc.boot(), reads the URL straight off disk via
SettingsManager.
"""

from __future__ import annotations

import os
import shutil
import time
from urllib.parse import urlparse

import redis as redis_sync  # sync client — simplest for a one-shot CLI ping
import typer
from rich.console import Console

from arc.runtime import find_project_root
from arc.settings import SettingsManager

from . import URL_KEY

app = typer.Typer(help="Commands for the redix provider.")
console = Console()
err_console = Console(stderr=True, style="bold red")


def _url() -> str:
    root = find_project_root()
    if root is None:
        err_console.print(
            "Not inside an ARC project (no .arc/arc.toml found here or in any parent)."
        )
        raise typer.Exit(code=1)

    mgr = SettingsManager(root / ".arc")
    url = mgr.get(URL_KEY, reveal=True)
    if url is None:
        err_console.print(
            f"'{URL_KEY}' is not set. Run: "
            f"arc settings set {URL_KEY} redis://host:6379/0 --secret"
        )
        raise typer.Exit(code=1)
    return url


@app.command()
def status() -> None:
    """Check connectivity to the configured Redis/Valkey instance."""
    url = _url()
    parsed = urlparse(url)
    client = redis_sync.from_url(url, socket_connect_timeout=5, socket_timeout=5)
    try:
        start = time.monotonic()
        client.ping()
        elapsed = time.monotonic() - start
        info = client.info("server")
    except Exception as exc:
        err_console.print(
            f"redix: FAILED to connect to "
            f"{parsed.hostname}:{parsed.port or 6379} — {exc}"
        )
        raise typer.Exit(code=1)
    finally:
        client.close()

    console.print(f"[bold green]redix: OK[/bold green] ({elapsed * 1000:.0f}ms)")
    console.print(f"  host:   {parsed.hostname}:{parsed.port or 6379}")
    console.print(f"  server: redis {info.get('redis_version', '?')}")


@app.command()
def connect() -> None:
    """Drop into an interactive redis-cli shell against the configured instance."""
    url = _url()
    if shutil.which("redis-cli") is None:
        err_console.print(
            "`redis-cli` was not found on PATH. Install the Redis client "
            "(e.g. `apt-get install redis-tools`) and try again."
        )
        raise typer.Exit(code=1)

    parsed = urlparse(url)
    db_number = parsed.path.lstrip("/") or "0"
    argv = [
        "redis-cli",
        "-h", parsed.hostname or "localhost",
        "-p", str(parsed.port or 6379),
        "-n", db_number,
    ]

    env = os.environ.copy()
    if parsed.password:
        # REDISCLI_AUTH, not `-u redis://:pw@host` — the URL form would put
        # the password in argv, visible to every other user via `ps`.
        env["REDISCLI_AUTH"] = parsed.password

    console.print(f"[dim]$ {' '.join(argv)}[/dim]")
    os.execvpe("redis-cli", argv, env)
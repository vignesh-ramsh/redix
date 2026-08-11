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
from urllib.parse import quote_plus, urlparse

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
            f"'{URL_KEY}' is not set. Run: arc settings set {URL_KEY} redis://host:6379/0 --secret"
        )
        raise typer.Exit(code=1)
    return url


@app.command()
def setup() -> None:
    """Interactively configure the Redis/Valkey connection (host/port/db
    index/user/password), validating connectivity before saving — the
    guided alternative to hand-composing a URL for `arc settings set
    redix_url ... --secret`. Safe to re-run: asks before overwriting an
    existing redix_url, showing what it would replace. User and password
    are both optional — most local/dev Redis instances have neither."""
    root = find_project_root()
    if root is None:
        err_console.print(
            "Not inside an ARC project (no .arc/arc.toml found here or in any parent)."
        )
        raise typer.Exit(code=1)

    mgr = SettingsManager(root / ".arc")
    existing = mgr.get(URL_KEY, reveal=True)
    if existing is not None:
        parsed_existing = urlparse(existing)
        console.print(
            f"[yellow]redix_url is already set[/yellow] "
            f"({parsed_existing.hostname}:{parsed_existing.port or 6379})."
        )
        if not typer.confirm("Reconfigure it?", default=False):
            console.print("[dim]Aborted — nothing changed.[/dim]")
            raise typer.Exit(code=1)

    host = typer.prompt("Host", default="localhost")
    port = typer.prompt("Port", default=6379, type=int)
    db_index = typer.prompt("DB index", default=0, type=int)
    user = typer.prompt("User (blank for none)", default="", show_default=False)
    password = typer.prompt("Password (blank for none)", default="", show_default=False, hide_input=True)

    # quote_plus so a special character in user/password (@, :, /, ...)
    # can't be misparsed as URL structure — same reasoning psqldb.setup
    # applies to its DSN.
    auth = ""
    if user or password:
        auth = quote_plus(user)
        if password:
            auth += f":{quote_plus(password)}"
        auth += "@"
    url = f"redis://{auth}{host}:{port}/{db_index}"

    console.print(f"Connecting to {host}:{port}/{db_index}...")
    client = redis_sync.from_url(url, socket_connect_timeout=5, socket_timeout=5)
    try:
        client.ping()
        info = client.info("server")
    except Exception as exc:
        err_console.print(f"FAILED to connect: {exc}")
        console.print("[dim]Nothing saved — run `arc redix setup` again to retry.[/dim]")
        raise typer.Exit(code=1)
    finally:
        client.close()

    mgr.set(URL_KEY, url, secret=True)
    console.print(f"[bold green]Connected[/bold green] — redis {info.get('redis_version', '?')}")
    console.print(f"[dim]Saved to {URL_KEY} (encrypted, .arc/arc.secrets).[/dim]")


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
            f"redix: FAILED to connect to {parsed.hostname}:{parsed.port or 6379} — {exc}"
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
        "-h",
        parsed.hostname or "localhost",
        "-p",
        str(parsed.port or 6379),
        "-n",
        db_number,
    ]

    env = os.environ.copy()
    if parsed.password:
        # REDISCLI_AUTH, not `-u redis://:pw@host` — the URL form would put
        # the password in argv, visible to every other user via `ps`.
        env["REDISCLI_AUTH"] = parsed.password

    console.print(f"[dim]$ {' '.join(argv)}[/dim]")
    os.execvpe("redis-cli", argv, env)

"""Run the broker. Configuration comes from the environment; the add-on's run.sh maps its options there."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
from collections.abc import Sequence
from pathlib import Path

import uvicorn
from starlette.types import ASGIApp, Receive, Scope, Send

from .api import Readiness, Settings, create_app
from .connector import public_path
from .store import Store

SERVICE_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,39}\Z")


def services_from(raw: str) -> dict[str, str]:
    """Named service tokens from a JSON list of {"name", "token"}; an entry without both is skipped."""
    if not raw.strip():
        return {}
    try:
        entries = json.loads(raw)
    except ValueError as exc:
        raise SystemExit(f"DUPLEX_SERVICES is not JSON: {exc}") from exc
    services: dict[str, str] = {}
    for entry in entries if isinstance(entries, list) else []:
        name, token = str(entry.get("name", "")), str(entry.get("token", ""))
        if not name and not token:
            continue
        if not SERVICE_NAME.match(name):
            raise SystemExit(f"service name {name!r} must be lowercase letters, digits and hyphens")
        if token:
            services[name] = token
    return services


def settings_from_env() -> Settings:
    auth = os.environ.get("DUPLEX_USER_AUTH", "pairing")
    if auth not in ("ingress", "pairing"):
        raise SystemExit(f"DUPLEX_USER_AUTH must be ingress or pairing, got {auth!r}")
    proxies = os.environ.get("DUPLEX_TRUSTED_PROXIES", "172.30.32.2")
    return Settings(
        owner=os.environ.get("DUPLEX_OWNER", "owner"),
        owner_name=os.environ.get("DUPLEX_OWNER_NAME") or "Owner",
        connector_title=os.environ.get("DUPLEX_CONNECTOR_TITLE") or "ChatGPT",
        services=services_from(os.environ.get("DUPLEX_SERVICES", "")),
        model_url=os.environ.get("DUPLEX_MODEL_URL", ""),
        model_name=os.environ.get("DUPLEX_MODEL_NAME", ""),
        model_tools_name=os.environ.get("DUPLEX_MODEL_TOOLS_NAME", ""),
        model_member_title=os.environ.get("DUPLEX_MODEL_MEMBER_TITLE") or "Assistant",
        model_timeout=float(os.environ.get("DUPLEX_MODEL_TIMEOUT") or 90),
        model_actions=os.environ.get("DUPLEX_MODEL_ACTIONS", "false") == "true",
        model_no_thinking=os.environ.get("DUPLEX_MODEL_NO_THINKING", "false") == "true",
        model_assist_token=os.environ.get("SUPERVISOR_TOKEN", ""),
        config_dir=Path(os.environ["DUPLEX_CONFIG_DIR"]) if os.environ.get("DUPLEX_CONFIG_DIR") else None,
        user_auth=auth,  # type: ignore[arg-type]
        pairing_code=os.environ.get("DUPLEX_PAIRING_CODE", ""),
        enrollment_token=os.environ.get("DUPLEX_ENROLLMENT_TOKEN", ""),
        waker_token=os.environ.get("DUPLEX_WAKER_TOKEN", ""),
        trusted_proxies=frozenset(p.strip() for p in proxies.split(",") if p.strip()),
        device_token_grace=float(os.environ.get("DUPLEX_DEVICE_TOKEN_GRACE") or 300),
        wake_timeout=float(os.environ.get("DUPLEX_WAKE_TIMEOUT") or 300),
        rewake_interval=float(os.environ.get("DUPLEX_REWAKE_INTERVAL") or 300),
        rewake_limit=int(os.environ.get("DUPLEX_REWAKE_LIMIT") or 3),
        hold_after=int(os.environ.get("DUPLEX_HOLD_AFTER") or 2),
        connector_url=os.environ.get("DUPLEX_CONNECTOR_URL", ""),
    )


class Listener(uvicorn.Server):
    """A uvicorn server that counts itself into the broker's readiness once its socket is listening."""

    def __init__(self, config: uvicorn.Config, readiness: Readiness) -> None:
        super().__init__(config)
        self._readiness = readiness
        readiness.listeners += 1

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets)
        if self.started:
            self._readiness.bound += 1


async def serve(servers: Sequence[uvicorn.Server]) -> None:
    await asyncio.gather(*(server.serve() for server in servers))


class PublicOnly:
    """The app as the tunnel sees it: the ChatGPT connector's paths only; every other request, WebSockets
    included, is refused before the app sees it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and public_path(scope["path"]):
            await self.app(scope, receive, send)
        elif scope["type"] == "http":
            body = b'{"error":"not_found"}'
            headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
            await send({"type": "http.response.start", "status": 404, "headers": headers})
            await send({"type": "http.response.body", "body": body})
        elif scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 4404})


LOG_LEVELS = ("debug", "info", "warning", "error")


def log_level() -> str:
    """The configured level, or info when the option is empty or unknown."""
    level = os.environ.get("DUPLEX_LOG_LEVEL", "").strip().lower()
    return level if level in LOG_LEVELS else "info"


def main() -> None:
    logging.basicConfig(
        level=log_level().upper(),
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    data = Path(os.environ.get("DUPLEX_DATA_DIR", "runtime"))
    data.mkdir(parents=True, exist_ok=True)
    settings = settings_from_env()
    store = Store(data / "broker.db", owner=settings.owner)
    app = create_app(store, settings)
    logging.getLogger("duplex_broker").info(
        "starting: user_auth=%s pairing=%s enrollment=%s data=%s model=%s actions=%s assist_token=%s"
        " ladder=%ss+%dx%ss hold_after=%d",
        settings.user_auth,
        "set" if settings.pairing_code else "unset",
        "set" if settings.enrollment_token else "unset",
        data,
        "on" if settings.model_url else "off",
        "on" if settings.model_actions else "off",
        "set" if settings.model_assist_token else "unset",
        round(settings.wake_timeout),
        settings.rewake_limit,
        round(settings.rewake_interval),
        settings.hold_after,
    )
    level = log_level()
    host = os.environ.get("DUPLEX_HOST", "127.0.0.1")
    readiness: Readiness = app.state.readiness
    servers = [
        Listener(
            uvicorn.Config(app, host=host, port=int(os.environ.get("DUPLEX_PORT", "8790")), log_level=level),
            readiness,
        )
    ]
    if settings.connector_url:
        # The tunnel's origin: the same app (one store, one hub), its lifespan run once by the first server.
        port = int(os.environ.get("DUPLEX_CONNECTOR_PORT", "8791"))
        config = uvicorn.Config(PublicOnly(app), host=host, port=port, log_level=level, lifespan="off")
        servers.append(Listener(config, readiness))
        logging.getLogger("duplex_broker").info(
            "ChatGPT connector at %s on port %d", settings.connector_url, port
        )

    asyncio.run(serve(servers))


if __name__ == "__main__":
    main()

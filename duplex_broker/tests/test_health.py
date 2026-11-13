from __future__ import annotations

import asyncio
import json
import socket
import urllib.request
from collections.abc import Iterator

import pytest
import uvicorn
from fastapi.testclient import TestClient

from duplex_broker import __version__
from duplex_broker.__main__ import Listener, serve
from duplex_broker.api import Readiness, Settings, create_app
from duplex_broker.store import SCHEMA_VERSION, Store


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


def test_health_reports_the_version_and_schema_and_is_not_ready_until_the_listeners_are_bound(
    store: Store,
) -> None:
    app = create_app(store, Settings())
    with TestClient(app) as client:
        answer = client.get("/health")
        assert answer.status_code == 200
        assert answer.json() == {"ok": True, "version": __version__, "schema": SCHEMA_VERSION, "ready": False}
        readiness: Readiness = app.state.readiness
        readiness.listeners = 2
        readiness.bound = 1
        assert client.get("/health").json()["ready"] is False
        readiness.bound = 2
        assert client.get("/health").json()["ready"] is True


def test_the_schema_reported_is_the_databases_own(store: Store) -> None:
    assert store.schema == SCHEMA_VERSION == 14


class Noted(Listener):
    """A listener that sets an event when its socket is listening, and can be held back before that."""

    def __init__(
        self, config: uvicorn.Config, readiness: Readiness, hold: asyncio.Event | None = None
    ) -> None:
        super().__init__(config, readiness)
        self.listening = asyncio.Event()
        self._hold = hold

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        if self._hold is not None:
            await self._hold.wait()
        await super().startup(sockets)
        self.listening.set()

    def port(self) -> int:
        return int(self.servers[0].sockets[0].getsockname()[1])


def read_health(port: int) -> dict[str, object]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=10) as reply:
        body: dict[str, object] = json.loads(reply.read())
    return body


def test_a_broker_serving_two_ports_is_ready_only_once_both_are_bound(store: Store) -> None:
    app = create_app(store, Settings())
    readiness: Readiness = app.state.readiness

    async def scenario() -> tuple[list[bool], list[bool]]:
        hold = asyncio.Event()
        first = Noted(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"), readiness)
        second = Noted(
            uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="off"),
            readiness,
            hold,
        )
        running = asyncio.create_task(serve([first, second]))
        await first.listening.wait()
        before = [readiness.ready, bool((await asyncio.to_thread(read_health, first.port()))["ready"])]
        hold.set()
        await second.listening.wait()
        after = [readiness.ready, bool((await asyncio.to_thread(read_health, first.port()))["ready"])]
        first.should_exit = second.should_exit = True
        await running
        return before, after

    before, after = asyncio.run(scenario())
    assert before == [False, False]
    assert after == [True, True]

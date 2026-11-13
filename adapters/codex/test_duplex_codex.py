"""The Codex adapter against a scripted App Server and the real broker, in-process over HTTP."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import io
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from email.message import Message
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import uvicorn
from duplex_broker.api import Settings, create_app
from duplex_broker.store import Store

sys.path.insert(0, str(Path(__file__).parent))
from duplex_codex import (
    BUNDLED_CODEX,
    CAPABILITIES,
    AppServer,
    Broker,
    CodexAdapter,
    ManagedThread,
    RpcError,
    child_env,
    find_codex,
    load,
)

HERE = Path(__file__).parent
CODE, ENROLL = "pair", "enroll"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@pytest.fixture
def broker_url() -> Iterator[str]:
    store = Store(":memory:")
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL)),
            host="127.0.0.1",
            port=port,
            log_level="error",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)
    store.close()


def http(url: str, method: str, path: str, body: Any = None, token: str = "") -> Any:
    request = urllib.request.Request(
        url + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "content-type": "application/json",
            **({"authorization": f"Bearer {token}"} if token else {}),
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def owner(url: str) -> str:
    token: str = http(url, "POST", "/api/device/enroll", {"pairing_code": CODE, "device_name": "test"})[
        "token"
    ]
    return token


def app_server(tmp_path: Path, known: str = "") -> AppServer:
    argv = [sys.executable, str(HERE / "fake_app_server.py"), str(tmp_path / "calls.jsonl"), known]
    return AppServer(argv, tmp_path)


async def handled(adapter: CodexAdapter, thread: ManagedThread) -> None:
    """Handle everything the thread's input holds, as a claim does before it reads the turn."""
    async with thread.handling:
        await adapter._drain(thread)


async def drained(adapter: CodexAdapter, thread: ManagedThread) -> None:
    """Wait until the thread's turn has ended and every reply is posted."""
    await handled(adapter, thread)
    await asyncio.wait_for(thread.idle.wait(), 10)
    await adapter.settle()


def calls(tmp_path: Path) -> list[dict[str, Any]]:
    path = tmp_path / "calls.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def test_a_dm_becomes_one_turn_and_the_answer_comes_back(broker_url: str, tmp_path: Path) -> None:
    token = owner(broker_url)

    async def scenario() -> tuple[list[dict[str, Any]], ManagedThread]:
        server = app_server(tmp_path)
        await server.start()
        thread = ManagedThread("Codex test", str(tmp_path))
        adapter = CodexAdapter(Broker(broker_url, ENROLL), server, [thread], tmp_path / "state.json")
        await adapter.open_threads()
        await adapter.register(thread)
        dm = http(
            broker_url,
            "POST",
            "/api/direct-conversations",
            {"binding_id": thread.binding_id},
            token,
        )
        http(
            broker_url,
            "POST",
            f"/api/conversations/{dm['id']}/messages",
            {"client_message_id": uuid.uuid4().hex, "text": "hello codex"},
            token,
        )
        assert await adapter.handle_one(thread) is True
        await drained(adapter, thread)
        assert await adapter.handle_one(thread) is False
        events = http(broker_url, "GET", f"/api/conversations/{dm['id']}/events", token=token)
        await server.close()
        return events, thread

    events, thread = asyncio.run(scenario())
    texts = [e["payload"].get("text") for e in events if e["type"] == "message.accepted"]
    states = [e["payload"]["state"] for e in events if e["type"] == "delivery.changed"]
    assert texts == ["hello codex", "echo: hello codex"]
    assert states == ["queued", "dispatching", "accepted", "responded"]
    reply = [e for e in events if e["type"] == "message.accepted"][1]
    assert reply["actor"] == {"kind": "binding", "id": thread.binding_id}
    turn_starts = [c for c in calls(tmp_path) if c["method"] == "turn/start"]
    assert len(turn_starts) == 1
    assert turn_starts[0]["params"]["threadId"] == thread.thread_id
    accepted = next(
        e for e in events if e["type"] == "delivery.changed" and e["payload"]["state"] == "accepted"
    )
    assert accepted["payload"]["native_turn_id"].startswith("u-")


def test_threads_are_started_once_and_resumed_after_a_restart(tmp_path: Path) -> None:
    state = tmp_path / "state.json"

    async def first() -> str:
        server = app_server(tmp_path)
        await server.start()
        thread = ManagedThread("Codex test", str(tmp_path))
        await CodexAdapter(Broker("http://unused", ENROLL), server, [thread], state).open_threads()
        await server.close()
        return thread.thread_id

    thread_id = asyncio.run(first())
    assert json.loads(state.read_text()) == {"Codex test": thread_id}

    async def second() -> str:
        server = app_server(tmp_path, known=thread_id)
        await server.start()
        thread = ManagedThread("Codex test", str(tmp_path))
        await CodexAdapter(Broker("http://unused", ENROLL), server, [thread], state).open_threads()
        await server.close()
        return thread.thread_id

    assert asyncio.run(second()) == thread_id
    methods = [c["method"] for c in calls(tmp_path)]
    assert methods.count("thread/start") == 1
    assert methods.count("thread/resume") == 1


def test_a_peer_message_is_refused_without_a_turn(broker_url: str, tmp_path: Path) -> None:
    token = owner(broker_url)

    async def scenario() -> list[dict[str, Any]]:
        server = app_server(tmp_path)
        await server.start()
        codex = ManagedThread("Codex test", str(tmp_path))
        adapter = CodexAdapter(Broker(broker_url, ENROLL), server, [codex], tmp_path / "state.json")
        await adapter.open_threads()
        await adapter.register(codex)
        peer = http(
            broker_url,
            "POST",
            "/api/adapter/register",
            {
                "provider": "mock",
                "native_id": "peer",
                "title": "peer",
                "workspace": "",
                "endpoint_ref": "",
                "capabilities": {},
            },
            ENROLL,
        )
        group = http(
            broker_url,
            "POST",
            "/api/conversations",
            {"title": "g", "mode": "consult"},
            token,
        )
        for binding in (codex.binding_id, peer["binding"]["id"]):
            http(
                broker_url,
                "POST",
                f"/api/conversations/{group['id']}/members",
                {"binding_id": binding},
                token,
            )
        root = http(
            broker_url,
            "POST",
            f"/api/conversations/{group['id']}/messages",
            {"client_message_id": "o1", "text": "ask codex", "recipients": [peer["binding"]["id"]]},
            token,
        )
        http(
            broker_url,
            "POST",
            "/api/adapter/messages",
            {
                "conversation_id": group["id"],
                "client_message_id": "p1",
                "text": "do this",
                "recipients": [codex.binding_id],
                "parent_id": root["message"]["id"],
            },
            peer["token"],
        )
        assert await adapter.handle_one(codex) is True
        events: list[dict[str, Any]] = http(
            broker_url, "GET", f"/api/conversations/{group['id']}/events", token=token
        )
        await server.close()
        return events

    events = asyncio.run(scenario())
    to_codex = [
        e["payload"]["state"]
        for e in events
        if e["type"] == "delivery.changed" and e["payload"]["binding_id"] != peer_id(events)
    ]
    assert to_codex == ["queued", "dispatching", "failed"]
    assert not [c for c in calls(tmp_path) if c["method"] == "turn/start"]


def peer_id(events: list[dict[str, Any]]) -> str:
    """The binding the owner's opening message went to."""
    first = next(e for e in events if e["type"] == "message.accepted")
    recipients: list[str] = first["payload"]["recipients"]
    return recipients[0]


def test_a_failed_turn_marks_the_delivery_failed(broker_url: str, tmp_path: Path) -> None:
    token = owner(broker_url)

    async def scenario() -> list[dict[str, Any]]:
        server = app_server(tmp_path)
        await server.start()
        thread = ManagedThread("Codex test", str(tmp_path))
        adapter = CodexAdapter(Broker(broker_url, ENROLL), server, [thread], tmp_path / "state.json")
        await adapter.open_threads()
        await adapter.register(thread)
        dm = http(
            broker_url,
            "POST",
            "/api/direct-conversations",
            {"binding_id": thread.binding_id},
            token,
        )
        http(
            broker_url,
            "POST",
            f"/api/conversations/{dm['id']}/messages",
            {"client_message_id": "x", "text": "please FAIL"},
            token,
        )
        await adapter.handle_one(thread)
        await drained(adapter, thread)
        events: list[dict[str, Any]] = http(
            broker_url, "GET", f"/api/conversations/{dm['id']}/events", token=token
        )
        await server.close()
        return events

    events = asyncio.run(scenario())
    states = [e["payload"]["state"] for e in events if e["type"] == "delivery.changed"]
    assert states == ["queued", "dispatching", "accepted", "failed"]


def test_a_restart_keeps_the_binding_and_a_stranger_is_refused(broker_url: str, tmp_path: Path) -> None:
    import urllib.error

    def adapter(folder: str) -> tuple[CodexAdapter, ManagedThread]:
        thread = ManagedThread("Codex test", str(tmp_path), thread_id="thread-1")
        state = tmp_path / folder / "codex-threads.json"
        return CodexAdapter(Broker(broker_url, ENROLL), AppServer([], tmp_path), [thread], state), thread

    async def scenario() -> tuple[str, str, int]:
        first, one = adapter("a")
        await first.register(one)
        restarted, two = adapter("a")
        await restarted.register(two)
        stranger, three = adapter("b")
        try:
            await stranger.register(three)
        except urllib.error.HTTPError as err:
            return one.binding_id, two.binding_id, err.code
        finally:
            for each in (first, restarted, stranger):
                await each.close()
        return one.binding_id, two.binding_id, 0

    first_id, again_id, refused = asyncio.run(scenario())
    assert first_id == again_id
    assert refused == 409
    kept = json.loads((tmp_path / "a" / "codex-tokens.json").read_text(encoding="utf-8"))
    assert list(kept) == ["thread-1"]


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


class ScriptedBroker(Broker):
    """Hands out the claims it is given and records everything else the adapter sends."""

    def __init__(self) -> None:
        super().__init__("http://unused", ENROLL)
        self.fail: dict[str, list[Exception]] = {}  # path fragment -> errors to raise, one per call
        self.hold: dict[str, asyncio.Event] = {}  # path fragment -> a call waits until its event is set
        self.held: dict[str, asyncio.Event] = {}  # path fragment -> set once a call waits at that hold
        self.readings: list[int] = []  # the seq of each turn reading, in the order sent
        self.claims: list[dict[str, Any]] = []
        self.sent: list[tuple[str, Any]] = []
        self.claim_bodies: list[Any] = []

    async def call(self, method: str, path: str, body: Any = None, bearer: str = "") -> Any:
        for fragment, gate in list(self.hold.items()):
            if fragment in path:
                del self.hold[fragment]  # the first matching call waits; later ones pass
                self.parked(fragment).set()
                await gate.wait()
        for fragment, errors in self.fail.items():
            if fragment in path and errors:
                raise errors.pop(0)
        if path == "/api/adapter/claim":
            self.claim_bodies.append(body)
            self.readings.append(body["turn"]["seq"])
            return self.claims.pop(0) if self.claims else None
        self.sent.append((path, body))
        if path == "/api/adapter/turn":
            self.readings.append(body["seq"])
        if path == "/api/adapter/register":
            return {"binding": {"id": "b1"}, "token": "tok"}
        return None

    def parked(self, fragment: str) -> asyncio.Event:
        """Set once a call waits at the hold on `fragment`."""
        return self.held.setdefault(fragment, asyncio.Event())

    def claim(
        self, name: str, text: str, mode: str, actor: dict[str, str] | None = None, conversation: str = "c1"
    ) -> None:
        self.claims.append(
            {
                "delivery": {"id": f"d-{name}"},
                "message": {
                    "id": f"m-{name}",
                    "text": text,
                    "mode": mode,
                    "actor": actor or {"kind": "user"},
                },
                "conversation": {"id": conversation},
            }
        )

    def states(self) -> dict[str, tuple[str, str | None]]:
        """Each delivery's last state and its reason."""
        out: dict[str, tuple[str, str | None]] = {}
        for path, body in self.sent:
            if path.startswith("/api/adapter/deliveries/"):
                out[path.split("/")[4]] = (body["state"], body.get("reason"))
        return out

    def turn_reports(self) -> list[str]:
        return [body["state"] for path, body in self.sent if path == "/api/adapter/turn"]

    def replies(self) -> list[tuple[str, str]]:
        return [
            (body["parent_id"], body["text"]) for path, body in self.sent if path == "/api/adapter/messages"
        ]


class Rig:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.broker = ScriptedBroker()
        self.server = app_server(tmp_path)
        self.thread = ManagedThread("Codex test", str(tmp_path))
        self.adapter = CodexAdapter(
            self.broker, self.server, [self.thread], tmp_path / "state.json", sleep=no_sleep
        )

    async def start(self) -> None:
        await self.server.start()
        await self.adapter.open_threads()
        await self.adapter.register(self.thread)

    async def deliver(
        self, name: str, text: str, mode: str, actor: dict[str, str] | None = None, conversation: str = "c1"
    ) -> None:
        self.broker.claim(name, text, mode, actor, conversation)
        assert await self.adapter.handle_one(self.thread) is True

    async def finish(self) -> None:
        await drained(self.adapter, self.thread)

    async def running(self) -> str:
        """The running turn's id once everything received is handled."""
        await handled(self.adapter, self.thread)
        return self.thread.active

    def methods(self) -> list[str]:
        return [c["method"] for c in calls(self.tmp_path) if c["method"].startswith("turn/")]

    async def stop(self) -> None:
        await self.adapter.close()
        await self.server.close()


@contextlib.asynccontextmanager
async def rig(tmp_path: Path) -> AsyncIterator[Rig]:
    r = Rig(tmp_path)
    await r.start()
    try:
        yield r
    finally:
        await r.stop()


def run_rig(tmp_path: Path, scenario: Callable[[Rig], Awaitable[None]]) -> None:
    async def go() -> None:
        async with rig(tmp_path) as r:
            await scenario(r)

    asyncio.run(go())


ALL_MODES = "auto, queue, steer, restart"


def test_registration_declares_the_send_modes_and_reports_idle(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        register = next(b for p, b in r.broker.sent if p == "/api/adapter/register")
        assert register["capabilities"]["send_modes"] == ["auto", "queue", "steer", "restart"]
        assert register["capabilities"]["turn_state"] is True
        assert r.broker.turn_reports() == ["idle"]

    run_rig(tmp_path, scenario)


def test_each_turn_reports_busy_then_idle(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "one", "auto")
        await r.finish()
        await r.deliver("b", "two", "auto")
        await r.finish()
        assert r.broker.turn_reports() == ["idle", "busy", "idle", "busy", "idle"]

    run_rig(tmp_path, scenario)


@pytest.mark.parametrize("mode", ["auto", "queue", "restart"])
def test_an_idle_thread_starts_a_turn_in_every_mode_but_steer(tmp_path: Path, mode: str) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "hello", mode)
        await r.finish()
        assert r.methods() == ["turn/start"]
        assert r.broker.states()["d-a"] == ("responded", None)
        assert r.broker.replies() == [("m-a", "echo: hello")]

    run_rig(tmp_path, scenario)


def test_steer_to_an_idle_thread_fails_and_starts_no_turn(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "hello", "steer")
        state, reason = r.broker.states()["d-a"]
        assert state == "failed"
        assert reason and ALL_MODES in reason
        assert r.methods() == []
        assert r.broker.replies() == []

    run_rig(tmp_path, scenario)


def test_an_unknown_mode_fails_naming_the_supported_ones(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "hello", "teleport")
        state, reason = r.broker.states()["d-a"]
        assert state == "failed"
        assert reason and ALL_MODES in reason
        assert r.methods() == []

    run_rig(tmp_path, scenario)


@pytest.mark.parametrize("mode", ["auto", "steer"])
def test_auto_and_steer_join_the_running_turn(tmp_path: Path, mode: str) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "HOLD first", "auto")
        running = await r.running()
        await r.deliver("b", "second", mode)
        await r.finish()
        assert r.methods() == ["turn/start", "turn/steer"]
        steer = next(c for c in calls(r.tmp_path) if c["method"] == "turn/steer")
        assert steer["params"]["expectedTurnId"] == running
        assert steer["params"]["threadId"] == r.thread.thread_id
        assert r.broker.states()["d-a"] == ("responded", None)
        assert r.broker.states()["d-b"] == ("responded", None)
        # One turn, one conversation: one reply, under the message that started the turn.
        assert r.broker.replies() == [("m-a", "echo: HOLD first | second")]

    run_rig(tmp_path, scenario)


def test_a_turn_joined_from_another_conversation_answers_each_conversation_once(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "HOLD first", "auto")
        await r.running()
        await r.deliver("b", "second", "steer")
        await r.deliver("c", "third", "steer", conversation="c2")
        await r.finish()
        assert {d: r.broker.states()[d] for d in ("d-a", "d-b", "d-c")} == {
            d: ("responded", None) for d in ("d-a", "d-b", "d-c")
        }
        reply = "echo: HOLD first | second | third"
        assert r.broker.replies() == [("m-a", reply), ("m-c", reply)]

    run_rig(tmp_path, scenario)


def test_a_refused_steer_in_auto_waits_for_the_turn_and_starts_one(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "HOLD first", "auto")
        await r.deliver("b", "REFUSE second", "auto")
        await r.finish()
        assert r.methods() == ["turn/start", "turn/steer", "turn/start"]
        assert r.broker.states()["d-b"] == ("responded", None)

    run_rig(tmp_path, scenario)


def test_a_refused_steer_in_steer_mode_fails_with_the_reason(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "HOLD first", "auto")
        await r.deliver("b", "REFUSE second", "steer")
        await r.finish()
        state, reason = r.broker.states()["d-b"]
        assert state == "failed"
        assert reason and "steer" in reason
        assert r.methods() == ["turn/start", "turn/steer"]

    run_rig(tmp_path, scenario)


def test_queue_waits_for_the_running_turn_and_never_steers(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "HOLD first", "auto")
        await r.deliver("b", "second", "queue")
        await r.finish()
        assert r.methods() == ["turn/start", "turn/start"]
        assert r.broker.states()["d-a"] == ("responded", None)
        assert r.broker.states()["d-b"] == ("responded", None)
        assert r.broker.replies() == [("m-a", "echo: HOLD first"), ("m-b", "echo: second")]

    run_rig(tmp_path, scenario)


def test_restart_interrupts_the_running_turn_before_it_starts_another(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "HOLD first", "auto")
        first = await r.running()
        await r.deliver("b", "second", "restart")
        await r.finish()
        assert r.methods() == ["turn/start", "turn/interrupt", "turn/start"]
        interrupt = next(c for c in calls(r.tmp_path) if c["method"] == "turn/interrupt")
        assert interrupt["params"] == {"threadId": r.thread.thread_id, "turnId": first}
        assert r.broker.states()["d-a"][0] == "failed"
        assert r.broker.states()["d-b"] == ("responded", None)
        assert r.broker.replies() == [("m-b", "echo: second")]

    run_rig(tmp_path, scenario)


def test_every_claim_carries_the_current_turn_state(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        assert await r.adapter.handle_one(r.thread) is False
        await r.deliver("a", "HOLD first", "auto")
        await r.deliver("b", "second", "auto")
        await r.finish()
        assert await r.adapter.handle_one(r.thread) is False
        states = [b["turn"]["state"] for b in r.broker.claim_bodies]
        assert states == ["idle", "idle", "busy", "idle"]
        assert all(b["turn"]["waiting_for"] is None for b in r.broker.claim_bodies)

    run_rig(tmp_path, scenario)


def notify(r: Rig, method: str, params: dict[str, Any]) -> None:
    """A notification the app-server sent and the adapter received, which the pump has not run for yet."""
    r.server.events(r.thread.thread_id).put_nowait(
        {"method": method, "params": {"threadId": r.thread.thread_id, **params}}
    )


def test_a_claim_carries_a_turn_change_the_app_server_sent_before_it(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        notify(r, "turn/started", {"turn": {"id": "u-early", "status": "inProgress"}})
        assert await r.adapter.handle_one(r.thread) is False
        assert r.broker.claim_bodies[-1]["turn"]["state"] == "busy"

    run_rig(tmp_path, scenario)


def test_a_notification_a_claim_handled_is_not_handled_again_by_the_pump(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        notify(r, "turn/started", {"turn": {"id": "u-early", "status": "inProgress"}})
        assert await r.adapter.handle_one(r.thread) is False
        notify(r, "turn/completed", {"turn": {"id": "u-early", "status": "completed"}})
        await r.finish()  # the pump handles the end after anything queued before it
        assert r.broker.turn_reports() == ["idle", "busy", "idle"]

    run_rig(tmp_path, scenario)


def test_a_claim_goes_ahead_while_an_ended_turn_s_reply_is_still_pending(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        gate = asyncio.Event()
        r.broker.hold["/api/adapter/messages"] = gate
        await r.deliver("a", "one", "auto")
        await asyncio.wait_for(r.broker.parked("/api/adapter/messages").wait(), 10)
        await asyncio.wait_for(r.deliver("b", "two", "auto"), 10)  # while a's reply waits at the gate
        assert ("m-a", "echo: one") not in r.broker.replies()
        assert r.broker.claim_bodies[-1]["turn"]["state"] == "idle"
        gate.set()
        await r.finish()
        assert sorted(r.broker.replies()) == [("m-a", "echo: one"), ("m-b", "echo: two")]

    run_rig(tmp_path, scenario)


def test_no_credential_variable_reaches_the_app_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "CODEX_API_KEY", "GITHUB_TOKEN", "SOME_NEW_SECRET"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("Openai_Api_Key", "x")

    async def scenario(r: Rig) -> None:
        keys = next(c["keys"] for c in calls(tmp_path) if c["method"] == "env")
        assert "CODEX_HOME" in keys
        assert not [k for k in keys if k.upper() == "OPENAI_API_KEY"]
        assert {
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "CODEX_API_KEY",
            "GITHUB_TOKEN",
            "SOME_NEW_SECRET",
        }.isdisjoint(keys)
        # Each variable is kept or dropped by its name alone, so a new credential name needs no list change.
        assert child_env({"Path": "p", "NEW_API_KEY": "k", "FOO": "f"}) == {"Path": "p"}

    run_rig(tmp_path, scenario)


def test_the_adapter_does_not_declare_task_support() -> None:
    assert "tasks" not in CAPABILITIES


BROKER = {"kind": "binding", "id": "broker"}


def test_a_broker_notice_becomes_a_turn_and_a_peer_message_does_not(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("n", "Continue where you left off.", "auto", BROKER)
        await r.finish()
        assert r.broker.states()["d-n"] == ("responded", None)
        await r.deliver("p", "do this", "auto", {"kind": "binding", "id": "some-peer"})
        assert r.broker.states()["d-p"][0] == "failed"
        assert r.methods() == ["turn/start"]

    run_rig(tmp_path, scenario)


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://unused", code, "x", Message(), io.BytesIO(b""))


def test_a_lost_accepted_answer_cannot_orphan_the_turn(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        r.broker.fail["/state"] = [OSError("lost"), http_error(503)]
        await r.deliver("a", "hello", "auto")
        await r.finish()
        sent = [b["state"] for p, b in r.broker.sent if "/deliveries/d-a/state" in p]
        assert sent == ["accepted", "responded"]
        assert r.broker.replies() == [("m-a", "echo: hello")]

    run_rig(tmp_path, scenario)


def test_a_reply_and_its_state_are_retried_under_one_stable_id(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        r.broker.fail["/messages"] = [OSError("down"), http_error(500)]
        await r.deliver("a", "hello", "auto")
        await r.finish()
        posts = [b for p, b in r.broker.sent if p == "/api/adapter/messages"]
        assert [b["client_message_id"] for b in posts] == ["reply-d-a"]
        assert r.broker.states()["d-a"] == ("responded", None)

    run_rig(tmp_path, scenario)


def test_a_client_error_ends_the_retries(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        r.broker.fail["/state"] = [http_error(409)]
        await r.deliver("a", "hello", "auto")
        await r.finish()
        assert r.broker.states()["d-a"] == ("responded", None)

    run_rig(tmp_path, scenario)


def test_reconcile_fails_what_no_run_holds_and_reacknowledges_what_one_does(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        r.broker.fail["/deliveries/d-a/state"] = [http_error(409)]
        await r.deliver("a", "HOLD first", "auto")
        listed = [
            {"id": "d-a", "state": "dispatching", "native_turn_id": await r.running()},
            {"id": "d-gone", "state": "accepted", "native_turn_id": "u-old"},
        ]
        await r.adapter.reconcile(r.thread, listed)
        assert r.broker.states()["d-gone"][0] == "failed"
        accepted = [b for p, b in r.broker.sent if "/deliveries/d-a/state" in p]
        assert accepted and accepted[0]["state"] == "accepted"
        await r.finish()

    run_rig(tmp_path, scenario)


def test_turn_readings_are_numbered_in_order_and_restart_with_a_registration(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        await r.deliver("a", "one", "auto")
        await r.finish()
        assert await r.adapter.handle_one(r.thread) is False
        assert r.broker.readings == sorted(set(r.broker.readings))
        assert len(r.broker.readings) >= 4
        await r.adapter.register(r.thread)
        assert r.broker.readings[-1] == 1

    run_rig(tmp_path, scenario)


def test_a_result_never_overtakes_the_accepted_state(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        gate = asyncio.Event()
        r.broker.hold["/deliveries/d-a/state"] = gate
        delivery = asyncio.ensure_future(r.deliver("a", "hello", "auto"))
        await asyncio.wait_for(r.thread.idle.wait(), 10)
        await asyncio.sleep(0.3)  # the turn is over and its reply could go; it must not
        assert r.broker.replies() == []
        gate.set()
        await delivery
        await r.finish()
        order = [b["state"] for p, b in r.broker.sent if "/deliveries/d-a/state" in p]
        assert order == ["accepted", "responded"]

    run_rig(tmp_path, scenario)


class FlakyBroker(Broker):
    """The real broker, with its first claim lost on the way."""

    def __init__(self, url: str, on_loss: Callable[[], None]) -> None:
        super().__init__(url, ENROLL)
        self.on_loss = on_loss
        self.lost = False

    async def call(self, method: str, path: str, body: Any = None, bearer: str = "") -> Any:
        if path == "/api/adapter/claim" and not self.lost:
            self.lost = True
            self.on_loss()
            raise OSError("link dropped")
        return await super().call(method, path, body, bearer)


def test_a_delivery_left_accepted_with_no_live_run_is_failed_once_serving_resumes(
    broker_url: str, tmp_path: Path
) -> None:
    token = owner(broker_url)

    async def scenario() -> tuple[list[dict[str, Any]], ManagedThread]:
        server = app_server(tmp_path)
        await server.start()
        thread = ManagedThread("Codex test", str(tmp_path))
        adapter = CodexAdapter(Broker(broker_url, ENROLL), server, [thread], tmp_path / "state.json")
        await adapter.open_threads()
        await adapter.register(thread)
        dm = http(broker_url, "POST", "/api/direct-conversations", {"binding_id": thread.binding_id}, token)

        def strand() -> None:
            """What an earlier moment left behind: a delivery the broker holds accepted, with no live run."""
            http(
                broker_url,
                "POST",
                f"/api/conversations/{dm['id']}/messages",
                {"client_message_id": "m1", "text": "hello"},
                token,
            )
            claimed = http(broker_url, "POST", "/api/adapter/claim", token=thread.token)
            state = {"state": "accepted", "native_turn_id": "u-old"}
            path = f"/api/adapter/deliveries/{claimed['delivery']['id']}/state"
            http(broker_url, "POST", path, state, thread.token)

        # The broker link drops once, at the first claim; when serving resumes the stale delivery is settled.
        adapter.broker = FlakyBroker(broker_url, strand)
        sleeps = 0

        async def stop(_: float) -> None:
            nonlocal sleeps
            sleeps += 1
            if sleeps == 2:
                raise asyncio.CancelledError  # the first idle poll after recovery

        with pytest.raises(asyncio.CancelledError):
            await adapter.serve_thread(thread, stop)
        events: list[dict[str, Any]] = http(
            broker_url, "GET", f"/api/conversations/{dm['id']}/events", token=token
        )
        await adapter.close()
        await server.close()
        return events, thread

    events, _ = asyncio.run(scenario())
    changes = [e["payload"] for e in events if e["type"] == "delivery.changed"]
    assert [c["state"] for c in changes] == ["queued", "dispatching", "accepted", "failed"]
    assert "no longer holds" in changes[-1]["reason"]


def test_a_route_error_at_reconcile_does_not_stop_the_serve_loop(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        r.broker.fail["/deliveries?"] = [http_error(500)]
        await r.adapter._reconcile_with_broker(r.thread)

    run_rig(tmp_path, scenario)


class HoldFirstTurnReading(Broker):
    """The real broker, with the first turn reading held back before it is sent and its answer kept."""

    def __init__(self, url: str) -> None:
        super().__init__(url, ENROLL)
        self.gate = asyncio.Event()
        self.parked = asyncio.Event()
        self.held_answer: Any = None
        self._taken = False

    async def call(self, method: str, path: str, body: Any = None, bearer: str = "") -> Any:
        if path == "/api/adapter/turn" and not self._taken:
            self._taken = True
            self.parked.set()
            await self.gate.wait()
            self.held_answer = await super().call(method, path, body, bearer)
            return self.held_answer
        return await super().call(method, path, body, bearer)


def test_the_newest_reading_reaches_the_broker_though_an_older_one_arrives_late(
    broker_url: str, tmp_path: Path
) -> None:
    token = owner(broker_url)

    async def scenario() -> tuple[Any, str]:
        server = app_server(tmp_path)
        await server.start()
        held = HoldFirstTurnReading(broker_url)
        thread = ManagedThread("Codex test", str(tmp_path))
        adapter = CodexAdapter(held, server, [thread], tmp_path / "state.json")
        await adapter.open_threads()
        registering = asyncio.ensure_future(adapter.register(thread))  # parks at its first reading
        await asyncio.wait_for(held.parked.wait(), 10)
        dm = http(broker_url, "POST", "/api/direct-conversations", {"binding_id": thread.binding_id}, token)
        http(
            broker_url,
            "POST",
            f"/api/conversations/{dm['id']}/messages",
            {"client_message_id": "m1", "text": "HOLD one"},
            token,
        )
        assert await adapter.handle_one(thread) is True  # busy reaches the broker first
        await asyncio.sleep(0.3)
        held.gate.set()  # the older idle reading lands now
        await registering
        await drained(adapter, thread)
        binding = http(broker_url, "GET", "/api/adapter/binding", token=thread.token)
        await adapter.close()
        await server.close()
        return held.held_answer, binding["turn_state"]

    answer, final = asyncio.run(scenario())
    assert answer["applied"] is False
    assert final == "idle"


def test_a_reply_the_broker_refuses_fails_the_delivery_and_tells_the_sender(
    broker_url: str, tmp_path: Path
) -> None:
    token = owner(broker_url)

    async def scenario() -> list[dict[str, Any]]:
        server = app_server(tmp_path)
        await server.start()
        thread = ManagedThread("Codex test", str(tmp_path))
        adapter = CodexAdapter(Broker(broker_url, ENROLL), server, [thread], tmp_path / "state.json")
        await adapter.open_threads()
        await adapter.register(thread)
        dm = http(broker_url, "POST", "/api/direct-conversations", {"binding_id": thread.binding_id}, token)
        http(
            broker_url,
            "POST",
            f"/api/conversations/{dm['id']}/messages",
            {"client_message_id": "m1", "text": "BIG answer"},  # the fake answers with 70000 bytes
            token,
        )
        await adapter.handle_one(thread)
        await drained(adapter, thread)
        events: list[dict[str, Any]] = http(
            broker_url, "GET", f"/api/conversations/{dm['id']}/events", token=token
        )
        found = [
            c
            for c in http(broker_url, "GET", "/api/conversations", token=token)
            if c["title"] == "Duplex broker"
        ]
        told: list[dict[str, Any]] = (
            http(broker_url, "GET", f"/api/conversations/{found[0]['id']}/events", token=token)
            if found
            else []
        )
        await adapter.close()
        await server.close()
        return [*events, *told]

    events = asyncio.run(scenario())
    changes = [e["payload"] for e in events if e["type"] == "delivery.changed"]
    assert changes[-1]["state"] == "failed"
    assert "the reply was refused" in changes[-1]["reason"]
    assert not [
        e
        for e in events
        if e["type"] == "message.accepted"
        and e["actor"]["kind"] == "binding"
        and e["actor"]["id"] != "broker"
    ]
    told = [e["payload"].get("text", "") for e in events if e["type"] == "message.accepted"]
    assert any("ended failed" in t and "the reply was refused" in t for t in told)


def turn_state_writers(source: str) -> dict[str, list[int]]:
    """Each function that assigns `.active` or `.idle` or calls `.idle.set()` or `.idle.clear()`: lines."""
    writers: dict[str, list[int]] = {}

    def writes(node: ast.AST) -> bool:
        if isinstance(node, ast.Attribute) and node.attr in ("active", "idle"):
            return isinstance(node.ctx, (ast.Store, ast.Del))
        if not isinstance(node, ast.Call):
            return False
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in ("set", "clear"):
            return isinstance(func.value, ast.Attribute) and func.value.attr == "idle"
        if isinstance(func, ast.Name) and func.id in ("setattr", "delattr"):
            return any(isinstance(a, ast.Constant) and a.value in ("active", "idle") for a in node.args)
        return False

    def visit(node: ast.AST, owner: str) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            owner = node.name
        if writes(node):
            writers.setdefault(owner, []).append(getattr(node, "lineno", 0))
        for child in ast.iter_child_nodes(node):
            visit(child, owner)

    visit(ast.parse(source), "<module>")
    return writers


def test_only_the_handler_writes_the_turn_state() -> None:
    writers = turn_state_writers((HERE / "duplex_codex.py").read_text(encoding="utf-8"))
    assert set(writers) == {"_on_event"}, writers


async def answer(r: Rig, turn_id: str) -> None:
    """The adapter's own turn/start path, given an app-server answer that names `turn_id`."""

    async def answered(method: str, params: dict[str, Any], timeout: float = 120) -> dict[str, Any]:
        assert method == "turn/start"
        return {"turn": {"id": turn_id, "status": "inProgress"}}

    with patch.object(r.server, "request", answered):
        await r.adapter._start(r.thread, {"id": "m-x", "text": "x"})


def queued(r: Rig) -> int:
    """Inputs received for the thread and not yet handled."""
    return r.server.events(r.thread.thread_id).qsize() + len(r.thread.taken)


async def reading(r: Rig) -> str:
    """The turn a claim carries now, after it handles everything received."""
    assert await r.adapter.handle_one(r.thread) is False
    state: str = r.broker.claim_bodies[-1]["turn"]["state"]
    return state


@pytest.mark.parametrize(
    "order",
    [("answer", "started", "end"), ("started", "answer", "end"), ("started", "end", "answer")],
    ids="-".join,
)
def test_a_turn_reads_busy_once_then_idle_in_every_order_its_inputs_arrive(
    tmp_path: Path, order: tuple[str, str, str]
) -> None:
    async def scenario(r: Rig) -> None:
        for step in order:
            if step == "answer":
                await answer(r, "u-x")
            elif step == "started":
                notify(r, "turn/started", {"turn": {"id": "u-x"}})
            else:
                notify(r, "turn/completed", {"turn": {"id": "u-x", "status": "completed"}})
        assert await reading(r) == "idle"
        assert r.broker.turn_reports() == ["idle", "busy", "idle"]
        assert r.thread.active == "" and r.thread.idle.is_set()

    run_rig(tmp_path, scenario)


def test_a_start_answer_handled_while_an_older_turn_s_end_is_reported_reads_busy(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        notify(r, "turn/started", {"turn": {"id": "u-a"}})
        assert await reading(r) == "busy"
        gate = asyncio.Event()
        r.broker.hold["/api/adapter/turn"] = gate
        notify(r, "turn/completed", {"turn": {"id": "u-a", "status": "completed"}})
        await asyncio.wait_for(r.broker.parked("/api/adapter/turn").wait(), 10)  # a's idle is mid-report
        await answer(r, "u-b")
        gate.set()
        assert await reading(r) == "busy"
        assert r.broker.turn_reports() == ["idle", "busy", "idle", "busy"]
        assert r.thread.active == "u-b" and not r.thread.idle.is_set()

    run_rig(tmp_path, scenario)


@pytest.mark.parametrize("failure", ["error", "ended"])
def test_a_failed_turn_start_queues_nothing_and_changes_no_turn_state(tmp_path: Path, failure: str) -> None:
    async def scenario(r: Rig) -> None:
        if failure == "error":

            async def refused(method: str, params: dict[str, Any], timeout: float = 120) -> dict[str, Any]:
                raise RpcError('{"code": -32600, "message": "a turn is already running"}')

            with patch.object(r.server, "request", refused), pytest.raises(RpcError):
                await r.adapter._start(r.thread, {"id": "m-x", "text": "x"})
            reports = ["idle"]
        else:  # the turn's end was handled before its answer came back
            notify(r, "turn/started", {"turn": {"id": "u-x"}})
            notify(r, "turn/completed", {"turn": {"id": "u-x", "status": "completed"}})
            await handled(r.adapter, r.thread)
            await answer(r, "u-x")
            reports = ["idle", "busy", "idle"]
        assert queued(r) == 0
        assert await reading(r) == "idle"
        assert r.broker.turn_reports() == reports
        assert r.thread.active == "" and r.thread.idle.is_set()

    run_rig(tmp_path, scenario)


def test_a_socket_frame_named_like_the_start_answer_changes_nothing(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        async def frame(method: str) -> None:
            """A notification on the app-server's socket, read by the adapter's own reader."""
            params = {"threadId": r.thread.thread_id, "turnId": "u-x", "turn": {"id": "u-x"}}
            await r.server.request("test/notify", {"frame": {"method": method, "params": params}})

        await frame("StartAnswer")  # the internal event's name; its answer follows the frame on the socket
        assert await reading(r) == "idle"
        assert r.broker.turn_reports() == ["idle"]
        assert r.thread.active == "" and r.thread.idle.is_set()
        await frame("turn/started")  # the same path carries a real notification through
        assert await reading(r) == "busy"

    run_rig(tmp_path, scenario)


def applied(sent: list[tuple[str, Any]]) -> str:
    """The turn state a broker ends at that resets at each registration and applies only a newer reading."""
    state, last = "unknown", 0
    for path, body in sent:
        if path == "/api/adapter/register":
            state, last = "unknown", 0
        elif path == "/api/adapter/turn" and body["seq"] > last:
            state, last = body["state"], body["seq"]
    return state


def test_a_registration_reset_and_its_first_reading_are_one_step(tmp_path: Path) -> None:
    async def scenario(r: Rig) -> None:
        gate = asyncio.Event()
        r.broker.hold["/api/adapter/turn"] = gate
        notify(r, "turn/started", {"turn": {"id": "u-a"}})
        await asyncio.wait_for(r.broker.parked("/api/adapter/turn").wait(), 10)  # the pump's busy, in flight
        notify(r, "turn/completed", {"turn": {"id": "u-a", "status": "completed"}})  # received, not handled
        registering = asyncio.ensure_future(r.adapter.register(r.thread))
        # Bounded: a registration that does not wait for the reading in flight is done well inside it.
        await asyncio.wait({registering}, timeout=0.5)
        gate.set()
        await asyncio.wait_for(registering, 10)
        await handled(r.adapter, r.thread)
        cut = max(i for i, (path, _) in enumerate(r.broker.sent) if path == "/api/adapter/register")
        after = [body for path, body in r.broker.sent[cut + 1 :] if path == "/api/adapter/turn"]
        assert [b["seq"] for b in after] == list(range(1, len(after) + 1))  # none numbered before the reset
        assert after[0]["state"] == "idle"  # the end received before the registration is in its first reading
        assert applied(r.broker.sent) == "idle"
        assert r.thread.active == "" and r.thread.idle.is_set()

    run_rig(tmp_path, scenario)


@pytest.mark.parametrize("mode", ["restart", "auto", "steer"])
def test_an_interrupt_and_a_steer_aimed_at_a_turn_that_just_ended_leave_the_turn_and_delivery_correct(
    tmp_path: Path, mode: str
) -> None:
    async def scenario(r: Rig) -> None:
        notify(r, "turn/started", {"turn": {"id": "u-gone"}})  # the app-server no longer runs it
        request = r.server.request

        async def ended_first(method: str, params: dict[str, Any], timeout: float = 120) -> dict[str, Any]:
            """The turn's end arrives after the adapter read the turn and before the app-server answers."""
            if method in ("turn/interrupt", "turn/steer"):
                notify(r, "turn/completed", {"turn": {"id": "u-gone", "status": "completed"}})
            return await request(method, params, timeout)

        with patch.object(r.server, "request", ended_first):
            await r.deliver("b", "second", mode)
            await r.finish()
        assert await reading(r) == "idle"
        assert r.thread.active == "" and r.thread.idle.is_set()
        if mode == "steer":
            assert r.methods() == ["turn/steer"]
            state, reason = r.broker.states()["d-b"]
            assert state == "failed" and reason and "the turn ended before the steer arrived" in reason
            assert r.broker.replies() == []
            assert r.broker.turn_reports() == ["idle", "busy", "idle"]
        else:
            first = "turn/interrupt" if mode == "restart" else "turn/steer"
            assert r.methods() == [first, "turn/start"]
            assert r.broker.states()["d-b"] == ("responded", None)
            assert r.broker.replies() == [("m-b", "echo: second")]
            assert r.broker.turn_reports() == ["idle", "busy", "idle", "busy", "idle"]

    run_rig(tmp_path, scenario)


TURN_STATE = frozenset({"active", "idle", "seq", "ended", "_reading"})
# Touches of the turn state outside the handler and the lock, each accepted by the lead; adding one is a
# decision for the lead too. Keyed by (function, attribute).
TURN_STATE_ALLOWED = {
    ("_deliver", "active"): "F-d residual: the mode's route is chosen from the turn handled at the claim; "
    "a turn that ends before the request reaches the app-server is refused there, and the steer falls back",
    ("_end_running_turn", "active"): "F-d residual: the interrupt names the turn handled at the claim; "
    "the app-server refuses one that already ended, and that refusal is suppressed",
    ("_end_running_turn", "idle"): "F-d residual: waits on the handler's idle event, which the handler "
    "sets last, after its idle reading is sent",
}


def turn_state_touches(source: str) -> list[tuple[str, str, int]]:
    """Each read or write of the turn state outside `_on_event`, a `thread.handling` block and a class body
    (construction), as (function, attribute, line). `_reading`'s own body is checked through its callers."""
    found: list[tuple[str, str, int]] = []

    def touched(node: ast.AST) -> str | None:
        if isinstance(node, ast.Attribute) and node.attr in TURN_STATE:
            return node.attr
        by_name = ("getattr", "setattr", "delattr", "hasattr")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in by_name:
            for arg in node.args:
                if isinstance(arg, ast.Constant) and arg.value in TURN_STATE:
                    return str(arg.value)
        return None

    def under_lock(node: ast.AsyncWith) -> bool:
        held = [i.context_expr for i in node.items]
        return any(isinstance(e, ast.Attribute) and e.attr == "handling" for e in held)

    def visit(node: ast.AST, owner: str, locked: bool) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            owner, locked = node.name, False
        elif isinstance(node, ast.Lambda):
            owner, locked = "<lambda>", False
        elif isinstance(node, ast.ClassDef):
            owner = "<class>"
        name = touched(node)
        exempt = locked or owner in ("_on_event", "_reading", "<class>")
        if name and not exempt:
            found.append((owner, name, getattr(node, "lineno", 0)))
        for child in ast.iter_child_nodes(node):
            inside = locked or (isinstance(node, ast.AsyncWith) and under_lock(node) and child in node.body)
            visit(child, owner, inside)

    visit(ast.parse(source), "<module>", False)
    return found


def test_turn_state_is_touched_only_under_the_lock_or_in_the_handler() -> None:
    touches = turn_state_touches((HERE / "duplex_codex.py").read_text(encoding="utf-8"))
    unexpected = [t for t in touches if (t[0], t[1]) not in TURN_STATE_ALLOWED]
    assert not unexpected, (
        f"turn state touched outside _on_event and thread.handling: {unexpected}. "
        "Take the reading under the lock; a new TURN_STATE_ALLOWED entry is a decision to bring to the lead."
    )
    assert {(f, a) for f, a, _ in touches} == set(TURN_STATE_ALLOWED), "an allowlist entry no code needs"


# Finding Codex and the example config


def fake_codex(folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    exe = folder / ("codex.exe" if sys.platform == "win32" else "codex")
    exe.write_text("", encoding="utf-8")
    exe.chmod(0o755)
    return exe


def test_codex_is_the_variable_then_the_bundled_exe_then_the_path(tmp_path: Path) -> None:
    on_path = fake_codex(tmp_path / "bin")
    bundled = tmp_path / "appdata" / BUNDLED_CODEX
    fake_codex(bundled.parent).rename(bundled)
    environ = {"PATH": str(tmp_path / "bin"), "APPDATA": str(tmp_path / "appdata")}

    assert find_codex({**environ, "DUPLEX_CODEX": "/opt/codex"}) == "/opt/codex"
    assert find_codex(environ) == str(bundled)
    assert Path(find_codex({"PATH": environ["PATH"]})).samefile(on_path)


def test_codex_missing_says_how_to_point_at_it(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="DUPLEX_CODEX"):
        find_codex({"PATH": str(tmp_path)})


def test_the_example_config_loads_as_a_codex_config(tmp_path: Path) -> None:
    adapter = {"broker_url": "http://broker.example:8790", "enrollment_token": "t"}
    (tmp_path / "adapter.json").write_text(json.dumps(adapter), encoding="utf-8")
    (tmp_path / "codex.json").write_text((HERE / "codex.example.json").read_text(encoding="utf-8"), "utf-8")
    broker, threads = load(tmp_path)
    assert [(t.title, t.cwd) for t in threads] == [("Codex example", "/path/to/project")]
    assert broker.base_url == "http://broker.example:8790"

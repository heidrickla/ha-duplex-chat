from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.model_member import SYSTEM_PROMPT, ModelConfig, render_tool_calls, trim_history
from duplex_broker.store import MIGRATIONS, SCHEMA_VERSION, Store

CODE = "pair-me"
ENROLL = "enroll-me"


class FakeRouter:
    """An OpenAI-style endpoint that records requests and answers as told."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status = 200
        self.delay = 0.0
        self.message: dict[str, Any] = {"role": "assistant", "content": "hello from model"}
        router = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["content-length"])))
                router.requests.append(body)
                time.sleep(router.delay)
                payload = json.dumps({"choices": [{"message": router.message}]}).encode()
                if router.status != 200:
                    payload = b'{"error": "no"}'
                self.send_response(router.status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def router() -> Iterator[FakeRouter]:
    r = FakeRouter()
    yield r
    r.close()


def make_client(url: str, timeout: float = 5.0, no_thinking: bool = False) -> TestClient:
    store = Store(":memory:")
    app = create_app(
        store,
        Settings(
            pairing_code=CODE,
            enrollment_token=ENROLL,
            model_url=url,
            model_name="model-chat",
            model_timeout=timeout,
            model_no_thinking=no_thinking,
        ),
    )
    return TestClient(app)


@pytest.fixture
def client(router: FakeRouter) -> Iterator[TestClient]:
    with make_client(router.url) as c:
        yield c


def paired(client: TestClient) -> dict[str, str]:
    r = client.post("/api/pair", json={"code": CODE})
    assert r.status_code == 200, r.text
    return {"x-csrf-token": r.json()["csrf"]}


def model_id(client: TestClient) -> str:
    for _ in range(100):
        found = [s for s in client.get("/api/sessions").json() if s["provider"] == "model"]
        if found:
            return str(found[0]["id"])
        time.sleep(0.05)
    raise AssertionError("the Assistant member never appeared")


def open_dm(client: TestClient) -> tuple[str, dict[str, str]]:
    csrf = paired(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": model_id(client)}, headers=csrf)
    return dm.json()["id"], csrf


def say(client: TestClient, dm: str, csrf: dict[str, str], text: str) -> str:
    r = client.post(
        f"/api/conversations/{dm}/messages",
        json={"client_message_id": uuid.uuid4().hex, "text": text},
        headers=csrf,
    )
    assert r.status_code == 201, r.text
    return str(r.json()["message"]["id"])


def wait_reply(client: TestClient, dm: str, count: int = 1, timeout: float = 8.0) -> list[str]:
    """Texts of the member's replies once `count` of them are posted and the last delivery is settled."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        events = client.get(f"/api/conversations/{dm}/events").json()
        replies = [
            e["payload"]["text"]
            for e in events
            if e["type"] == "message.accepted" and e["actor"]["kind"] == "binding"
        ]
        states = [e["payload"]["state"] for e in events if e["type"] == "delivery.changed"]
        if len(replies) >= count and states[-1] in ("responded", "failed"):
            return replies
        time.sleep(0.05)
    raise AssertionError("no reply")


def delivery_states(client: TestClient, dm: str) -> list[str]:
    events = client.get(f"/api/conversations/{dm}/events").json()
    return [e["payload"]["state"] for e in events if e["type"] == "delivery.changed"]


def test_model_is_listed_like_a_registered_session(client: TestClient) -> None:
    paired(client)
    model_id(client)
    listed = [s for s in client.get("/api/sessions").json() if s["provider"] == "model"]
    assert len(listed) == 1
    assert listed[0]["title"] == "Assistant"
    assert listed[0]["active"] is True


def test_the_member_is_absent_without_a_router_url() -> None:
    with make_client("") as c:
        paired(c)
        assert c.get("/api/sessions").json() == []


def test_a_dm_round_trip_through_the_router(client: TestClient, router: FakeRouter) -> None:
    dm, csrf = open_dm(client)
    say(client, dm, csrf, "what is 2+2")
    assert wait_reply(client, dm) == ["hello from model"]
    assert delivery_states(client, dm) == ["queued", "dispatching", "accepted", "responded"]
    sent = router.requests[0]
    expected = ModelConfig("", title="Assistant", owner_name="Owner").system(SYSTEM_PROMPT)
    assert sent["messages"][0] == {"role": "system", "content": expected}
    assert sent["messages"][-1] == {"role": "user", "content": "what is 2+2"}
    assert "tools" not in sent
    assert sent["model"] == "model-chat"


def test_the_request_has_no_template_kwargs_unless_the_option_asks_for_them(router: FakeRouter) -> None:
    with make_client(router.url) as plain:
        dm, csrf = open_dm(plain)
        say(plain, dm, csrf, "hi")
        wait_reply(plain, dm)
    assert "chat_template_kwargs" not in router.requests[0]
    with make_client(router.url, no_thinking=True) as quiet:
        dm, csrf = open_dm(quiet)
        say(quiet, dm, csrf, "hi")
        wait_reply(quiet, dm)
    assert router.requests[1]["chat_template_kwargs"] == {"enable_thinking": False}


def test_later_turns_carry_the_earlier_ones(client: TestClient, router: FakeRouter) -> None:
    dm, csrf = open_dm(client)
    say(client, dm, csrf, "first")
    wait_reply(client, dm, 1)
    say(client, dm, csrf, "second")
    wait_reply(client, dm, 2)
    roles = [(m["role"], m["content"]) for m in router.requests[1]["messages"][1:]]
    assert roles == [("user", "first"), ("assistant", "hello from model"), ("user", "second")]


def test_history_is_trimmed_to_the_window_and_keeps_the_newest_turn() -> None:
    turns = [{"role": "user", "content": f"{n} " + "x" * 5000} for n in range(60)]
    kept = trim_history(turns)
    assert kept[-1] == turns[-1]
    assert 1 < len(kept) < len(turns)
    assert kept == turns[-len(kept) :]
    total = len(SYSTEM_PROMPT) + sum(len(json.dumps(t)) for t in kept)
    assert total <= (32768 - 1024) * 2

    huge = [{"role": "user", "content": "y" * 500_000}]
    cut = trim_history(huge)
    assert len(cut) == 1
    assert len(SYSTEM_PROMPT) + len(json.dumps(cut[0])) <= (32768 - 1024) * 2


def test_a_busy_router_becomes_a_short_reply(client: TestClient, router: FakeRouter) -> None:
    router.status = 503
    dm, csrf = open_dm(client)
    say(client, dm, csrf, "hi")
    reply = wait_reply(client, dm)[0]
    assert "busy" in reply and len(reply) < 100
    assert delivery_states(client, dm)[-1] == "responded"


def test_a_router_over_the_window_says_so(client: TestClient, router: FakeRouter) -> None:
    router.status = 400
    dm, csrf = open_dm(client)
    say(client, dm, csrf, "hi")
    assert "too long" in wait_reply(client, dm)[0]


def test_a_router_that_never_answers_times_out_with_a_reply(router: FakeRouter) -> None:
    router.delay = 3.0
    with make_client(router.url, timeout=0.5) as c:
        dm, csrf = open_dm(c)
        say(c, dm, csrf, "hi")
        assert "did not answer" in wait_reply(c, dm)[0]


def test_an_unreachable_router_becomes_a_reply() -> None:
    with make_client("http://127.0.0.1:9/v1") as c:
        dm, csrf = open_dm(c)
        say(c, dm, csrf, "hi")
        assert "not reachable" in wait_reply(c, dm)[0]


def test_tool_calls_are_shown_as_text(client: TestClient, router: FakeRouter) -> None:
    router.message = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {"function": {"name": "HassTurnOn", "arguments": '{"name": "kitchen light"}'}},
            {"function": {"name": "HassTurnOff", "arguments": {"name": "fan"}}},
        ],
    }
    dm, csrf = open_dm(client)
    say(client, dm, csrf, "turn on the kitchen light")
    assert wait_reply(client, dm)[0] == (
        'tool: HassTurnOn {"name": "kitchen light"}\ntool: HassTurnOff {"name": "fan"}'
    )


def test_render_tool_calls_keeps_content_before_the_calls() -> None:
    text = render_tool_calls(
        {"content": "ok", "tool_calls": [{"function": {"name": "f", "arguments": ""}}, "junk"]}
    )
    assert text == "ok\ntool: f {}"


def test_a_slow_router_does_not_block_the_event_loop(router: FakeRouter) -> None:
    router.delay = 1.5
    with make_client(router.url) as c:
        dm, csrf = open_dm(c)
        say(c, dm, csrf, "slow one")
        end = time.monotonic() + 5
        while "accepted" not in delivery_states(c, dm) and time.monotonic() < end:
            time.sleep(0.02)
        assert len(router.requests) <= 1
        worst = 0.0
        for _ in range(5):
            started = time.monotonic()
            assert c.get("/health").status_code == 200
            worst = max(worst, time.monotonic() - started)
        assert delivery_states(c, dm)[-1] == "accepted", "the router call finished before the loop was probed"
        assert worst < 0.5, f"/health waited {worst:.2f} s behind the router call"
        assert wait_reply(c, dm) == ["hello from model"]


def test_an_adapter_cannot_register_as_the_built_in_member(client: TestClient) -> None:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "model",
            "native_id": "model",
            "title": "x",
            "workspace": "x",
            "endpoint_ref": "x",
            "capabilities": {},
        },
    )
    assert r.status_code == 422


def test_a_version_4_database_accepts_the_model_provider(tmp_path: Any) -> None:
    path = tmp_path / "v4.db"
    db = sqlite3.connect(path)
    for step in range(1, 5):
        for statement in MIGRATIONS[step].split(";"):
            if statement.strip():
                db.execute(statement)
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (4)")
    db.execute(
        "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref, capabilities,"
        " lease_generation, token_sha, active, heartbeat_at, created_at)"
        " VALUES ('b1', 'claude', 'n1', 'kept', 'w', 'e', '{}', 1, 't', 1, NULL, 0)"
    )
    db.execute(
        "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
        " VALUES ('c1', 'direct', 'kept', 'owner', 'user_directed', 'b1', 0)"
    )
    db.commit()
    db.close()

    store = Store(path)
    binding, _ = store.register_binding("model", "model", "Assistant", "built-in", "broker", {})
    assert binding.provider == "model"
    assert [b.title for b in store.list_bindings()] == ["kept", "Assistant"]
    store.close()
    check = sqlite3.connect(path)
    assert check.execute("SELECT version FROM schema_version").fetchone()[0] == SCHEMA_VERSION
    assert check.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert check.execute("PRAGMA foreign_key_check").fetchall() == []
    assert check.execute("SELECT dm_binding_id FROM conversations").fetchone()[0] == "b1"
    with pytest.raises(sqlite3.IntegrityError):
        check.execute(
            "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref,"
            " capabilities, lease_generation, token_sha, active, created_at)"
            " VALUES ('b9', 'other', 'n', 't', 'w', 'e', '{}', 1, 'u', 1, 0)"
        )
    check.close()

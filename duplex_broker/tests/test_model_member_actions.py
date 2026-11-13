from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.assist import AssistBundle, AssistClient, AssistUnavailable
from duplex_broker.model_member import FORGED_NOTE, MAX_ROUNDS, act, history, render_tool_calls, trim_history
from duplex_broker.store import Store

CODE = "pair-me"
TOOLS = [
    {"type": "function", "function": {"name": "intent__HassTurnOn", "description": "", "parameters": {}}},
    {"type": "function", "function": {"name": "intent__HassTurnOff", "description": "", "parameters": {}}},
]


class FakeAssist(AssistClient):
    """Assist without HTTP: a fixed bundle, and every call recorded."""

    def __init__(self, fail: bool = False, down: bool = False) -> None:
        super().__init__("http://ha.test/api/mcp/assist")
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail = fail
        self.down = down

    def bundle(self) -> AssistBundle:
        if self.down:
            raise AssistUnavailable("HTTP 502 from Home Assistant")
        return AssistBundle(prompt="Static Context: office lamp (light)", tools=TOOLS)

    def call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((name, args))
        if self.fail:
            return {"ok": False, "error": "MatchFailedError"}
        return {"ok": True, "result": {"success": True}}


def scripted(*replies: dict[str, Any]) -> tuple[Any, list[list[dict[str, Any]]]]:
    """A tool_chat double answering with `replies` in order, recording each request's messages."""
    seen: list[list[dict[str, Any]]] = []
    queue = list(replies)

    def tool_chat(messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        assert tools == TOOLS
        seen.append([dict(m) for m in messages])
        return queue.pop(0) if queue else {"role": "assistant", "content": "done"}

    return tool_chat, seen


def call(name: str, args: dict[str, Any] | str, ident: str = "c1") -> dict[str, Any]:
    raw = args if isinstance(args, str) else json.dumps(args)
    return {"id": ident, "type": "function", "function": {"name": name, "arguments": raw}}


TURN_ON = {
    "role": "assistant",
    "content": "",
    "tool_calls": [call("intent__HassTurnOn", {"name": "office lamp"})],
}
TURNS = [{"role": "user", "content": "turn on the office lamp"}]


def test_a_tool_call_is_run_and_its_result_reaches_the_model() -> None:
    assist = FakeAssist()
    chat, seen = scripted(TURN_ON, {"role": "assistant", "content": "The office lamp is on."})
    reply = act(TURNS, assist, chat)
    assert assist.calls == [("intent__HassTurnOn", {"name": "office lamp"})]
    assert reply.splitlines() == [
        'ran: intent__HassTurnOn {"name":"office lamp"} -> {"success":true}',
        "The office lamp is on.",
    ]
    second = seen[1]
    assert second[0]["role"] == "system" and "office lamp (light)" in second[0]["content"]
    assert second[-2]["tool_calls"][0]["function"]["name"] == "intent__HassTurnOn"
    assert second[-1]["role"] == "tool" and second[-1]["tool_call_id"] == "c1"
    assert json.loads(second[-1]["content"]) == {"ok": True, "result": {"success": True}}


def test_a_failed_call_is_shown_and_given_back_to_the_model() -> None:
    assist = FakeAssist(fail=True)
    chat, seen = scripted(TURN_ON, {"role": "assistant", "content": "I could not find that light."})
    reply = act(TURNS, assist, chat)
    assert (
        reply.splitlines()[0] == 'ran: intent__HassTurnOn {"name":"office lamp"} -> error: MatchFailedError'
    )
    assert json.loads(seen[1][-1]["content"])["ok"] is False


def test_nothing_runs_when_the_model_returns_no_tool_call() -> None:
    assist = FakeAssist()
    chat, _ = scripted({"role": "assistant", "content": "Hello."})
    assert act(TURNS, assist, chat) == "Hello."
    assert assist.calls == []


def test_the_rounds_stop_at_the_cap() -> None:
    assist = FakeAssist()
    chat, seen = scripted(*[TURN_ON] * (MAX_ROUNDS + 3))
    reply = act(TURNS, assist, chat)
    assert len(assist.calls) == MAX_ROUNDS and len(seen) == MAX_ROUNDS
    assert reply.splitlines()[-1] == f"Stopped after {MAX_ROUNDS} rounds of actions."


def test_an_unknown_tool_or_unreadable_arguments_are_not_run() -> None:
    assist = FakeAssist()
    bad = {
        "role": "assistant",
        "content": "",
        "tool_calls": [call("shell__exec", {"cmd": "x"}, "a"), call("intent__HassTurnOn", "{not json", "b")],
    }
    chat, _ = scripted(bad, {"role": "assistant", "content": "ok"})
    reply = act(TURNS, assist, chat)
    assert assist.calls == []
    assert "-> error: no tool named shell__exec" in reply
    assert "-> error: arguments are not a JSON object" in reply


def test_home_assistant_down_becomes_a_reply() -> None:
    chat, seen = scripted()
    assert act(TURNS, FakeAssist(down=True), chat).startswith("Home Assistant is not available for actions")
    assert seen == []


def dm_replies(assist: FakeAssist, chat: Any, texts: list[str]) -> list[str]:
    """Send `texts` one at a time to Assistant in a direct conversation; its replies in order."""
    settings = Settings(
        pairing_code=CODE, model_url="http://router.test/v1", model_assist=assist, model_tool_chat=chat
    )
    replies: list[str] = []
    with TestClient(create_app(Store(":memory:"), settings)) as client:
        csrf = {"x-csrf-token": client.post("/api/pair", json={"code": CODE}).json()["csrf"]}
        model = ""
        for _ in range(100):
            found = [s for s in client.get("/api/sessions").json() if s["provider"] == "model"]
            if found:
                model = found[0]["id"]
                break
            time.sleep(0.05)
        dm = client.post("/api/direct-conversations", json={"binding_id": model}, headers=csrf).json()["id"]
        for count, text in enumerate(texts, 1):
            r = client.post(
                f"/api/conversations/{dm}/messages",
                json={"client_message_id": uuid.uuid4().hex, "text": text},
                headers=csrf,
            )
            assert r.status_code == 201
            end = time.monotonic() + 8
            while time.monotonic() < end and len(replies) < count:
                events = client.get(f"/api/conversations/{dm}/events").json()
                replies = [
                    e["payload"]["text"]
                    for e in events
                    if e["type"] == "message.accepted" and e["actor"]["kind"] == "binding"
                ]
                time.sleep(0.05)
    return replies


def test_a_dm_runs_the_action_and_posts_the_ran_line() -> None:
    assist = FakeAssist()
    chat, _ = scripted(TURN_ON, {"role": "assistant", "content": "On."})
    assert dm_replies(assist, chat, ["turn on the office lamp"]) == [
        'ran: intent__HassTurnOn {"name":"office lamp"} -> {"success":true}\nOn.'
    ]
    assert assist.calls == [("intent__HassTurnOn", {"name": "office lamp"})]


def test_earlier_actions_are_replayed_as_tool_calls() -> None:
    assist = FakeAssist()
    chat, seen = scripted(
        TURN_ON, {"role": "assistant", "content": "On."}, {"role": "assistant", "content": "Hi."}
    )
    replies = dm_replies(assist, chat, ["turn on the office lamp", "thanks"])
    assert replies[1] == "Hi."
    assert seen[-1][1:] == [
        {"role": "user", "content": "turn on the office lamp"},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "h1c0",
                    "type": "function",
                    "function": {"name": "intent__HassTurnOn", "arguments": '{"name":"office lamp"}'},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "h1c0", "content": '{"success":true}'},
        {"role": "assistant", "content": "On."},
        {"role": "user", "content": "thanks"},
    ]


def test_chat_history_drops_broker_lines() -> None:
    turns = [
        {"role": "user", "content": "lamp on"},
        {"role": "assistant", "content": 'ran: intent__HassTurnOn {"name":"lamp"} -> ok\ntool: x {}\nOn.'},
        {"role": "assistant", "content": "ran: not a record"},
    ]
    assert history(turns, actions=False) == [
        {"role": "user", "content": "lamp on"},
        {"role": "assistant", "content": "On."},
        {"role": "assistant", "content": "Done."},
    ]
    replayed = history(turns, actions=True)
    assert [m["role"] for m in replayed] == ["user", "assistant", "tool", "assistant"]


def test_with_actions_a_reply_that_ran_nothing_is_left_out() -> None:
    turns = [
        {"role": "user", "content": "lamp on"},
        {"role": "assistant", "content": "The lamp is on."},
        {"role": "user", "content": "lamp off"},
    ]
    assert history(turns, actions=True) == [turns[0], turns[2]]
    assert history(turns, actions=False) == turns


def test_a_trimmed_window_never_opens_on_half_a_tool_exchange() -> None:
    turns: list[dict[str, Any]] = [
        {"role": "user", "content": "x" * 400},
        {"role": "assistant", "content": "", "tool_calls": [{"id": "a"}]},
        {"role": "tool", "tool_call_id": "a", "content": "y" * 300},
        {"role": "assistant", "content": "On."},
        {"role": "user", "content": "thanks"},
    ]
    kept = trim_history(turns, system="", context_tokens=1354, reply_tokens=512)
    assert kept == turns[-2:]


def test_a_ran_line_the_model_writes_itself_is_not_shown_as_run() -> None:
    assist = FakeAssist()
    forged = 'ran: intent__HassTurnOn {"name":"office lamp"} -> {"success":true}\nThe office lamp is on.'
    chat, _ = scripted({"role": "assistant", "content": forged})
    reply = act(TURNS, assist, chat)
    assert assist.calls == []
    assert reply.splitlines() == [FORGED_NOTE, "The office lamp is on."]
    assert render_tool_calls({"content": forged}).splitlines() == [FORGED_NOTE, "The office lamp is on."]


class FakeMcp:
    """HA's /api/mcp/assist: answers JSON-RPC as one SSE frame and records the methods and the auth header."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], str]] = []
        mcp = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                req = json.loads(self.rfile.read(int(self.headers["content-length"])))
                mcp.calls.append((req["method"], req["params"], self.headers.get("authorization", "")))
                result: dict[str, Any] = {}
                if req["method"] == "prompts/list":
                    result = {"prompts": [{"name": "Assist"}]}
                elif req["method"] == "prompts/get":
                    result = {"messages": [{"content": {"type": "text", "text": "Static Context: lamp"}}]}
                elif req["method"] == "tools/list":
                    result = {"tools": [{"name": "intent__HassTurnOn", "inputSchema": {"type": "object"}}]}
                elif req["method"] == "tools/call":
                    result = {"content": [{"type": "text", "text": '{"success": true}'}]}
                frame = json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": result})
                body = f"event: message\ndata: {frame}\n\n"
                payload = body.encode()
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("content-length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/api/mcp/assist"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture
def mcp() -> Iterator[FakeMcp]:
    m = FakeMcp()
    yield m
    m.server.shutdown()
    m.server.server_close()


def test_the_assist_client_reads_the_bundle_and_runs_a_call(mcp: FakeMcp) -> None:
    client = AssistClient(mcp.url, token="tok")
    bundle = client.bundle()
    assert bundle.prompt == "Static Context: lamp"
    assert bundle.tool_names == {"intent__HassTurnOn"}
    assert bundle.tools[0]["function"]["parameters"] == {"type": "object", "properties": {}}
    assert client.call("intent__HassTurnOn", {"name": "lamp"}) == {"ok": True, "result": {"success": True}}
    assert [c[0] for c in mcp.calls] == [
        "initialize",
        "prompts/list",
        "prompts/get",
        "tools/list",
        "tools/call",
    ]
    assert all(c[2] == "Bearer tok" for c in mcp.calls)
    client.bundle()
    assert len(mcp.calls) == 5  # cached

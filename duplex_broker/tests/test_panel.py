"""The owner's panel calls the broker by a table of routes (`ROUTES` in panel.html). Every route in it must
exist, every owner route the broker has must be reachable from it, and the calls the panel makes in the
order it makes them must work. Nothing here runs the page's script; a browser is needed for that."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from importlib import resources
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.store import Store

CODE = "pair-me"
ENROLL = "enroll-me"
FULL = ["auto", "queue", "steer", "restart"]
Headers = dict[str, str]
PANEL = resources.files("duplex_broker").joinpath("panel.html").read_text(encoding="utf-8")


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture
def app(store: Store) -> FastAPI:
    return create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL, owner_name="Pat"))


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


def _shape(path: str) -> str:
    """A route path with its parameter names removed and no leading slash, the form both sides compare in."""
    return re.sub(r"\{[^}]*\}", "{}", path.lstrip("/"))


def panel_routes() -> dict[str, tuple[str, str]]:
    block = PANEL.split("const ROUTES = {", 1)[1].split("\n};", 1)[0]
    found = re.findall(r'^\s+(\w+): \["([A-Z]+)", "([^"]+)"\],?$', block, re.M)
    assert found, "the ROUTES table was not found in panel.html"
    return {name: (method, _shape(path)) for name, method, path in found}


def app_routes(app: FastAPI, owner_only: bool = False) -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        by_owner = any(getattr(d.call, "__name__", "") == "user" for d in route.dependant.dependencies)
        if owner_only and not by_owner:
            continue
        found.update((method, _shape(route.path)) for method in route.methods or ())
    return found


def test_every_route_the_panel_calls_exists(app: FastAPI) -> None:
    missing = {name: route for name, route in panel_routes().items() if route not in app_routes(app)}
    assert not missing, f"the panel calls routes the broker does not have: {missing}"


def test_every_owner_route_is_reachable_from_the_panel(app: FastAPI) -> None:
    unreachable = app_routes(app, owner_only=True) - set(panel_routes().values())
    assert not unreachable, f"owner routes with no call in the panel: {sorted(unreachable)}"


def test_the_panel_only_calls_routes_it_names_and_names_only_routes_it_calls() -> None:
    table = set(panel_routes())
    called = set(re.findall(r'\bcall\("(\w+)"', PANEL))
    assert called <= table, f"calls with no ROUTES entry: {sorted(called - table)}"
    assert table <= called, f"ROUTES entries nothing calls: {sorted(table - called)}"


def test_every_element_the_script_looks_up_is_in_the_page() -> None:
    ids = set(re.findall(r'\bid="([^"]+)"', PANEL))
    looked_up = set(re.findall(r'\$\("([^"]+)"\)', PANEL))
    dynamic = {f"{base}-{suffix}" for base in ("msg", "del", "read") for suffix in ("wrap", "tip")}
    assert not (looked_up - ids - dynamic), f"no such element: {sorted(looked_up - ids - dynamic)}"


def test_the_panel_paths_are_relative_so_ingress_can_prefix_them() -> None:
    assert all(not path.startswith("/") for _, path in panel_routes().values())
    assert not any(quote + "/api" in PANEL for quote in "\"'`")


# The calls the panel makes, in its order


def adapter(client: TestClient, title: str, modes: list[str], tasks: bool = False) -> tuple[str, Headers]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "claude",
            "native_id": uuid.uuid4().hex,
            "title": title,
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {"receive_messages": "verified", "send_modes": modes, "tasks": tasks},
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def sign_in(client: TestClient) -> Headers:
    """Pairing, then api/session: the panel's CSRF token and the owner's name."""
    assert client.get("/api/status").status_code == 401
    pair = client.post("/api/pair", json={"code": CODE})
    assert pair.status_code == 200
    session = client.get("/api/session").json()
    assert session["csrf"] == pair.json()["csrf"] and session["owner_name"] == "Pat"
    return {"x-csrf-token": session["csrf"]}


def events(client: TestClient, conversation: str, after: int = 0) -> list[dict[str, Any]]:
    r = client.get(f"/api/conversations/{conversation}/events", params={"after": after, "limit": 1000})
    assert r.status_code == 200
    found: list[dict[str, Any]] = r.json()
    return found


def test_a_direct_chat_sends_in_each_mode_and_the_delivery_state_shows(client: TestClient) -> None:
    csrf = sign_in(client)
    session, auth = adapter(client, "claude one", FULL)
    listed = client.get("/api/sessions").json()
    assert [s["capabilities"]["send_modes"] for s in listed if s["id"] == session] == [FULL]

    direct = client.post("/api/direct-conversations", json={"binding_id": session}, headers=csrf).json()
    detail = client.get(f"/api/conversations/{direct['id']}").json()
    members = {m["kind"]: m for m in detail["members"]}
    assert members["binding"]["title"] == "claude one" and members["binding"]["active"] is True

    for mode in FULL:
        body = {"client_message_id": uuid.uuid4().hex, "text": f"in {mode}", "mode": mode}
        posted = client.post(f"/api/conversations/{direct['id']}/messages", json=body, headers=csrf)
        assert posted.status_code == 201, posted.text
        assert posted.json()["message"]["mode"] == mode
    # a lost reply is retried with the same id and does not post a second time
    again = client.post(f"/api/conversations/{direct['id']}/messages", json=body, headers=csrf)
    assert again.status_code == 200

    accepted = [e for e in events(client, direct["id"]) if e["type"] == "message.accepted"]
    assert [e["payload"]["text"] for e in accepted] == [f"in {mode}" for mode in FULL]
    assert accepted[0]["payload"]["from"]["kind"] == "user"
    assert [t["title"] for t in accepted[0]["payload"]["to"]] == ["claude one"]

    first = client.post("/api/adapter/claim", headers=auth).json()["delivery"]["id"]
    for state in ("accepted", "consumed"):
        client.post(f"/api/adapter/deliveries/{first}/state", json={"state": state}, headers=auth)
    changes = [
        (e["payload"]["delivery_id"], e["payload"]["binding_id"], e["payload"]["state"])
        for e in events(client, direct["id"])
        if e["type"] == "delivery.changed" and e["payload"]["delivery_id"] == first
    ]
    assert [state for _, _, state in changes] == ["queued", "dispatching", "accepted", "consumed"]
    assert {binding for _, binding, _ in changes} == {session}

    message = client.get(f"/api/messages/{accepted[0]['message_id']}").json()
    assert message["deliveries"][0]["state"] == "consumed" and message["deliveries"][0]["read"] is True
    assert message["message"]["sent_at"].endswith("Z")


def test_a_mode_a_session_does_not_take_is_refused_with_the_reason_the_panel_shows(
    client: TestClient,
) -> None:
    csrf = sign_in(client)
    session, _ = adapter(client, "plain", ["auto"])
    direct = client.post("/api/direct-conversations", json={"binding_id": session}, headers=csrf).json()
    refused = client.post(
        f"/api/conversations/{direct['id']}/messages",
        json={"client_message_id": uuid.uuid4().hex, "text": "x", "mode": "steer"},
        headers=csrf,
    )
    assert refused.status_code == 422 and "supports: auto" in refused.json()["detail"]


def test_a_group_is_created_run_and_removed_the_way_the_panel_does(client: TestClient) -> None:
    csrf = sign_in(client)
    first, _ = adapter(client, "first", FULL)
    second, _ = adapter(client, "second", FULL)
    group = client.post(
        "/api/conversations", json={"title": "project", "mode": "user_directed"}, headers=csrf
    )
    assert group.status_code == 201
    gid = group.json()["id"]
    for member in (first, second):
        add = client.post(f"/api/conversations/{gid}/members", json={"binding_id": member}, headers=csrf)
        assert add.status_code == 204
    detail = client.get(f"/api/conversations/{gid}").json()
    assert {m["title"] for m in detail["members"] if m["kind"] == "binding"} == {"first", "second"}

    # a group message needs a recipient; the panel asks for one before it posts
    none = client.post(
        f"/api/conversations/{gid}/messages",
        json={"client_message_id": uuid.uuid4().hex, "text": "x"},
        headers=csrf,
    )
    assert none.status_code == 422
    sent = client.post(
        f"/api/conversations/{gid}/messages",
        json={"client_message_id": uuid.uuid4().hex, "text": "to first", "recipients": [first]},
        headers=csrf,
    ).json()
    delivery = sent["deliveries"][0]["id"]

    redirected = client.post(
        f"/api/deliveries/{delivery}/redirect", json={"to": second, "expected_state": "queued"}, headers=csrf
    )
    assert redirected.status_code == 201 and redirected.json()["disposition"] == "moved"
    types = [e["type"] for e in events(client, gid)]
    assert "delivery.redirected" in types

    mode = client.post(f"/api/conversations/{gid}/mode", json={"mode": "consult"}, headers=csrf).json()
    assert mode["mode"] == "consult"
    started = client.post(
        f"/api/conversations/{gid}/collaboration",
        json={"objective": "agree a plan", "max_turns": 4, "minutes": 5},
        headers=csrf,
    ).json()
    collaboration = started["collaboration"]
    assert started["mode"] == "collaborate" and collaboration["active"] is True
    assert {"objective", "turns_used", "max_turns", "expires_at"} <= set(collaboration)
    stopped = client.delete(f"/api/conversations/{gid}/collaboration", headers=csrf).json()
    assert stopped["mode"] == "user_directed" and stopped["collaboration"]["active"] is False

    paused = client.post(f"/api/conversations/{gid}/control", json={"action": "pause"}, headers=csrf)
    assert paused.json()["paused"] is True
    resumed = client.post(f"/api/conversations/{gid}/control", json={"action": "resume"}, headers=csrf)
    assert resumed.json()["paused"] is False
    cancelled = client.post(f"/api/conversations/{gid}/control", json={"action": "stop"}, headers=csrf).json()
    assert {d["state"] for d in cancelled["cancelled"]} == {"cancelled"} and cancelled["cancelled"]

    assert client.post(f"/api/conversations/{gid}/archive", headers=csrf).status_code == 200
    removed = [c for c in client.get("/api/status").json()["conversations"] if c["id"] == gid]
    assert removed[0]["archived"] is True and removed[0]["kind"] == "group"
    assert gid not in [c["id"] for c in client.get("/api/conversations").json()]
    assert client.post(f"/api/conversations/{gid}/restore", headers=csrf).status_code == 200
    assert gid in [c["id"] for c in client.get("/api/conversations").json()]


def test_a_session_is_released_and_retired_from_the_status_page(client: TestClient) -> None:
    csrf = sign_in(client)
    session, _ = adapter(client, "gone soon", FULL)
    assert client.post(f"/api/sessions/{session}/release", headers=csrf).status_code == 200
    retired = client.post(f"/api/sessions/{session}/retire", headers=csrf).json()
    assert retired["active"] is False
    assert [s["active"] for s in client.get("/api/status").json()["sessions"]] == [False]


def test_the_task_list_detail_and_cancel_the_panel_offers(client: TestClient) -> None:
    csrf = sign_in(client)
    _, boss = adapter(client, "boss", FULL, tasks=True)
    adapter(client, "worker", FULL, tasks=True)
    made = client.post(
        "/api/adapter/tasks", json={"to": "worker", "task": "build it", "done_when": "built"}, headers=boss
    ).json()

    # the owner reads a task by the plain id its list gives, not the dashed one a session shows
    listed = client.get("/api/tasks", params={"open": 1}).json()
    task = listed[0]["id"]
    assert [t["id"] for t in listed] == [made["id"].replace("-", "")]
    assert {"requester", "assignee", "status", "task", "created_at", "ended_at", "questions"} <= set(
        listed[0]
    )
    detail = client.get(f"/api/tasks/{task}").json()
    assert detail["done_when"] == "built" and detail["events"] and detail["result"] is None

    cancelled = client.post(f"/api/tasks/{task}/cancel", json={}, headers=csrf).json()
    assert cancelled["ended_at"] is not None
    assert client.get("/api/tasks", params={"open": 0}).json()[0]["id"] == task


def test_the_system_lists_and_the_device_switch_answer_the_panel(client: TestClient) -> None:
    csrf = sign_in(client)
    enrolled = client.post("/api/device/enroll", json={"pairing_code": CODE, "device_name": "tablet"}).json()
    for route in ("/api/refusals", "/api/limits", "/api/waker/starts", "/api/subscriptions"):
        assert client.get(route).json() == [], route
    devices = client.get("/api/devices").json()
    assert [(d["id"], d["label"], d["disabled"]) for d in devices] == [(enrolled["id"], "tablet", False)]
    off = client.post(f"/api/devices/{enrolled['id']}/disable", headers=csrf).json()
    assert off["disabled"] is True
    on = client.post(f"/api/devices/{enrolled['id']}/enable", headers=csrf).json()
    assert on["disabled"] is False


def test_a_write_without_the_csrf_token_is_refused_so_the_panel_fetches_it_first(client: TestClient) -> None:
    sign_in(client)
    refused = client.post("/api/conversations", json={"title": "g"})
    assert refused.status_code == 403 and "CSRF" in refused.json()["detail"]

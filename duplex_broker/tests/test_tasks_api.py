from __future__ import annotations

import itertools
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.store import Store

SEQ = itertools.count(1)
CODE = "pair-me"
ENROLL = "enroll-me"
FULL = ["auto", "queue", "steer", "restart"]
Headers = dict[str, str]


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture
def client(store: Store) -> Iterator[TestClient]:
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL))
    with TestClient(app) as c:
        yield c


def register(
    client: TestClient,
    title: str,
    native: str | None = None,
    token: str = "",
    modes: list[str] | None = None,
    tasks: Any = True,
) -> tuple[str, Headers]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "claude",
            "native_id": native or uuid.uuid4().hex,
            "title": title,
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {
                "receive_messages": "verified",
                "send_modes": FULL if modes is None else modes,
                "tasks": tasks,
            },
            "current_token": token,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def owner(client: TestClient) -> Headers:
    return {"x-csrf-token": client.post("/api/pair", json={"code": CODE}).json()["csrf"]}


def create(
    client: TestClient, headers: Headers, to: str, task: str = "do it", **extra: Any
) -> dict[str, Any]:
    r = client.post(
        "/api/adapter/tasks", json={"to": to, "task": task, "done_when": "done", **extra}, headers=headers
    )
    assert r.status_code == 201, r.text
    body: dict[str, Any] = r.json()
    return body


def test_the_task_routes_end_to_end(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    worker, worker_h = register(client, "worker")
    _, stranger_h = register(client, "stranger")
    task = create(client, boss_h, "worker", "build the thing")
    assert task["status"] == "working" and task["assignee"] == {"id": worker, "title": "worker"}
    tid = task["id"]
    assert len(tid) == 36 and tid.count("-") == 4

    # The assignee claims the assigned notice, whose envelope names the task in the displayed form.
    claimed = client.post("/api/adapter/claim", headers=worker_h).json()
    notice = claimed["message"]["notice"]
    assert notice == {"kind": "task_assigned", "task_id": tid}
    assert claimed["conversation"]["title"] == "Task: build the thing"

    assert client.get(f"/api/adapter/tasks/{tid}", headers=stranger_h).status_code == 403
    for ident in (tid, tid.replace("-", "")):
        got = client.get(f"/api/adapter/tasks/{ident}", headers=worker_h)
        assert got.status_code == 200 and got.json()["id"] == tid and got.json()["event_count"] == 1
    assert client.get("/api/adapter/tasks/" + uuid.uuid4().hex, headers=worker_h).status_code == 404
    listed = client.get("/api/adapter/tasks?role=assignee", headers=worker_h).json()
    assert [t["id"] for t in listed] == [tid]
    assert client.get("/api/adapter/tasks?role=requester", headers=worker_h).json() == []
    assert client.get("/api/adapter/tasks?role=nobody", headers=worker_h).status_code == 422

    # A question and its answer.
    asked = client.post(f"/api/adapter/tasks/{tid}/questions", json={"question": "which?"}, headers=worker_h)
    assert asked.status_code == 201
    qid = asked.json()["id"]
    assert (
        client.post(
            f"/api/adapter/questions/{qid}/answer", json={"answer": "x"}, headers=worker_h
        ).status_code
        == 403
    )
    answered = client.post(f"/api/adapter/questions/{qid}/answer", json={"answer": "this"}, headers=boss_h)
    assert answered.status_code == 200 and answered.json()["state"] == "answered"
    again = client.post(f"/api/adapter/questions/{qid}/answer", json={"answer": "that"}, headers=boss_h)
    assert again.status_code == 409
    second = client.post(
        f"/api/adapter/tasks/{tid}/questions", json={"question": "and?"}, headers=worker_h
    ).json()
    withdrawn = client.post(f"/api/adapter/questions/{second['id']}/withdraw", headers=worker_h)
    assert withdrawn.status_code == 200 and withdrawn.json()["state"] == "withdrawn"
    assert client.post(f"/api/adapter/questions/{second['id']}/withdraw", headers=worker_h).status_code == 409

    # The report, the result's first read, and a second report.
    body = {"outcome": "done", "result": "built"}
    assert client.post(f"/api/adapter/tasks/{tid}/report", json=body, headers=boss_h).status_code == 403
    reported = client.post(f"/api/adapter/tasks/{tid}/report", json=body, headers=worker_h)
    assert reported.status_code == 200 and reported.json()["status"] == "result_available"
    assert client.post(f"/api/adapter/tasks/{tid}/report", json=body, headers=worker_h).status_code == 409
    undelivered = client.get("/api/adapter/tasks?role=requester", headers=boss_h).json()
    assert [(t["id"], t["delivered_at"]) for t in undelivered] == [(tid, None)]
    read = client.get(f"/api/adapter/tasks/{tid}", headers=boss_h).json()
    assert read["result"] == "built" and read["delivered_at"] is not None
    assert client.get("/api/adapter/tasks?role=requester", headers=boss_h).json() == []
    assert len(client.get("/api/adapter/tasks?role=requester&open=0", headers=boss_h).json()) == 1
    assert client.post(f"/api/adapter/tasks/{tid}/cancel", headers=boss_h).status_code == 409


def test_task_input_is_bounded_and_the_assignee_must_exist(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    _, worker_h = register(client, "worker")
    register(client, "plain", modes=["auto"])
    post = client.post
    assert (
        post(
            "/api/adapter/tasks", json={"to": "nobody", "task": "t", "done_when": "d"}, headers=boss_h
        ).status_code
        == 422
    )
    assert (
        post(
            "/api/adapter/tasks", json={"to": "worker", "task": "", "done_when": "d"}, headers=boss_h
        ).status_code
        == 422
    )
    assert (
        post(
            "/api/adapter/tasks", json={"to": "worker", "task": "t", "done_when": ""}, headers=boss_h
        ).status_code
        == 422
    )
    assert (
        post(
            "/api/adapter/tasks", json={"to": "worker", "task": "x" * 20001, "done_when": "d"}, headers=boss_h
        ).status_code
        == 422
    )
    assert post("/api/adapter/tasks", json={"to": "worker", "task": "t", "done_when": "d"}).status_code == 401
    body = {"task": "t", "done_when": "d", "mode": "steer"}
    refused = post("/api/adapter/tasks", json={**body, "to": "plain"}, headers=boss_h)
    assert refused.status_code == 422 and refused.json()["error"] == "mode_unsupported"
    assert refused.json()["detail"] == 'steer is not supported by "plain" (supports: auto)'
    assert post("/api/adapter/tasks", json={**body, "to": "worker"}, headers=boss_h).status_code == 201
    client.post("/api/adapter/turn", json={"state": "idle", "seq": next(SEQ)}, headers=worker_h)
    idle = post("/api/adapter/tasks", json={**body, "to": "worker"}, headers=boss_h)
    assert idle.status_code == 422 and "has no running turn" in idle.json()["detail"]
    assert client.get("/api/adapter/tasks?role=requester", headers=boss_h).json()[0]["status"] == "working"
    assert len(client.get("/api/adapter/tasks?role=requester", headers=boss_h).json()) == 1


def test_cancel_over_the_api_reaches_the_children_and_the_owner_cancels_too(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    _, mid_h = register(client, "mid")
    _, leaf_h = register(client, "leaf")
    top = create(client, boss_h, "mid", "top")
    child = create(client, mid_h, "leaf", "child", parent_task_id=top["id"])
    other = register(client, "someone")[1]
    assert client.post(f"/api/adapter/tasks/{top['id']}/cancel", headers=other).status_code == 403
    cancelled = client.post(f"/api/adapter/tasks/{top['id']}/cancel", json={"reason": "stop"}, headers=boss_h)
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    assert client.get(f"/api/adapter/tasks/{child['id']}", headers=leaf_h).json()["status"] == "cancelled"

    csrf = owner(client)
    third = create(client, boss_h, "leaf", "third")
    stopped = client.post(
        f"/api/tasks/{third['id'].replace('-', '')}/cancel", json={"reason": "owner"}, headers=csrf
    )
    assert stopped.status_code == 200 and stopped.json()["status"] == "cancelled"
    assert [e["type"] for e in stopped.json()["events"]] == ["created", "cancelled"]
    assert client.post(f"/api/tasks/{third['id']}/cancel", headers=csrf).status_code == 409


def test_the_owner_lists_and_reads_tasks_with_their_events(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    _, worker_h = register(client, "worker")
    task = create(client, boss_h, "worker", "owned")
    assert client.get("/api/tasks").status_code == 401
    csrf = owner(client)
    listed = client.get("/api/tasks", headers=csrf).json()
    assert [t["id"] for t in listed] == [task["id"].replace("-", "")]
    detail = client.get(f"/api/tasks/{listed[0]['id']}", headers=csrf).json()
    assert detail["status"] == "working" and [e["type"] for e in detail["events"]] == ["created"]
    client.post(
        f"/api/adapter/tasks/{task['id']}/report",
        json={"outcome": "failed", "result": "no"},
        headers=worker_h,
    )
    client.get(f"/api/adapter/tasks/{task['id']}", headers=boss_h)
    assert client.get("/api/tasks", headers=csrf).json() == []
    everything = client.get("/api/tasks?open=0", headers=csrf).json()
    assert [(t["status"], t["outcome"]) for t in everything] == [("failed", "failed")]
    assert client.get(f"/api/tasks/{uuid.uuid4().hex}", headers=csrf).status_code == 404


def test_the_turn_route_drives_the_nudge_and_the_observed_question(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    _, worker_h = register(client, "worker")
    task = create(client, boss_h, "worker", "wait for me")
    client.post(
        "/api/adapter/turn",
        json={"state": "waiting", "waiting_for": "approval", "seq": next(SEQ)},
        headers=worker_h,
    )
    seen = client.get(f"/api/adapter/tasks/{task['id']}", headers=boss_h).json()["questions"]
    assert [(q["kind"], q["state"]) for q in seen] == [("observed", "open")]
    assert "waiting for approval" in seen[0]["text"]
    client.post("/api/adapter/turn", json={"state": "idle", "seq": next(SEQ)}, headers=worker_h)
    assert client.get(f"/api/adapter/tasks/{task['id']}", headers=boss_h).json()["questions"] == []
    claimed = []
    while (r := client.post("/api/adapter/claim", headers=worker_h)).status_code == 200:
        claimed.append(r.json()["message"]["notice"]["kind"])
        client.post(
            f"/api/adapter/deliveries/{r.json()['delivery']['id']}/state",
            json={"state": "accepted"},
            headers=worker_h,
        )
        client.post(
            f"/api/adapter/deliveries/{r.json()['delivery']['id']}/state",
            json={"state": "consumed"},
            headers=worker_h,
        )
    assert claimed == ["task_assigned", "task_nudge"]


def test_re_registering_a_waiting_session_withdraws_its_observed_questions(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    native = uuid.uuid4().hex
    _, worker_h = register(client, "worker", native)
    task = create(client, boss_h, "worker", "t")
    client.post(
        "/api/adapter/turn",
        json={"state": "waiting", "waiting_for": "approval", "seq": next(SEQ)},
        headers=worker_h,
    )
    assert len(client.get(f"/api/adapter/tasks/{task['id']}", headers=boss_h).json()["questions"]) == 1
    register(client, "worker", native, worker_h["authorization"].removeprefix("Bearer "))
    assert client.get(f"/api/adapter/tasks/{task['id']}", headers=boss_h).json()["questions"] == []


@pytest.mark.parametrize("declared", [False, None, "yes", 1, "true"])
def test_a_session_whose_adapter_does_not_declare_tasks_takes_none(
    client: TestClient, store: Store, declared: Any
) -> None:
    boss, boss_h = register(client, "boss")
    plain, plain_h = register(client, "plain", tasks=declared)
    body = {"task": "t", "done_when": "d"}
    for sender_h, to in ((boss_h, "plain"), (plain_h, "boss")):
        refused = client.post("/api/adapter/tasks", json={**body, "to": to}, headers=sender_h)
        assert refused.status_code == 422 and refused.json()["error"] == "tasks_unsupported"
        assert "plain" in refused.json()["detail"] and plain in refused.json()["detail"]
    with store._read() as db:
        counts = [db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("tasks", "task_events")]
    assert counts == [0, 0] and boss
    # The same two sessions delegate once the adapter declares tasks.
    other, other_h = register(client, "other")
    assert client.post("/api/adapter/tasks", json={**body, "to": "other"}, headers=boss_h).status_code == 201
    assert client.post("/api/adapter/tasks", json={**body, "to": "boss"}, headers=other_h).status_code == 201
    assert other


def test_a_child_task_follows_the_same_rule(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    worker, worker_h = register(client, "worker")
    register(client, "plain", tasks=False)
    parent = create(client, boss_h, "worker")
    refused = client.post(
        "/api/adapter/tasks",
        json={"to": "plain", "task": "t", "done_when": "d", "parent_task_id": parent["id"]},
        headers=worker_h,
    )
    assert refused.status_code == 422 and refused.json()["error"] == "tasks_unsupported"
    assert worker


def test_a_task_result_read_once_is_returned_by_every_later_read(client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    _, worker_h = register(client, "worker")
    tid = create(client, boss_h, "worker", "find it")["id"]
    reported = client.post(
        f"/api/adapter/tasks/{tid}/report", json={"outcome": "done", "result": "found"}, headers=worker_h
    )
    assert reported.status_code == 200
    csrf = owner(client)
    before = client.get(f"/api/tasks/{tid}", headers=csrf).json()
    assert (before["result"], before["delivered_at"]) == ("found", None)  # the owner's read delivers nothing

    first = client.get(f"/api/adapter/tasks/{tid}", headers=boss_h).json()
    second = client.get(f"/api/adapter/tasks/{tid}", headers=boss_h).json()
    assert first["result"] == second["result"] == "found"
    assert first["delivered_at"] is not None and second["delivered_at"] == first["delivered_at"]
    for _ in range(2):
        owned = client.get(f"/api/tasks/{tid}", headers=csrf).json()
        assert (owned["result"], owned["delivered_at"]) == ("found", first["delivered_at"])
    assert [e["type"] for e in owned["events"]].count("delivered") == 1

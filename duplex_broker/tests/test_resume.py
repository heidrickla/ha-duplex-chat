from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker import resume, tasks
from duplex_broker.api import Settings, create_app
from duplex_broker.store import BROKER_BINDING_ID, Actor, Binding, Conflict, Forbidden, Invalid, Store

OWNER = Actor.user("owner")
HOUR = 3600.0
ENROLL = "enroll-me"
CODE = "pair-me"


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(clock: Clock) -> Iterator[Store]:
    s = Store(":memory:", now=clock)
    yield s
    s.close()


def bind(store: Store, title: str = "worker") -> Binding:
    binding, _ = store.register_binding("claude", uuid.uuid4().hex, title, "w", "e", {"tasks": True})
    return binding


def arm(
    store: Store,
    binding: Binding,
    run: str = "run-1",
    failed: float = 1_000_000.0,
    reset: float = 1_000_600.0,
) -> dict[str, Any]:
    row, created = resume.record_limit(
        store, binding, run, "claude-transcript", "limit reached", failed, reset
    )
    assert created
    return row


def sent_messages(store: Store) -> list[tuple[str, str, str, str]]:
    with store._read() as db:
        return [
            (r["client_message_id"], r["text"], r["mode"], r["actor_id"])
            for r in db.execute("SELECT * FROM messages WHERE client_message_id LIKE 'limit-resume:%'")
        ]


def test_a_limit_failure_arms_one_row_and_a_repeat_answers_it(store: Store) -> None:
    worker = bind(store)
    row = arm(store, worker, reset=1_000_600.7)
    assert row["id"] == f"{worker.id}:run-1:1000600"
    assert (row["state"], row["reason"], row["message_id"]) == ("armed", None, None)
    assert (row["binding_id"], row["run_id"], row["source"]) == (worker.id, "run-1", "claude-transcript")
    assert (row["failed_at"], row["reset_at"]) == (1_000_000.0, 1_000_600.7)
    again, created = resume.record_limit(
        store, worker, "run-1", "claude-transcript", "other words", 1_000_000.0, 1_000_600.2
    )
    assert not created and again == row
    other, created = resume.record_limit(
        store, worker, "run-2", "codex-rollout", "limit", 1_000_000.0, 1_000_600.0
    )
    assert created and other["id"] != row["id"]
    assert len(resume.list_resumes(store)) == 2


def test_only_the_matched_signal_is_kept_and_at_most_300_characters(store: Store) -> None:
    worker = bind(store)
    row, _ = resume.record_limit(store, worker, "r", "codex-rollout", "x" * 900, 10.0, 20.0)
    assert row["matched"] == "x" * 300
    for bad in (float("nan"), float("inf"), -1.0, 0.0):
        with pytest.raises(Invalid):
            resume.record_limit(store, worker, "bad", "codex-rollout", "m", 10.0, bad)
    with pytest.raises(Invalid):
        resume.record_limit(store, worker, "bad", "somewhere", "m", 10.0, 20.0)


def test_a_reset_already_past_at_the_failure_never_arms_and_never_sends(store: Store, clock: Clock) -> None:
    worker = bind(store)
    for run, reset in (("same", 1_000_000.0), ("before", 999_000.0)):
        row, _ = resume.record_limit(store, worker, run, "claude-transcript", "m", 1_000_000.0, reset)
        assert row["state"] == "not_armed" and row["reason"] == "reset already past at the failure"
    clock.t += 30 * HOUR
    assert resume.sweep(store) == 0
    clock.t = 1_000_000.0
    assert sent_messages(store) == []
    assert {r["state"] for r in resume.list_resumes(store)} == {"not_armed"}


def test_the_sweep_waits_for_the_reset_then_posts_continue_once(store: Store, clock: Clock) -> None:
    worker = bind(store)
    row = arm(store, worker)
    assert resume.sweep(store) == 0
    clock.t = 1_000_599.0
    assert resume.sweep(store) == 0 and sent_messages(store) == []
    clock.t = 1_000_600.0
    assert resume.sweep(store) == 1
    ((client_id, text, mode, actor),) = sent_messages(store)
    assert (client_id, text, mode, actor) == (
        f"limit-resume:{row['id']}",
        "Continue where you left off.",
        "auto",
        BROKER_BINDING_ID,
    )
    sent = resume.list_resumes(store)[0]
    assert sent["state"] == "sent" and sent["message_id"] is not None
    # It lands in the owner's direct conversation with that session and is delivered to it.
    conversation = store.open_direct("owner", worker.id)
    events = store.events_after(OWNER, conversation.id)
    accepted = next(e for e in events if e.type == "message.accepted")
    assert accepted.message_id == sent["message_id"] and accepted.payload["recipients"] == [worker.id]
    assert accepted.payload["notice"] == {"kind": "limit_resume", "resume_id": row["id"]}
    claimed = store.claim_next(worker.id, worker.lease_generation)
    assert claimed is not None and claimed[1].text == "Continue where you left off."
    # Another sweep, now or later, posts nothing more for the same id.
    assert resume.sweep(store) == 0
    clock.t += 2 * HOUR
    assert resume.sweep(store) == 0
    assert len(sent_messages(store)) == 1


def test_a_second_sweep_over_the_same_resume_posts_nothing(store: Store, clock: Clock) -> None:
    worker = bind(store)
    arm(store, worker)
    clock.t = 1_000_700.0
    assert [resume.sweep(store), resume.sweep(store)] == [1, 0]
    assert len(sent_messages(store)) == 1


def test_a_reset_more_than_a_day_old_is_dropped_not_sent(store: Store, clock: Clock) -> None:
    worker = bind(store)
    arm(store, worker, "late", reset=1_000_600.0)
    arm(store, worker, "edge", reset=1_000_700.0)
    clock.t = 1_000_700.0 + 24 * HOUR
    assert resume.sweep(store) == 2
    states = {r["run_id"]: (r["state"], r["reason"]) for r in resume.list_resumes(store)}
    assert states["late"] == ("dropped", "more than 24 h past the reset")
    assert states["edge"][0] == "sent"  # exactly 24 h overdue is still inside the window
    assert len(sent_messages(store)) == 1


def test_a_retired_session_is_dropped_with_its_reason(store: Store, clock: Clock) -> None:
    worker = bind(store)
    arm(store, worker)
    store.retire_binding(OWNER, worker.id)
    clock.t = 1_000_700.0
    assert resume.sweep(store) == 1
    row = resume.list_resumes(store)[0]
    assert (row["state"], row["reason"]) == ("dropped", "the session was retired")
    assert sent_messages(store) == []


def test_the_adapter_skips_an_armed_resume_and_it_is_never_sent(store: Store, clock: Clock) -> None:
    worker, other = bind(store), bind(store, "other")
    row = arm(store, worker)
    with pytest.raises(Forbidden):
        resume.skip(store, other, row["id"], "answered")
    skipped = resume.skip(store, worker, row["id"], "answered")
    assert (skipped["state"], skipped["reason"]) == ("skipped", "answered")
    assert resume.skip(store, worker, row["id"], "answered")["state"] == "skipped"
    clock.t = 1_000_700.0
    assert resume.sweep(store) == 0 and sent_messages(store) == []
    sent = arm(store, worker, "run-2")
    clock.t = 1_001_300.0
    resume.sweep(store)
    with pytest.raises(Conflict):
        resume.skip(store, worker, sent["id"], "answered")


# Delegated work that was cancelled


def open_tasks_at_failure(store: Store, worker: Binding, count: int) -> list[dict[str, Any]]:
    boss = bind(store, "boss")
    return [tasks.create_task(store, boss, worker.id, f"job {i}", "done", None, "auto") for i in range(count)]


def test_a_resume_is_skipped_when_every_task_open_at_the_failure_was_cancelled(
    store: Store, clock: Clock
) -> None:
    worker = bind(store)
    first, second = open_tasks_at_failure(store, worker, 2)
    clock.t += 10
    arm(store, worker, failed=clock.t, reset=clock.t + 600)
    boss = Actor.binding(first["requester"]["id"])
    clock.t += 5
    tasks.cancel(store, boss, first["id"], None)
    tasks.cancel(store, boss, second["id"], None)
    clock.t += 700
    assert resume.sweep(store) == 1
    row = resume.list_resumes(store)[0]
    assert (row["state"], row["reason"]) == ("skipped", "task cancelled")
    assert sent_messages(store) == []


def test_a_resume_is_sent_when_any_open_task_was_not_cancelled_or_there_were_none(
    store: Store, clock: Clock
) -> None:
    worker, idle = bind(store), bind(store, "idle")
    first, second = open_tasks_at_failure(store, worker, 2)
    clock.t += 10
    arm(store, worker, failed=clock.t, reset=clock.t + 600)
    arm(store, idle, failed=clock.t, reset=clock.t + 600)
    clock.t += 5
    tasks.cancel(store, Actor.binding(first["requester"]["id"]), first["id"], None)
    tasks.report(store, worker, second["id"], "done", "finished anyway")
    clock.t += 700
    assert resume.sweep(store) == 2
    assert {r["state"] for r in resume.list_resumes(store)} == {"sent"}


def test_tasks_cancelled_before_the_failure_or_created_after_it_do_not_count(
    store: Store, clock: Clock
) -> None:
    worker = bind(store)
    (before,) = open_tasks_at_failure(store, worker, 1)
    tasks.cancel(store, Actor.user("owner"), before["id"], None)
    clock.t += 10
    arm(store, worker, failed=clock.t, reset=clock.t + 600)
    clock.t += 10
    (after,) = open_tasks_at_failure(store, worker, 1)
    tasks.cancel(store, Actor.user("owner"), after["id"], None)
    clock.t += 700
    assert resume.sweep(store) == 1
    assert resume.list_resumes(store)[0]["state"] == "sent"


# The API and the broker's sweep


@pytest.fixture
def client(store: Store) -> Iterator[TestClient]:
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL, collaboration_sweep=3600))
    with TestClient(app) as c:
        yield c


def register(client: TestClient) -> tuple[str, dict[str, str]]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "claude",
            "native_id": uuid.uuid4().hex,
            "title": "worker",
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {},
        },
    )
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def test_the_limit_routes_arm_skip_and_list(client: TestClient) -> None:
    binding, headers = register(client)
    body = {
        "run_id": "uuid-1",
        "source": "claude-transcript",
        "matched": "You've hit your limit - resets 8pm (America/Chicago)",
        "failed_at": 1_700_000_000.5,
        "reset_at": 1_700_003_600.0,
    }
    first = client.post("/api/adapter/limits", json=body, headers=headers)
    assert first.status_code == 201 and first.json()["state"] == "armed"
    again = client.post("/api/adapter/limits", json=body, headers=headers)
    assert again.status_code == 200 and again.json() == first.json()
    resume_id = first.json()["id"]
    assert resume_id == f"{binding}:uuid-1:1700003600"
    bad = {**body, "run_id": "a/b"}
    assert client.post("/api/adapter/limits", json=bad, headers=headers).status_code == 422
    assert (
        client.post("/api/adapter/limits", json={**body, "source": "x"}, headers=headers).status_code == 422
    )
    assert client.post("/api/adapter/limits", json=body).status_code == 401

    assert client.get("/api/limits").status_code == 401
    csrf = {"x-csrf-token": client.post("/api/pair", json={"code": CODE}).json()["csrf"]}
    assert [r["id"] for r in client.get("/api/limits", headers=csrf).json()] == [resume_id]

    skipped = client.post(
        f"/api/adapter/limits/{resume_id}/skip", json={"reason": "answered"}, headers=headers
    )
    assert skipped.status_code == 200 and skipped.json()["state"] == "skipped"
    assert client.post(f"/api/adapter/limits/{resume_id}/skip", json={}, headers=headers).status_code == 200
    missing = client.post("/api/adapter/limits/nope/skip", json={}, headers=headers)
    assert missing.status_code == 404
    other = register(client)[1]
    assert client.post(f"/api/adapter/limits/{resume_id}/skip", json={}, headers=other).status_code == 403


def test_the_brokers_own_sweep_posts_the_resume(clock: Clock) -> None:
    store = Store(":memory:", now=clock)
    posted = threading.Event()

    def watch(events: Any) -> None:
        if any(
            e.type == "message.accepted"
            and "notice" in e.payload
            and e.payload["notice"].get("kind") == "limit_resume"
            for e in events
        ):
            posted.set()

    store.subscribe(watch)
    app = create_app(store, Settings(enrollment_token=ENROLL, collaboration_sweep=0.02))
    with TestClient(app) as client:
        binding, headers = register(client)
        body = {
            "run_id": "r1",
            "source": "codex-rollout",
            "matched": "usage_limit_exceeded",
            "failed_at": clock.t - 100,
            "reset_at": clock.t + 10,
        }
        assert client.post("/api/adapter/limits", json=body, headers=headers).status_code == 201
        clock.t += 20
        assert posted.wait(timeout=10)
        rows = resume.list_resumes(store)
        assert [(r["state"], r["binding_id"]) for r in rows] == [("sent", binding)]
        text = json.dumps(store.events_after(OWNER, store.open_direct("owner", binding).id)[0].payload)
        assert "Continue where you left off." in text
    store.close()


def test_a_limit_resume_is_a_scheduled_send_with_its_kind_fields_in_details(store: Store) -> None:
    worker = bind(store)
    row = arm(store, worker, reset=1_000_600.5)
    with store._read() as db:
        stored = db.execute("SELECT * FROM scheduled_sends").fetchall()
    assert len(stored) == 1
    send = stored[0]
    assert (send["id"], send["kind"], send["binding_id"], send["state"]) == (
        row["id"],
        "limit_resume",
        worker.id,
        "armed",
    )
    assert (send["text"], send["mode"], send["due_at"]) == (
        "Continue where you left off.",
        "auto",
        1_000_600.5,
    )
    assert json.loads(send["details"]) == {
        "run_id": "run-1",
        "source": "claude-transcript",
        "matched": "limit reached",
        "failed_at": 1_000_000.0,
    }

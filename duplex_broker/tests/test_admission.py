from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import duplex_broker
from duplex_broker import resume, tasks
from duplex_broker.store import (
    Actor,
    Binding,
    Invalid,
    ModeUnsupported,
    Store,
    TasksUnsupported,
)

OWNER = Actor.user("owner")
FULL = ["auto", "queue", "steer", "restart"]
NO_STEER = ["auto", "queue", "restart"]


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


class Session:
    """A registered session that can register again with other capabilities, as its adapter would."""

    def __init__(self, store: Store, title: str, tasks_: bool = True, modes: list[str] | None = None) -> None:
        self.store = store
        self.native = uuid.uuid4().hex
        self.title = title
        self.binding, self.token = self._register(tasks_, modes, None)

    def _register(self, tasks_: bool, modes: list[str] | None, current: str | None) -> tuple[Binding, str]:
        caps: dict[str, Any] = {"receive_messages": "verified", "send_modes": modes or FULL}
        if tasks_:
            caps["tasks"] = True
        return self.store.register_binding(
            "claude", self.native, self.title, "w", "e", caps, current_token=current
        )

    def register_again(self, tasks_: bool = True, modes: list[str] | None = None) -> Binding:
        self.binding, self.token = self._register(tasks_, modes, self.token)
        return self.binding

    @property
    def id(self) -> str:
        return self.binding.id


def delegate(store: Store, boss: Session, worker: Session) -> str:
    made = tasks.create_task(store, boss.binding, worker.id, "build it", "it builds", None, "auto")
    return str(made["id"]).replace("-", "")


def events(store: Store, task_id: str, type_: str) -> list[dict[str, Any]]:
    with store._read() as db:
        rows = db.execute(
            "SELECT payload FROM task_events WHERE task_id = ? AND type = ? ORDER BY seq", (task_id, type_)
        ).fetchall()
    return [json.loads(r["payload"]) for r in rows]


def task_status(store: Store, task_id: str) -> str:
    with store._read() as db:
        return str(db.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()[0])


def notices_to(store: Store, binding_id: str, kind: str) -> list[tuple[str, str]]:
    """Each broker notice of one kind addressed to a session, with its delivery's state."""
    with store._read() as db:
        rows = db.execute(
            "SELECT d.state, d.reason, e.payload FROM deliveries d"
            " JOIN events e ON e.message_id = d.message_id AND e.type = 'message.accepted'"
            " WHERE d.binding_id = ? ORDER BY d.seq",
            (binding_id,),
        ).fetchall()
    return [
        (r["state"], r["reason"] or "")
        for r in rows
        if json.loads(r["payload"]).get("notice", {}).get("kind") == kind
    ]


# One rule for every message the broker creates a delivery for.


def test_a_user_post_to_two_sessions_is_refused_whole_when_one_cannot_take_it(store: Store) -> None:
    a = Session(store, "a")
    b = Session(store, "b", modes=NO_STEER)
    group = store.create_group("owner", "g")
    for s in (a, b):
        store.add_member(OWNER, group.id, s.id)
    store.set_turn_state(a.id, "busy", None)
    store.set_turn_state(b.id, "busy", None)
    with pytest.raises(ModeUnsupported, match='"b"'):
        store.post_message(OWNER, group.id, uuid.uuid4().hex, "x", [a.id, b.id], mode="steer")
    with store._read() as db:
        assert db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0


def test_an_auto_message_is_admitted_to_any_session_and_a_bad_mode_name_is_invalid(store: Store) -> None:
    only_queue = Session(store, "q", modes=["auto"])
    conversation = store.open_direct("owner", only_queue.id)
    assert store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "hi").created
    with pytest.raises(Invalid, match="mode must be one of"):
        store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "hi", mode="shout")


# A task notice goes only to a session that has the task tools, now and not only when the task was made.

AFTER_STATUS = {
    "assigned": "failed",
    "result": "result_available",
    "question": "working",
    "answer": "working",
    "cancelled": "cancelled",
    "nudge": "working",
}
AFTER_LOSS = ["assigned", "result", "question", "answer", "cancelled", "nudge"]


def take_assignment(store: Store, worker: Session) -> None:
    """The worker's adapter claims and reads the assignment, as it would before the session works."""
    claimed = store.claim_next(worker.id, worker.binding.lease_generation)
    assert claimed is not None
    for step in ("accepted", "consumed"):
        store.transition(claimed[0].id, worker.id, worker.binding.lease_generation, step)


def make_refusal(store: Store, kind: str) -> tuple[str, Session]:
    """Drive a task to the point where the notice of this kind is produced for a session that, by then,
    declares no tasks. Returns the task id and that session."""
    boss = Session(store, "boss")
    worker = Session(store, "worker")
    task_id = delegate(store, boss, worker)
    if kind == "assigned":
        # Queued under the old declaration; the session registers again without tasks.
        worker.register_again(tasks_=False)
        return task_id, worker
    if kind == "result":
        boss.register_again(tasks_=False)
        tasks.report(store, worker.binding, task_id, "done", "ok")
        return task_id, boss
    if kind == "question":
        boss.register_again(tasks_=False)
        tasks.ask(store, worker.binding, task_id, "which one?")
        return task_id, boss
    if kind == "answer":
        asked = tasks.ask(store, worker.binding, task_id, "which one?")
        take_assignment(store, worker)
        worker.register_again(tasks_=False)
        tasks.answer(store, boss.binding, str(asked["id"]).replace("-", ""), "this one")
        return task_id, worker
    if kind == "cancelled":
        take_assignment(store, worker)
        worker.register_again(tasks_=False)
        tasks.cancel(store, Actor.binding(boss.id), task_id, "no longer needed")
        return task_id, worker
    assert kind == "nudge"
    take_assignment(store, worker)
    worker.register_again(tasks_=False)
    tasks.report_turn(store, worker.binding, "idle", None, 1)
    return task_id, worker


@pytest.mark.parametrize("kind", AFTER_LOSS)
def test_a_task_notice_to_a_session_that_no_longer_declares_tasks_is_refused_and_recorded(
    store: Store, kind: str
) -> None:
    task_id, target = make_refusal(store, kind)
    refused = [e for e in events(store, task_id, "notice_refused") if e["kind"] == f"task_{kind}"]
    assert len(refused) == 1 and refused[0]["binding_id"] == target.id
    assert "no task tools" in refused[0]["reason"]
    # No live delivery of that notice to it: either never made, or ended failed with the reason.
    states = [state for state, _ in notices_to(store, target.id, f"task_{kind}") if state != "failed"]
    assert states == []
    # Only a refused assignment is a lifecycle transition; the rest are recorded and the task goes on.
    assert task_status(store, task_id) == AFTER_STATUS[kind]


def test_registering_again_without_tasks_fails_the_queued_assignment_with_its_reason(store: Store) -> None:
    boss = Session(store, "boss")
    worker = Session(store, "worker")
    task_id = delegate(store, boss, worker)
    assert [s for s, _ in notices_to(store, worker.id, "task_assigned")] == ["queued"]
    worker.register_again(tasks_=False)
    ((state, reason),) = notices_to(store, worker.id, "task_assigned")
    assert state == "failed" and "no task tools" in reason
    assert [e["kind"] for e in events(store, task_id, "notice_refused")] == ["task_assigned"]


def test_a_refused_assignment_fails_the_task_and_the_requester_is_told_through_the_result_path(
    store: Store,
) -> None:
    boss = Session(store, "boss")
    worker = Session(store, "worker")
    task_id = delegate(store, boss, worker)
    worker.register_again(tasks_=False)
    assert task_status(store, task_id) == "failed"
    view = tasks.owner_task(store, task_id)
    assert view["outcome"] == "failed"
    assert (
        view["result"].startswith("The assignment could not be delivered:")
        and "no task tools" in view["result"]
    )
    assert [s for s, _ in notices_to(store, boss.id, "task_result")] == ["queued"]
    # Restoring the capability does not bring a dead assignment back, and nothing is left queued for it.
    worker.register_again(tasks_=True)
    assert task_status(store, task_id) == "failed"
    assert [s for s, _ in notices_to(store, worker.id, "task_assigned")] == ["failed"]


def test_a_refused_assignment_to_a_child_releases_the_parents_held_result(store: Store) -> None:
    boss = Session(store, "boss")
    mid = Session(store, "mid")
    leaf = Session(store, "leaf")
    parent = delegate(store, boss, mid)
    take_assignment(store, mid)
    child = tasks.create_task(store, mid.binding, leaf.id, "sub", "done", parent, "auto")
    tasks.report(store, mid.binding, parent, "done", "waiting on the child")
    assert task_status(store, parent) == "waiting_for_children"
    leaf.register_again(tasks_=False)
    assert task_status(store, str(child["id"]).replace("-", "")) == "failed"
    assert task_status(store, parent) == "result_available"


def test_registering_again_with_tasks_keeps_the_queued_assignment(store: Store) -> None:
    boss = Session(store, "boss")
    worker = Session(store, "worker")
    delegate(store, boss, worker)
    worker.register_again(tasks_=True)
    assert [s for s, _ in notices_to(store, worker.id, "task_assigned")] == ["queued"]


def test_a_requester_without_tasks_cannot_delegate_and_creates_nothing(store: Store) -> None:
    plain = Session(store, "plain", tasks_=False)
    worker = Session(store, "worker")
    with pytest.raises(TasksUnsupported, match='"plain"'):
        tasks.create_task(store, plain.binding, worker.id, "t", "d", None, "auto")
    with store._read() as db:
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0


# A queued message whose mode the session no longer declares.


def test_registering_again_without_a_mode_fails_the_queued_messages_that_need_it(store: Store) -> None:
    worker = Session(store, "worker")
    conversation = store.open_direct("owner", worker.id)
    store.set_turn_state(worker.id, "busy", None)
    posted = store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "now", mode="steer")
    kept = store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "later", mode="queue")
    worker.register_again(modes=NO_STEER)
    with store._read() as db:
        states = {
            r["message_id"]: (r["state"], r["reason"])
            for r in db.execute("SELECT message_id, state, reason FROM deliveries")
        }
    assert (
        states[posted.message.id][0] == "failed" and "steer is not supported" in states[posted.message.id][1]
    )
    assert states[kept.message.id][0] == "queued"
    # The owner who sent it is told, like any failed delivery.
    with store._read() as db:
        told = [
            json.loads(r["payload"]).get("notice", {})
            for r in db.execute("SELECT payload FROM events WHERE type = 'message.accepted'")
        ]
    assert [n["reason"] for n in told if n.get("kind") == "delivery_failed"] == [states[posted.message.id][1]]


# A scheduled send ends `refused` with the reason, at the sweep or at a later registration.


def schedule(store: Store, clock: Clock, session: Session, mode: str) -> str:
    row, _ = resume.record_limit(
        store, session.binding, "run-1", "claude-transcript", "limit", clock.t, clock.t + 60
    )
    with store._write() as db:
        db.execute("UPDATE scheduled_sends SET mode = ? WHERE id = ?", (mode, row["id"]))
    clock.t += 61
    return str(row["id"])


def state_of(store: Store, row_id: str) -> tuple[str, str | None]:
    with store._read() as db:
        row = db.execute("SELECT state, reason FROM scheduled_sends WHERE id = ?", (row_id,)).fetchone()
    return row["state"], row["reason"]


def test_a_due_scheduled_send_its_target_cannot_take_ends_refused_with_the_reason(
    store: Store, clock: Clock
) -> None:
    worker = Session(store, "worker", modes=NO_STEER)
    store.set_turn_state(worker.id, "busy", None)
    row_id = schedule(store, clock, worker, "steer")
    assert resume.sweep(store) == 1
    state, reason = state_of(store, row_id)
    assert state == "refused" and reason is not None and "steer is not supported" in reason
    assert resume.sweep(store) == 0


def test_a_sent_scheduled_send_ends_refused_when_the_session_registers_without_its_mode(
    store: Store, clock: Clock
) -> None:
    worker = Session(store, "worker")
    store.set_turn_state(worker.id, "busy", None)
    row_id = schedule(store, clock, worker, "steer")
    assert resume.sweep(store) == 1 and state_of(store, row_id)[0] == "sent"
    worker.register_again(modes=NO_STEER)
    state, reason = state_of(store, row_id)
    assert state == "refused" and reason is not None and "steer is not supported" in reason


# The class: every producer reaches admission, because every delivery is created in one place.


def test_every_delivery_is_created_by_insert_message() -> None:
    """A second INSERT INTO deliveries would be a second way past admission."""
    sites = [
        (path.name, line.strip())
        for path in Path(duplex_broker.__file__).parent.glob("*.py")
        for line in path.read_text(encoding="utf-8").splitlines()
        if "INSERT INTO deliveries" in line
    ]
    assert [name for name, _ in sites] == ["store.py"]

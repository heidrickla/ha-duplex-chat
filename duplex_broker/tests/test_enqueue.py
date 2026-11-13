from __future__ import annotations

import ast
import re
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

import duplex_broker
from duplex_broker import resume, tasks, waker
from duplex_broker.store import TRANSITIONS, Actor, Binding, Store

SRC = Path(duplex_broker.__file__).parent
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


def register(
    store: Store, native: str, modes: list[str], tasks_: bool, token: str | None
) -> tuple[Binding, str]:
    caps: dict[str, object] = {"receive_messages": "verified", "send_modes": modes}
    if tasks_:
        caps["tasks"] = True
    return store.register_binding("claude", native, "worker", "w", "e", caps, current_token=token)


def state_of(store: Store, delivery_id: str) -> tuple[str, str | None]:
    with store._read() as db:
        row = db.execute("SELECT state, reason FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
    return row["state"], row["reason"]


def sent_steer_resume(store: Store, clock: Clock, binding: Binding) -> str:
    """A due scheduled send in steer mode, handed to the session's adapter. Returns its delivery id."""
    row, _ = resume.record_limit(store, binding, "run-1", "claude-transcript", "limit", clock.t, clock.t + 60)
    with store._write() as db:
        db.execute("UPDATE scheduled_sends SET mode = 'steer' WHERE id = ?", (row["id"],))
    store.set_turn_state(binding.id, "busy", None)
    clock.t += 61
    assert resume.sweep(store) == 1
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert claimed is not None
    return claimed[0].id


def test_a_returned_delivery_is_admitted_against_what_the_session_declares_now(
    store: Store, clock: Clock
) -> None:
    native = uuid.uuid4().hex
    old, token = register(store, native, FULL, True, None)
    delivery = sent_steer_resume(store, clock, old)
    assert (
        waker.record_start(store, delivery, f"{delivery}:1:1", True, "claude-resume", "started")[0]["ok"]
        is True
    )
    # The desktop's adapter registers again, and declares no steer: the in-flight delivery is uncertain.
    new, _ = register(store, native, NO_STEER, True, token)
    assert state_of(store, delivery)[0] == "uncertain"
    waker.record_start(
        store,
        delivery,
        f"{delivery}:1:2",
        False,
        "claude-resume",
        "the session was opened while the waker ran it",
    )
    state, reason = state_of(store, delivery)
    assert state == "failed" and reason is not None and "steer is not supported" in reason
    assert store.claim_next(new.id, new.lease_generation) is None
    with store._read() as db:
        row = db.execute("SELECT state, reason FROM scheduled_sends").fetchone()
    assert row["state"] == "refused" and "steer is not supported" in row["reason"]


def test_a_returned_task_assignment_for_a_session_that_lost_tasks_fails_the_task(store: Store) -> None:
    boss, _ = register(store, "n-boss", FULL, True, None)
    native = uuid.uuid4().hex
    worker, token = register(store, native, FULL, True, None)
    other = store.register_binding("claude", "n-boss", "boss", "w", "e", {"tasks": True}, current_token=None)
    assert other is not None
    made = tasks.create_task(store, boss, worker.id, "t", "d", None, "auto")
    claimed = store.claim_next(worker.id, worker.lease_generation)
    assert claimed is not None
    again, _ = register(store, native, FULL, False, token)
    assert state_of(store, claimed[0].id)[0] == "uncertain"
    with store._write() as db:
        assert store.requeue_unanswered(db, claimed[0].id, "the run ended") is False
    assert state_of(store, claimed[0].id)[0] == "failed"
    assert store.claim_next(again.id, again.lease_generation) is None
    assert tasks.owner_task(store, str(made["id"]).replace("-", ""))["status"] == "failed"


# Every way a delivery enters the queue goes through the one function that admits it.


def test_no_state_change_can_put_a_delivery_in_the_queue_except_through_enqueue(store: Store) -> None:
    binding, _ = register(store, "n1", FULL, True, None)
    conversation = store.open_direct("owner", binding.id)
    store.post_message(Actor.user("owner"), conversation.id, uuid.uuid4().hex, "hello")
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert claimed is not None
    with store._write() as db, pytest.raises(AssertionError, match="only through _enqueue"):
        store._set_state(db, claimed[0].id, "queued", None, Actor.binding(binding.id), reason="again")
    assert state_of(store, claimed[0].id)[0] == "dispatching"
    # Nor can an adapter's own transition lead there.
    assert all("queued" not in targets for targets in TRANSITIONS.values())


def writes_to_deliveries() -> list[tuple[str, str, str]]:
    """Every place the source writes the deliveries table or hands the queued state to `_set_state`, as
    (file, enclosing function, what)."""
    found = []
    for path in sorted(SRC.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for node in ast.walk(fn):
                text = node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else ""
                if re.search(r"\bINSERT INTO\s+deliveries\b", text):
                    found.append((path.name, fn.name, "insert"))
                if re.search(r"\bUPDATE\s+deliveries\s+SET[^\"]*\bstate\s*=", text):
                    found.append((path.name, fn.name, "update-state"))
                if isinstance(node, ast.Call) and any(
                    isinstance(a, ast.Constant) and a.value == "queued"
                    for a in [*node.args, *[k.value for k in node.keywords]]
                ):
                    found.append((path.name, fn.name, "queued-argument"))
    return sorted(set(found))


def test_the_source_writes_deliveries_in_three_places_and_the_queue_in_one() -> None:
    assert writes_to_deliveries() == [
        ("store.py", "_enqueue", "insert"),
        ("store.py", "_enqueue", "queued-argument"),
        ("store.py", "_set_state", "update-state"),
    ]

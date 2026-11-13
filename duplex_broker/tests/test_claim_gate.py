from __future__ import annotations

import itertools
import uuid
from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker import tasks
from duplex_broker.api import Settings, create_app
from duplex_broker.store import Actor, Binding, Store

SEQ = itertools.count(1)
OWNER = Actor.user("owner")
CODE = "pair-me"
ENROLL = "enroll-me"
WITH_STEER = ["auto", "queue", "steer", "restart"]
WITHOUT_STEER = ["auto", "queue", "restart"]
Headers = dict[str, str]


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


def bind(store: Store, modes: list[str], title: str = "worker") -> Binding:
    binding, _ = store.register_binding(
        "codex", uuid.uuid4().hex, title, "w", "e", {"receive_messages": "verified", "send_modes": modes}
    )
    return binding


def send(store: Store, binding: Binding, mode: str, text: str) -> str:
    """Post one owner message to the session in a mode; returns its delivery id. A steer needs a running
    turn at the time it is posted, whatever the turn is afterwards."""
    before = store.get_binding(binding.id).turn_state
    if mode == "steer":
        store.set_turn_state(binding.id, "busy", None)
    conversation = store.open_direct("owner", binding.id)
    posted = store.post_message(OWNER, conversation.id, uuid.uuid4().hex, text, mode=mode)
    store.set_turn_state(binding.id, before, None)
    return posted.deliveries[0].id


def expected(declares_steer: bool, in_flight: str, mode: str, turn: str) -> bool:
    """The claim gate as the contract states it, written out case by case."""
    if in_flight == "dispatching":
        return False
    if in_flight == "accepted":
        return declares_steer and mode in ("auto", "steer", "restart")
    return not (mode == "queue" and turn in ("busy", "waiting"))


CASES = [
    (declares_steer, in_flight, mode, turn)
    for declares_steer in (True, False)
    for in_flight in ("none", "dispatching", "accepted")
    # A session that does not take steer is never sent one.
    for mode in (("auto", "queue", "restart", "steer") if declares_steer else ("auto", "queue", "restart"))
    for turn in ("idle", "busy", "waiting", "unknown")
]


@pytest.mark.parametrize(("declares_steer", "in_flight", "mode", "turn"), CASES)
def test_a_queued_head_is_handed_out_by_its_mode_and_what_is_in_flight(
    store: Store, declares_steer: bool, in_flight: str, mode: str, turn: str
) -> None:
    binding = bind(store, WITH_STEER if declares_steer else WITHOUT_STEER)
    if in_flight != "none":
        send(store, binding, "auto", "first")
        first = store.claim_next(binding.id, binding.lease_generation)
        assert first is not None
        if in_flight == "accepted":
            store.transition(first[0].id, binding.id, binding.lease_generation, "accepted")
    head = send(store, binding, mode, "head")
    store.set_turn_state(binding.id, turn, None)
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert (claimed is not None) == expected(declares_steer, in_flight, mode, turn)
    if claimed is not None:
        assert claimed[0].id == head and claimed[1].mode == mode


def test_the_gate_keeps_fifo_behind_a_held_head(store: Store) -> None:
    binding = bind(store, WITH_STEER)
    send(store, binding, "auto", "running")
    running = store.claim_next(binding.id, binding.lease_generation)
    assert running is not None
    store.transition(running[0].id, binding.id, binding.lease_generation, "accepted")
    held = send(store, binding, "queue", "later")
    send(store, binding, "steer", "now")
    # The steer is behind a queue-mode head that waits for the turn, so nothing is handed out.
    assert store.claim_next(binding.id, binding.lease_generation) is None
    store.transition(running[0].id, binding.id, binding.lease_generation, "responded")
    store.set_turn_state(binding.id, "idle", None)
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert claimed is not None and claimed[0].id == held


# A claim carries the adapter's current reading of its turn.


@pytest.fixture
def client(store: Store) -> Iterator[TestClient]:
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL))
    with TestClient(app) as c:
        yield c


def register(client: TestClient, title: str = "codex") -> tuple[str, Headers]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "codex",
            "native_id": uuid.uuid4().hex,
            "title": title,
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {"receive_messages": "verified", "send_modes": WITH_STEER, "tasks": True},
            "current_token": "",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def claim(client: TestClient, headers: Headers, turn: dict[str, Any] | None = None) -> Any:
    return client.post(
        "/api/adapter/claim",
        headers=headers,
        json=None if turn is None else {"turn": {"seq": next(SEQ), **turn}},
    )


@pytest.mark.parametrize(
    ("reading", "released"),
    [
        ({"state": "idle"}, True),
        ({"state": "unknown"}, True),
        ({"state": "busy"}, False),
        ({"state": "waiting", "waiting_for": "approval"}, False),
    ],
)
def test_a_claim_reading_is_applied_before_the_queue_hold_is_judged(
    store: Store, client: TestClient, reading: dict[str, Any], released: bool
) -> None:
    binding_id, headers = register(client)
    assert (
        client.post(
            "/api/adapter/turn", headers=headers, json={"state": "busy", "seq": next(SEQ)}
        ).status_code
        == 200
    )
    conversation = store.open_direct("owner", binding_id)
    store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "later", mode="queue")
    # The idle report was lost: without a reading the claim still finds the session busy.
    assert claim(client, headers).status_code == 204
    answer = claim(client, headers, reading)
    assert (answer.status_code == 200) == released
    kept = store.get_binding(binding_id)
    assert kept.turn_state == reading["state"]


def test_a_claim_without_a_reading_changes_nothing(store: Store, client: TestClient) -> None:
    binding_id, headers = register(client)
    client.post("/api/adapter/turn", headers=headers, json={"state": "busy", "seq": next(SEQ)})
    before = store.get_binding(binding_id)
    assert claim(client, headers).status_code == 204
    assert client.post("/api/adapter/claim", headers=headers, json={}).status_code == 204
    assert store.get_binding(binding_id) == before


def test_an_unchanged_reading_writes_nothing(store: Store, client: TestClient) -> None:
    binding_id, headers = register(client)
    client.post("/api/adapter/turn", headers=headers, json={"state": "busy", "seq": next(SEQ)})
    before = store.get_binding(binding_id)
    assert claim(client, headers, {"state": "busy"}).status_code == 204
    after = store.get_binding(binding_id)
    # Only the sequence the reading was numbered with moves; the state and its time stay.
    assert after.turn_seq > before.turn_seq
    assert replace(after, turn_seq=before.turn_seq) == before


def test_a_bad_reading_is_refused_and_the_claim_hands_nothing_out(store: Store, client: TestClient) -> None:
    binding_id, headers = register(client)
    conversation = store.open_direct("owner", binding_id)
    store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "hello")
    assert claim(client, headers, {"state": "dancing"}).status_code == 422
    assert claim(client, headers, {"state": "waiting", "waiting_for": "Some Prompt Text"}).status_code == 422
    assert claim(client, headers, {"state": "idle"}).status_code == 200


def test_a_claim_reading_has_every_effect_a_turn_report_has(store: Store, client: TestClient) -> None:
    _, boss_h = register(client, "boss")
    worker_id, worker_h = register(client, "worker")
    made = client.post(
        "/api/adapter/tasks", headers=boss_h, json={"to": worker_id, "task": "t", "done_when": "d"}
    )
    assert made.status_code == 201, made.text
    task_id = made.json()["id"]
    claim(client, worker_h)
    # Waiting, sent only with a claim, opens the observed question exactly as POST /turn does.
    claim(client, worker_h, {"state": "waiting", "waiting_for": "approval"})
    questions = client.get(f"/api/adapter/tasks/{task_id}", headers=boss_h).json()["questions"]
    assert [q["kind"] for q in questions] == ["observed"]
    assert tasks.owner_task(store, task_id.replace("-", ""))["questions"][0]["state"] == "open"
    claim(client, worker_h, {"state": "busy"})
    assert client.get(f"/api/adapter/tasks/{task_id}", headers=boss_h).json()["questions"] == []
    # Idle with an unreported task nudges the session, once.
    claim(client, worker_h, {"state": "idle"})
    claim(client, worker_h, {"state": "busy"})
    claim(client, worker_h, {"state": "idle"})
    with store._read() as db:
        nudges = db.execute(
            "SELECT COUNT(*) FROM messages WHERE client_message_id LIKE 'task:%:nudge'"
        ).fetchone()
    assert nudges[0] == 1

from __future__ import annotations

import itertools
import uuid
from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.store import Store

CODE = "pair-me"
ENROLL = "enroll-me"
FULL = ["auto", "queue", "steer", "restart"]
Headers = dict[str, str]
SEQ = itertools.count(1000)


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


def register(client: TestClient, native: str, token: str = "") -> tuple[str, Headers]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "codex",
            "native_id": native,
            "title": "codex",
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {"receive_messages": "verified", "send_modes": FULL, "tasks": True},
            "current_token": token,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}", "x-token": body["token"]}


def reading(
    client: TestClient,
    headers: Headers,
    via: str,
    state: str,
    seq: int | None,
    waiting_for: str | None = None,
) -> Any:
    turn: dict[str, Any] = {"state": state}
    if seq is not None:
        turn["seq"] = seq
    if waiting_for is not None:
        turn["waiting_for"] = waiting_for
    auth = {"authorization": headers["authorization"]}
    if via == "turn":
        return client.post("/api/adapter/turn", headers=auth, json=turn)
    return client.post("/api/adapter/claim", headers=auth, json={"turn": turn})


ROUTES = ["turn", "claim"]


@pytest.mark.parametrize("via", ROUTES)
def test_a_reading_without_a_sequence_number_is_refused(client: TestClient, store: Store, via: str) -> None:
    binding_id, headers = register(client, "n1")
    assert reading(client, headers, via, "busy", None).status_code == 422
    assert store.get_binding(binding_id).turn_state == "unknown"


@pytest.mark.parametrize("via", ROUTES)
def test_a_bad_reading_is_refused_even_when_it_changes_nothing(
    client: TestClient, store: Store, via: str
) -> None:
    binding_id, headers = register(client, "n1")
    assert reading(client, headers, "turn", "busy", 5).status_code == 200
    before = store.get_binding(binding_id)
    # The state is the one already held: only a valid-looking compare would let this through.
    assert reading(client, headers, via, "busy", 6, "Allow Bash(rm -rf)?").status_code == 422
    assert reading(client, headers, via, "busy", 0).status_code == 422
    assert store.get_binding(binding_id) == before


@pytest.mark.parametrize("first", ROUTES)
@pytest.mark.parametrize("second", ROUTES)
def test_an_older_or_equal_reading_changes_nothing_on_either_route(
    client: TestClient, store: Store, first: str, second: str
) -> None:
    binding_id, headers = register(client, "n1")
    assert reading(client, headers, first, "busy", 10).status_code in (200, 204)
    held = store.get_binding(binding_id)
    assert (held.turn_state, held.turn_seq) == ("busy", 10)
    for seq in (10, 9, 1):
        answer = reading(client, headers, second, "idle", seq)
        if second == "turn":
            assert answer.status_code == 200 and answer.json()["applied"] is False
        assert store.get_binding(binding_id) == held
    assert reading(client, headers, second, "idle", 11).status_code in (200, 204)
    assert store.get_binding(binding_id).turn_state == "idle"


def test_the_answer_says_whether_the_reading_applied(client: TestClient) -> None:
    _, headers = register(client, "n1")
    assert reading(client, headers, "turn", "busy", 3).json() == {
        "state": "busy",
        "waiting_for": None,
        "applied": True,
    }
    assert reading(client, headers, "turn", "idle", 2).json() == {
        "state": "busy",
        "waiting_for": None,
        "applied": False,
    }


def test_a_reading_sent_under_an_old_lease_is_not_applied_and_registration_restarts_the_sequence(
    client: TestClient, store: Store
) -> None:
    native = uuid.uuid4().hex
    binding_id, old = register(client, native)
    assert reading(client, old, "turn", "busy", 50).status_code == 200
    _, new = register(client, native, token=old["x-token"])
    assert store.get_binding(binding_id).turn_seq == 0
    # The old adapter's token no longer authenticates; a reading it had in flight cannot land either way.
    assert reading(client, old, "turn", "idle", 99).status_code == 403
    assert reading(client, new, "turn", "waiting", 1, "approval").json()["applied"] is True
    kept = store.get_binding(binding_id)
    assert (kept.turn_state, kept.turn_waiting_for, kept.turn_seq) == ("waiting", "approval", 1)


def test_the_store_applies_a_reading_only_under_the_lease_it_was_sent_from(store: Store) -> None:
    binding, _ = store.register_binding("codex", "n1", "c", "w", "e", {"send_modes": FULL})
    with store._write() as db:
        _, applied = store._write_turn_state(
            db, binding.id, "busy", None, lease=binding.lease_generation + 1, seq=5
        )
    assert applied is False and store.get_binding(binding.id) == binding
    with store._write() as db:
        _, applied = store._write_turn_state(
            db, binding.id, "busy", None, lease=binding.lease_generation, seq=5
        )
    assert applied is True
    assert replace(store.get_binding(binding.id), turn_state_at=None) == replace(
        binding, turn_state="busy", turn_seq=5
    )

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.ids import reply_id
from duplex_broker.store import DELIVERY_STATES, Actor, Store

CODE = "pair-me"
ENROLL = "enroll-me"
OWNER = Actor.user("owner")
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


def register(client: TestClient, title: str) -> tuple[str, Headers]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "codex",
            "native_id": uuid.uuid4().hex,
            "title": title,
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {"receive_messages": "verified"},
            "current_token": "",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def send(store: Store, binding_id: str, text: str) -> None:
    conversation = store.open_direct("owner", binding_id)
    store.post_message(OWNER, conversation.id, uuid.uuid4().hex, text)


def listed(client: TestClient, headers: Headers, state: str) -> Any:
    return client.get("/api/adapter/deliveries", params={"state": state}, headers=headers)


def test_a_session_lists_its_own_deliveries_in_the_states_it_asks_for(
    client: TestClient, store: Store
) -> None:
    mine, mine_h = register(client, "mine")
    theirs, theirs_h = register(client, "theirs")
    for n in range(3):
        send(store, mine, f"mine {n}")
        send(store, theirs, f"theirs {n}")
    first = client.post("/api/adapter/claim", headers=mine_h).json()["delivery"]
    other = client.post("/api/adapter/claim", headers=theirs_h).json()["delivery"]
    client.post(
        f"/api/adapter/deliveries/{first['id']}/state",
        json={"state": "accepted", "native_turn_id": "turn-1"},
        headers=mine_h,
    )
    rows = listed(client, mine_h, "dispatching,accepted").json()
    assert [(r["id"], r["state"], r["native_turn_id"]) for r in rows] == [(first["id"], "accepted", "turn-1")]
    assert set(rows[0]) == {"id", "state", "native_turn_id", "message_id", "conversation_id"}
    assert rows[0]["message_id"] == reply_id(first["message_id"])
    assert rows[0]["conversation_id"] == reply_id(store.open_direct("owner", mine).id)
    # Another session's deliveries are never listed, whatever state is asked for.
    assert other["id"] not in {r["id"] for r in listed(client, mine_h, ",".join(DELIVERY_STATES)).json()}
    assert {r["id"] for r in listed(client, theirs_h, "dispatching").json()} == {other["id"]}
    queued = listed(client, mine_h, "queued").json()
    assert len(queued) == 2 and {r["state"] for r in queued} == {"queued"}


@pytest.mark.parametrize("bad", ["", "bogus", "queued,bogus", "dispatching,,accepted", " "])
def test_a_state_outside_the_delivery_states_is_422(client: TestClient, bad: str) -> None:
    _, headers = register(client, "mine")
    assert listed(client, headers, bad).status_code == 422
    assert client.get("/api/adapter/deliveries", headers=headers).status_code == 422


def test_the_route_needs_the_adapter_token(client: TestClient) -> None:
    assert client.get("/api/adapter/deliveries", params={"state": "queued"}).status_code == 401
    bad = {"authorization": "Bearer nope"}
    assert client.get("/api/adapter/deliveries", params={"state": "queued"}, headers=bad).status_code in (
        401,
        403,
    )


def test_listing_changes_nothing_and_is_no_claim_poll(client: TestClient, store: Store) -> None:
    mine, headers = register(client, "mine")
    send(store, mine, "hello")
    with store._read() as db:
        before = [tuple(r) for r in db.execute("SELECT * FROM deliveries")]
        events = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert len(listed(client, headers, "queued").json()) == 1
    with store._read() as db:
        assert [tuple(r) for r in db.execute("SELECT * FROM deliveries")] == before
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == events
    # Not a poll: liveness, which the owner's status view reports, is still unset for the session.
    client.post("/api/pair", json={"code": CODE})
    sessions = client.get("/api/status").json()["sessions"]
    assert [x["polled_at"] for x in sessions if x["id"] == mine] == [None]
    client.post("/api/adapter/claim", headers=headers)
    sessions = client.get("/api/status").json()["sessions"]
    assert [x["polled_at"] is not None for x in sessions if x["id"] == mine] == [True]

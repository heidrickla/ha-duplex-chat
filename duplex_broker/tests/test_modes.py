from __future__ import annotations

import itertools
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.store import Actor, Binding, Conflict, Invalid, ModeUnsupported, Store

SEQ = itertools.count(1)
OWNER = Actor.user("owner")
ENROLL = "enroll-me"
CODE = "pair-me"
FULL = ["auto", "queue", "steer", "restart"]


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


def bind(store: Store, title: str, send_modes: list[str] | None = None, provider: str = "claude") -> Binding:
    capabilities: dict[str, Any] = {"receive_messages": "verified"}
    if send_modes is not None:
        capabilities["send_modes"] = send_modes
    binding, _ = store.register_binding(
        provider,  # type: ignore[arg-type]
        uuid.uuid4().hex,
        title,
        "w",
        "e",
        capabilities,
    )
    return binding


def post(store: Store, binding: Binding, text: str = "hi", mode: str = "auto") -> Any:
    conversation = store.open_direct("owner", binding.id)
    return store.post_message(OWNER, conversation.id, uuid.uuid4().hex, text, mode=mode)


def counts(store: Store) -> tuple[int, int, int]:
    with store._read() as db:
        return tuple(
            db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("messages", "deliveries", "events")
        )


def test_a_binding_without_send_modes_takes_auto_only(store: Store) -> None:
    plain = bind(store, "plain")
    assert plain.send_modes == ("auto",)
    assert bind(store, "declared", ["queue", "auto", "bogus"]).send_modes == ("auto", "queue")
    assert bind(store, "only-auto", ["auto"]).send_modes == ("auto",)
    assert post(store, plain).message.mode == "auto"


def test_a_mode_a_recipient_cannot_take_is_refused_with_what_it_supports(store: Store) -> None:
    codex = bind(store, "codex-repo", ["auto", "queue"], "codex")
    with pytest.raises(ModeUnsupported) as refused:
        post(store, codex, mode="steer")
    assert str(refused.value) == 'steer is not supported by "codex-repo" (supports: auto, queue)'
    plain = bind(store, "SSLReverseProxy")
    with pytest.raises(ModeUnsupported) as refused:
        post(store, plain, mode="restart")
    assert str(refused.value) == 'restart is not supported by "SSLReverseProxy" (supports: auto)'


def test_a_group_post_names_every_recipient_that_cannot_take_the_mode(store: Store) -> None:
    ok = bind(store, "full", FULL)
    queue_only = bind(store, "queue-only", ["auto", "queue"])
    plain = bind(store, "plain")
    group = store.create_group("owner", "g")
    for member in (ok, queue_only, plain):
        store.add_member(OWNER, group.id, member.id)
    with pytest.raises(ModeUnsupported) as refused:
        store.post_message(
            OWNER, group.id, uuid.uuid4().hex, "go", [ok.id, queue_only.id, plain.id], mode="steer"
        )
    detail = str(refused.value)
    assert 'steer is not supported by "queue-only" (supports: auto, queue)' in detail
    assert 'steer is not supported by "plain" (supports: auto)' in detail
    assert '"full"' not in detail


def test_a_refused_mode_creates_no_message(store: Store) -> None:
    plain = bind(store, "plain")
    conversation = store.open_direct("owner", plain.id)
    before = counts(store)
    with pytest.raises(ModeUnsupported):
        store.post_message(OWNER, conversation.id, "one", "hello", mode="steer")
    assert counts(store) == before
    # The same client id is free to be used again with a mode the recipient takes: nothing was kept.
    sent = store.post_message(OWNER, conversation.id, "one", "hello")
    assert sent.created and sent.message.mode == "auto"


def test_an_unknown_mode_is_invalid_not_a_mode_refusal(store: Store) -> None:
    plain = bind(store, "plain")
    with pytest.raises(Invalid) as refused:
        post(store, plain, mode="shout")
    assert not isinstance(refused.value, ModeUnsupported)


def test_a_steer_to_an_idle_session_is_refused_and_to_a_running_one_is_not(store: Store) -> None:
    session = bind(store, "worker", FULL)
    store.set_turn_state(session.id, "idle", None)
    with pytest.raises(ModeUnsupported) as refused:
        post(store, session, mode="steer")
    assert str(refused.value) == '"worker" has no running turn (supports: auto, queue, steer, restart)'
    for state in ("busy", "waiting", "unknown"):
        store.set_turn_state(session.id, state, "approval" if state == "waiting" else None)
        assert post(store, session, mode="steer").message.mode == "steer"


def test_a_retry_with_another_mode_is_a_conflict_and_the_same_mode_returns_the_original(store: Store) -> None:
    session = bind(store, "worker", FULL)
    conversation = store.open_direct("owner", session.id)
    first = store.post_message(OWNER, conversation.id, "c1", "do it", mode="queue")
    again = store.post_message(OWNER, conversation.id, "c1", "do it", mode="queue")
    assert not again.created and again.message.id == first.message.id
    with pytest.raises(Conflict):
        store.post_message(OWNER, conversation.id, "c1", "do it", mode="auto")


def test_a_redirect_keeps_the_mode_of_the_message_it_copies(store: Store) -> None:
    first, second = bind(store, "first", FULL), bind(store, "second", FULL)
    sent = post(store, first, mode="queue")
    moved = store.redirect(OWNER, sent.deliveries[0].id, second.id, "queued")
    assert moved.message.mode == "queue"


# Turn state and the queue-mode hold


def hand_over(store: Store, binding: Binding) -> Any:
    return store.claim_next(binding.id, binding.lease_generation)


def test_a_busy_session_is_handed_no_queue_mode_message_and_an_idle_one_is(store: Store) -> None:
    session = bind(store, "worker", FULL)
    store.set_turn_state(session.id, "busy", None)
    post(store, session, "later", "queue")
    assert hand_over(store, session) is None
    store.set_turn_state(session.id, "waiting", "approval")
    assert hand_over(store, session) is None
    store.set_turn_state(session.id, "idle", None)
    claimed = hand_over(store, session)
    assert claimed is not None and claimed[1].mode == "queue"


def test_a_session_that_reports_unknown_is_handed_a_queue_mode_message(store: Store) -> None:
    session = bind(store, "worker", FULL)
    store.set_turn_state(session.id, "busy", None)
    post(store, session, "later", "queue")
    store.set_turn_state(session.id, "unknown", None)
    assert hand_over(store, session) is not None


def test_a_held_queue_message_holds_the_ones_behind_it_and_other_modes_flow_when_first(store: Store) -> None:
    session = bind(store, "worker", FULL)
    store.set_turn_state(session.id, "busy", None)
    post(store, session, "auto first", "auto")
    queued = post(store, session, "queue second", "queue")
    post(store, session, "auto third", "auto")
    first = hand_over(store, session)
    assert first is not None and first[1].text == "auto first"
    for state in ("accepted", "responded"):
        store.transition(first[0].id, session.id, session.lease_generation, state)
    assert hand_over(store, session) is None  # the queue-mode head holds the auto message behind it
    with store._read() as db:
        states = [r[0] for r in db.execute("SELECT state FROM deliveries ORDER BY seq")]
    assert states == ["responded", "queued", "queued"]
    store.set_turn_state(session.id, "idle", None)
    second = hand_over(store, session)
    assert second is not None and second[1].id == queued.message.id


def test_a_held_delivery_never_enters_the_wake_ladder(store: Store) -> None:
    from duplex_broker.ladder import Policy, sweep

    session, _ = store.register_binding(
        "claude", uuid.uuid4().hex, "worker", "w", "e", {"rewake": "verified", "send_modes": FULL}
    )
    store.set_turn_state(session.id, "busy", None)
    post(store, session, "later", "queue")
    assert hand_over(store, session) is None
    assert sweep(store, Policy(wake_timeout=0, rewake_interval=1, rewake_limit=1, hold_after=2), "owner") == 0
    with store._read() as db:
        assert db.execute("SELECT state, wake_due_at FROM deliveries").fetchone()[:] == ("queued", None)


def test_leaving_a_running_turn_nudges_the_claim_for_a_held_queue_message(store: Store) -> None:
    session = bind(store, "worker", FULL)
    conversation = store.open_direct("owner", session.id)
    store.set_turn_state(session.id, "busy", None)
    post(store, session, "later", "queue")
    store.set_turn_state(session.id, "idle", None)
    types = [e.type for e in store.events_after(OWNER, conversation.id)]
    assert types[-1] == "delivery.released"
    # Going busy again, or leaving a state that held nothing, nudges nobody.
    store.set_turn_state(session.id, "busy", None)
    store.set_turn_state(session.id, "idle", None)
    assert [e.type for e in store.events_after(OWNER, conversation.id)].count("delivery.released") == 2


def test_registration_resets_the_turn_state(store: Store) -> None:
    session, token = store.register_binding("claude", "n1", "worker", "w", "e", {}, current_token=None)
    store.set_turn_state(session.id, "waiting", "approval")
    held = store.get_binding(session.id)
    assert (held.turn_state, held.turn_waiting_for) == ("waiting", "approval")
    assert held.turn_state_at is not None
    again, _ = store.register_binding("claude", "n1", "worker", "w", "e", {}, current_token=token)
    assert (again.turn_state, again.turn_waiting_for, again.turn_state_at) == ("unknown", None, None)


def test_a_wait_is_a_kind_and_never_text(store: Store) -> None:
    session = bind(store, "worker")
    for bad in ("Approval", "run rm -rf /", "a" * 41, "1abc", "", "x y"):
        with pytest.raises(Invalid):
            store.set_turn_state(session.id, "waiting", bad)
    kept = store.set_turn_state(session.id, "waiting", "a" * 40)
    assert kept.turn_waiting_for == "a" * 40
    assert store.set_turn_state(session.id, "idle", "approval").turn_waiting_for is None
    with pytest.raises(Invalid):
        store.set_turn_state(session.id, "napping", None)


# The API


@pytest.fixture
def client(store: Store) -> Iterator[TestClient]:
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL))
    with TestClient(app) as c:
        yield c


def register(
    client: TestClient, title: str, send_modes: list[str] | None = None
) -> tuple[str, dict[str, str]]:
    capabilities: dict[str, Any] = {"receive_messages": "verified"}
    if send_modes is not None:
        capabilities["send_modes"] = send_modes
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": "claude",
            "native_id": uuid.uuid4().hex,
            "title": title,
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": capabilities,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def owner(client: TestClient) -> dict[str, str]:
    r = client.post("/api/pair", json={"code": CODE})
    return {"x-csrf-token": r.json()["csrf"]}


def test_the_owner_post_refuses_an_unsupported_mode_with_422_and_names_the_recipient(
    client: TestClient,
) -> None:
    binding, _ = register(client, "SSLReverseProxy", ["auto", "queue", "steer"])
    csrf = owner(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding}, headers=csrf).json()
    refused = client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": "c1", "text": "restart it", "mode": "restart"},
        headers=csrf,
    )
    assert refused.status_code == 422
    assert refused.json() == {
        "error": "mode_unsupported",
        "detail": 'restart is not supported by "SSLReverseProxy" (supports: auto, queue, steer)',
    }
    assert client.get(f"/api/conversations/{dm['id']}/events", headers=csrf).json() == []
    assert (
        client.post(
            f"/api/conversations/{dm['id']}/messages",
            json={"client_message_id": "c2", "text": "x", "mode": "shout"},
            headers=csrf,
        ).status_code
        == 422
    )


def test_an_adapter_post_carries_its_mode_and_the_claim_hands_it_over(client: TestClient) -> None:
    sender, sender_headers = register(client, "sender")
    receiver, receiver_headers = register(client, "receiver", ["auto", "queue"])
    csrf = owner(client)
    group = client.post("/api/conversations", json={"title": "g", "mode": "consult"}, headers=csrf).json()
    for member in (sender, receiver):
        client.post(f"/api/conversations/{group['id']}/members", json={"binding_id": member}, headers=csrf)
    client.post(f"/api/conversations/{group['id']}/collaboration", json={"objective": "o"}, headers=csrf)
    body = {
        "conversation_id": group["id"],
        "client_message_id": "m1",
        "text": "after",
        "recipients": [receiver],
    }
    refused = client.post("/api/adapter/messages", json={**body, "mode": "steer"}, headers=sender_headers)
    assert refused.status_code == 422 and refused.json()["error"] == "mode_unsupported"
    sent = client.post("/api/adapter/messages", json={**body, "mode": "queue"}, headers=sender_headers)
    assert sent.status_code == 201 and sent.json()["message"]["mode"] == "queue"
    claimed = client.post("/api/adapter/claim", headers=receiver_headers).json()
    assert claimed["message"]["mode"] == "queue"


def test_the_turn_route_stores_state_and_refuses_a_wait_that_is_text(
    client: TestClient, store: Store
) -> None:
    binding, headers = register(client, "worker", FULL)
    ok = client.post(
        "/api/adapter/turn",
        json={"state": "waiting", "waiting_for": "approval", "seq": next(SEQ)},
        headers=headers,
    )
    assert ok.status_code == 200 and ok.json() == {
        "state": "waiting",
        "waiting_for": "approval",
        "applied": True,
    }
    assert store.get_binding(binding).turn_state == "waiting"
    for bad in (
        {"state": "waiting", "waiting_for": "Allow Bash(rm -rf)?", "seq": next(SEQ)},
        {"state": "napping", "seq": next(SEQ)},
        {"state": "idle"},
    ):
        assert client.post("/api/adapter/turn", json=bad, headers=headers).status_code == 422
    assert client.post("/api/adapter/turn", json={"state": "idle", "seq": next(SEQ)}).status_code == 401
    assert store.get_binding(binding).turn_waiting_for == "approval"


def test_a_busy_session_claims_no_queue_message_over_the_api(client: TestClient) -> None:
    binding, headers = register(client, "worker", FULL)
    csrf = owner(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding}, headers=csrf).json()
    client.post("/api/adapter/turn", json={"state": "busy", "seq": next(SEQ)}, headers=headers)
    posted = client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": "c1", "text": "after", "mode": "queue"},
        headers=csrf,
    )
    assert posted.status_code == 201
    assert client.post("/api/adapter/claim", headers=headers).status_code == 204
    client.post("/api/adapter/turn", json={"state": "idle", "seq": next(SEQ)}, headers=headers)
    claimed = client.post("/api/adapter/claim", headers=headers)
    assert claimed.status_code == 200 and claimed.json()["message"]["mode"] == "queue"


@pytest.mark.parametrize("declared", [[], ["queue"], ["queue", "steer"], "auto", None, {"auto": True}])
def test_registration_refuses_send_modes_without_auto(store: Store, declared: Any) -> None:
    with pytest.raises(Invalid, match="includes auto"):
        store.register_binding("claude", "n", "t", "w", "e", {"send_modes": declared})
    assert store.find_binding("claude", "n") is None


def test_the_register_route_answers_422_for_send_modes_without_auto(client: TestClient) -> None:
    body = {
        "provider": "claude",
        "native_id": "n1",
        "title": "t",
        "workspace": "w",
        "endpoint_ref": "e",
        "capabilities": {"send_modes": ["queue", "steer"]},
        "current_token": "",
    }
    refused = client.post("/api/adapter/register", json=body, headers={"authorization": "Bearer enroll-me"})
    assert refused.status_code == 422 and "includes auto" in refused.json()["detail"]
    body["capabilities"] = {"send_modes": ["auto", "queue"]}
    ok = client.post("/api/adapter/register", json=body, headers={"authorization": "Bearer enroll-me"})
    assert ok.status_code == 201

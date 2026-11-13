from __future__ import annotations

import re
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.store import Actor, Event, Store

CODE = "pair-me"
ENROLL = "enroll-me"


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


def paired(client: TestClient) -> dict[str, str]:
    r = client.post("/api/pair", json={"code": CODE})
    assert r.status_code == 200, r.text
    return {"x-csrf-token": r.json()["csrf"]}


def register(
    client: TestClient, provider: str = "claude", native: str | None = None
) -> tuple[str, dict[str, str]]:
    r = client.post(
        "/api/adapter/register",
        headers={"authorization": f"Bearer {ENROLL}"},
        json={
            "provider": provider,
            "native_id": native or uuid.uuid4().hex,
            "title": f"{provider} session",
            "workspace": "D:/work/x",
            "endpoint_ref": "local",
            "capabilities": {"receive_messages": "verified"},
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["binding"]["id"], {"authorization": f"Bearer {body['token']}"}


def cid() -> str:
    return uuid.uuid4().hex


# Browser authentication


def test_pairing_sets_a_session_and_locks_out_guessing(client: TestClient) -> None:
    assert client.get("/api/conversations").status_code == 401
    for _ in range(5):
        assert client.post("/api/pair", json={"code": "wrong"}).status_code == 403
    assert client.post("/api/pair", json={"code": CODE}).status_code == 403  # locked out


def test_writes_need_the_csrf_token(client: TestClient) -> None:
    csrf = paired(client)
    assert client.post("/api/conversations", json={"title": "g"}).status_code == 403
    assert (
        client.post("/api/conversations", json={"title": "g"}, headers={"x-csrf-token": "x"}).status_code
        == 403
    )
    assert client.post("/api/conversations", json={"title": "g"}, headers=csrf).status_code == 201
    assert client.get("/api/session").json()["csrf"] == csrf["x-csrf-token"]


def test_job_queue_notice_has_separate_sender_and_narrow_token(store: Store) -> None:
    app = create_app(store, Settings(enrollment_token=ENROLL, services={"job-queue": "fqn-test"}))
    with TestClient(app) as client:
        binding, adapter_headers = register(client)
        body = {"binding_id": binding, "client_message_id": "job-42-running", "text": "Repo job is running"}
        queue_headers = {"authorization": "Bearer fqn-test"}
        assert client.post("/api/service/job-queue/notify", json=body).status_code == 401
        first = client.post("/api/service/job-queue/notify", json=body, headers=queue_headers)
        assert first.status_code == 201
        again = client.post("/api/service/job-queue/notify", json=body, headers=queue_headers)
        assert again.status_code == 200
        assert again.json()["message_id"] == first.json()["message_id"]
        assert client.get("/api/sessions", headers=queue_headers).status_code == 401
        listed = client.get("/api/service/job-queue/sessions", headers=queue_headers)
        assert listed.status_code == 200
        assert listed.json() == [
            {"id": binding, "title": "claude session", "provider": "claude", "active": True}
        ]
        claimed = client.post("/api/adapter/claim", headers=adapter_headers)
        assert claimed.status_code == 200
        message = claimed.json()["message"]
        assert message["actor"] == {"kind": "user", "id": "job-queue"}
        assert message["text"].startswith("[job-queue status data] ")


def test_ingress_mode_trusts_only_the_supervisor_proxy(store: Store) -> None:
    trusted = create_app(store, Settings(user_auth="ingress", trusted_proxies=frozenset({"testclient"})))
    with TestClient(trusted) as c:
        csrf = {"x-csrf-token": c.get("/api/session").json()["csrf"]}
        assert c.post("/api/conversations", json={"title": "g"}, headers=csrf).status_code == 201
    untrusted = create_app(store, Settings(user_auth="ingress"))
    with TestClient(untrusted) as c:
        assert c.get("/api/conversations").status_code == 401
        assert c.post("/api/pair", json={"code": CODE}).status_code == 403


# Adapters


def test_registration_needs_the_enrollment_token(client: TestClient) -> None:
    body = {
        "provider": "claude",
        "native_id": "n",
        "title": "t",
        "workspace": "",
        "endpoint_ref": "",
        "capabilities": {},
    }
    assert client.post("/api/adapter/register", json=body).status_code == 401
    assert (
        client.post("/api/adapter/register", json=body, headers={"authorization": "Bearer no"}).status_code
        == 401
    )
    assert client.post("/api/adapter/claim", headers={"authorization": "Bearer no"}).status_code == 403


def test_direct_round_trip_through_an_adapter(client: TestClient) -> None:
    csrf = paired(client)
    binding_id, auth = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()

    assert client.post("/api/adapter/claim", headers=auth).status_code == 204
    sent = client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "nonce-123"},
        headers=csrf,
    )
    assert sent.status_code == 201

    claimed = client.post("/api/adapter/claim", headers=auth).json()
    assert claimed["message"]["text"] == "nonce-123"
    assert claimed["conversation"]["kind"] == "direct"
    reply_address = str(uuid.UUID(hex=dm["id"]))
    assert claimed["conversation"]["id"] == reply_address
    assert claimed["message"]["id"] == str(uuid.UUID(hex=sent.json()["message"]["id"]))
    legacy_secret_shape = re.compile(r"[0-9a-f]{32,}")
    assert legacy_secret_shape.search(claimed["conversation"]["id"]) is None
    assert legacy_secret_shape.search(claimed["message"]["id"]) is None
    assert client.get("/api/adapter/conversations", headers=auth).json()[0]["id"] == reply_address
    detail = client.get(f"/api/adapter/conversations/{reply_address}", headers=auth).json()
    assert detail["id"] == reply_address
    delivery = claimed["delivery"]["id"]
    r = client.post(
        f"/api/adapter/deliveries/{delivery}/state",
        json={"state": "accepted", "native_turn_id": "t1"},
        headers=auth,
    )
    assert r.json()["state"] == "accepted"
    reply = client.post(
        "/api/adapter/messages",
        headers=auth,
        json={
            "conversation_id": reply_address,
            "client_message_id": cid(),
            "text": "echo nonce-123",
            "parent_id": claimed["message"]["id"],
        },
    )
    assert reply.status_code == 201
    assert reply.json()["message"]["id"] == str(uuid.UUID(hex=reply.json()["message"]["id"].replace("-", "")))
    client.post(f"/api/adapter/deliveries/{delivery}/state", json={"state": "responded"}, headers=auth)

    adapter_events = client.get(f"/api/adapter/conversations/{reply_address}/events", headers=auth).json()
    assert all(event["conversation_id"] == reply_address for event in adapter_events)
    assert all(event["message_id"] is None or "-" in event["message_id"] for event in adapter_events)

    events = client.get(f"/api/conversations/{dm['id']}/events").json()
    assert [e["type"] for e in events][-2:] == ["message.accepted", "delivery.changed"]
    assert any(e["payload"].get("text") == "echo nonce-123" for e in events)
    assert events[-1]["payload"]["state"] == "responded"


def test_a_member_reads_one_message_with_each_delivery_and_its_trail(client: TestClient) -> None:
    csrf = paired(client)
    claude_id, auth = register(client, "claude")
    _, codex_auth = register(client, "codex")
    dm = client.post("/api/direct-conversations", json={"binding_id": claude_id}, headers=csrf).json()
    for text in ("first", "the one"):
        client.post(
            f"/api/conversations/{dm['id']}/messages",
            json={"client_message_id": cid(), "text": text},
            headers=csrf,
        )
    claimed = client.post("/api/adapter/claim", headers=auth).json()
    delivery = claimed["delivery"]["id"]
    for state in ("accepted", "consumed"):
        client.post(f"/api/adapter/deliveries/{delivery}/state", json={"state": state}, headers=auth)
    target = client.post("/api/adapter/claim", headers=auth).json()["message"]["id"]

    record = client.get(f"/api/adapter/messages/{target}", headers=auth)
    assert record.status_code == 200, record.text
    body = record.json()
    assert body["message"]["text"] == "the one" and body["message"]["id"] == target
    assert body["message"]["conversation_id"] == str(uuid.UUID(hex=dm["id"]))
    assert body["seq"] > 0
    assert [(d["recipient_title"], d["delivery"]["state"]) for d in body["deliveries"]] == [
        ("claude session", "dispatching")
    ]
    assert all(e["message_id"] == target for e in body["events"])
    assert body["events"][0]["type"] == "message.accepted"

    first = client.get(f"/api/adapter/messages/{claimed['message']['id']}", headers=auth).json()
    assert [e["payload"].get("state") for e in first["events"] if e["type"] == "delivery.changed"][-2:] == [
        "accepted",
        "consumed",
    ]
    assert first["deliveries"][0]["updated_at"] >= first["events"][0]["created_at"]

    assert body["deliveries"][0]["seen_by"] == "poll" and body["deliveries"][0]["last_seen"] > 0
    assert body["broker_started_at"] > 0

    assert client.get(f"/api/adapter/messages/{target}", headers=codex_auth).status_code == 403
    assert client.get(f"/api/adapter/messages/{uuid.uuid4()}", headers=auth).status_code == 404
    assert client.get(f"/api/adapter/messages/{target}").status_code == 401


def test_a_send_reports_when_each_recipient_was_last_heard_from(client: TestClient) -> None:
    csrf = paired(client)
    group, claude, claude_auth, codex, codex_auth = group_of_two(client, csrf)
    client.post(f"/api/conversations/{group}/mode", json={"mode": "consult"}, headers=csrf)

    def ask() -> Any:
        client.post(
            f"/api/conversations/{group}/messages",
            json={"client_message_id": cid(), "text": "ask codex", "recipients": [claude]},
            headers=csrf,
        )
        claimed = client.post("/api/adapter/claim", headers=claude_auth).json()
        root = claimed["message"]["id"]
        for state in ("accepted", "consumed"):  # one delivery in flight holds the next one back
            client.post(
                f"/api/adapter/deliveries/{claimed['delivery']['id']}/state",
                json={"state": state},
                headers=claude_auth,
            )
        sent = client.post(
            "/api/adapter/messages",
            headers=claude_auth,
            json={
                "conversation_id": group,
                "client_message_id": cid(),
                "text": "codex, are you there?",
                "recipients": [codex],
                "parent_id": root,
            },
        )
        assert sent.status_code == 201, sent.text
        return sent.json()

    never_polled = ask()["recipients_seen"]
    assert [(r["binding_id"], r["title"], r["seen_by"]) for r in never_polled] == [
        (codex, "codex session", "registration")
    ]
    client.post("/api/adapter/claim", headers=codex_auth)
    polled = ask()
    assert [(r["binding_id"], r["seen_by"]) for r in polled["recipients_seen"]] == [(codex, "poll")]
    assert polled["recipients_seen"][0]["last_seen"] >= never_polled[0]["last_seen"]
    assert polled["broker_started_at"] > 0


def test_an_adapter_stream_nudges_its_session_when_a_message_waits(client: TestClient) -> None:
    csrf = paired(client)
    binding_id, auth = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()
    with client.websocket_connect("/api/adapter/stream", headers=auth) as ws:
        assert ws.receive_json()["type"] == "ready"
        assert ws.receive_json() == {"type": "nudge"}  # on connect, for anything already waiting
        assert client.post("/api/adapter/claim", headers=auth).status_code == 204
        sent = client.post(
            f"/api/conversations/{dm['id']}/messages",
            json={"client_message_id": cid(), "text": "you've got mail"},
            headers=csrf,
        )
        assert ws.receive_json() == {"type": "nudge"}
        claimed = client.post("/api/adapter/claim", headers=auth).json()
        assert claimed["message"]["text"] == "you've got mail"
        record = client.get(f"/api/adapter/messages/{claimed['message']['id']}", headers=auth).json()
        assert [d["seen_by"] for d in record["deliveries"]] == ["stream"]
    assert sent.status_code == 201
    record = client.get(f"/api/adapter/messages/{claimed['message']['id']}", headers=auth).json()
    assert [d["seen_by"] for d in record["deliveries"]] == ["poll"]


def test_an_adapter_stream_is_not_nudged_for_another_session(store: Store) -> None:
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL, stream_ping=0.2))
    with TestClient(app) as client:
        csrf = paired(client)
        _, auth = register(client)
        other, _ = register(client)
        dm = client.post("/api/direct-conversations", json={"binding_id": other}, headers=csrf).json()
        with client.websocket_connect("/api/adapter/stream", headers=auth) as ws:
            assert ws.receive_json()["type"] == "ready"
            assert ws.receive_json() == {"type": "nudge"}
            client.post(
                f"/api/conversations/{dm['id']}/messages",
                json={"client_message_id": cid(), "text": "for the other one"},
                headers=csrf,
            )
            assert ws.receive_json() == {"type": "ping"}


def test_an_adapter_stream_refuses_a_bad_token(client: TestClient) -> None:
    with client.websocket_connect("/api/adapter/stream", headers={"authorization": "Bearer nope"}) as ws:
        assert ws.receive_json()["type"] == "error"
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
    assert closed.value.code == 4401


def test_another_session_cannot_read_or_write_a_dm(client: TestClient) -> None:
    csrf = paired(client)
    claude_id, _ = register(client, "claude")
    _, codex_auth = register(client, "codex")
    dm = client.post("/api/direct-conversations", json={"binding_id": claude_id}, headers=csrf).json()
    client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "secret"},
        headers=csrf,
    )

    assert client.get(f"/api/adapter/conversations/{dm['id']}/events", headers=codex_auth).status_code == 403
    displayed = str(uuid.UUID(hex=dm["id"]))
    assert client.get(f"/api/adapter/conversations/{displayed}", headers=codex_auth).status_code == 403
    r = client.post(
        "/api/adapter/messages",
        headers=codex_auth,
        json={"conversation_id": displayed, "client_message_id": cid(), "text": "intrude"},
    )
    assert r.status_code == 403
    assert client.get("/api/adapter/conversations", headers=codex_auth).json() == []


# Live events


def receive_until(ws: Any, kind: str) -> list[dict[str, Any]]:
    seen = []
    while True:
        msg = ws.receive_json()
        seen.append(msg)
        if msg["type"] == kind:
            return seen


def test_websocket_needs_the_csrf_hello(client: TestClient) -> None:
    paired(client)
    with client.websocket_connect("/api/events") as ws:
        ws.send_json({"type": "hello", "csrf": "wrong"})
        assert ws.receive_json()["error"] == "forbidden"


def test_websocket_replays_then_streams_without_duplicates(client: TestClient) -> None:
    csrf = paired(client)
    binding_id, _ = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()
    for i in range(3):
        client.post(
            f"/api/conversations/{dm['id']}/messages",
            json={"client_message_id": cid(), "text": f"m{i}"},
            headers=csrf,
        )

    with client.websocket_connect("/api/events") as ws:
        ws.send_json({"type": "hello", "csrf": csrf["x-csrf-token"]})
        assert ws.receive_json()["type"] == "ready"
        ws.send_json({"type": "subscribe", "conversation_id": dm["id"], "after": 2})
        replay = receive_until(ws, "subscribed")
        seqs = [m["event"]["seq"] for m in replay if m["type"] == "event"]
        assert seqs == [3, 4, 5, 6]

        client.post(
            f"/api/conversations/{dm['id']}/messages",
            json={"client_message_id": cid(), "text": "live"},
            headers=csrf,
        )
        live = [ws.receive_json(), ws.receive_json()]
        assert [m["event"]["seq"] for m in live] == [7, 8]
        assert live[0]["event"]["payload"]["text"] == "live"

        ws.send_json({"type": "subscribe", "conversation_id": "missing"})
        assert ws.receive_json()["error"] == "not_found"

        ws.send_json({"type": "subscribe", "conversation_id": dm["id"], "after": 0})
        again = receive_until(ws, "subscribed")
        assert [m["type"] for m in again] == ["subscribed"]
        assert again[0]["seq"] == 8


def test_event_committed_during_replay_is_sent_once(
    client: TestClient, store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    csrf = paired(client)
    binding_id, _ = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()
    client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "before"},
        headers=csrf,
    )
    original = store.events_after
    raced = False

    def racing(reader: Actor, conversation_id: str, after: int = 0, limit: int = 500) -> list[Event]:
        # Commit a message after the subscription is live but before the replay reads: its events are
        # both queued for live delivery and returned by the replay.
        nonlocal raced
        if not raced:
            raced = True
            store.post_message(Actor.user("owner"), conversation_id, cid(), "during replay")
        return original(reader, conversation_id, after, limit)

    monkeypatch.setattr(store, "events_after", racing)
    with client.websocket_connect("/api/events") as ws:
        ws.send_json({"type": "hello", "csrf": csrf["x-csrf-token"]})
        assert ws.receive_json()["type"] == "ready"
        ws.send_json({"type": "subscribe", "conversation_id": dm["id"]})
        replay = receive_until(ws, "subscribed")
        assert [m["event"]["seq"] for m in replay if m["type"] == "event"] == [1, 2, 3, 4]

        client.post(
            f"/api/conversations/{dm['id']}/messages",
            json={"client_message_id": cid(), "text": "after"},
            headers=csrf,
        )
        nxt = [ws.receive_json(), ws.receive_json()]
        assert [m["event"]["seq"] for m in nxt] == [5, 6]


# Devices


def enroll(client: TestClient, name: str = "pixel") -> dict[str, Any]:
    r = client.post("/api/device/enroll", json={"pairing_code": CODE, "device_name": name})
    assert r.status_code == 201, r.text
    body: dict[str, Any] = r.json()
    assert body["token"].startswith("dcd_")
    return body


def test_a_device_enrolls_once_and_needs_no_csrf(client: TestClient) -> None:
    assert client.post("/api/device/enroll", json={"pairing_code": "wrong"}).status_code == 403
    phone = enroll(client)
    auth = {"authorization": f"Bearer {phone['token']}"}
    assert client.get("/api/conversations", headers=auth).status_code == 200
    assert client.post("/api/conversations", json={"title": "g"}, headers=auth).status_code == 201
    assert client.get("/api/conversations", headers={"authorization": "Bearer dcd_forged"}).status_code == 403


def test_enrollment_shares_the_pairing_lockout(client: TestClient) -> None:
    for _ in range(5):
        assert client.post("/api/device/enroll", json={"pairing_code": "wrong"}).status_code == 403
    assert client.post("/api/device/enroll", json={"pairing_code": CODE}).status_code == 403


def test_the_pairing_lockout_counts_each_address_alone(store: Store) -> None:
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL))
    with (
        TestClient(app, client=("192.0.2.1", 5000)) as guesser,
        TestClient(app, client=("192.0.2.2", 5000)) as phone,
    ):
        for _ in range(5):
            assert guesser.post("/api/device/enroll", json={"pairing_code": "wrong"}).status_code == 403
        assert guesser.post("/api/device/enroll", json={"pairing_code": CODE}).status_code == 403
        enrolled = phone.post("/api/device/enroll", json={"pairing_code": CODE, "device_name": "pixel"})
        assert enrolled.status_code == 201


def test_rotation_keeps_the_old_token_for_the_grace_window(store: Store) -> None:
    clock = [1000.0]
    timed = Store(":memory:", now=lambda: clock[0])
    app = create_app(timed, Settings(pairing_code=CODE, device_token_grace=60))
    with TestClient(app) as c:
        old = enroll(c)["token"]
        rotated = c.post("/api/device/token/rotate", headers={"authorization": f"Bearer {old}"}).json()
        new = rotated["token"]
        assert new != old
        for token in (old, new):
            assert (
                c.get("/api/conversations", headers={"authorization": f"Bearer {token}"}).status_code == 200
            )
        clock[0] += 61
        assert c.get("/api/conversations", headers={"authorization": f"Bearer {old}"}).status_code == 403
        assert c.get("/api/conversations", headers={"authorization": f"Bearer {new}"}).status_code == 200
    timed.close()


def test_a_disabled_device_is_refused_until_enabled(client: TestClient) -> None:
    csrf = paired(client)
    phone = enroll(client)
    auth = {"authorization": f"Bearer {phone['token']}"}
    assert client.post(f"/api/devices/{phone['id']}/disable", headers=csrf).json()["disabled"] is True
    assert client.get("/api/conversations", headers=auth).status_code == 403
    client.post(f"/api/devices/{phone['id']}/enable", headers=csrf)
    assert client.get("/api/conversations", headers=auth).status_code == 200
    assert [d["label"] for d in client.get("/api/devices", headers=auth).json()] == ["pixel"]


def test_a_device_opens_the_websocket_with_its_bearer(client: TestClient) -> None:
    phone = enroll(client)
    auth = {"authorization": f"Bearer {phone['token']}"}
    binding_id, _ = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=auth).json()
    client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "hi"},
        headers=auth,
    )
    with client.websocket_connect("/api/events", headers=auth) as ws:
        assert ws.receive_json()["type"] == "ready"
        ws.send_json({"type": "subscribe", "conversation_id": dm["id"]})
        replay = receive_until(ws, "subscribed")
        assert [m["event"]["type"] for m in replay if m["type"] == "event"] == [
            "message.accepted",
            "delivery.changed",
        ]


def test_a_version_1_database_upgrades_in_place(tmp_path: Any) -> None:
    import sqlite3

    from duplex_broker.store import MIGRATIONS, SCHEMA_VERSION

    path = tmp_path / "v1.db"
    db = sqlite3.connect(path)
    for statement in MIGRATIONS[1].split(";"):
        if statement.strip():
            db.execute(statement)
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (1)")
    db.execute(
        "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
        " VALUES ('c1', 'group', 'kept', 'owner', 'user_directed', NULL, 0)"
    )
    db.execute(
        "INSERT INTO messages (id, conversation_id, actor_kind, actor_id, client_message_id, content_sha,"
        " text, recipients, parent_id, created_at) VALUES ('m1', 'c1', 'user', 'owner', 'x', 's', 'old',"
        " '[]', NULL, 0)"
    )
    db.commit()
    db.close()

    upgraded = Store(path)
    assert upgraded.list_devices() == []
    upgraded.enroll_device("pixel")
    assert [d.label for d in upgraded.list_devices()] == ["pixel"]
    upgraded.close()
    check = sqlite3.connect(path)
    assert check.execute("SELECT version FROM schema_version").fetchone()[0] == SCHEMA_VERSION
    assert check.execute("SELECT title FROM conversations").fetchone()[0] == "kept"
    assert check.execute("SELECT root_id, depth FROM messages").fetchone() == (None, 0)
    assert check.execute("SELECT COUNT(*) FROM collaborations").fetchone()[0] == 0
    assert check.execute("SELECT COUNT(adapter) FROM session_bindings").fetchone()[0] == 0
    check.close()


def test_the_owner_retires_a_session_and_its_adapter_is_told_it_is_gone(client: TestClient) -> None:
    csrf = paired(client)
    binding_id, auth = register(client, "codex", native="thread-x")
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()
    sent = client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "hi"},
        headers=csrf,
    ).json()

    assert client.post(f"/api/sessions/{binding_id}/retire", headers=auth).status_code in (401, 403)
    retired = client.post(f"/api/sessions/{binding_id}/retire", headers=csrf)
    assert retired.json()["active"] is False
    assert client.post("/api/adapter/claim", headers=auth).status_code == 410
    assert [s["id"] for s in client.get("/api/sessions").json()] == []
    events = client.get(f"/api/conversations/{dm['id']}/events").json()
    states = [e["payload"]["state"] for e in events if e["type"] == "delivery.changed"]
    assert states == ["queued", "cancelled"]
    assert sent["deliveries"][0]["state"] == "queued"

    again, _ = register(client, "codex", native="thread-x")
    assert again != binding_id


def register_again(client: TestClient, native: str, current_token: str | None = None) -> Any:
    body: dict[str, Any] = {
        "provider": "codex",
        "native_id": native,
        "title": "codex session",
        "workspace": "D:/work/x",
        "endpoint_ref": "local",
        "capabilities": {"receive_messages": "verified"},
    }
    if current_token is not None:
        body["current_token"] = current_token
    return client.post("/api/adapter/register", headers={"authorization": f"Bearer {ENROLL}"}, json=body)


def test_an_active_session_registers_again_only_with_its_token_or_after_release(
    client: TestClient,
) -> None:
    csrf = paired(client)
    binding_id, auth = register(client, "codex", native="thread-y")
    held = auth["authorization"].removeprefix("Bearer ")

    taken = register_again(client, "thread-y")
    assert taken.status_code == 409, taken.text
    assert register_again(client, "thread-y", "not-the-token").status_code == 409
    assert client.post("/api/adapter/claim", headers=auth).status_code == 204

    again = register_again(client, "thread-y", held)
    assert again.status_code == 201, again.text
    assert again.json()["binding"]["id"] == binding_id
    assert client.post("/api/adapter/claim", headers=auth).status_code == 403
    current = {"authorization": f"Bearer {again.json()['token']}"}

    assert client.post(f"/api/sessions/{binding_id}/release", headers=current).status_code in (401, 403)
    released = client.post(f"/api/sessions/{binding_id}/release", headers=csrf)
    assert released.status_code == 200, released.text
    assert client.post("/api/adapter/claim", headers=current).status_code == 403
    after = register_again(client, "thread-y")
    assert after.status_code == 201, after.text
    assert after.json()["binding"]["id"] == binding_id
    assert register_again(client, "thread-y").status_code == 409


# Modes


def group_of_two(
    client: TestClient, csrf: dict[str, str]
) -> tuple[str, str, dict[str, str], str, dict[str, str]]:
    claude, claude_auth = register(client, "claude")
    codex, codex_auth = register(client, "codex")
    group = client.post("/api/conversations", json={"title": "project"}, headers=csrf).json()["id"]
    for member in (claude, codex):
        client.post(f"/api/conversations/{group}/members", json={"binding_id": member}, headers=csrf)
    return group, claude, claude_auth, codex, codex_auth


def test_the_owner_grants_a_consult_and_a_session_uses_it(client: TestClient) -> None:
    csrf = paired(client)
    group, claude, claude_auth, codex, codex_auth = group_of_two(client, csrf)
    mode_url = f"/api/conversations/{group}/mode"

    assert client.post(mode_url, json={"mode": "consult"}, headers=claude_auth).status_code in (401, 403)
    assert client.post(mode_url, json={"mode": "collaborate"}, headers=csrf).status_code == 422
    assert client.post(mode_url, json={"mode": "consult"}, headers=csrf).json()["mode"] == "consult"

    root = client.post(
        f"/api/conversations/{group}/messages",
        json={"client_message_id": cid(), "text": "ask codex", "recipients": [claude]},
        headers=csrf,
    ).json()["message"]["id"]
    claimed = client.post("/api/adapter/claim", headers=claude_auth).json()
    titles = {m["id"]: m["title"] for m in claimed["conversation"]["members"]}
    assert titles == {"owner": "owner", claude: "claude session", codex: "codex session"}
    assert claimed["conversation"]["mode"] == "consult"
    assert "workspace" not in claimed["conversation"]["members"][1]

    def ask(recipients: list[str], parent: str | None) -> Any:
        return client.post(
            "/api/adapter/messages",
            headers=claude_auth,
            json={
                "conversation_id": group,
                "client_message_id": cid(),
                "text": "codex, which one?",
                "recipients": recipients,
                "parent_id": parent,
            },
        )

    assert ask([codex], None).status_code == 403
    assert ask([codex], root).status_code == 201
    assert ask([codex], root).status_code == 403
    assert client.post("/api/adapter/claim", headers=codex_auth).json()["message"]["depth"] == 1


def test_a_collaboration_is_started_seen_and_stopped_over_http(client: TestClient) -> None:
    csrf = paired(client)
    group, _, claude_auth, _, _ = group_of_two(client, csrf)
    url = f"/api/conversations/{group}/collaboration"

    assert client.post(url, json={"objective": "x", "max_turns": 13}, headers=csrf).status_code == 422
    assert client.post(url, json={"objective": "x"}, headers=claude_auth).status_code in (401, 403)
    started = client.post(url, json={"objective": "agree a schedule", "max_turns": 2}, headers=csrf).json()
    assert (started["mode"], started["collaboration"]["max_turns"]) == ("collaborate", 2)

    seen = client.get(f"/api/adapter/conversations/{group}", headers=claude_auth).json()
    assert seen["collaboration"]["objective"] == "agree a schedule"
    assert client.delete(url, headers=claude_auth).status_code in (401, 403)

    stopped = client.delete(url, headers=csrf).json()
    assert stopped["mode"] == "user_directed"
    assert stopped["collaboration"]["stopped_reason"] == "stopped by owner"
    assert client.delete(url).status_code == 403  # a browser write needs the CSRF token


def test_the_sweep_stops_a_collaboration_at_its_deadline(tmp_path: Any) -> None:
    now = [1_000_000.0]
    store = Store(tmp_path / "broker.db", now=lambda: now[0])
    app = create_app(store, Settings(pairing_code=CODE, enrollment_token=ENROLL, collaboration_sweep=0.02))
    with TestClient(app) as client:
        csrf = paired(client)
        group, *_ = group_of_two(client, csrf)
        client.post(
            f"/api/conversations/{group}/collaboration", json={"objective": "x", "minutes": 1}, headers=csrf
        )
        now[0] += 61
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            detail = client.get(f"/api/conversations/{group}").json()
            if detail["mode"] == "user_directed":
                break
            time.sleep(0.02)
        assert detail["collaboration"]["stopped_reason"] == "time limit"
    store.close()


def test_the_owner_removes_and_restores_a_group_over_http(client: TestClient) -> None:
    csrf = paired(client)
    group, _, claude_auth, _, _ = group_of_two(client, csrf)

    assert client.post(f"/api/conversations/{group}/archive", headers=claude_auth).status_code in (401, 403)
    assert client.post(f"/api/conversations/{group}/archive", headers=csrf).json()["archived"] is True
    assert group not in [c["id"] for c in client.get("/api/conversations").json()]
    assert group not in [
        c["id"] for c in client.get("/api/adapter/conversations", headers=claude_auth).json()
    ]
    refused = client.post(
        f"/api/conversations/{group}/messages",
        json={"client_message_id": cid(), "text": "x", "recipients": []},
        headers=csrf,
    )
    assert refused.status_code == 403
    assert client.post(f"/api/conversations/{group}/restore", headers=csrf).json()["archived"] is False
    assert group in [c["id"] for c in client.get("/api/conversations").json()]
    assert (
        client.post(f"/api/conversations/{group}/mode", json={"mode": "consult"}, headers=csrf).status_code
        == 200
    )


def test_the_owner_redirects_a_message_and_a_session_cannot(client: TestClient) -> None:
    csrf = paired(client)
    group, claude, claude_auth, codex, _ = group_of_two(client, csrf)
    sent = client.post(
        f"/api/conversations/{group}/messages",
        json={"client_message_id": cid(), "text": "for codex", "recipients": [claude]},
        headers=csrf,
    ).json()
    delivery = sent["deliveries"][0]["id"]
    body = {"to": codex, "expected_state": "queued"}

    assert client.post(
        f"/api/deliveries/{delivery}/redirect", json=body, headers=claude_auth
    ).status_code in (
        401,
        403,
    )
    first = client.post(f"/api/deliveries/{delivery}/redirect", json=body, headers=csrf)
    assert first.status_code == 201, first.text
    assert (first.json()["disposition"], first.json()["delivery"]["binding_id"]) == ("moved", codex)
    again = client.post(f"/api/deliveries/{delivery}/redirect", json=body, headers=csrf)
    assert again.status_code == 200
    assert again.json()["message"]["id"] == first.json()["message"]["id"]
    stale = client.post(
        f"/api/deliveries/{delivery}/redirect", json={"to": claude, "expected_state": "queued"}, headers=csrf
    )
    assert stale.status_code == 409


# Status page


def test_the_page_is_served_and_reads_by_relative_path(client: TestClient) -> None:
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert r.headers["cache-control"] == "no-store"
    assert '["GET", "api/status"]' in r.text
    # an absolute path escapes the ingress prefix
    assert not any(q + "/api" in r.text for q in "\"'`")


def test_status_needs_the_owner(client: TestClient) -> None:
    _, auth = register(client)
    assert client.get("/api/status").status_code == 401
    assert client.get("/api/status", headers=auth).status_code == 401
    paired(client)
    assert client.get("/api/status").status_code == 200
    assert client.get("/api/status?days=0").status_code == 422


def test_status_counts_a_round_trip_and_carries_no_text(client: TestClient) -> None:
    csrf = paired(client)
    binding_id, auth = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()
    before = client.get("/api/status").json()
    assert before["sessions"][0]["polled_at"] is None

    client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "nonce-status-1"},
        headers=csrf,
    )
    claimed = client.post("/api/adapter/claim", headers=auth).json()
    delivery = claimed["delivery"]["id"]
    for state in ("accepted", "consumed"):
        client.post(f"/api/adapter/deliveries/{delivery}/state", json={"state": state}, headers=auth)
    client.post(
        "/api/adapter/messages",
        headers=auth,
        json={"conversation_id": dm["id"], "client_message_id": cid(), "text": "nonce-status-2"},
    )

    r = client.get("/api/status")
    assert "nonce-status" not in r.text
    doc = r.json()
    session = doc["sessions"][0]
    assert session["id"] == binding_id
    assert session["polled_at"] is not None and session["polled_at"] >= before["now"]
    assert session["messages"] == 1
    assert session["deliveries"] == {"consumed": 1}
    assert [m["sender"] for m in doc["messages"]] == ["user", "claude"]
    assert [(d["state"], d["provider"]) for d in doc["deliveries"]] == [("consumed", "claude")]
    assert doc["deliveries"][0]["read_after"] >= 0
    conversation = next(c for c in doc["conversations"] if c["id"] == dm["id"])
    assert (conversation["messages"], conversation["open"]) == (2, 0)
    assert sorted(conversation["members"]) == ["claude", "user"]
    assert doc["open"] == []
    assert doc["model"]["enabled"] is False


def test_status_lists_an_open_delivery(client: TestClient) -> None:
    csrf = paired(client)
    binding_id, auth = register(client)
    dm = client.post("/api/direct-conversations", json={"binding_id": binding_id}, headers=csrf).json()
    client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": cid(), "text": "x"},
        headers=csrf,
    )
    queued = client.get("/api/status").json()["open"]
    assert [(o["state"], o["binding_id"], o["conversation_id"]) for o in queued] == [
        ("queued", binding_id, dm["id"])
    ]
    delivery = client.post("/api/adapter/claim", headers=auth).json()["delivery"]["id"]
    client.post(f"/api/adapter/deliveries/{delivery}/state", json={"state": "accepted"}, headers=auth)
    assert [o["state"] for o in client.get("/api/status").json()["open"]] == ["accepted"]
    assert client.get("/api/status").json()["deliveries"][0]["read_after"] is None


# Envelope and receipts


def test_a_claimed_message_carries_its_envelope_and_the_owner_reads_each_recipients_read(
    client: TestClient,
) -> None:
    csrf = paired(client)
    group, claude, claude_auth, codex, codex_auth = group_of_two(client, csrf)
    sent = client.post(
        f"/api/conversations/{group}/messages",
        json={"client_message_id": cid(), "text": "both", "recipients": [claude, codex]},
        headers=csrf,
    ).json()["message"]
    owner = {"kind": "user", "id": "owner", "title": "owner"}
    assert sent["from"] == owner and len(sent["to"]) == 2 and sent["sent_at"].endswith("Z")
    message = client.post("/api/adapter/claim", headers=claude_auth).json()["message"]
    assert (message["from"], message["to"], message["sent_at"]) == (owner, sent["to"], sent["sent_at"])
    events = client.get(f"/api/conversations/{group}/events").json()
    payload = next(e["payload"] for e in events if e["type"] == "message.accepted")
    assert (payload["from"], payload["to"], payload["sent_at"]) == (owner, sent["to"], sent["sent_at"])

    claimed = client.post("/api/adapter/claim", headers=codex_auth).json()["delivery"]["id"]
    for state in ("accepted", "consumed"):
        client.post(f"/api/adapter/deliveries/{claimed}/state", json={"state": state}, headers=codex_auth)
    record = client.get(f"/api/messages/{sent['id']}").json()
    reads = {d["to"]: (d["state"], d["read"], d["read_at"] is not None) for d in record["deliveries"]}
    assert reads == {
        "claude session": ("dispatching", False, False),
        "codex session": ("consumed", True, True),
    }
    assert record["message"]["to"] == sent["to"]


def test_a_session_takes_its_receipts_once_and_is_not_woken_for_them(client: TestClient) -> None:
    csrf = paired(client)
    group, claude, claude_auth, codex, codex_auth = group_of_two(client, csrf)
    client.post(f"/api/conversations/{group}/mode", json={"mode": "consult"}, headers=csrf)
    root = client.post(
        f"/api/conversations/{group}/messages",
        json={"client_message_id": cid(), "text": "ask codex", "recipients": [claude]},
        headers=csrf,
    ).json()["message"]["id"]
    handed = client.post("/api/adapter/claim", headers=claude_auth).json()["delivery"]["id"]
    for state in ("accepted", "consumed"):
        client.post(f"/api/adapter/deliveries/{handed}/state", json={"state": state}, headers=claude_auth)
    question = client.post(
        "/api/adapter/messages",
        headers=claude_auth,
        json={
            "conversation_id": group,
            "client_message_id": cid(),
            "text": "codex?",
            "recipients": [codex],
            "parent_id": root,
        },
    ).json()["message"]["id"]
    asked = client.post("/api/adapter/claim", headers=codex_auth).json()["delivery"]["id"]
    for state in ("accepted", "consumed"):
        client.post(f"/api/adapter/deliveries/{asked}/state", json={"state": state}, headers=codex_auth)
    assert client.post("/api/adapter/claim", headers=claude_auth).status_code == 204  # no wake for a receipt
    (receipt,) = client.post("/api/adapter/receipts", headers=claude_auth).json()
    assert (receipt["message_id"], receipt["recipient"], receipt["state"]) == (
        question,
        "codex session",
        "consumed",
    )
    assert receipt["read_at"].endswith("Z") and receipt["conversation_id"] == str(uuid.UUID(hex=group))
    assert client.post("/api/adapter/receipts", headers=claude_auth).json() == []


# Adapter versions

BUILT = {
    "claude_code": {"version": "2.1.318", "reviewed": "2026-11-04", "notes": "inbox"},
    "codex": {"version": "0.166.0", "reviewed": "2026-11-04", "notes": "queue"},
}


def register_versioned(
    client: TestClient, native: str, version: str | None, provider: str = "claude", token: str = ""
) -> tuple[str, dict[str, str], dict[str, Any]]:
    body: dict[str, Any] = {
        "provider": provider,
        "native_id": native,
        "title": f"{native} session",
        "workspace": "w",
        "endpoint_ref": "claude-inbox",
        "capabilities": {},
        "current_token": token,
    }
    if version is not None:
        body["adapter"] = {
            "version": version,
            "commit": "4c88199",
            "built_against": BUILT,
            "runtime": {"claude_code": "2.1.317"},
            "later": "ignored",
        }
    r = client.post("/api/adapter/register", headers={"authorization": f"Bearer {ENROLL}"}, json=body)
    assert r.status_code == 201, r.text
    answer = r.json()
    return answer["binding"]["id"], {"authorization": f"Bearer {answer['token']}"}, answer["binding"]


def test_each_binding_keeps_its_adapter_and_says_when_it_is_behind(client: TestClient) -> None:
    csrf = paired(client)
    new, new_auth, answered = register_versioned(client, "new", "1.10.0")
    assert answered["adapter"]["version"] == "1.10.0" and answered["adapter_behind"] is False
    assert "later" not in answered["adapter"]
    assert answered["adapter"]["built_against"]["claude_code"]["version"] == "2.1.318"
    old, old_auth, _ = register_versioned(client, "old", None, provider="codex")
    mid, _, _ = register_versioned(client, "mid", "1.9.0")

    listed = {s["id"]: s for s in client.get("/api/sessions").json()}
    assert listed[new]["adapter_behind"] is False
    assert listed[old]["adapter"] is None and listed[old]["adapter_behind"] is True
    assert listed[mid]["adapter_behind"] is True  # 1.9 is below 1.10, numerically

    group = client.post("/api/conversations", json={"title": "versions"}, headers=csrf).json()["id"]
    for member in (new, old):
        client.post(f"/api/conversations/{group}/members", json={"binding_id": member}, headers=csrf)
    detail = client.get(f"/api/adapter/conversations/{group}", headers=new_auth).json()
    members = {m["id"]: m for m in detail["members"]}
    assert members[old]["adapter_behind"] is True and members[new]["adapter"]["commit"] == "4c88199"
    assert members["owner"]["adapter_behind"] is None

    own = client.get("/api/adapter/binding", headers=old_auth).json()
    assert own["id"] == old and own["adapter_behind"] is True

    # The newest session restarts under an older adapter: nobody is behind 1.9.0 but the unversioned one.
    held = new_auth["authorization"].removeprefix("Bearer ")
    register_versioned(client, "new", "1.9.0", token=held)
    listed = {s["id"]: s for s in client.get("/api/sessions").json()}
    assert (listed[new]["adapter_behind"], listed[mid]["adapter_behind"], listed[old]["adapter_behind"]) == (
        False,
        False,
        True,
    )


def test_a_retired_newer_adapter_no_longer_sets_the_bar(client: TestClient) -> None:
    csrf = paired(client)
    new, _, _ = register_versioned(client, "new", "2.0.0")
    old, _, _ = register_versioned(client, "old", "1.0.0")
    assert client.post(f"/api/sessions/{new}/retire", headers=csrf).json()["adapter_behind"] is False
    listed = {s["id"]: s for s in client.get("/api/sessions").json()}
    assert listed[old]["adapter_behind"] is False


def test_an_adapter_report_out_of_bounds_is_refused(client: TestClient) -> None:
    body = {
        "provider": "claude",
        "native_id": "n",
        "title": "t",
        "workspace": "w",
        "endpoint_ref": "e",
        "capabilities": {},
        "adapter": {"version": "1" * 41},
    }
    r = client.post("/api/adapter/register", headers={"authorization": f"Bearer {ENROLL}"}, json=body)
    assert r.status_code == 422

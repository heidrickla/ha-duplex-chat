from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from duplex_broker.api import Settings, create_app
from duplex_broker.ladder import Policy, late_read, record_wake, sweep, take_rewake
from duplex_broker.store import (
    BROKER_BINDING_ID,
    MIGRATIONS,
    NEW_PROVIDERS,
    OLD_PROVIDERS,
    SCHEMA_VERSION,
    Actor,
    Binding,
    Conflict,
    Forbidden,
    Invalid,
    Store,
)

OWNER = Actor.user("owner")
POLICY = Policy(wake_timeout=300, rewake_interval=120, rewake_limit=3, hold_after=2)
REWAKE = {"receive_messages": "verified", "rewake": "verified"}
CLOSED_AFTER_THREE = "not read within 660 s; 3 re-wakes asked for, 3 sent by the adapter"


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000.0

    def __call__(self) -> float:
        return self.t


def bind(store: Store, capabilities: dict[str, object] | None = None, native: str = "") -> Binding:
    binding, _ = store.register_binding(
        "claude", native or uuid.uuid4().hex, "laddered", "w", "claude-inbox", capabilities or REWAKE
    )
    return binding


def dm(store: Store, binding: Binding, *texts: str) -> list[str]:
    conversation = store.open_direct("owner", binding.id)
    return [
        store.post_message(OWNER, conversation.id, uuid.uuid4().hex, text).deliveries[0].id for text in texts
    ]


def hand_over(store: Store, binding: Binding) -> str:
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert claimed is not None
    delivery, _ = claimed
    store.transition(delivery.id, binding.id, binding.lease_generation, "accepted")
    return delivery.id


def types(store: Store, conversation_id: str) -> list[str]:
    return [e.type for e in store.events_after(OWNER, conversation_id)]


def run_out(store: Store, clock: Clock, binding: Binding, policy: Policy = POLICY) -> None:
    """Walk one in-flight delivery through every re-wake to its close, as the sweep and adapter would."""
    sweep(store, policy, "owner")  # sets the deadline
    clock.t += policy.wake_timeout
    for _ in range(policy.rewake_limit):
        assert sweep(store, policy, "owner") == 1
        due = take_rewake(store, binding.id, binding.lease_generation)
        assert due is not None
        record_wake(store, due[0].id, binding.id, binding.lease_generation, due[0].rewakes, True, None)
        clock.t += policy.rewake_interval
    assert sweep(store, policy, "owner") == 1


def test_n_rewakes_then_uncertain_and_the_next_message_goes_out(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    first, second = dm(store, session, "first", "second")
    assert hand_over(store, session) == first
    assert store.claim_next(session.id, session.lease_generation) is None  # head of line

    assert sweep(store, POLICY, "owner") == 0  # deadline set, nothing due
    clock.t += POLICY.wake_timeout - 1
    assert sweep(store, POLICY, "owner") == 0
    attempts = []
    clock.t += 1
    for _ in range(POLICY.rewake_limit):
        assert sweep(store, POLICY, "owner") == 1
        due = take_rewake(store, session.id, session.lease_generation)
        assert due is not None
        delivery, message, kind = due
        assert (delivery.id, message.text, kind) == (first, "first", "direct")
        attempts.append(delivery.rewakes)
        record_wake(store, first, session.id, session.lease_generation, delivery.rewakes, True, None)
        assert take_rewake(store, session.id, session.lease_generation) is None  # handed out once
        assert sweep(store, POLICY, "owner") == 0
        assert store.claim_next(session.id, session.lease_generation) is None
        clock.t += POLICY.rewake_interval
    assert attempts == [1, 2, 3]

    assert sweep(store, POLICY, "owner") == 1
    claimed = store.claim_next(session.id, session.lease_generation)
    assert claimed is not None and claimed[0].id == second
    conversation = store.open_direct("owner", session.id).id
    events = store.events_after(OWNER, conversation)
    uncertain = [e for e in events if e.type == "delivery.changed" and e.payload["state"] == "uncertain"]
    assert len(uncertain) == 1
    assert uncertain[0].payload["delivery_id"] == first
    assert uncertain[0].payload["reason"] == CLOSED_AFTER_THREE
    assert [e.payload["attempt"] for e in events if e.type == "delivery.wake_requested"] == [1, 2, 3]
    assert types(store, conversation).count("delivery.expired") == 1
    store.close()


def test_a_restart_mid_ladder_keeps_the_count(tmp_path: Path) -> None:
    clock = Clock()
    path = tmp_path / "b.db"
    store = Store(path, now=clock)
    session = bind(store)
    (first,) = dm(store, session, "first")
    hand_over(store, session)
    sweep(store, POLICY, "owner")
    clock.t += POLICY.wake_timeout
    assert sweep(store, POLICY, "owner") == 1
    assert take_rewake(store, session.id, session.lease_generation) is not None
    clock.t += POLICY.rewake_interval
    assert sweep(store, POLICY, "owner") == 1  # second re-wake requested, not yet handed out
    store.close()

    store = Store(path, now=clock)
    due = take_rewake(store, session.id, session.lease_generation)
    assert due is not None and due[0].rewakes == 2
    clock.t += POLICY.rewake_interval
    assert sweep(store, POLICY, "owner") == 1
    clock.t += POLICY.rewake_interval
    assert sweep(store, POLICY, "owner") == 1
    states = [
        e.payload.get("state")
        for e in store.events_after(OWNER, store.open_direct("owner", session.id).id)
        if e.type == "delivery.changed"
    ]
    assert states[-1] == "uncertain"
    conversation = store.open_direct("owner", session.id).id
    assert [
        e.payload["attempt"]
        for e in store.events_after(OWNER, conversation)
        if e.type == "delivery.wake_requested"
    ] == [1, 2, 3]
    assert store.deliveries_for(store.events_after(OWNER, conversation)[0].message_id or "")[0].id == first
    store.close()


def test_the_circuit_holds_after_k_closures_and_resumes_on_a_read(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    first, second, third, fourth = dm(store, session, "one", "two", "three", "four")
    hand_over(store, session)
    run_out(store, clock, session)
    assert store.get_binding(session.id).expiries == 1
    assert store.get_binding(session.id).held_at is None
    assert hand_over(store, session) == second
    run_out(store, clock, session)

    held = store.get_binding(session.id)
    assert (held.expiries, held.held_at) == (2, clock.t)
    assert store.claim_next(session.id, session.lease_generation) is None  # third stays queued
    notices = next(c for c in store.conversations_for(OWNER) if c.dm_binding_id == BROKER_BINDING_ID)
    posts = [e for e in store.events_after(OWNER, notices.id) if e.type == "message.accepted"]
    # Each closure is a failure notice to the owner, who sent the message; the hold is one more post.
    assert [e.payload["notice"]["state"] for e in posts if "notice" in e.payload] == ["uncertain"] * 2
    notice = [e for e in posts if "notice" not in e.payload]
    assert len(notice) == 1
    assert notice[0].actor == Actor.binding(BROKER_BINDING_ID)
    assert "Delivery to laddered is held" in notice[0].payload["text"]
    assert "2 more wait in its queue" in notice[0].payload["text"]
    assert types(store, notices.id).count("session.held") == 1
    assert sweep(store, POLICY, "owner") == 0  # held: nothing more times out one by one
    clock.t += 10 * POLICY.window
    assert sweep(store, POLICY, "owner") == 0
    assert [b.id for b in store.list_bindings()] == [session.id]  # the notices member is not offered

    pointer = late_read(store, second, session.id, session.lease_generation)
    assert pointer["state"] == "uncertain"
    resumed = store.get_binding(session.id)
    assert (resumed.expiries, resumed.held_at) == (0, None)
    assert types(store, notices.id).count("session.resumed") == 1
    assert hand_over(store, session) == third
    store.transition(third, session.id, session.lease_generation, "consumed")
    assert hand_over(store, session) == fourth
    assert first != fourth
    store.close()


def test_a_read_resets_the_count_and_a_registration_ends_a_hold(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store, native="native-1")
    dm(store, session, "one", "two", "three", "four", "five")
    hand_over(store, session)
    run_out(store, clock, session)
    read = hand_over(store, session)
    store.transition(read, session.id, session.lease_generation, "consumed")
    assert store.get_binding(session.id).expiries == 0
    hand_over(store, session)
    run_out(store, clock, session)
    hand_over(store, session)
    run_out(store, clock, session)
    assert store.get_binding(session.id).held_at is not None

    again, _ = store.register_binding("claude", "native-1", "laddered", "w", "claude-inbox", REWAKE)
    assert (again.held_at, again.expiries) == (None, 0)
    assert store.claim_next(again.id, again.lease_generation) is not None
    store.close()


def test_a_late_read_gets_a_pointer_never_the_text(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    (first,) = dm(store, session, "PAYLOAD-MARKER do the thing")
    hand_over(store, session)
    run_out(store, clock, session)
    with pytest.raises(Invalid):
        store.transition(first, session.id, session.lease_generation, "consumed")

    pointer = late_read(store, first, session.id, session.lease_generation)
    assert "PAYLOAD-MARKER" not in json.dumps(pointer)
    conversation = store.open_direct("owner", session.id)
    assert pointer["conversation_id"] == conversation.id
    assert pointer["from"] == {"kind": "user", "id": "owner", "title": "owner"}
    assert pointer["age_seconds"] == round(POLICY.window)
    assert pointer["reason"] == CLOSED_AFTER_THREE
    accepted = next(e for e in store.events_after(OWNER, conversation.id) if e.type == "message.accepted")
    assert pointer["history_after"] == accepted.seq - 1
    assert "delivery.read_late" in types(store, conversation.id)

    other = bind(store)
    with pytest.raises(Forbidden):
        late_read(store, first, other.id, other.lease_generation)
    (open_one,) = dm(store, session, "still open")
    hand_over(store, session)
    with pytest.raises(Invalid):
        late_read(store, open_one, session.id, session.lease_generation)
    store.close()


def test_a_read_before_the_deadline_is_untouched(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    (first,) = dm(store, session, "quick")
    hand_over(store, session)
    assert sweep(store, POLICY, "owner") == 0
    clock.t += POLICY.wake_timeout - 5
    store.transition(first, session.id, session.lease_generation, "consumed")
    clock.t += 10 * POLICY.window
    assert sweep(store, POLICY, "owner") == 0
    assert take_rewake(store, session.id, session.lease_generation) is None
    conversation = store.open_direct("owner", session.id).id
    assert not {"delivery.wake_requested", "delivery.expired"} & set(types(store, conversation))
    assert (
        store.deliveries_for(store.events_after(OWNER, conversation)[0].message_id or "")[0].state
        == "consumed"
    )
    store.close()


def test_a_session_without_the_capability_is_never_laddered(tmp_path: Path) -> None:
    # The App Server Codex adapter holds `accepted` while its turn runs.
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store, {"receive_messages": "verified"})
    dm(store, session, "long turn")
    hand_over(store, session)
    for _ in range(10):
        clock.t += POLICY.window
        assert sweep(store, POLICY, "owner") == 0
    assert take_rewake(store, session.id, session.lease_generation) is None
    store.close()


def test_every_non_normal_end_carries_a_reason(tmp_path: Path) -> None:
    store = Store(tmp_path / "b.db")
    session = bind(store, native="n")
    a, b, c = dm(store, session, "a", "b", "c")
    conversation = store.open_direct("owner", session.id).id
    hand_over(store, session)
    store.transition(a, session.id, session.lease_generation, "failed")
    claimed = store.claim_next(session.id, session.lease_generation)
    assert claimed is not None
    store.transition(b, session.id, session.lease_generation, "failed", reason="wake failed: pipe closed")
    store.cancel_pending(OWNER, conversation)
    ends = {
        e.payload["delivery_id"]: (e.payload["state"], e.payload.get("reason"))
        for e in store.events_after(OWNER, conversation)
        if e.type == "delivery.changed" and e.payload["state"] in ("failed", "cancelled", "uncertain")
    }
    assert ends == {
        a: ("failed", "the adapter reported failed with no reason"),
        b: ("failed", "wake failed: pipe closed"),
        c: ("cancelled", "the owner stopped the conversation"),
    }
    assert store.deliveries_for(store.events_after(OWNER, conversation)[0].message_id or "")[0].reason
    with pytest.raises(ValueError), store._write() as db:
        store._set_state(db, a, "uncertain", None, OWNER)
    store.close()


def test_the_owner_cannot_post_to_the_notices(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    dm(store, session, "one", "two")
    hand_over(store, session)
    run_out(store, clock, session)
    hand_over(store, session)
    run_out(store, clock, session)
    notices = next(c for c in store.conversations_for(OWNER) if c.dm_binding_id == BROKER_BINDING_ID)
    with pytest.raises(Forbidden):
        store.post_message(OWNER, notices.id, uuid.uuid4().hex, "hello?")
    with pytest.raises(Invalid):
        store.open_direct("owner", BROKER_BINDING_ID)
    store.close()


def test_a_version_5_database_upgrades(tmp_path: Path) -> None:
    path = tmp_path / "v5.db"
    v5 = sqlite3.connect(path, isolation_level=None)
    v5.row_factory = sqlite3.Row
    for step in range(1, 6):
        if step == 5:
            Store._widen_providers(v5, OLD_PROVIDERS, NEW_PROVIDERS)
        for statement in MIGRATIONS[step].split(";"):
            if statement.strip():
                v5.execute(statement)
    v5.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    v5.execute("INSERT INTO schema_version VALUES (5)")
    v5.execute(
        "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref, capabilities,"
        " lease_generation, token_sha, active, heartbeat_at, created_at)"
        " VALUES ('b1', 'claude', 'n1', 'kept', 'w', 'e', ?, 1, 't', 1, NULL, 0)",
        (json.dumps(REWAKE),),
    )
    v5.execute(
        "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
        " VALUES ('c1', 'direct', 'kept', 'owner', 'user_directed', 'b1', 0)"
    )
    v5.execute("INSERT INTO conversation_members VALUES ('c1', 'user', 'owner', 'owner')")
    v5.execute("INSERT INTO conversation_members VALUES ('c1', 'binding', 'b1', 'session')")
    v5.execute(
        "INSERT INTO messages (id, conversation_id, actor_kind, actor_id, client_message_id, content_sha,"
        " text, recipients, parent_id, created_at) VALUES ('m1', 'c1', 'user', 'owner', 'x', 's', 'old',"
        " '[\"b1\"]', NULL, 0)"
    )
    v5.execute(
        "INSERT INTO deliveries (id, message_id, binding_id, state, attempt, native_turn_id,"
        " lease_generation, seq, updated_at) VALUES ('d1', 'm1', 'b1', 'accepted', 1, NULL, 1, 1, 0)"
    )
    v5.execute(
        "INSERT INTO events VALUES ('c1', 1, 'e1', 'message.accepted', 'user', 'owner', 'm1', ?, 0)",
        (json.dumps({"text": "old", "recipients": ["b1"], "parent_id": None}),),
    )
    v5.commit()
    assert v5.execute("SELECT version FROM schema_version").fetchone()[0] == 5
    v5.close()

    clock = Clock()
    clock.t = 0
    store = Store(path, now=clock)
    binding = store.get_binding("b1")
    assert (binding.expiries, binding.held_at) == (0, None)
    clock.t = POLICY.wake_timeout
    assert sweep(store, POLICY, "owner") == 1  # the in-flight delivery from 5 joins the ladder
    due = take_rewake(store, "b1", 1)
    assert due is not None and due[0].id == "d1"
    record_wake(store, "d1", "b1", 1, 1, True, None)
    store.close()
    check = sqlite3.connect(path)
    assert check.execute("SELECT version FROM schema_version").fetchone()[0] == SCHEMA_VERSION == 14
    assert check.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert check.execute("PRAGMA foreign_key_check").fetchall() == []
    assert check.execute("SELECT text FROM messages").fetchone()[0] == "old"
    assert check.execute("SELECT rewakes, reason FROM deliveries").fetchone() == (1, None)
    # Schema 10 gives the message written before it the envelope new messages carry.
    assert json.loads(check.execute("SELECT payload FROM events WHERE event_id = 'e1'").fetchone()[0]) == {
        "text": "old",
        "recipients": ["b1"],
        "parent_id": None,
        "from": {"kind": "user", "id": "owner", "title": "owner"},
        "to": [{"kind": "binding", "id": "b1", "title": "kept"}],
        "sent_at": "1970-01-01T00:00:00Z",
    }
    check.execute(
        "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref,"
        " capabilities, lease_generation, token_sha, active, created_at)"
        " VALUES ('b8', 'broker', 'n', 't', 'w', 'e', '{}', 1, 'u', 0, 0)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        check.execute(
            "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref,"
            " capabilities, lease_generation, token_sha, active, created_at)"
            " VALUES ('b9', 'other', 'n', 't', 'w', 'e', '{}', 1, 'v', 1, 0)"
        )
    check.close()


def test_an_adapter_that_never_polls_is_asked_once_and_the_delivery_still_closes(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    first, second = dm(store, session, "first", "second")
    hand_over(store, session)
    conversation = store.open_direct("owner", session.id).id
    sweep(store, POLICY, "owner")
    clock.t += POLICY.wake_timeout
    assert sweep(store, POLICY, "owner") == 1  # the first re-wake is asked for and never taken
    while clock.t < 1_000_000.0 + POLICY.window - POLICY.rewake_interval:
        clock.t += POLICY.rewake_interval
        assert sweep(store, POLICY, "owner") == 0
        assert (
            store.deliveries_for(store.events_after(OWNER, conversation)[0].message_id or "")[0].rewakes == 1
        )
    clock.t = 1_000_000.0 + POLICY.window
    assert sweep(store, POLICY, "owner") == 1
    expired = next(e for e in store.events_after(OWNER, conversation) if e.type == "delivery.expired")
    assert (expired.payload["rewakes_asked"], expired.payload["rewakes_sent"]) == (1, 0)
    assert expired.payload["reason"] == "not read within 660 s; 1 re-wakes asked for, 0 sent by the adapter"
    assert [
        e.payload["attempt"]
        for e in store.events_after(OWNER, conversation)
        if e.type == "delivery.wake_requested"
    ] == [1]
    claimed = store.claim_next(session.id, session.lease_generation)
    assert claimed is not None and claimed[0].id == second
    assert first != second
    store.close()


def test_the_counts_follow_what_the_adapter_reports_and_the_hold_notice_states_them(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    dm(store, session, "one", "two")
    for _ in range(2):
        hand_over(store, session)
        sweep(store, POLICY, "owner")
        clock.t += POLICY.wake_timeout
        for attempt in (1, 2, 3):
            assert sweep(store, POLICY, "owner") == 1
            due = take_rewake(store, session.id, session.lease_generation)
            assert due is not None
            record_wake(
                store, due[0].id, session.id, session.lease_generation, attempt, attempt != 2, "pipe closed"
            )
            clock.t += POLICY.rewake_interval
        assert sweep(store, POLICY, "owner") == 1
    conversation = store.open_direct("owner", session.id).id
    reasons = [
        e.payload["reason"] for e in store.events_after(OWNER, conversation) if e.type == "delivery.expired"
    ]
    assert reasons == ["not read within 660 s; 3 re-wakes asked for, 2 sent by the adapter"] * 2
    notices = next(c for c in store.conversations_for(OWNER) if c.dm_binding_id == BROKER_BINDING_ID)
    text = next(
        e.payload["text"]
        for e in store.events_after(OWNER, notices.id)
        if e.type == "message.accepted" and "notice" not in e.payload
    )
    assert "6 re-wakes were asked for and the adapter sent 4" in text
    store.close()


def test_a_wake_report_for_a_closed_delivery_or_an_unasked_attempt_changes_nothing(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    (first,) = dm(store, session, "one")
    hand_over(store, session)
    sweep(store, POLICY, "owner")
    clock.t += POLICY.wake_timeout
    sweep(store, POLICY, "owner")
    lease = session.lease_generation
    with pytest.raises(Invalid):
        record_wake(store, first, session.id, lease, 2, True, None)  # only one was asked for
    with pytest.raises(Invalid):
        record_wake(store, first, session.id, lease, 0, True, None)
    clock.t += POLICY.window
    assert sweep(store, POLICY, "owner") == 1
    before = store.events_after(OWNER, store.open_direct("owner", session.id).id)
    with pytest.raises(Conflict):
        record_wake(store, first, session.id, lease, 1, True, None)
    after = store.events_after(OWNER, store.open_direct("owner", session.id).id)
    assert len(after) == len(before)
    assert store.deliveries_for(after[0].message_id or "")[0].wakes_sent == 0
    store.close()


def test_a_reply_to_a_delivery_already_read_does_not_reset_the_count(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    a, _, _ = dm(store, session, "a", "b", "c")
    lease = session.lease_generation
    assert hand_over(store, session) == a
    store.transition(a, session.id, lease, "consumed")
    hand_over(store, session)
    run_out(store, clock, session)
    assert store.get_binding(session.id).expiries == 1
    store.transition(a, session.id, lease, "responded")  # a reply to A, which was read earlier
    assert store.get_binding(session.id).expiries == 1
    hand_over(store, session)
    run_out(store, clock, session)
    assert store.get_binding(session.id).held_at is not None
    store.close()


def test_a_first_read_by_any_route_counts_and_a_second_does_not(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    session = bind(store)
    a, b, c, d = dm(store, session, "a", "b", "c", "d")
    lease = session.lease_generation
    hand_over(store, session)
    run_out(store, clock, session)
    assert store.get_binding(session.id).expiries == 1
    assert hand_over(store, session) == b
    store.transition(b, session.id, lease, "responded")  # accepted straight to responded: a first read
    assert store.get_binding(session.id).expiries == 0
    assert hand_over(store, session) == c
    run_out(store, clock, session)
    assert hand_over(store, session) == d
    run_out(store, clock, session)
    assert store.get_binding(session.id).held_at is not None
    late_read(store, d, session.id, lease)
    assert store.get_binding(session.id).held_at is None
    assert a != d
    (e,) = dm(store, session, "e")
    hand_over(store, session)
    run_out(store, clock, session)
    assert store.get_binding(session.id).expiries == 1
    late_read(store, d, session.id, lease)  # D was already read late
    assert store.get_binding(session.id).expiries == 1
    assert e != d
    conversation = store.open_direct("owner", session.id).id
    late = [x for x in store.events_after(OWNER, conversation) if x.type == "delivery.read_late"]
    assert [x.payload["delivery_id"] for x in late] == [d]
    store.close()


# The adapter's view, over HTTP


ENROLL = "enroll-me"


def test_the_claim_poll_carries_a_due_rewake_and_the_late_read_route_answers(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    settings = Settings(enrollment_token=ENROLL, collaboration_sweep=3600)
    with TestClient(create_app(store, settings)) as client:
        r = client.post(
            "/api/adapter/register",
            headers={"authorization": f"Bearer {ENROLL}"},
            json={
                "provider": "claude",
                "native_id": "n",
                "title": "laddered",
                "workspace": "w",
                "endpoint_ref": "claude-inbox",
                "capabilities": REWAKE,
            },
        )
        auth = {"authorization": f"Bearer {r.json()['token']}"}
        session = store.get_binding(r.json()["binding"]["id"])
        (first,) = dm(store, session, "SECRET-TEXT")
        claimed = client.post("/api/adapter/claim", headers=auth).json()
        assert claimed["delivery"]["id"] == first
        client.post(f"/api/adapter/deliveries/{first}/state", headers=auth, json={"state": "accepted"})
        assert client.post("/api/adapter/claim", headers=auth).status_code == 204

        policy = Policy(
            settings.wake_timeout, settings.rewake_interval, settings.rewake_limit, settings.hold_after
        )
        sweep(store, policy, "owner")
        clock.t += settings.wake_timeout
        sweep(store, policy, "owner")
        r = client.post("/api/adapter/claim", headers=auth)
        assert r.status_code == 200
        rewake = r.json()["rewake"]
        assert rewake == {
            "delivery_id": first,
            "message_id": rewake["message_id"],
            "conversation_id": rewake["conversation_id"],
            "conversation_kind": "direct",
            "attempt": 1,
            "limit": 3,
        }
        assert "SECRET-TEXT" not in r.text
        assert client.post("/api/adapter/claim", headers=auth).status_code == 204
        r = client.post(
            f"/api/adapter/deliveries/{first}/wake", headers=auth, json={"attempt": 1, "ok": True}
        )
        assert r.status_code == 204
        conversation = rewake["conversation_id"]
        outcome = [e for e in store.events_after(OWNER, conversation) if e.type == "delivery.wake_outcome"]
        assert [(e.payload["attempt"], e.payload["ok"]) for e in outcome] == [(1, True)]

        r = client.post(f"/api/adapter/deliveries/{first}/late-read", headers=auth)
        assert r.status_code == 422  # still in flight
        for _ in range(3):
            clock.t += settings.rewake_interval
            sweep(store, policy, "owner")
        r = client.post(f"/api/adapter/deliveries/{first}/state", headers=auth, json={"state": "consumed"})
        assert r.status_code == 422
        r = client.post(f"/api/adapter/deliveries/{first}/late-read", headers=auth)
        assert r.status_code == 200
        assert r.json()["state"] == "uncertain"
        assert "SECRET-TEXT" not in r.text
        r = client.post(
            f"/api/adapter/deliveries/{first}/wake", headers=auth, json={"attempt": 1, "ok": True}
        )
        assert r.status_code == 409  # closed: a late report counts for nothing
        assert store.deliveries_for(rewake["message_id"])[0].wakes_sent == 1
    store.close()

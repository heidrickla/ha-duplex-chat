from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from duplex_broker.store import (
    MAX_MESSAGE_BYTES,
    Actor,
    Binding,
    Conflict,
    Forbidden,
    Gone,
    Invalid,
    Mode,
    Store,
    adapter_behind,
    version_key,
)

OWNER = Actor.user("owner")


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


def bind(store: Store, provider: str = "claude", native: str | None = None) -> Binding:
    return bind_with_token(store, provider, native)[0]


def bind_with_token(store: Store, provider: str = "claude", native: str | None = None) -> tuple[Binding, str]:
    return store.register_binding(
        provider,  # type: ignore[arg-type]
        native or uuid.uuid4().hex,
        f"{provider} session",
        "D:/work/x",
        "local",
        {"receive_messages": "verified"},
    )


def cid() -> str:
    return uuid.uuid4().hex


def all_text(store: Store, reader: Actor, conversation_id: str) -> str:
    return json.dumps([e.payload for e in store.events_after(reader, conversation_id)])


# Direct conversations


def test_direct_message_never_reaches_the_group_or_the_other_session(store: Store) -> None:
    claude, codex = bind(store, "claude"), bind(store, "codex")
    group = store.create_group("owner", "project")
    store.add_member(OWNER, group.id, claude.id)
    store.add_member(OWNER, group.id, codex.id)
    dm = store.open_direct("owner", claude.id)
    marker = f"dm-marker-{uuid.uuid4().hex}"

    store.post_message(OWNER, dm.id, cid(), marker)
    store.post_message(Actor.binding(claude.id), dm.id, cid(), f"reply {marker}")
    store.post_message(OWNER, group.id, cid(), "group hello", [claude.id, codex.id])

    assert marker in all_text(store, OWNER, dm.id)
    assert marker not in all_text(store, OWNER, group.id)
    assert marker not in all_text(store, Actor.binding(codex.id), group.id)
    with pytest.raises(Forbidden):
        store.events_after(Actor.binding(codex.id), dm.id)
    assert dm.id not in {c.id for c in store.conversations_for(Actor.binding(codex.id))}


def test_direct_recipient_is_fixed(store: Store) -> None:
    claude, codex = bind(store, "claude"), bind(store, "codex")
    dm = store.open_direct("owner", claude.id)

    with pytest.raises(Invalid):
        store.post_message(OWNER, dm.id, cid(), "hi", [codex.id])
    with pytest.raises(Invalid):
        store.post_message(OWNER, dm.id, cid(), "hi", [claude.id, codex.id])
    with pytest.raises(Invalid):
        store.post_message(Actor.binding(claude.id), dm.id, cid(), "reply", [claude.id])

    sent = store.post_message(OWNER, dm.id, cid(), "hi")
    assert sent.message.recipients == (claude.id,)
    reply = store.post_message(Actor.binding(claude.id), dm.id, cid(), "reply")
    assert reply.message.recipients == ()
    assert reply.deliveries == ()


def test_direct_membership_is_fixed_and_open_is_idempotent(store: Store) -> None:
    claude, codex = bind(store, "claude"), bind(store, "codex")
    dm = store.open_direct("owner", claude.id)

    assert store.open_direct("owner", claude.id).id == dm.id
    with pytest.raises(Forbidden):
        store.add_member(OWNER, dm.id, codex.id)
    assert store.members(OWNER, dm.id) == [OWNER, Actor.binding(claude.id)]


def test_a_session_can_have_a_dm_without_any_group(store: Store) -> None:
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)
    assert [c.id for c in store.conversations_for(Actor.binding(claude.id))] == [dm.id]


# Messages


def test_retry_returns_the_original_and_different_content_is_refused(store: Store) -> None:
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)
    other = store.create_group("owner", "g")
    store.add_member(OWNER, other.id, claude.id)
    key = cid()

    first = store.post_message(OWNER, dm.id, key, "same")
    again = store.post_message(OWNER, dm.id, key, "same")
    assert again.created is False
    assert again.message.id == first.message.id
    assert len(store.events_after(OWNER, dm.id)) == 2
    with pytest.raises(Conflict):
        store.post_message(OWNER, dm.id, key, "different")
    with pytest.raises(Conflict):
        store.post_message(OWNER, other.id, key, "same", [claude.id])


def test_reply_parent_must_be_in_the_same_conversation(store: Store) -> None:
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)
    group = store.create_group("owner", "g")
    store.add_member(OWNER, group.id, claude.id)
    in_dm = store.post_message(OWNER, dm.id, cid(), "private question")

    with pytest.raises(Invalid):
        store.post_message(Actor.binding(claude.id), group.id, cid(), "answer", parent_id=in_dm.message.id)
    ok = store.post_message(Actor.binding(claude.id), dm.id, cid(), "answer", parent_id=in_dm.message.id)
    assert ok.message.parent_id == in_dm.message.id


def test_group_recipients_must_be_named_members(store: Store) -> None:
    claude, codex, outsider = bind(store, "claude"), bind(store, "codex"), bind(store, "mock")
    group = store.create_group("owner", "g")
    store.add_member(OWNER, group.id, claude.id)
    store.add_member(OWNER, group.id, codex.id)

    with pytest.raises(Invalid):
        store.post_message(OWNER, group.id, cid(), "to nobody")
    with pytest.raises(Invalid):
        store.post_message(OWNER, group.id, cid(), "x", [outsider.id])
    with pytest.raises(Invalid):
        store.post_message(OWNER, group.id, cid(), "x", [claude.id, claude.id])
    with pytest.raises(Forbidden):
        store.post_message(Actor.binding(outsider.id), group.id, cid(), "let me in")

    both = store.post_message(OWNER, group.id, cid(), "both", [codex.id, claude.id])
    assert {d.binding_id for d in both.deliveries} == {claude.id, codex.id}
    assert all(d.state == "queued" for d in both.deliveries)


def trio(store: Store, mode: Mode = "user_directed") -> tuple[Binding, Binding, Binding, str]:
    claude, codex, third = bind(store, "claude"), bind(store, "codex"), bind(store, "codex")
    group = store.create_group("owner", "project", mode=mode)
    for member in (claude, codex, third):
        store.add_member(OWNER, group.id, member.id)
    return claude, codex, third, group.id


def test_a_directed_group_delivers_no_peer_message(store: Store) -> None:
    claude, codex, _, group = trio(store)
    agent = Actor.binding(claude.id)
    asked = store.post_message(OWNER, group, cid(), "status?", [claude.id])

    reply = store.post_message(agent, group, cid(), "a reply to the group", parent_id=asked.message.id)
    assert reply.deliveries == ()
    with pytest.raises(Forbidden):
        store.post_message(agent, group, cid(), "ask codex", [codex.id], asked.message.id)
    with pytest.raises(Invalid):
        store.post_message(agent, group, cid(), "ask myself", [claude.id], asked.message.id)


def test_consult_is_one_question_and_one_answer_per_owner_message(store: Store) -> None:
    claude, codex, third, group = trio(store, "consult")
    a, b = Actor.binding(claude.id), Actor.binding(codex.id)
    root = store.post_message(OWNER, group, cid(), "is the backup current?", [claude.id]).message

    with pytest.raises(Forbidden):
        store.post_message(a, group, cid(), "no parent", [codex.id])
    with pytest.raises(Forbidden):
        store.post_message(b, group, cid(), "not asked of me", [claude.id], root.id)
    question = store.post_message(a, group, cid(), "codex, when did it last run?", [codex.id], root.id)
    assert [d.binding_id for d in question.deliveries] == [codex.id]
    assert (question.message.root_id, question.message.depth) == (root.id, 1)
    with pytest.raises(Forbidden):
        store.post_message(a, group, cid(), "a second question", [third.id], root.id)

    with pytest.raises(Forbidden):
        store.post_message(b, group, cid(), "answer to a bystander", [third.id], question.message.id)
    with pytest.raises(Forbidden):
        store.post_message(
            Actor.binding(third.id), group, cid(), "not mine", [claude.id], question.message.id
        )
    answer = store.post_message(b, group, cid(), "04:31 today", [claude.id], question.message.id)
    assert [d.binding_id for d in answer.deliveries] == [claude.id]
    assert (answer.message.root_id, answer.message.depth) == (root.id, 2)
    with pytest.raises(Forbidden):
        store.post_message(b, group, cid(), "a second answer", [claude.id], question.message.id)
    with pytest.raises(Forbidden):
        store.post_message(a, group, cid(), "a third hop", [codex.id], answer.message.id)

    summary = store.post_message(a, group, cid(), "Backup ran at 04:31.", parent_id=answer.message.id)
    assert summary.deliveries == ()
    fresh = store.post_message(OWNER, group, cid(), "and the one before?", [claude.id]).message
    assert store.post_message(a, group, cid(), "codex, the previous run?", [codex.id], fresh.id).deliveries


def test_a_paused_group_delivers_no_peer_message(store: Store) -> None:
    claude, codex, _, group = trio(store, "consult")
    agent = Actor.binding(claude.id)
    root = store.post_message(OWNER, group, cid(), "ask codex", [claude.id]).message
    with pytest.raises(Forbidden):
        store.set_paused(agent, group, True)
    store.set_paused(OWNER, group, True)
    with pytest.raises(Forbidden):
        store.post_message(agent, group, cid(), "question", [codex.id], root.id)
    store.set_paused(OWNER, group, False)
    assert store.post_message(agent, group, cid(), "question", [codex.id], root.id).deliveries


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000.0

    def __call__(self) -> float:
        return self.t


def event_types(store: Store, group: str) -> list[str]:
    return [e.type for e in store.events_after(OWNER, group)]


def test_a_collaboration_stops_at_its_turn_limit(store: Store) -> None:
    claude, codex, _, group = trio(store)
    a, b = Actor.binding(claude.id), Actor.binding(codex.id)
    started = store.start_collaboration(OWNER, group, "  agree a backup schedule ", max_turns=3)
    assert (started.objective, started.active, store.get_conversation(OWNER, group).mode) == (
        "agree a backup schedule",
        True,
        "collaborate",
    )

    first = store.post_message(a, group, cid(), "nightly at 04:30?", [codex.id]).message
    second = store.post_message(b, group, cid(), "04:30 collides with the scrub", [claude.id], first.id)
    with pytest.raises(Invalid):
        store.post_message(a, group, cid(), " 04:30 collides with the scrub ", [codex.id], second.message.id)
    free = store.post_message(a, group, cid(), "the owner: working on it", parent_id=second.message.id)
    assert free.deliveries == ()
    store.post_message(a, group, cid(), "05:00 then", [codex.id], second.message.id)

    stopped = store.collaboration(OWNER, group)
    assert stopped is not None
    assert (stopped.active, stopped.turns_used, stopped.stopped_reason) == (False, 3, "turn limit")
    assert store.get_conversation(OWNER, group).mode == "user_directed"
    assert event_types(store, group).count("collaboration.stopped") == 1
    with pytest.raises(Forbidden):
        store.post_message(b, group, cid(), "agreed", [claude.id])
    assert store.post_message(b, group, cid(), "Agreed on 05:00.").deliveries == ()


def test_a_collaboration_stops_at_its_deadline(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "broker.db", now=clock)
    claude, codex, _, group = trio(store)
    a = Actor.binding(claude.id)
    store.start_collaboration(OWNER, group, "objective", max_turns=12, minutes=5)
    store.post_message(a, group, cid(), "inside the window", [codex.id])
    assert store.expire_collaborations() == 0

    clock.t += 5 * 60
    with pytest.raises(Forbidden):
        store.post_message(a, group, cid(), "after the deadline", [codex.id])
    stopped = store.collaboration(OWNER, group)
    assert stopped is not None
    assert (stopped.active, stopped.stopped_reason, stopped.turns_used) == (False, "time limit", 1)
    assert store.expire_collaborations() == 0
    assert event_types(store, group).count("collaboration.stopped") == 1
    store.close()


def test_the_sweep_stops_an_expired_collaboration_without_traffic(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "broker.db", now=clock)
    _, _, _, group = trio(store)
    store.start_collaboration(OWNER, group, "objective", minutes=1)
    clock.t += 61
    assert store.expire_collaborations() == 1
    assert store.get_conversation(OWNER, group).mode == "user_directed"
    store.close()


def test_only_the_owner_steers_modes_and_only_in_groups(store: Store) -> None:
    claude, codex, _, group = trio(store)
    agent = Actor.binding(claude.id)
    direct = store.open_direct("owner", claude.id)

    with pytest.raises(Forbidden):
        store.set_mode(agent, group, "consult")
    with pytest.raises(Forbidden):
        store.start_collaboration(agent, group, "objective")
    with pytest.raises(Invalid):
        store.set_mode(OWNER, direct.id, "consult")
    with pytest.raises(Invalid):
        store.start_collaboration(OWNER, direct.id, "objective")
    with pytest.raises(Invalid):
        store.set_mode(OWNER, group, "collaborate")
    for bad in ({"objective": " "}, {"max_turns": 0}, {"max_turns": 13}, {"minutes": 0}, {"minutes": 61}):
        with pytest.raises(Invalid):
            store.start_collaboration(OWNER, group, **{"objective": "objective", **bad})  # type: ignore[arg-type]

    store.start_collaboration(OWNER, group, "objective")
    with pytest.raises(Forbidden):
        store.stop_collaboration(agent, group)
    assert store.set_mode(OWNER, group, "consult").mode == "consult"
    collaboration = store.collaboration(OWNER, group)
    assert collaboration is not None
    assert (collaboration.active, collaboration.stopped_reason) == (False, "mode changed")
    with pytest.raises(Forbidden):
        store.post_message(agent, group, cid(), "no owner message to consult on", [codex.id])

    store.start_collaboration(OWNER, group, "second objective")
    assert store.stop_collaboration(OWNER, group).mode == "user_directed"
    assert store.stop_collaboration(OWNER, group).mode == "user_directed"
    stopped = store.collaboration(OWNER, group)
    assert stopped is not None and stopped.stopped_reason == "stopped by owner"
    assert event_types(store, group).count("collaboration.started") == 2
    assert event_types(store, group).count("collaboration.stopped") == 2


def test_message_size_limit(store: Store) -> None:
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)
    store.post_message(OWNER, dm.id, cid(), "x" * MAX_MESSAGE_BYTES)
    with pytest.raises(Invalid):
        store.post_message(OWNER, dm.id, cid(), "x" * (MAX_MESSAGE_BYTES + 1))


def test_event_sequence_is_per_conversation(store: Store) -> None:
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)
    group = store.create_group("owner", "g")
    store.add_member(OWNER, group.id, claude.id)
    for _ in range(3):
        store.post_message(OWNER, dm.id, cid(), "d")
        store.post_message(OWNER, group.id, cid(), "g", [claude.id])

    for conversation in (dm, group):
        seqs = [e.seq for e in store.events_after(OWNER, conversation.id)]
        assert seqs == list(range(1, len(seqs) + 1))
    tail = store.events_after(OWNER, dm.id, after=4)
    assert [e.seq for e in tail] == [5, 6]


# Dispatch


def test_one_turn_at_a_time_per_session_across_conversations(store: Store) -> None:
    claude = bind(store)
    group = store.create_group("owner", "g")
    store.add_member(OWNER, group.id, claude.id)
    dm = store.open_direct("owner", claude.id)
    first = store.post_message(OWNER, group.id, cid(), "group turn", [claude.id])
    second = store.post_message(OWNER, dm.id, cid(), "dm during the group turn")
    lease = claude.lease_generation

    claimed = store.claim_next(claude.id, lease)
    assert claimed is not None
    delivery, message = claimed
    assert message.id == first.message.id
    assert store.claim_next(claude.id, lease) is None

    store.transition(delivery.id, claude.id, lease, "accepted", native_turn_id="turn-1")
    assert store.claim_next(claude.id, lease) is None
    store.transition(delivery.id, claude.id, lease, "responded")

    nxt = store.claim_next(claude.id, lease)
    assert nxt is not None
    assert nxt[1].id == second.message.id


def test_a_read_delivery_releases_the_session_and_survives_reconnect(store: Store) -> None:
    claude = bind(store, native="native-3")
    dm = store.open_direct("owner", claude.id)
    first = store.post_message(OWNER, dm.id, cid(), "one")
    second = store.post_message(OWNER, dm.id, cid(), "two")
    lease = claude.lease_generation

    claimed = store.claim_next(claude.id, lease)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, lease, "accepted")
    assert store.claim_next(claude.id, lease) is None
    store.transition(claimed[0].id, claude.id, lease, "consumed")
    nxt = store.claim_next(claude.id, lease)
    assert nxt is not None
    assert nxt[1].id == second.message.id

    bind(store, native="native-3")
    assert store.deliveries_for(first.message.id)[0].state == "consumed"
    assert store.deliveries_for(second.message.id)[0].state == "uncertain"


def test_invalid_and_foreign_transitions_are_refused(store: Store) -> None:
    claude, codex = bind(store, "claude"), bind(store, "codex")
    dm = store.open_direct("owner", claude.id)
    sent = store.post_message(OWNER, dm.id, cid(), "hi")
    delivery = sent.deliveries[0]

    with pytest.raises(Invalid):
        store.transition(delivery.id, claude.id, claude.lease_generation, "responded")
    with pytest.raises(Forbidden):
        store.transition(delivery.id, codex.id, codex.lease_generation, "dispatching")


def test_reconnect_marks_in_flight_uncertain_and_refuses_the_old_lease(store: Store) -> None:
    claude = bind(store, native="native-1")
    dm = store.open_direct("owner", claude.id)
    store.post_message(OWNER, dm.id, cid(), "in flight")
    queued = store.post_message(OWNER, dm.id, cid(), "still queued")
    old = claude.lease_generation
    claimed = store.claim_next(claude.id, old)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, old, "accepted")

    again = bind(store, native="native-1")
    assert again.id == claude.id
    assert again.lease_generation == old + 1
    assert store.deliveries_for(claimed[1].id)[0].state == "uncertain"
    with pytest.raises(Conflict):
        store.transition(claimed[0].id, claude.id, old, "responded")
    with pytest.raises(Conflict):
        store.claim_next(claude.id, old)

    nxt = store.claim_next(claude.id, again.lease_generation)
    assert nxt is not None
    assert nxt[1].id == queued.message.id
    assert nxt[0].lease_generation == again.lease_generation


def test_conversation_stop_cancels_only_its_own_queue(store: Store) -> None:
    claude = bind(store)
    group = store.create_group("owner", "g")
    store.add_member(OWNER, group.id, claude.id)
    dm = store.open_direct("owner", claude.id)
    g1 = store.post_message(OWNER, group.id, cid(), "one", [claude.id])
    d1 = store.post_message(OWNER, dm.id, cid(), "private")

    with pytest.raises(Forbidden):
        store.cancel_pending(Actor.binding(claude.id), group.id)
    cancelled = store.cancel_pending(OWNER, group.id)
    assert [d.message_id for d in cancelled] == [g1.message.id]
    assert store.deliveries_for(d1.message.id)[0].state == "queued"


def test_one_active_binding_per_native_session(store: Store) -> None:
    a = bind(store, "codex", native="thread-1")
    b = bind(store, "codex", native="thread-1")
    c = bind(store, "claude", native="thread-1")
    assert a.id == b.id
    assert c.id != a.id
    store.deactivate_binding(a.id)
    fresh = bind(store, "codex", native="thread-1")
    assert fresh.id != a.id
    with pytest.raises(Invalid):
        store.open_direct("owner", a.id)


def test_reconnect_rotates_the_adapter_token(store: Store) -> None:
    first, old_token = bind_with_token(store, native="native-2")
    assert store.authenticate(old_token).id == first.id
    again, new_token = bind_with_token(store, native="native-2")

    assert new_token != old_token
    assert store.authenticate(new_token).lease_generation == again.lease_generation
    with pytest.raises(Forbidden):
        store.authenticate(old_token)
    store.deactivate_binding(first.id)
    with pytest.raises(Gone):
        store.authenticate(new_token)


def test_listeners_see_committed_events_only(store: Store) -> None:
    seen: list[str] = []
    unsubscribe = store.subscribe(lambda events: seen.extend(e.type for e in events))
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)

    store.post_message(OWNER, dm.id, cid(), "hi")
    assert seen == ["message.accepted", "delivery.changed"]
    with pytest.raises(Invalid):
        store.post_message(OWNER, dm.id, cid(), "x" * (MAX_MESSAGE_BYTES + 1))
    with pytest.raises(Invalid):
        store.post_message(Actor.binding(claude.id), dm.id, cid(), "reply", [claude.id])
    assert len(seen) == 2

    with pytest.raises(RuntimeError), store._write() as db:
        store._append(db, dm.id, "rolled.back", OWNER, None, {})
        raise RuntimeError
    store.post_message(OWNER, dm.id, cid(), "after the rollback")
    assert "rolled.back" not in seen
    assert "rolled.back" not in [e.type for e in store.events_after(OWNER, dm.id)]
    assert len(seen) == 4

    unsubscribe()
    store.post_message(OWNER, dm.id, cid(), "unheard")
    assert len(seen) == 4


# Persistence


def test_reopen_and_backup_keep_everything(tmp_path: Path) -> None:
    path = tmp_path / "broker.db"
    s = Store(path)
    claude = bind(s)
    dm = s.open_direct("owner", claude.id)
    s.post_message(OWNER, dm.id, cid(), "kept")
    s.backup(tmp_path / "copy.db")
    s.close()

    for p in (path, tmp_path / "copy.db"):
        reopened = Store(p)
        assert "kept" in all_text(reopened, OWNER, dm.id)
        reopened.close()


# Removing a group


def test_a_removed_group_leaves_every_list_keeps_its_history_and_comes_back(store: Store) -> None:
    claude, codex, _, group = trio(store)
    agent = Actor.binding(claude.id)
    store.post_message(OWNER, group, cid(), "kept", [claude.id])
    queued = store.post_message(OWNER, group, cid(), "still queued", [codex.id])
    store.start_collaboration(OWNER, group, "objective")

    with pytest.raises(Forbidden):
        store.set_archived(agent, group, True)
    removed = store.set_archived(OWNER, group, True)
    assert removed.archived and removed.mode == "user_directed"
    assert group not in [c.id for c in store.conversations_for(OWNER)]
    assert group not in [c.id for c in store.conversations_for(agent)]
    assert store.deliveries_for(queued.message.id)[0].state == "cancelled"
    stopped = store.collaboration(OWNER, group)
    assert stopped is not None and stopped.stopped_reason == "group removed"
    assert "kept" in all_text(store, OWNER, group)
    for refused in (
        lambda: store.post_message(OWNER, group, cid(), "into a removed group", [claude.id]),
        lambda: store.add_member(OWNER, group, codex.id),
        lambda: store.set_mode(OWNER, group, "consult"),
        lambda: store.start_collaboration(OWNER, group, "objective"),
    ):
        with pytest.raises(Forbidden):
            refused()
    assert store.set_archived(OWNER, group, True).archived  # removing twice changes nothing
    assert event_types(store, group).count("conversation.archived") == 1

    restored = store.set_archived(OWNER, group, False)
    assert not restored.archived
    assert group in [c.id for c in store.conversations_for(agent)]
    assert store.post_message(OWNER, group, cid(), "back", [claude.id]).deliveries
    assert event_types(store, group)[-4:] == [
        "conversation.restored",
        "collaboration.reopened",
        "message.accepted",
        "delivery.changed",
    ]


def test_removing_a_group_tells_each_member_session_what_stopped(store: Store) -> None:
    claude, codex, third, group = trio(store)
    store.start_collaboration(OWNER, group, "objective", max_turns=12, minutes=60)
    store.set_archived(OWNER, group, True)
    notices = [
        e
        for e in store.events_after(OWNER, group)
        if e.type == "message.accepted"
        and (e.payload.get("notice") or {}).get("kind") == "conversation_archived"
    ]
    assert len(notices) == 1
    payload = notices[0].payload
    assert payload["notice"]["collaboration_stopped"] is True
    assert "12 turns" in payload["text"]
    assert "reopens the group and resumes the collaboration" in payload["text"]
    assert notices[0].message_id is not None
    told = {d.binding_id for d in store.deliveries_for(notices[0].message_id)}
    assert told == {claude.id, codex.id, third.id}


def test_a_member_sessions_post_reopens_a_removed_group_and_resumes_its_collaboration(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "broker.db", now=clock)
    claude, codex, _, group = trio(store)
    a = Actor.binding(codex.id)
    store.start_collaboration(OWNER, group, "objective", max_turns=12, minutes=60)
    store.post_message(a, group, cid(), "first turn", [claude.id])
    store.set_archived(OWNER, group, True)

    clock.t += 10 * 60
    posted = store.post_message(a, group, cid(), "CI is green", [claude.id])
    assert posted.deliveries
    conversation = store.get_conversation(OWNER, group)
    assert not conversation.archived and conversation.mode == "collaborate"
    resumed = store.collaboration(OWNER, group)
    assert resumed is not None and resumed.active and resumed.stopped_reason is None
    assert resumed.turns_used == 2
    types = event_types(store, group)
    assert types.count("conversation.restored") == 1 and types.count("collaboration.reopened") == 1
    assert group in [c.id for c in store.conversations_for(OWNER)]
    store.close()


def test_a_reopened_group_does_not_resume_a_collaboration_past_its_deadline(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "broker.db", now=clock)
    claude, codex, _, group = trio(store)
    a = Actor.binding(codex.id)
    store.start_collaboration(OWNER, group, "objective", max_turns=12, minutes=5)
    store.set_archived(OWNER, group, True)

    clock.t += 6 * 60
    with pytest.raises(Forbidden):
        store.post_message(a, group, cid(), "too late to address", [claude.id])
    assert store.get_conversation(OWNER, group).archived  # the refused post changed nothing
    store.post_message(a, group, cid(), "a reply to the group")
    conversation = store.get_conversation(OWNER, group)
    assert not conversation.archived and conversation.mode == "user_directed"
    stopped = store.collaboration(OWNER, group)
    assert stopped is not None and not stopped.active and stopped.stopped_reason == "group removed"
    store.close()


def test_the_owner_restoring_a_group_resumes_a_collaboration_the_removal_stopped(store: Store) -> None:
    _, _, _, group = trio(store)
    store.start_collaboration(OWNER, group, "objective", max_turns=12, minutes=60)
    store.set_archived(OWNER, group, True)
    restored = store.set_archived(OWNER, group, False)
    assert restored.mode == "collaborate"
    collaboration = store.collaboration(OWNER, group)
    assert collaboration is not None and collaboration.active


def test_a_direct_conversation_cannot_be_removed(store: Store) -> None:
    claude = bind(store)
    dm = store.open_direct("owner", claude.id)
    with pytest.raises(Invalid):
        store.set_archived(OWNER, dm.id, True)


# Redirect


def redirected(store: Store, conversation_id: str) -> list[dict[str, object]]:
    return [e.payload for e in store.events_after(OWNER, conversation_id) if e.type == "delivery.redirected"]


def test_a_queued_group_message_moves_to_the_right_session(store: Store) -> None:
    claude, codex, _, group = trio(store)
    sent = store.post_message(OWNER, group, cid(), "for codex really", [claude.id])

    result = store.redirect(OWNER, sent.deliveries[0].id, codex.id, "queued")

    assert (result.disposition, result.created, result.source.state) == ("moved", True, "cancelled")
    copy = result.message
    assert (copy.conversation_id, copy.actor, copy.text, copy.recipients) == (
        group,
        OWNER,
        "for codex really",
        (codex.id,),
    )
    assert store.claim_next(claude.id, claude.lease_generation) is None
    claimed = store.claim_next(codex.id, codex.lease_generation)
    assert claimed is not None and claimed[1].id == copy.id
    [event] = redirected(store, group)
    assert (event["delivery_id"], event["to_message_id"], event["disposition"]) == (
        sent.deliveries[0].id,
        copy.id,
        "moved",
    )


def test_a_delivery_already_handed_over_keeps_its_state_and_is_copied(store: Store) -> None:
    claude, codex, _, group = trio(store)
    sent = store.post_message(OWNER, group, cid(), "taken by the wrong session", [claude.id])
    claimed = store.claim_next(claude.id, claude.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, claude.lease_generation, "accepted")

    result = store.redirect(OWNER, sent.deliveries[0].id, codex.id, "accepted")

    assert (result.disposition, result.source.state) == ("copied", "accepted")
    assert store.deliveries_for(sent.message.id)[0].state == "accepted"
    assert store.claim_next(claude.id, claude.lease_generation) is None


def test_a_direct_message_is_copied_into_the_targets_own_direct_conversation(store: Store) -> None:
    old = bind(store, "codex", native="thread-old")
    new = bind(store, "codex", native="thread-new")
    dm = store.open_direct("owner", old.id)
    earlier = f"earlier-{uuid.uuid4().hex}"
    store.post_message(OWNER, dm.id, cid(), earlier)
    sent = store.post_message(OWNER, dm.id, cid(), "Hello")
    store.retire_binding(OWNER, old.id)
    assert store.deliveries_for(sent.message.id)[0].state == "cancelled"

    result = store.redirect(OWNER, sent.deliveries[0].id, new.id, "cancelled")

    target_dm = store.open_direct("owner", new.id)
    assert result.message.conversation_id == target_dm.id != dm.id
    assert (result.message.text, result.delivery.binding_id, result.disposition) == (
        "Hello",
        new.id,
        "copied",
    )
    assert earlier not in all_text(store, OWNER, target_dm.id)
    assert {m.id for m in store.members(OWNER, dm.id)} == {"owner", old.id}
    claimed = store.claim_next(new.id, new.lease_generation)
    assert claimed is not None and claimed[1].id == result.message.id
    [event] = redirected(store, dm.id)
    assert event["to_conversation_id"] == target_dm.id


def test_only_the_owner_redirects_and_only_their_own_messages(store: Store) -> None:
    claude, codex, third, group = trio(store, "consult")
    root = store.post_message(OWNER, group, cid(), "ask codex", [claude.id])
    question = store.post_message(
        Actor.binding(claude.id), group, cid(), "codex?", [codex.id], root.message.id
    )

    with pytest.raises(Forbidden):
        store.redirect(Actor.binding(claude.id), root.deliveries[0].id, third.id, "queued")
    with pytest.raises(Invalid):
        store.redirect(OWNER, question.deliveries[0].id, third.id, "queued")
    assert store.deliveries_for(root.message.id)[0].state == "queued"


def test_a_redirect_refuses_a_moved_state_a_wrong_target_or_a_second_copy(store: Store) -> None:
    claude, codex, third, group = trio(store)
    outsider = bind(store, "claude")
    gone = bind(store, "codex")
    store.add_member(OWNER, group, gone.id)
    store.retire_binding(OWNER, gone.id)
    both = store.post_message(OWNER, group, cid(), "for both", [claude.id, codex.id])
    source = next(d.id for d in both.deliveries if d.binding_id == claude.id)

    for target, expected, error in (
        (third.id, "accepted", Conflict),
        (claude.id, "queued", Invalid),
        (gone.id, "queued", Invalid),
        (outsider.id, "queued", Invalid),
        (codex.id, "queued", Conflict),
    ):
        with pytest.raises(error):
            store.redirect(OWNER, source, target, expected)  # type: ignore[arg-type]
    assert {d.state for d in store.deliveries_for(both.message.id)} == {"queued"}
    assert redirected(store, group) == []
    assert store.redirect(OWNER, source, third.id, "queued").created


def test_a_repeated_redirect_returns_the_first_copy(store: Store) -> None:
    claude, codex, _, group = trio(store)
    sent = store.post_message(OWNER, group, cid(), "once", [claude.id])
    first = store.redirect(OWNER, sent.deliveries[0].id, codex.id, "queued")

    again = store.redirect(OWNER, sent.deliveries[0].id, codex.id, "queued")

    assert (again.created, again.message.id, again.delivery.id, again.disposition) == (
        False,
        first.message.id,
        first.delivery.id,
        "moved",
    )
    assert len(redirected(store, group)) == 1


def test_refusals_expire_after_a_day_and_stay_under_the_cap(store: Store) -> None:
    from duplex_broker.store import REFUSAL_CAP, REFUSAL_TTL

    store.record_refusal("tool", "old", "stale", now=1000.0)
    store.record_refusal("tool", "new", "fresh", now=1000.0 + REFUSAL_TTL - 1)
    assert [r["what"] for r in store.refusals(now=1000.0 + REFUSAL_TTL + 1)] == ["new"]
    for i in range(REFUSAL_CAP + 5):
        store.record_refusal("mcp", f"m{i}", "x", now=5000.0)
    rows = store.refusals(now=5000.0)
    assert len(rows) == REFUSAL_CAP and rows[0]["what"] == f"m{REFUSAL_CAP + 4}"
    assert store.refusals(now=5000.0 + REFUSAL_TTL + 1) == []


def test_only_the_chatgpt_connector_goes_by_chatgpt(store: Store) -> None:
    for title in ("ChatGPT", "ChatGPT duplex relay", "relay for CHATGPT"):
        with pytest.raises(Invalid):
            store.register_binding("codex", f"n-{title}", title, "ws", "pipe", {})
    store.register_binding("claude", "n-codey", "Codeyard", "ws", "pipe", {})
    binding, _ = store.register_binding("chatgpt", "chatgpt", "ChatGPT", "ChatGPT", "mcp-events", {})
    assert binding.title == "ChatGPT"


def test_the_configured_connector_title_is_reserved(store: Store) -> None:
    store.connector_title = "Relay"
    with pytest.raises(Invalid):
        store.register_binding("codex", "relay-session", "Relay helper", "ws", "pipe", {})
    binding, _ = store.register_binding("chatgpt", "chatgpt", "Relay", "ChatGPT", "mcp-events", {})
    assert binding.title == "Relay"


# Envelope, receipts and failure notices


def accepted(store: Store, reader: Actor, conversation_id: str) -> list[dict[str, object]]:
    return [e.payload for e in store.events_after(reader, conversation_id) if e.type == "message.accepted"]


def party(binding: Binding) -> dict[str, str]:
    return {"kind": "binding", "id": binding.id, "title": binding.title}


def hand_and_read(store: Store, binding: Binding, to_state: str = "consumed") -> str:
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, binding.id, binding.lease_generation, "accepted")
    store.transition(claimed[0].id, binding.id, binding.lease_generation, to_state)  # type: ignore[arg-type]
    return claimed[0].id


def test_every_message_carries_its_sender_recipients_and_sent_time(tmp_path: Path) -> None:
    store = Store(tmp_path / "b.db", now=lambda: 1_800_000_000.0)
    claude, codex, _, group = trio(store)
    asked = store.post_message(OWNER, group, cid(), "status?", [codex.id, claude.id])
    reply = store.post_message(Actor.binding(claude.id), group, cid(), "fine", parent_id=asked.message.id)
    owner = {"kind": "user", "id": "owner", "title": "owner"}
    first, second = accepted(store, OWNER, group)
    assert (first["from"], first["to"], first["sent_at"]) == (
        owner,
        sorted([party(claude), party(codex)], key=lambda p: p["id"]),
        "2027-01-15T08:00:00Z",
    )
    # No explicit recipients: the message reaches the owner, through the app, and nobody else.
    assert (second["from"], second["to"]) == (party(claude), [owner])
    assert store.envelope(reply.message) == {k: second[k] for k in ("from", "to", "sent_at")}
    direct = store.open_direct("owner", claude.id)
    store.post_message(OWNER, direct.id, cid(), "hi")
    assert accepted(store, OWNER, direct.id)[0]["to"] == [party(claude)]
    store.close()


def test_a_session_sender_gets_one_receipt_per_recipient_once(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "b.db", now=clock)
    claude, codex, third, group = trio(store, "consult")
    root = store.post_message(OWNER, group, cid(), "ask codex", [claude.id]).message
    question = store.post_message(Actor.binding(claude.id), group, cid(), "when?", [codex.id], root.id)
    hand_and_read(store, claude)
    assert store.take_receipts(claude.id) == []
    clock.t += 5
    delivery = hand_and_read(store, codex)
    receipts = store.take_receipts(claude.id)
    assert [(r.message_id, r.recipient, r.state, r.read_at) for r in receipts] == [
        (question.message.id, "codex session", "consumed", clock.t)
    ]
    assert (receipts[0].conversation_id, receipts[0].sent_at) == (group, 1_000_000.0)
    assert store.take_receipts(claude.id) == []  # reported once
    store.transition(delivery, codex.id, codex.lease_generation, "responded")
    assert store.take_receipts(claude.id) == []  # the first read only
    assert store.take_receipts(third.id) == []
    # the owner reads his own message's read state from the record, with the time of the first read.
    (view,) = store.message_record(OWNER, root.id).deliveries
    assert (view.delivery.state, view.read_at) == ("consumed", 1_000_000.0)
    store.close()


def test_a_failed_delivery_to_the_owners_message_is_posted_to_the_owner(store: Store) -> None:
    claude = bind(store)
    direct = store.open_direct("owner", claude.id)
    sent = store.post_message(OWNER, direct.id, cid(), "hello").message
    claimed = store.claim_next(claude.id, claude.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, claude.lease_generation, "failed", None, "wake failed: gone")
    notices = next(c for c in store.conversations_for(OWNER) if c.title == "Duplex broker")
    (posted,) = accepted(store, OWNER, notices.id)
    notice = posted["notice"]
    assert isinstance(notice, dict)
    assert (notice["message_id"], notice["recipient"], notice["state"], notice["reason"]) == (
        sent.id,
        party(claude),
        "failed",
        "wake failed: gone",
    )
    assert posted["from"] == {"kind": "binding", "id": "broker", "title": "Duplex broker"}
    assert "to claude session ended failed" in str(posted["text"])
    assert posted["recipients"] == []


def test_a_failed_delivery_wakes_the_session_that_sent_it_and_a_notice_makes_no_other(store: Store) -> None:
    claude, codex, _, group = trio(store, "consult")
    root = store.post_message(OWNER, group, cid(), "ask codex", [claude.id]).message
    hand_and_read(store, claude)
    question = store.post_message(Actor.binding(claude.id), group, cid(), "when?", [codex.id], root.id)
    claimed = store.claim_next(codex.id, codex.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, codex.id, codex.lease_generation, "failed", None, "codex queue timed out")
    posts = accepted(store, OWNER, group)
    notice = posts[-1]
    assert notice["from"] == {"kind": "binding", "id": "broker", "title": "Duplex broker"}
    assert (notice["to"], notice["parent_id"]) == ([party(claude)], question.message.id)
    # The notice is a delivery to the sender, so it wakes it like any message.
    handed = store.claim_next(claude.id, claude.lease_generation)
    assert handed is not None and handed[1].actor == Actor.binding("broker")
    assert store.envelope(handed[1])["notice"]["recipient"] == party(codex)
    store.transition(handed[0].id, claude.id, claude.lease_generation, "failed", None, "pipe gone")
    assert len(accepted(store, OWNER, group)) == len(posts)  # no notice of a notice
    assert [c.title for c in store.conversations_for(OWNER)] == ["project"]


def test_the_owner_is_not_told_of_deliveries_his_own_retire_ended(store: Store) -> None:
    claude = bind(store)
    direct = store.open_direct("owner", claude.id)
    store.post_message(OWNER, direct.id, cid(), "hello")
    assert store.claim_next(claude.id, claude.lease_generation) is not None
    store.retire_binding(OWNER, claude.id)
    assert [c.title for c in store.conversations_for(OWNER) if c.title == "Duplex broker"] == []


def test_another_users_message_gets_no_notice(store: Store) -> None:
    claude = bind(store)
    direct = store.open_direct("job-queue", claude.id)
    store.post_message(Actor.user("job-queue"), direct.id, cid(), "[job-queue status data] ok", [claude.id])
    claimed = store.claim_next(claude.id, claude.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, claude.lease_generation, "failed", None, "wake failed")
    assert store.conversations_for(OWNER) == []
    assert len(accepted(store, Actor.user("job-queue"), direct.id)) == 1


@pytest.mark.parametrize(
    ("a", "b"),
    [("2.1.1000", "2.1.318"), ("1.10.0", "1.9.9"), ("1.0.1", "1"), ("1.0.0", "0"), ("1.0.0", "unknown")],
)
def test_adapter_versions_compare_as_numbers(a: str, b: str) -> None:
    assert version_key(a) > version_key(b)
    assert version_key("1.0") == version_key("1.0.0") == version_key("v1")


def test_built_in_members_have_no_adapter_to_be_behind(store: Store) -> None:
    claude = bind(store, "claude")
    chatgpt, _ = store.register_binding("chatgpt", "chatgpt", "ChatGPT", "ChatGPT", "mcp-events", {})
    assert adapter_behind(chatgpt, "9.0.0") is None
    assert adapter_behind(claude, "9.0.0") is True
    assert adapter_behind(claude, "0") is False

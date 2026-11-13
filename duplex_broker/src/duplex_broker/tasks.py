"""Delegated tasks: one session asks another to do a piece of work and gets its result back.

A task has a requester and an assignee (both session bindings) and its own group conversation, where every
notice is a broker message with a stable client_message_id, so the ordinary delivery, wake, receipt and
ladder machinery carries it and a repeat posts nothing. The assignee reports a result, asks the requester
questions, or is cancelled; a result reported while child tasks are live is held until the last child ends.
The assignee's reported turn state adds two things: a nudge once when it goes idle with the task unreported,
and an observed question when it waits on something only the owner or that session can clear.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from typing import Any

from .ids import match_sessions, reply_id, stored_id
from .store import (
    ADAPTER_PROVIDERS,
    BROKER_BINDING_ID,
    Actor,
    Binding,
    Conflict,
    Forbidden,
    Invalid,
    ModeUnsupported,
    NotFound,
    PostResult,
    Store,
    TasksUnsupported,
    utc_iso,
)

BROKER = Actor.binding(BROKER_BINDING_ID)
LIVE = ("working", "waiting_for_children")
# A finished task whose requester has not yet read the result is still open.
RESULT = ("result_available", "failed")
OPEN = (
    "(status IN ('working', 'waiting_for_children')"
    " OR (status IN ('result_available', 'failed') AND delivered_at IS NULL))"
)
TITLE_CHARS = 60
Shown = Callable[[str], str]


def _new_id() -> str:
    return uuid.uuid4().hex


def _plain(value: str) -> str:
    return value


# Reading


def _task(db: sqlite3.Connection, task_id: str) -> sqlite3.Row:
    row: sqlite3.Row | None = db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise NotFound(f"task {task_id}")
    return row


def _question(db: sqlite3.Connection, question_id: str) -> sqlite3.Row:
    row: sqlite3.Row | None = db.execute(
        "SELECT * FROM task_questions WHERE id = ?", (question_id,)
    ).fetchone()
    if row is None:
        raise NotFound(f"question {question_id}")
    return row


def _live_children(db: sqlite3.Connection, task_id: str) -> list[sqlite3.Row]:
    rows: list[sqlite3.Row] = db.execute(
        "SELECT * FROM tasks WHERE parent_id = ? AND status IN ('working', 'waiting_for_children')",
        (task_id,),
    ).fetchall()
    return rows


def _descendants(db: sqlite3.Connection, task_id: str) -> list[sqlite3.Row]:
    """Every live task below one, at any depth."""
    rows: list[sqlite3.Row] = db.execute(
        "WITH RECURSIVE below(id) AS (SELECT id FROM tasks WHERE parent_id = ?"
        " UNION SELECT t.id FROM tasks t JOIN below b ON t.parent_id = b.id)"
        " SELECT * FROM tasks WHERE id IN (SELECT id FROM below)"
        " AND status IN ('working', 'waiting_for_children') ORDER BY created_at, id",
        (task_id,),
    ).fetchall()
    return rows


def _title(db: sqlite3.Connection, binding_id: str) -> str:
    row = db.execute("SELECT title FROM session_bindings WHERE id = ?", (binding_id,)).fetchone()
    return str(row["title"]) if row else binding_id


def _at(value: float | None) -> str | None:
    return None if value is None else utc_iso(value)


def _view(db: sqlite3.Connection, task: sqlite3.Row, shown: Shown) -> dict[str, Any]:
    """A task as its two sessions and the owner read it. The result appears once the task has ended with
    one; a result still held for live children is not shown."""
    children = db.execute(
        "SELECT id, status FROM tasks WHERE parent_id = ? ORDER BY created_at, id", (task["id"],)
    ).fetchall()
    questions = db.execute(
        "SELECT id, kind, text, state FROM task_questions WHERE task_id = ? AND state = 'open'"
        " ORDER BY created_at, id",
        (task["id"],),
    ).fetchall()
    return {
        "id": shown(task["id"]),
        "requester": {"id": task["requester_id"], "title": _title(db, task["requester_id"])},
        "assignee": {"id": task["assignee_id"], "title": _title(db, task["assignee_id"])},
        "parent_task_id": shown(task["parent_id"]) if task["parent_id"] else None,
        "conversation_id": shown(task["conversation_id"]),
        "task": task["task"],
        "done_when": task["done_when"],
        "status": task["status"],
        "outcome": task["outcome"],
        "result": task["result"] if task["status"] in RESULT else None,
        "delivered_at": _at(task["delivered_at"]),
        "children": [{"id": shown(c["id"]), "status": c["status"]} for c in children],
        "questions": [
            {"id": shown(q["id"]), "kind": q["kind"], "text": q["text"], "state": q["state"]}
            for q in questions
        ],
        "created_at": _at(task["created_at"]),
        "updated_at": _at(task["updated_at"]),
        "ended_at": _at(task["ended_at"]),
    }


def _events(db: sqlite3.Connection, task_id: str) -> list[sqlite3.Row]:
    rows: list[sqlite3.Row] = db.execute(
        "SELECT seq, type, actor_kind, actor_id, payload, created_at FROM task_events"
        " WHERE task_id = ? ORDER BY seq",
        (task_id,),
    ).fetchall()
    return rows


def get_task(store: Store, reader: Binding, task_id: str) -> dict[str, Any]:
    """A task for its requester or assignee. The requester's first read of an ended task is its delivery."""
    with store._write() as db:
        task = _task(db, stored_id(task_id))
        if reader.id not in (task["requester_id"], task["assignee_id"]):
            raise Forbidden("only the requester or the assignee reads a task")
        if reader.id == task["requester_id"] and task["status"] in RESULT and task["delivered_at"] is None:
            db.execute("UPDATE tasks SET delivered_at = ? WHERE id = ?", (store._now(), task["id"]))
            _event(store, db, task["id"], "delivered", Actor.binding(reader.id), {})
            task = _task(db, task["id"])
        view = _view(db, task, reply_id)
        view["event_count"] = len(_events(db, task["id"]))
        return view


def list_tasks(store: Store, reader: Binding, role: str, open_only: bool) -> list[dict[str, Any]]:
    """The reader's own tasks. `open_only` keeps live ones and ended ones whose result is unread."""
    own = {
        "requester": "requester_id = ?",
        "assignee": "assignee_id = ?",
        "both": "(requester_id = ? OR assignee_id = ?)",
    }[role]
    marks = (reader.id,) * own.count("?")
    with store._read() as db:
        rows = db.execute(
            f"SELECT * FROM tasks WHERE {own}{' AND ' + OPEN if open_only else ''} ORDER BY created_at, id",
            marks,
        ).fetchall()
        return [_view(db, row, reply_id) for row in rows]


def owner_tasks(store: Store, open_only: bool) -> list[dict[str, Any]]:
    with store._read() as db:
        rows = db.execute(
            f"SELECT * FROM tasks{' WHERE ' + OPEN if open_only else ''} ORDER BY created_at, id"
        ).fetchall()
        return [_view(db, row, _plain) for row in rows]


def owner_task(store: Store, task_id: str) -> dict[str, Any]:
    """One task for the owner, with every event it went through."""
    with store._read() as db:
        task = _task(db, stored_id(task_id))
        view = _view(db, task, _plain)
        view["events"] = [
            {
                "seq": e["seq"],
                "type": e["type"],
                "actor": {"kind": e["actor_kind"], "id": e["actor_id"]},
                "payload": json.loads(e["payload"]),
                "created_at": _at(e["created_at"]),
            }
            for e in _events(db, task["id"])
        ]
        return view


# Writing


def _event(
    store: Store, db: sqlite3.Connection, task_id: str, type_: str, actor: Actor, payload: dict[str, Any]
) -> None:
    seq = db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM task_events WHERE task_id = ?", (task_id,))
    db.execute(
        "INSERT INTO task_events (task_id, seq, type, actor_kind, actor_id, payload, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (task_id, seq.fetchone()[0], type_, actor.kind, actor.id, json.dumps(payload), store._now()),
    )


def _notice(
    store: Store,
    db: sqlite3.Connection,
    task: sqlite3.Row,
    kind: str,
    to_binding_id: str,
    text: str,
    question_id: str | None = None,
    mode: str = "auto",
    strict: bool = False,
) -> PostResult | None:
    """Post one notice into the task's conversation, addressed to one session. Its client_message_id is
    fixed by task, kind and question, so posting it twice delivers it once. A session that was retired
    hears nothing and gets None. A session that cannot take it (admission) gets None too and the task
    records `notice_refused`; with `strict` the refusal is raised for the caller instead."""
    target = store._binding(db, to_binding_id)
    if not target.active:
        return None
    suffix = kind if question_id is None else f"{kind}:{question_id}"
    notice = {"kind": f"task_{kind}", "task_id": task["id"]}
    if question_id is not None:
        notice["question_id"] = question_id
    try:
        return store._post_broker(
            db,
            store._conversation(db, task["conversation_id"]),
            f"task:{task['id']}:{suffix}",
            text,
            (to_binding_id,),
            notice,
            mode,
        )
    except (ModeUnsupported, TasksUnsupported) as refused:
        if strict:
            raise
        record_refused(store, db, notice, to_binding_id, str(refused))
        return None


# Which refused task notices are lifecycle transitions. An assignment nobody can act on ends the task as
# failed, with the reason, and the requester hears through the ordinary result path. The data of every other
# notice (result, question, answer, cancelled, nudge) stays readable through task status, so its refusal is
# only recorded.
SETTLES_TASK = frozenset({"task_assigned"})


def record_refused(
    store: Store, db: sqlite3.Connection, notice: dict[str, Any], binding_id: str, reason: str
) -> None:
    """A task notice its target could not take is not silent: the task's events say so, and a refused
    assignment settles the task."""
    payload = {"kind": notice["kind"], "binding_id": binding_id, "reason": reason}
    if "question_id" in notice:
        payload["question_id"] = notice["question_id"]
    _event(store, db, notice["task_id"], "notice_refused", BROKER, payload)
    if notice["kind"] in SETTLES_TASK:
        task = _task(db, notice["task_id"])
        if task["status"] in LIVE:
            _end(store, db, task, "failed", f"The assignment could not be delivered: {reason}")


def _message_id(posted: PostResult | None) -> str | None:
    return None if posted is None else posted.message.id


def _cancel_queued(store: Store, db: sqlite3.Connection, where: str, args: tuple[Any, ...], why: str) -> None:
    for row in db.execute(
        "SELECT d.id FROM deliveries d JOIN messages m ON m.id = d.message_id"
        f" WHERE d.state = 'queued' AND {where} ORDER BY d.seq",
        args,
    ).fetchall():
        store._set_state(db, row["id"], "cancelled", None, BROKER, reason=why)


def _close_question(
    store: Store,
    db: sqlite3.Connection,
    question: sqlite3.Row,
    state: str,
    why: str,
    answer: str | None = None,
) -> None:
    """Close an open question. Its notice to the requester, if still queued, is cancelled: nobody should be
    woken for a question that no longer needs them."""
    db.execute(
        "UPDATE task_questions SET state = ?, answer = ?, closed_at = ? WHERE id = ?",
        (state, answer, store._now(), question["id"]),
    )
    if question["notice_message_id"]:
        _cancel_queued(store, db, "m.id = ?", (question["notice_message_id"],), why)


def _withdraw_open(
    store: Store, db: sqlite3.Connection, where: str, args: tuple[Any, ...], actor: Actor
) -> None:
    for question in db.execute(
        f"SELECT * FROM task_questions WHERE state = 'open' AND {where} ORDER BY created_at, id", args
    ).fetchall():
        _close_question(store, db, question, "withdrawn", "question withdrawn")
        _event(store, db, question["task_id"], "withdrawn", actor, {"question_id": question["id"]})


def _resolve_assignee(store: Store, db: sqlite3.Connection, requester: Binding, to: str) -> Binding:
    found = match_sessions([b for b in store._active_bindings(db) if b.provider in ADAPTER_PROVIDERS], to)
    if not found:
        raise Invalid(f"no active claude or codex session is named {to!r}")
    if len(found) > 1:
        raise Invalid(f"{to!r} names {len(found)} active sessions; use a session id")
    if found[0].id == requester.id:
        raise Invalid("a session cannot delegate a task to itself")
    return found[0]


def create_task(
    store: Store,
    requester: Binding,
    to: str,
    task: str,
    done_when: str,
    parent_task_id: str | None,
    mode: str,
) -> dict[str, Any]:
    """Delegate a task: its conversation, its row and the assigned notice, in one transaction. A send mode
    the assignee cannot take refuses the whole task."""
    if requester.provider not in ADAPTER_PROVIDERS:
        raise Forbidden("only a claude or codex session delegates a task")
    with store._write() as db:
        assignee = _resolve_assignee(store, db, requester, to)
        # The result and every answer come back to the requester, so it must take task notices too.
        store.admit(db, (requester.id,), "auto", "task_result")
        parent_id: str | None = None
        if parent_task_id is not None:
            parent = _task(db, stored_id(parent_task_id))
            if parent["assignee_id"] != requester.id:
                raise Forbidden("only the assignee of a task delegates a child of it")
            if parent["status"] != "working":
                raise Conflict(f"task {reply_id(parent['id'])} is {parent['status']}: it takes no new child")
            parent_id = parent["id"]
        now = store._now()
        title = "Task: " + " ".join(task.split())[:TITLE_CHARS]
        conversation_id = store._insert_group(db, store.owner, title, "user_directed")
        db.executemany(
            "INSERT INTO conversation_members VALUES (?, 'binding', ?, 'session')",
            [(conversation_id, requester.id), (conversation_id, assignee.id)],
        )
        task_id = _new_id()
        db.execute(
            "INSERT INTO tasks (id, requester_id, assignee_id, parent_id, conversation_id, task, done_when,"
            " status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'working', ?, ?)",
            (
                task_id,
                requester.id,
                assignee.id,
                parent_id,
                conversation_id,
                task,
                done_when,
                now,
                now,
            ),
        )
        row = _task(db, task_id)
        text = (
            f"Task {reply_id(task_id)} from {requester.title}: {task}\n"
            f"Done when: {done_when}\n"
            f"Report the result with bridge_task_report, or ask {requester.title} with bridge_task_ask,"
            f" naming task {reply_id(task_id)}."
        )
        message_id = _message_id(
            _notice(store, db, row, "assigned", assignee.id, text, mode=mode, strict=True)
        )
        _event(
            store,
            db,
            task_id,
            "created",
            Actor.binding(requester.id),
            {"assignee_id": assignee.id, "parent_id": parent_id, "mode": mode, "notice": message_id},
        )
        return _view(db, row, reply_id)


def _end(store: Store, db: sqlite3.Connection, task: sqlite3.Row, outcome: str, result: str) -> None:
    """A task ends with its result: the status, the result notice to the requester, the questions closed.
    The parent's held result goes out when this was its last live child."""
    now = store._now()
    status = "result_available" if outcome == "done" else "failed"
    db.execute(
        "UPDATE tasks SET status = ?, outcome = ?, result = ?, held_result = NULL, held_outcome = NULL,"
        " ended_at = ?, updated_at = ? WHERE id = ?",
        (status, outcome, result, now, now, task["id"]),
    )
    ended = _task(db, task["id"])
    _withdraw_open(store, db, "task_id = ?", (task["id"],), BROKER)
    text = (
        f"Task {reply_id(task['id'])} ({_title(db, task['assignee_id'])}) ended {outcome}."
        " Read the result with bridge_task_status."
    )
    message_id = _message_id(_notice(store, db, ended, "result", task["requester_id"], text))
    _event(store, db, task["id"], "ended", BROKER, {"outcome": outcome, "notice": message_id})
    _release_parent(store, db, task["parent_id"])


def _release_parent(store: Store, db: sqlite3.Connection, parent_id: str | None) -> None:
    """Send a parent's held result once its last child has ended."""
    if parent_id is None:
        return
    parent = _task(db, parent_id)
    if parent["status"] == "waiting_for_children" and not _live_children(db, parent_id):
        _end(store, db, parent, parent["held_outcome"], parent["held_result"])


def report(store: Store, assignee: Binding, task_id: str, outcome: str, result: str) -> dict[str, Any]:
    """The assignee's result. With live children it is kept until the last one ends; either way a second
    report is refused."""
    with store._write() as db:
        task = _task(db, stored_id(task_id))
        if task["assignee_id"] != assignee.id:
            raise Forbidden("only the assignee reports a task")
        if task["status"] not in LIVE or task["held_result"] is not None:
            raise Conflict(f"task {reply_id(task['id'])} is {task['status']}: its result is already reported")
        actor = Actor.binding(assignee.id)
        _event(store, db, task["id"], "reported", actor, {"outcome": outcome})
        if _live_children(db, task["id"]):
            db.execute(
                "UPDATE tasks SET status = 'waiting_for_children', held_result = ?, held_outcome = ?,"
                " updated_at = ? WHERE id = ?",
                (result, outcome, store._now(), task["id"]),
            )
        else:
            _end(store, db, task, outcome, result)
        return _view(db, _task(db, task["id"]), reply_id)


def cancel(store: Store, actor: Actor, task_id: str, reason: str | None) -> dict[str, Any]:
    """Cancel a task and everything live below it. Each assignee hears once; what was still queued for it
    from the task is cancelled, and open questions are withdrawn. A parent waiting on this task's subtree
    gets its held result."""
    with store._write() as db:
        root = _task(db, stored_id(task_id))
        if actor.kind == "binding" and actor.id != root["requester_id"]:
            raise Forbidden("only the requester cancels a task")
        if root["status"] not in LIVE:
            raise Conflict(f"task {reply_id(root['id'])} is {root['status']}: it cannot be cancelled")
        who = _title(db, actor.id) if actor.kind == "binding" else actor.id
        for task in [root, *_descendants(db, root["id"])]:
            _cancel_task(store, db, task, actor, who, reason, below=task["id"] != root["id"])
        _release_parent(store, db, root["parent_id"])
        return _view(db, _task(db, root["id"]), reply_id)


def _cancel_task(
    store: Store,
    db: sqlite3.Connection,
    task: sqlite3.Row,
    actor: Actor,
    who: str,
    reason: str | None,
    below: bool,
) -> None:
    now = store._now()
    db.execute(
        "UPDATE tasks SET status = 'cancelled', held_result = NULL, held_outcome = NULL, ended_at = ?,"
        " updated_at = ? WHERE id = ?",
        (now, now, task["id"]),
    )
    _cancel_queued(
        store,
        db,
        "m.conversation_id = ? AND d.binding_id = ? AND m.actor_kind = 'binding' AND m.actor_id = ?",
        (task["conversation_id"], task["assignee_id"], BROKER_BINDING_ID),
        "task cancelled",
    )
    _withdraw_open(store, db, "task_id = ?", (task["id"],), actor)
    text = f"Task {reply_id(task['id'])} was cancelled by {who}"
    if below:
        text += " because a task above it was cancelled"
    text += (f": {reason}" if reason else ".") + " Stop work on it and report nothing."
    message_id = _message_id(
        _notice(store, db, _task(db, task["id"]), "cancelled", task["assignee_id"], text)
    )
    _event(store, db, task["id"], "cancelled", actor, {"reason": reason, "notice": message_id})


def ask(store: Store, assignee: Binding, task_id: str, text: str) -> dict[str, Any]:
    """The assignee asks its requester a question; the notice carries it."""
    with store._write() as db:
        task = _task(db, stored_id(task_id))
        if task["assignee_id"] != assignee.id:
            raise Forbidden("only the assignee asks about a task")
        if task["status"] != "working":
            raise Conflict(f"task {reply_id(task['id'])} is {task['status']}: it takes no question")
        return _open_question(store, db, task, "asked", text, Actor.binding(assignee.id))


def _open_question(
    store: Store, db: sqlite3.Connection, task: sqlite3.Row, kind: str, text: str, actor: Actor
) -> dict[str, Any]:
    question_id = _new_id()
    db.execute(
        "INSERT INTO task_questions (id, task_id, kind, text, state, created_at)"
        " VALUES (?, ?, ?, ?, 'open', ?)",
        (question_id, task["id"], kind, text, store._now()),
    )
    notice = text
    if kind == "asked":
        notice = (
            f"{_title(db, task['assignee_id'])} asks on task {reply_id(task['id'])}"
            f" (question {reply_id(question_id)}): {text}\n"
            f"Answer it with bridge_task_answer, naming question {reply_id(question_id)}."
        )
    message_id = _message_id(_notice(store, db, task, "question", task["requester_id"], notice, question_id))
    db.execute("UPDATE task_questions SET notice_message_id = ? WHERE id = ?", (message_id, question_id))
    _event(store, db, task["id"], "question", actor, {"question_id": question_id, "kind": kind})
    return _question_view(_question(db, question_id))


def _question_view(question: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": reply_id(question["id"]),
        "task_id": reply_id(question["task_id"]),
        "kind": question["kind"],
        "text": question["text"],
        "state": question["state"],
        "answer": question["answer"],
        "created_at": _at(question["created_at"]),
        "closed_at": _at(question["closed_at"]),
    }


def _own_question(db: sqlite3.Connection, binding: Binding, question_id: str, side: str) -> sqlite3.Row:
    question = _question(db, stored_id(question_id))
    task = _task(db, question["task_id"])
    if task[side] != binding.id:
        raise Forbidden(f"only the {side.removesuffix('_id')} of the task does that")
    if question["state"] != "open":
        raise Conflict(f"question {reply_id(question['id'])} is {question['state']}")
    return question


def answer(store: Store, requester: Binding, question_id: str, text: str) -> dict[str, Any]:
    """The requester answers an open question; the assignee gets the answer as a notice."""
    with store._write() as db:
        question = _own_question(db, requester, question_id, "requester_id")
        task = _task(db, question["task_id"])
        _close_question(store, db, question, "answered", "question answered", text)
        notice = (
            f"{requester.title} answers question {reply_id(question['id'])}"
            f" on task {reply_id(task['id'])}: {text}"
        )
        message_id = _message_id(
            _notice(store, db, task, "answer", task["assignee_id"], notice, question["id"])
        )
        _event(
            store,
            db,
            task["id"],
            "answer",
            Actor.binding(requester.id),
            {"question_id": question["id"], "notice": message_id},
        )
        return _question_view(_question(db, question["id"]))


def withdraw(store: Store, assignee: Binding, question_id: str) -> dict[str, Any]:
    """The assignee takes a question back; a notice of it still queued for the requester is cancelled."""
    with store._write() as db:
        question = _own_question(db, assignee, question_id, "assignee_id")
        _close_question(store, db, question, "withdrawn", "question withdrawn")
        _event(
            store,
            db,
            question["task_id"],
            "withdrawn",
            Actor.binding(assignee.id),
            {"question_id": question["id"]},
        )
        return _question_view(_question(db, question["id"]))


# Turn state


def report_turn(
    store: Store, binding: Binding, state: str, waiting_for: str | None, seq: int
) -> tuple[Binding, bool]:
    """Keep a session's turn reading and act on it: its working tasks get or lose their observed questions,
    and an idle session still holding an unreported task is nudged once. The reading is validated first,
    then compared and written in one transaction under the caller's lease; one that is not newer than the
    last applied does nothing and answers False. `/turn` and `/claim` both come here."""
    store.check_turn(state, waiting_for)
    with store._write() as db:
        kept, applied = store._write_turn_state(
            db, binding.id, state, waiting_for, lease=binding.lease_generation, seq=seq
        )
        if applied:
            _sync_observed(store, db, kept)
            if kept.turn_state == "idle":
                _nudge(store, db, kept)
        return kept, applied


def sync_observed(store: Store, binding_id: str) -> None:
    """Make the observed questions match the session's stored turn state; registration resets it."""
    with store._write() as db:
        _sync_observed(store, db, store._binding(db, binding_id))


def _sync_observed(store: Store, db: sqlite3.Connection, binding: Binding) -> None:
    if binding.turn_state != "waiting":
        _withdraw_open(
            store,
            db,
            "kind = 'observed' AND task_id IN (SELECT id FROM tasks WHERE assignee_id = ?)",
            (binding.id,),
            BROKER,
        )
        return
    for task in db.execute(
        "SELECT * FROM tasks WHERE assignee_id = ? AND status = 'working' AND NOT EXISTS"
        " (SELECT 1 FROM task_questions q WHERE q.task_id = tasks.id AND q.kind = 'observed'"
        " AND q.state = 'open') ORDER BY created_at, id",
        (binding.id,),
    ).fetchall():
        text = (
            f"{binding.title} is waiting for {binding.turn_waiting_for or 'input'} on task"
            f" {reply_id(task['id'])}; only the owner or that session can clear it."
        )
        _open_question(store, db, task, "observed", text, BROKER)


def _nudge(store: Store, db: sqlite3.Connection, binding: Binding) -> None:
    """Remind an idle assignee of each task it has not reported, once per task."""
    for task in db.execute(
        "SELECT * FROM tasks WHERE assignee_id = ? AND status = 'working' AND NOT EXISTS"
        " (SELECT 1 FROM tasks c WHERE c.parent_id = tasks.id"
        " AND c.status IN ('working', 'waiting_for_children')) ORDER BY created_at, id",
        (binding.id,),
    ).fetchall():
        text = (
            f"Task {reply_id(task['id'])} is open: report a result with bridge_task_report,"
            " or ask with bridge_task_ask."
        )
        posted = _notice(store, db, task, "nudge", binding.id, text)
        if posted is not None and posted.created:
            _event(store, db, task["id"], "nudge", BROKER, {"notice": posted.message.id})

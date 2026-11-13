"""Usage-limit resume: a session that hit its usage limit is told to continue once the limit resets.

An adapter that finds the limit signal in its session's transcript reports it with the failure time and the
reset time. The broker arms one scheduled send of kind `limit_resume` per (session, run, reset) and its sweep
posts "Continue where you left off." into the owner's direct conversation with that session when the reset
has passed, unless the reset was already past at the failure, the session answered since, the work it was
doing was cancelled, the session is gone, or the reset is more than a day old. Each send is posted at most
once, ever. Fields only this kind has (run, source, matched signal, failure time) live in `details`.
"""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Any

from .store import Binding, Conflict, Forbidden, Invalid, ModeUnsupported, NotFound, Store, TasksUnsupported

KIND = "limit_resume"
CONTINUE_TEXT = "Continue where you left off."
MATCHED_CHARS = 300
DROP_AFTER = 24 * 3600.0  # a reset this far in the past is no longer a pause the session is still in
SOURCES = ("claude-transcript", "codex-rollout")
LIST_LIMIT = 200


def _view(row: sqlite3.Row) -> dict[str, Any]:
    """A limit resume as its adapter and the owner read it: the send's columns and its details, flat."""
    details = json.loads(row["details"])
    return {
        "id": row["id"],
        "binding_id": row["binding_id"],
        "run_id": details["run_id"],
        "source": details["source"],
        "matched": details["matched"],
        "failed_at": details["failed_at"],
        "reset_at": row["due_at"],
        "state": row["state"],
        "reason": row["reason"],
        "message_id": row["message_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row(db: sqlite3.Connection, resume_id: str) -> sqlite3.Row:
    row: sqlite3.Row | None = db.execute(
        "SELECT * FROM scheduled_sends WHERE id = ? AND kind = ?", (resume_id, KIND)
    ).fetchone()
    if row is None:
        raise NotFound(f"limit resume {resume_id}")
    return row


def record_limit(
    store: Store,
    binding: Binding,
    run_id: str,
    source: str,
    matched: str,
    failed_at: float,
    reset_at: float,
) -> tuple[dict[str, Any], bool]:
    """Arm a resume for one limit failure. A repeat of the same run and reset returns the row it made. A
    reset that was already past when the run failed is kept as not_armed and never sends."""
    if source not in SOURCES:
        raise Invalid(f"source must be one of {', '.join(SOURCES)}")
    if not (math.isfinite(failed_at) and math.isfinite(reset_at) and failed_at > 0 and reset_at > 0):
        raise Invalid("failed_at and reset_at are epoch seconds")
    resume_id = f"{binding.id}:{run_id}:{int(reset_at)}"
    armed = reset_at > failed_at
    now = store._now()
    with store._write() as db:
        inserted = db.execute(
            "INSERT OR IGNORE INTO scheduled_sends (id, kind, binding_id, text, mode, due_at, state, reason,"
            " details, created_at, updated_at) VALUES (?, ?, ?, ?, 'auto', ?, ?, ?, ?, ?, ?)",
            (
                resume_id,
                KIND,
                binding.id,
                CONTINUE_TEXT,
                reset_at,
                "armed" if armed else "not_armed",
                None if armed else "reset already past at the failure",
                json.dumps(
                    {
                        "run_id": run_id,
                        "source": source,
                        "matched": matched[:MATCHED_CHARS],
                        "failed_at": failed_at,
                    }
                ),
                now,
                now,
            ),
        )
        return _view(_row(db, resume_id)), inserted.rowcount == 1


def skip(store: Store, binding: Binding, resume_id: str, reason: str) -> dict[str, Any]:
    """The adapter saw the session take a new turn after the failure: an armed resume is not sent."""
    with store._write() as db:
        row = _row(db, resume_id)
        if row["binding_id"] != binding.id:
            raise Forbidden("that resume belongs to another session")
        if row["state"] == "skipped":
            return _view(row)
        if row["state"] != "armed":
            raise Conflict(f"limit resume {resume_id} is {row['state']}")
        _settle(db, store, row, "skipped", reason)
        return _view(_row(db, resume_id))


def list_resumes(store: Store) -> list[dict[str, Any]]:
    """The newest resumes first, for the owner."""
    with store._read() as db:
        rows = db.execute(
            "SELECT * FROM scheduled_sends WHERE kind = ? ORDER BY created_at DESC, id LIMIT ?",
            (KIND, LIST_LIMIT),
        ).fetchall()
        return [_view(row) for row in rows]


def _settle(
    db: sqlite3.Connection,
    store: Store,
    row: sqlite3.Row,
    state: str,
    reason: str | None,
    message_id: str | None = None,
) -> None:
    db.execute(
        "UPDATE scheduled_sends SET state = ?, reason = ?, message_id = ?, updated_at = ? WHERE id = ?",
        (state, reason, message_id, store._now(), row["id"]),
    )


def _work_was_cancelled(db: sqlite3.Connection, binding_id: str, failed_at: float) -> bool:
    """Whether the session had tasks open when it failed and every one has since been cancelled."""
    statuses = [
        r["status"]
        for r in db.execute(
            "SELECT status FROM tasks WHERE assignee_id = ? AND created_at <= ?"
            " AND (ended_at IS NULL OR ended_at > ?)",
            (binding_id, failed_at, failed_at),
        )
    ]
    return bool(statuses) and all(status == "cancelled" for status in statuses)


# One predicate for the cheap check and the settling read, so they cannot disagree about what is due.
_DUE = "kind = ? AND state = 'armed' AND due_at <= ?"


def sweep(store: Store) -> int:
    """Settle every armed resume whose reset has passed. Returns how many it settled."""
    with store._read() as db:
        if (
            db.execute(
                f"SELECT 1 FROM scheduled_sends WHERE {_DUE} LIMIT 1",
                (KIND, store._now()),
            ).fetchone()
            is None
        ):
            return 0
    with store._write() as db:
        now = store._now()
        due = db.execute(
            f"SELECT * FROM scheduled_sends WHERE {_DUE} ORDER BY due_at, id",
            (KIND, now),
        ).fetchall()
        for row in due:
            binding = store._binding(db, row["binding_id"])
            if now - row["due_at"] > DROP_AFTER:
                _settle(db, store, row, "dropped", "more than 24 h past the reset")
            elif not binding.active:
                _settle(db, store, row, "dropped", "the session was retired")
            elif _work_was_cancelled(db, binding.id, json.loads(row["details"])["failed_at"]):
                _settle(db, store, row, "skipped", "task cancelled")
            else:
                try:
                    posted = store._post_broker(
                        db,
                        store._open_direct(db, store.owner, binding.id),
                        f"limit-resume:{row['id']}",
                        row["text"],
                        (binding.id,),
                        {"kind": KIND, "resume_id": row["id"]},
                        row["mode"],
                    )
                except (ModeUnsupported, TasksUnsupported) as refused:
                    _settle(db, store, row, "refused", str(refused))
                else:
                    _settle(db, store, row, "sent", None, posted.message.id)
        return len(due)

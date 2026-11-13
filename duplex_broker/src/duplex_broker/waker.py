"""What the waker is asked to do: open a closed session that has a scheduled send waiting.

A scheduled send (a usage-limit resume now) is posted as a queued delivery. A session whose adapter is
running claims it. One whose process is gone cannot, so the waker, a service on the owner's desktop holding
its own token, reads the requests listed here and starts those sessions. A request is a queued delivery of a
broker scheduled send to an active claude or codex session that is not live. Each start the waker reports is
kept; a request is not listed again for 15 minutes after one, nor after three failed starts, and the third
failure tells the owner once. A failed start right after a successful one (the run began and was killed)
also returns the delivery to the queue if the run took it without answering; a failure with no start
before it is a skip and changes no delivery.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from typing import Any

from .store import BROKER_BINDING_ID, Actor, Invalid, NotFound, Store, utc_iso

LIVE_SECONDS = 90.0  # a binding heard from this recently, or holding its stream open, is live
RETRY_AFTER = 15 * 60.0
MAX_FAILED_STARTS = 3
LIST_LIMIT = 100
REPORT_ID = re.compile(r"[A-Za-z0-9:-]{1,120}")  # opaque to the broker beyond the request-id prefix
BROKER = Actor.binding(BROKER_BINDING_ID)

# One row per queued scheduled-send delivery to an active claude or codex session; the callers drop the
# live ones and bind the cutoff for the last start.
_REQUESTS = (
    "SELECT d.id, d.binding_id, d.message_id, b.provider, b.native_id, b.title, b.workspace,"
    " s.kind, s.due_at FROM deliveries d"
    " JOIN scheduled_sends s ON s.message_id = d.message_id AND s.binding_id = d.binding_id"
    " JOIN session_bindings b ON b.id = d.binding_id"
    " WHERE d.state = 'queued' AND b.active = 1 AND b.provider IN ('claude', 'codex')"
    " AND NOT EXISTS (SELECT 1 FROM waker_starts w WHERE w.request_id = d.id AND w.at > ?)"
    " AND (SELECT COUNT(*) FROM waker_starts w WHERE w.request_id = d.id AND w.ok = 0) < ?"
    " ORDER BY d.seq"
)


def requests(store: Store, live: Callable[[str], bool]) -> list[dict[str, Any]]:
    """The sessions to start now, oldest delivery first."""
    with store._read() as db:
        rows = db.execute(_REQUESTS, (store._now() - RETRY_AFTER, MAX_FAILED_STARTS)).fetchall()
    return [
        {
            "id": row["id"],
            "binding_id": row["binding_id"],
            "provider": row["provider"],
            "native_id": row["native_id"],
            "title": row["title"],
            "workspace": row["workspace"],
            "reason": row["kind"],
            "message_id": row["message_id"],
            "due_at": utc_iso(row["due_at"]),
        }
        for row in rows
        if not live(row["binding_id"])
    ]


def record_start(
    store: Store, request_id: str, report_id: str, ok: bool, route: str, detail: str
) -> tuple[dict[str, Any], bool]:
    """Keep one start report per `report_id`, an opaque unique string the waker repeats on a retry. A repeat
    (the waker lost the answer) returns the report as first taken and changes nothing: no row, no failed-start
    count, no requeue. The third failed start for a request tells the owner, once."""
    if not (
        report_id.startswith(request_id + ":")
        and len(report_id) > len(request_id) + 1
        and REPORT_ID.fullmatch(report_id)
    ):
        raise Invalid("report_id must begin with <request id>: and use letters, digits, : and - only")
    with store._write() as db:
        row = db.execute(
            "SELECT d.binding_id, b.title FROM deliveries d"
            " JOIN scheduled_sends s ON s.message_id = d.message_id AND s.binding_id = d.binding_id"
            " JOIN session_bindings b ON b.id = d.binding_id WHERE d.id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"no waker request {request_id}")
        previous = db.execute(
            "SELECT ok FROM waker_starts WHERE request_id = ? ORDER BY id DESC LIMIT 1", (request_id,)
        ).fetchone()
        began = previous is not None and previous["ok"] == 1  # the last report was a run that began
        at = store._now()
        taken = db.execute(
            "INSERT OR IGNORE INTO waker_starts (report_id, request_id, binding_id, at, ok, route, detail)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (report_id, request_id, row["binding_id"], at, int(ok), route, detail),
        )
        if taken.rowcount == 0:
            first = db.execute("SELECT * FROM waker_starts WHERE report_id = ?", (report_id,)).fetchone()
            return _view(first), False
        if not ok and began:
            # A run the waker started ended; whatever it claimed or read it did not answer. A report with no
            # start before it is a skip: nothing of the waker's touched the delivery.
            store.requeue_unanswered(db, request_id, "the waker's run ended without the session answering it")
        failed = db.execute(
            "SELECT COUNT(*) FROM waker_starts WHERE request_id = ? AND ok = 0", (request_id,)
        ).fetchone()[0]
        if not ok and failed == MAX_FAILED_STARTS:
            _tell_owner(store, db, request_id, row["title"], route, detail)
        return {
            "report_id": report_id,
            "request_id": request_id,
            "binding_id": row["binding_id"],
            "at": utc_iso(at),
            "ok": ok,
            "route": route,
            "detail": detail,
        }, True


def _view(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "report_id": row["report_id"],
        "request_id": row["request_id"],
        "binding_id": row["binding_id"],
        "at": utc_iso(row["at"]),
        "ok": bool(row["ok"]),
        "route": row["route"],
        "detail": row["detail"],
    }


def _tell_owner(
    store: Store, db: sqlite3.Connection, request_id: str, title: str, route: str, detail: str
) -> None:
    notices = store._open_notices(db, store.owner)
    text = (
        f"Starting {title} for its scheduled send failed {MAX_FAILED_STARTS} times, last by {route}: {detail}"
        " The send stays queued until that session opens."
    )
    store._post(db, BROKER, notices, f"waker:{request_id}:failed", text, None, None)


def recent_starts(store: Store) -> list[dict[str, Any]]:
    """The newest starts first, for the owner."""
    with store._read() as db:
        rows = db.execute(
            "SELECT w.request_id, w.binding_id, b.title, w.at, w.ok, w.route, w.detail FROM waker_starts w"
            " JOIN session_bindings b ON b.id = w.binding_id ORDER BY w.id DESC LIMIT ?",
            (LIST_LIMIT,),
        ).fetchall()
    return [
        {
            "request_id": row["request_id"],
            "binding_id": row["binding_id"],
            "title": row["title"],
            "at": utc_iso(row["at"]),
            "ok": bool(row["ok"]),
            "route": row["route"],
            "detail": row["detail"],
        }
        for row in rows
    ]

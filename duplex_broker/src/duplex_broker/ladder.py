"""The wake ladder: a delivery handed to a session that does not read it is woken again, then closed.

A binding opts in by declaring `rewake: verified`. Each in-flight delivery to it gets a deadline. When the
deadline passes, the broker asks the adapter, on its next claim poll, to send its fixed wake line again,
up to `rewake_limit` times; the deadline after the last one closes the delivery as `uncertain` and frees
the session's queue. After `hold_after` such closures in a row the session is held: nothing more is handed
to it until it reads or registers again, and the owner gets one notice in the broker's direct conversation.
Counts and deadlines are stored with the delivery, so a broker restart continues a ladder where it was.
Two counts are kept apart: the re-wakes asked for (at most one waits for the adapter at a time) and the
ones the adapter reported sending. The close time is fixed when the delivery joins the ladder, so an
adapter that never polls cannot hold the queue past it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from .store import (
    BROKER_BINDING_ID,
    INFLIGHT,
    Actor,
    Conflict,
    Delivery,
    Forbidden,
    Invalid,
    Message,
    Store,
)

REWAKE_CAPABILITY = "rewake"
BROKER = Actor.binding(BROKER_BINDING_ID)
# States a late read finds: the delivery was closed without the session reading it.
LATE_STATES = ("uncertain", "failed", "cancelled")
_MARKS = ", ".join("?" * len(INFLIGHT))
_DUE = (
    "SELECT d.id, d.binding_id, d.updated_at, d.wake_due_at, d.close_at, d.rewakes, d.rewake_wanted"
    " FROM deliveries d"
    " JOIN session_bindings b ON b.id = d.binding_id"
    f" WHERE d.state IN ({_MARKS}) AND b.active = 1"
    f" AND json_extract(b.capabilities, '$.{REWAKE_CAPABILITY}') = 'verified'"
    " AND (d.wake_due_at IS NULL OR d.wake_due_at <= ?) ORDER BY d.seq"
)


@dataclass(frozen=True)
class Policy:
    wake_timeout: float = 300.0  # seconds from hand-over to the first re-wake
    rewake_interval: float = 300.0  # seconds between re-wakes, and from the last one to the close
    rewake_limit: int = 3
    hold_after: int = 2  # closures in a row before the session is held

    @property
    def window(self) -> float:
        """Seconds from hand-over to the close."""
        return self.wake_timeout + self.rewake_limit * self.rewake_interval


def sweep(store: Store, policy: Policy, owner: str) -> int:
    """One pass: give each new in-flight delivery its deadlines, ask for the re-wakes that are due and close
    the deliveries that ran out. Returns how many re-wakes asked for and closures it made."""
    with store._read() as db:
        if db.execute(_DUE + " LIMIT 1", (*INFLIGHT, store._now())).fetchone() is None:
            return 0
    changes = 0
    with store._write() as db:
        now = store._now()
        for row in db.execute(_DUE, (*INFLIGHT, now)).fetchall():
            due = row["wake_due_at"]
            if due is None:
                due = row["updated_at"] + policy.wake_timeout
            close_at = row["close_at"]
            if close_at is None:
                close_at = due + (policy.rewake_limit - row["rewakes"]) * policy.rewake_interval
            db.execute(
                "UPDATE deliveries SET wake_due_at = ?, close_at = ? WHERE id = ?",
                (due, close_at, row["id"]),
            )
            if now >= close_at:
                changes += 1
                _expire(store, db, row["id"], row["binding_id"], policy, owner, now)
            elif due > now:
                continue
            elif row["rewake_wanted"] or row["rewakes"] >= policy.rewake_limit:
                # The adapter has not taken the last request (or none is left to ask): ask for nothing
                # more, so the count stays what was asked of an adapter that could act on it.
                db.execute(
                    "UPDATE deliveries SET wake_due_at = ? WHERE id = ?",
                    (min(now + policy.rewake_interval, close_at), row["id"]),
                )
            else:
                changes += 1
                _request(store, db, row["id"], row["rewakes"] + 1, close_at, policy, now)
    return changes


def _request(
    store: Store,
    db: sqlite3.Connection,
    delivery_id: str,
    attempt: int,
    close_at: float,
    policy: Policy,
    now: float,
) -> None:
    db.execute(
        "UPDATE deliveries SET rewakes = ?, rewake_wanted = 1, wake_due_at = ? WHERE id = ?",
        (attempt, min(now + policy.rewake_interval, close_at), delivery_id),
    )
    _event(
        store,
        db,
        store._delivery(db, delivery_id),
        "delivery.wake_requested",
        BROKER,
        {"attempt": attempt, "limit": policy.rewake_limit},
    )


def _expire(
    store: Store,
    db: sqlite3.Connection,
    delivery_id: str,
    binding_id: str,
    policy: Policy,
    owner: str,
    now: float,
) -> None:
    """Close one delivery as uncertain, which frees the head of the queue, and hold the session when it
    has now let `hold_after` deliveries in a row run out."""
    asked = store._delivery(db, delivery_id)
    reason = (
        f"not read within {round(policy.window)} s; {asked.rewakes} re-wakes asked for, "
        f"{asked.wakes_sent} sent by the adapter"
    )
    delivery = store._set_state(db, delivery_id, "uncertain", None, BROKER, reason=reason)
    _event(
        store,
        db,
        delivery,
        "delivery.expired",
        BROKER,
        {"rewakes_asked": delivery.rewakes, "rewakes_sent": delivery.wakes_sent, "reason": reason},
    )
    db.execute("UPDATE session_bindings SET expiries = expiries + 1 WHERE id = ?", (binding_id,))
    binding = store._binding(db, binding_id)
    if binding.held_at is None and binding.expiries >= policy.hold_after:
        _hold(store, db, binding_id, binding.title, binding.expiries, policy, owner, now)


def _hold(
    store: Store,
    db: sqlite3.Connection,
    binding_id: str,
    title: str,
    expiries: int,
    policy: Policy,
    owner: str,
    now: float,
) -> None:
    db.execute("UPDATE session_bindings SET held_at = ? WHERE id = ?", (now, binding_id))
    queued = db.execute(
        "SELECT COUNT(*) FROM deliveries WHERE binding_id = ? AND state = 'queued'", (binding_id,)
    ).fetchone()[0]
    asked, sent = db.execute(
        "SELECT COALESCE(SUM(rewakes), 0), COALESCE(SUM(wakes_sent), 0) FROM"
        " (SELECT rewakes, wakes_sent FROM deliveries WHERE binding_id = ? AND state = 'uncertain'"
        " AND reason LIKE 'not read within%' ORDER BY seq DESC LIMIT ?)",
        (binding_id, expiries),
    ).fetchone()
    notices = store._open_notices(db, owner)
    text = (
        f"Delivery to {title} is held. {expiries} messages in a row were not read and are marked "
        f"uncertain; across them {asked} re-wakes were asked for and the adapter sent {sent}. "
        f"{queued} more wait in its queue. "
        "It resumes when the session reads a message or registers again. A waiting message can be "
        "redirected or stopped from its conversation."
    )
    store._post(db, BROKER, notices, f"hold:{binding_id}:{now}", text, None, None)
    store._append(
        db,
        notices,
        "session.held",
        BROKER,
        None,
        {"binding_id": binding_id, "title": title, "expiries": expiries, "queued": queued},
    )


def _event(
    store: Store,
    db: sqlite3.Connection,
    delivery: Delivery,
    event_type: str,
    actor: Actor,
    extra: dict[str, Any],
) -> None:
    message = store._message(db, delivery.message_id)
    payload = {"delivery_id": delivery.id, "binding_id": delivery.binding_id, **extra}
    store._append(db, message.conversation_id, event_type, actor, message.id, payload)


def _own(store: Store, db: sqlite3.Connection, delivery_id: str, binding_id: str, lease: int) -> Delivery:
    store._require_lease(db, binding_id, lease)
    delivery = store._delivery(db, delivery_id)
    if delivery.binding_id != binding_id:
        raise Forbidden("delivery belongs to another session")
    return delivery


def take_rewake(store: Store, binding_id: str, lease: int) -> tuple[Delivery, Message, str] | None:
    """The in-flight delivery whose re-wake is due, handed to the adapter once; with its conversation kind."""
    with store._read() as db:
        wanted = db.execute(
            "SELECT 1 FROM deliveries WHERE binding_id = ? AND rewake_wanted = 1 LIMIT 1", (binding_id,)
        ).fetchone()
    if wanted is None:
        return None
    with store._write() as db:
        store._require_lease(db, binding_id, lease)
        row = db.execute(
            "SELECT id FROM deliveries WHERE binding_id = ? AND rewake_wanted = 1"
            f" AND state IN ({_MARKS}) ORDER BY seq LIMIT 1",
            (binding_id, *INFLIGHT),
        ).fetchone()
        if row is None:
            return None
        db.execute("UPDATE deliveries SET rewake_wanted = 0 WHERE id = ?", (row["id"],))
        delivery = store._delivery(db, row["id"])
        message = store._message(db, delivery.message_id)
        return delivery, message, store._conversation(db, message.conversation_id).kind


def record_wake(
    store: Store,
    delivery_id: str,
    binding_id: str,
    lease: int,
    attempt: int,
    ok: bool,
    error: str | None,
) -> None:
    """The adapter's report of one re-wake: sent, or why not. A report for a delivery no longer in flight,
    or for an attempt never asked for, is refused and changes no count."""
    with store._write() as db:
        delivery = _own(store, db, delivery_id, binding_id, lease)
        if delivery.state not in INFLIGHT:
            raise Conflict(f"delivery {delivery_id} is {delivery.state}, no longer in flight")
        if not 1 <= attempt <= delivery.rewakes:
            raise Invalid(f"re-wake {attempt} was not asked for; {delivery.rewakes} were")
        if ok:
            db.execute("UPDATE deliveries SET wakes_sent = wakes_sent + 1 WHERE id = ?", (delivery_id,))
        extra: dict[str, Any] = {"attempt": attempt, "ok": ok}
        if error:
            extra["error"] = error[:300]
        _event(store, db, delivery, "delivery.wake_outcome", Actor.binding(binding_id), extra)


def late_read(store: Store, delivery_id: str, binding_id: str, lease: int) -> dict[str, Any]:
    """A session reading a delivery that was already closed unread. It gets where the message is, never
    the message as a fresh delivery. The first read of a delivery counts as the session reading, so a hold
    ends; a repeat returns the pointer and changes nothing."""
    with store._write() as db:
        delivery = _own(store, db, delivery_id, binding_id, lease)
        if delivery.state not in LATE_STATES:
            raise Invalid(f"delivery {delivery_id} is {delivery.state}, not closed")
        message = store._message(db, delivery.message_id)
        conversation = store._conversation(db, message.conversation_id)
        seq = db.execute(
            "SELECT seq FROM events WHERE conversation_id = ? AND message_id = ?"
            " AND type = 'message.accepted'",
            (conversation.id, message.id),
        ).fetchone()
        envelope = store._envelope_of(db, message)
        pointer = {
            "delivery_id": delivery.id,
            "message_id": message.id,
            "conversation_id": conversation.id,
            "conversation_kind": conversation.kind,
            "conversation_title": conversation.title,
            "from": store._party(db, message.actor),
            "to": envelope.get("to", []),
            "sent_at": envelope.get("sent_at"),
            "posted_at": message.created_at,
            "age_seconds": round(store._now() - message.created_at),
            "state": delivery.state,
            "reason": delivery.reason,
            "history_after": (seq[0] - 1) if seq else 0,
        }
        seen = db.execute(
            "SELECT 1 FROM events WHERE message_id = ? AND type = 'delivery.read_late'"
            " AND json_extract(payload, '$.delivery_id') = ?",
            (message.id, delivery.id),
        ).fetchone()
        if seen is None:
            _event(
                store,
                db,
                delivery,
                "delivery.read_late",
                Actor.binding(binding_id),
                {"state": delivery.state},
            )
            store._resume(db, binding_id, "the session read a closed delivery")
        return pointer

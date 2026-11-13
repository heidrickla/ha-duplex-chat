"""Durable conversation state: one SQLite writer, WAL, an append-only event log per conversation.

Every read is scoped by conversation membership. A direct conversation holds its owner and exactly one
session binding; nothing in it reaches a group, and replies to it cannot name one.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast, get_args

from .ids import reply_id

Kind = Literal["group", "direct"]
Mode = Literal["user_directed", "consult", "collaborate"]
Provider = Literal["claude", "codex", "mock", "model", "broker", "chatgpt", "opencode"]
ActorKind = Literal["user", "binding"]
DeliveryState = Literal[
    "queued", "dispatching", "accepted", "consumed", "responded", "cancelled", "failed", "uncertain"
]

DELIVERY_STATES: tuple[str, ...] = get_args(DeliveryState)

MAX_MESSAGE_BYTES = 64 * 1024
# How a message asks its recipient to take it. A binding says which it can do in capabilities.send_modes;
# one that says nothing takes auto only, and the broker never turns a mode into another.
SEND_MODES = ("auto", "queue", "steer", "restart")
TURN_STATES = ("busy", "idle", "waiting", "unknown")
# Notices a session acts on only with the task tools: a target that did not declare `tasks` is sent none.
TASK_NOTICE_KINDS = frozenset(
    f"task_{k}" for k in ("assigned", "nudge", "result", "question", "answer", "cancelled")
)
# A session in one of these is inside a turn: a queue-mode message waits for it to end.
HOLDS_QUEUE = ("busy", "waiting")
# What a session's turn waits on is a kind of wait, never text: a notice repeats it, and prompt text must not
# reach one.
WAITING_FOR = re.compile(r"[a-z][a-z0-9_]{0,39}\Z")
DEVICE_TOKEN_PREFIX = "dcd_"
# Handed to an adapter and not yet read by the session. A consumed delivery is no longer in flight: the
# session has the message, and a session that is still busy queues the next one itself (Claude Code reads
# its inbox between tool calls). An adapter that must hold the session until a turn ends (Codex) keeps the
# delivery `accepted` and moves it straight to `responded`.
INFLIGHT: tuple[DeliveryState, ...] = ("dispatching", "accepted")
INFLIGHT_BY_BINDING = (
    f"SELECT id FROM deliveries WHERE binding_id = ? AND state IN ({', '.join('?' * len(INFLIGHT))})"
)
INFLIGHT_STATES_BY_BINDING = INFLIGHT_BY_BINDING.replace("SELECT id", "SELECT DISTINCT state")
TRANSITIONS: dict[DeliveryState, frozenset[DeliveryState]] = {
    "queued": frozenset({"dispatching", "cancelled"}),
    "dispatching": frozenset({"accepted", "failed", "uncertain"}),
    "accepted": frozenset({"consumed", "responded", "failed", "uncertain"}),
    "consumed": frozenset({"responded", "failed", "uncertain"}),
    "responded": frozenset(),
    "cancelled": frozenset(),
    "failed": frozenset(),
    "uncertain": frozenset(),
}
# Why a delivery in flight when its session registered again ends uncertain (and what `requeue_unanswered`
# recognizes as that end).
REREGISTERED = "the session registered again before reading it"
# Terminal states that are not a normal end: each change into one records why.
REASON_STATES: frozenset[DeliveryState] = frozenset({"uncertain", "cancelled", "failed"})
READ_STATES: frozenset[DeliveryState] = frozenset({"consumed", "responded"})
# Ends the sender is told about with a failure notice.
FAILED_STATES: frozenset[DeliveryState] = frozenset({"failed", "uncertain"})
# What a message.accepted event carries beside the text: sender, recipients, sent time, and a broker
# notice's details.
ENVELOPE = ("from", "to", "sent_at", "notice")
# The broker's own notices member: an inactive binding with a fixed id, so it is never offered as a
# recipient, and its direct conversation with the owner carries escalations the phone already shows.
BROKER_BINDING_ID = "broker"
BROKER_TITLE = "Duplex broker"

SCHEMA_VERSION = 14
SCHEMA = """
CREATE TABLE session_bindings (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex', 'mock')),
  native_id TEXT NOT NULL,
  title TEXT NOT NULL,
  workspace TEXT NOT NULL,
  endpoint_ref TEXT NOT NULL,
  capabilities TEXT NOT NULL,
  lease_generation INTEGER NOT NULL,
  token_sha TEXT NOT NULL UNIQUE,
  active INTEGER NOT NULL CHECK (active IN (0, 1)),
  heartbeat_at REAL,
  created_at REAL NOT NULL
);
CREATE UNIQUE INDEX one_active_binding ON session_bindings (provider, native_id) WHERE active = 1;

CREATE TABLE conversations (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('group', 'direct')),
  title TEXT NOT NULL,
  owner TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('user_directed', 'consult', 'collaborate')),
  dm_binding_id TEXT REFERENCES session_bindings (id),
  paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
  created_at REAL NOT NULL,
  CHECK ((kind = 'direct') = (dm_binding_id IS NOT NULL)),
  CHECK (kind = 'group' OR mode = 'user_directed')
);
CREATE UNIQUE INDEX one_dm ON conversations (owner, dm_binding_id) WHERE kind = 'direct';

CREATE TABLE conversation_members (
  conversation_id TEXT NOT NULL REFERENCES conversations (id),
  member_kind TEXT NOT NULL CHECK (member_kind IN ('user', 'binding')),
  member_id TEXT NOT NULL,
  role TEXT NOT NULL,
  PRIMARY KEY (conversation_id, member_kind, member_id)
);

CREATE TABLE messages (
  id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL REFERENCES conversations (id),
  actor_kind TEXT NOT NULL CHECK (actor_kind IN ('user', 'binding')),
  actor_id TEXT NOT NULL,
  client_message_id TEXT NOT NULL,
  content_sha TEXT NOT NULL,
  text TEXT NOT NULL,
  recipients TEXT NOT NULL,
  parent_id TEXT REFERENCES messages (id),
  created_at REAL NOT NULL,
  UNIQUE (actor_kind, actor_id, client_message_id)
);

CREATE TABLE events (
  conversation_id TEXT NOT NULL REFERENCES conversations (id),
  seq INTEGER NOT NULL,
  event_id TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL,
  actor_kind TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  message_id TEXT REFERENCES messages (id),
  payload TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY (conversation_id, seq)
);

CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL REFERENCES messages (id),
  binding_id TEXT NOT NULL REFERENCES session_bindings (id),
  state TEXT NOT NULL CHECK (state IN
    ('queued', 'dispatching', 'accepted', 'consumed', 'responded', 'cancelled', 'failed', 'uncertain')),
  attempt INTEGER NOT NULL DEFAULT 0,
  native_turn_id TEXT,
  lease_generation INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  updated_at REAL NOT NULL,
  UNIQUE (message_id, binding_id)
);
CREATE INDEX deliveries_by_binding ON deliveries (binding_id, state, seq);
"""

# Phones and other owner devices. Tokens are stored as hashes; a rotated token keeps working until
# prev_expires_at so a phone that dies between the response and its own write is not stranded.
SCHEMA_V2 = """
CREATE TABLE devices (
  id TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  token_sha TEXT NOT NULL UNIQUE,
  prev_token_sha TEXT,
  prev_expires_at REAL,
  disabled INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1)),
  created_at REAL NOT NULL,
  last_seen_at REAL
);
"""

# Peer exchanges. root_id is the owner message a chain of agent messages answers; depth counts
# agent hops from it (the owner's own messages are depth 0). A collaboration is the owner's bounded
# grant for agents to address each other: an objective, a turn budget and a deadline.
SCHEMA_V3 = """
ALTER TABLE messages ADD COLUMN root_id TEXT;
ALTER TABLE messages ADD COLUMN depth INTEGER NOT NULL DEFAULT 0;
CREATE TABLE collaborations (
  conversation_id TEXT PRIMARY KEY REFERENCES conversations (id),
  objective TEXT NOT NULL,
  max_turns INTEGER NOT NULL,
  turns_used INTEGER NOT NULL DEFAULT 0,
  started_at REAL NOT NULL,
  expires_at REAL NOT NULL,
  active INTEGER NOT NULL CHECK (active IN (0, 1)),
  stopped_reason TEXT
);
"""

# Version 4: the owner removes a group from every list without losing its history.
SCHEMA_V4 = """
ALTER TABLE conversations ADD COLUMN archived INTEGER NOT NULL DEFAULT 0;
"""

# Names that databases created before schema 13 hold for the built-in model member and the connector.
LEGACY_MODEL = "bonsai"
LEGACY_CONNECTOR_NATIVE = "cody"

# Version 5: the built-in model member has the provider LEGACY_MODEL (renamed at version 13). The CHECK
# is widened in place with writable_schema (SQLite's documented route for a looser constraint); a table
# rebuild would have to rewrite every foreign key that points at session_bindings, by _widen_providers.
SCHEMA_V5 = ""
OLD_PROVIDERS = "('claude', 'codex', 'mock')"
NEW_PROVIDERS = f"('claude', 'codex', 'mock', '{LEGACY_MODEL}')"

# Version 6: the wake ladder. A delivery in flight to a session that re-wakes carries its next deadline,
# its close time, the re-wakes asked for, the ones the adapter reported sending and whether one waits for
# the adapter; every non-normal end keeps its
# reason. A binding counts expiries since it last read and is held after too many. Provider 'broker'
# is the notices member, widened like version 5.
SCHEMA_V6 = """
ALTER TABLE deliveries ADD COLUMN wake_due_at REAL;
ALTER TABLE deliveries ADD COLUMN rewakes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN rewake_wanted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN wakes_sent INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN close_at REAL;
ALTER TABLE deliveries ADD COLUMN reason TEXT;
ALTER TABLE session_bindings ADD COLUMN expiries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE session_bindings ADD COLUMN held_at REAL;
"""
V6_PROVIDERS = f"('claude', 'codex', 'mock', '{LEGACY_MODEL}', 'broker')"

# Version 7: the ChatGPT connector. Provider 'chatgpt' is its binding, widened like version 5. The tables
# hold the connector's OAuth clients and token hashes and ChatGPT's event subscriptions (connector.py).
SCHEMA_V7 = """
CREATE TABLE connector_clients (
  client_id TEXT PRIMARY KEY,
  redirect_uris TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE connector_tokens (
  token_sha TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('access', 'refresh')),
  client_id TEXT NOT NULL,
  expires_at REAL NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE connector_subscriptions (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  arguments TEXT NOT NULL,
  url TEXT NOT NULL,
  secret TEXT NOT NULL,
  refresh_before REAL NOT NULL,
  created_at REAL NOT NULL,
  UNIQUE (name, url)
);
"""
V7_PROVIDERS = f"('claude', 'codex', 'mock', '{LEGACY_MODEL}', 'broker', 'chatgpt')"

# Version 8: what the broker refused and why, for diagnosis. Rows older than REFUSAL_TTL and any beyond
# the newest REFUSAL_CAP are deleted on every write and read, so the table never outgrows a day.
SCHEMA_V8 = """
CREATE TABLE refusals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at REAL NOT NULL,
  source TEXT NOT NULL,
  what TEXT NOT NULL,
  reason TEXT NOT NULL
);
CREATE INDEX refusals_at ON refusals (at)
"""
# Version 9: each subscription's renewals and the last event ChatGPT answered, so a missed wake can be told
# apart from an event ChatGPT accepted and then did not act on (2026-11-03).
SCHEMA_V9 = """
ALTER TABLE connector_subscriptions ADD COLUMN renewed_at REAL;
ALTER TABLE connector_subscriptions ADD COLUMN renewals INTEGER NOT NULL DEFAULT 0;
ALTER TABLE connector_subscriptions ADD COLUMN last_event_at REAL;
ALTER TABLE connector_subscriptions ADD COLUMN last_event_id TEXT;
ALTER TABLE connector_subscriptions ADD COLUMN last_status INTEGER
"""
# Version 10: what each sender is told about its message's deliveries (2026-11-03). A `read` row is a receipt,
# reported once to a session sender on its next read; a `failed` row is a failure notice and names the
# notice message the broker posted for it. One row of each kind per delivery at most. Every earlier
# message.accepted event gets the envelope later ones are written with (Store._add_envelopes).
SCHEMA_V10 = """
CREATE TABLE sender_notices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL CHECK (kind IN ('read', 'failed')),
  delivery_id TEXT NOT NULL REFERENCES deliveries (id),
  message_id TEXT NOT NULL REFERENCES messages (id),
  sender_kind TEXT NOT NULL,
  sender_id TEXT NOT NULL,
  state TEXT NOT NULL,
  reason TEXT,
  at REAL NOT NULL,
  notice_message_id TEXT REFERENCES messages (id),
  reported_at REAL,
  UNIQUE (delivery_id, kind)
);
CREATE INDEX sender_notices_by_sender ON sender_notices (sender_kind, sender_id, kind, reported_at);
CREATE INDEX sender_notices_by_notice ON sender_notices (notice_message_id)
"""
# Version 11: what each session's adapter says it is at registration (2026-11-04): its version, checkout
# commit, the Claude Code and Codex releases it was built against and the runtime it found. NULL is an
# adapter from before versioning, which reads as version "0".
SCHEMA_V11 = """
ALTER TABLE session_bindings ADD COLUMN adapter TEXT
"""
# Version 12: send modes, turn state, delegated tasks, scheduled sends and the waker's starts (2026-11-11).
# A message carries how its recipient should take it; a binding keeps the turn state its adapter last
# reported. A task is one delegated piece of work between two sessions, with its events and questions. A
# scheduled send is a message the broker owes a session at due_at; its kind says why (limit_resume now) and
# the kind's own fields live in details. A waker start is one attempt to open a closed session for a queued
# scheduled send.
SCHEMA_V12 = """
ALTER TABLE messages ADD COLUMN mode TEXT NOT NULL DEFAULT 'auto'
  CHECK (mode IN ('auto', 'queue', 'steer', 'restart'));
ALTER TABLE session_bindings ADD COLUMN turn_state TEXT NOT NULL DEFAULT 'unknown'
  CHECK (turn_state IN ('busy', 'idle', 'waiting', 'unknown'));
ALTER TABLE session_bindings ADD COLUMN turn_waiting_for TEXT;
ALTER TABLE session_bindings ADD COLUMN turn_state_at REAL;
ALTER TABLE session_bindings ADD COLUMN turn_seq INTEGER NOT NULL DEFAULT 0;
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  requester_id TEXT NOT NULL REFERENCES session_bindings (id),
  assignee_id TEXT NOT NULL REFERENCES session_bindings (id),
  parent_id TEXT REFERENCES tasks (id),
  conversation_id TEXT NOT NULL REFERENCES conversations (id),
  task TEXT NOT NULL,
  done_when TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN
    ('working', 'waiting_for_children', 'result_available', 'failed', 'cancelled')),
  outcome TEXT CHECK (outcome IN ('done', 'failed')),
  result TEXT,
  held_result TEXT,
  held_outcome TEXT CHECK (held_outcome IN ('done', 'failed')),
  delivered_at REAL,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  ended_at REAL
);
CREATE INDEX tasks_by_assignee ON tasks (assignee_id, status);
CREATE INDEX tasks_by_requester ON tasks (requester_id, status);
CREATE INDEX tasks_by_parent ON tasks (parent_id);
CREATE TABLE task_events (
  task_id TEXT NOT NULL REFERENCES tasks (id),
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  actor_kind TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY (task_id, seq)
);
CREATE TABLE task_questions (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks (id),
  kind TEXT NOT NULL CHECK (kind IN ('asked', 'observed')),
  text TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('open', 'answered', 'withdrawn')),
  answer TEXT,
  notice_message_id TEXT REFERENCES messages (id),
  created_at REAL NOT NULL,
  closed_at REAL
);
CREATE INDEX task_questions_by_task ON task_questions (task_id, state);
CREATE TABLE scheduled_sends (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  binding_id TEXT NOT NULL REFERENCES session_bindings (id),
  text TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'auto' CHECK (mode IN ('auto', 'queue', 'steer', 'restart')),
  due_at REAL NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('armed', 'sent', 'skipped', 'dropped', 'not_armed', 'refused')),
  reason TEXT,
  message_id TEXT REFERENCES messages (id),
  details TEXT NOT NULL,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE INDEX scheduled_sends_by_state ON scheduled_sends (state, due_at);
CREATE INDEX scheduled_sends_by_message ON scheduled_sends (message_id);
CREATE TABLE waker_starts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_id TEXT NOT NULL UNIQUE,
  request_id TEXT NOT NULL REFERENCES deliveries (id),
  binding_id TEXT NOT NULL REFERENCES session_bindings (id),
  at REAL NOT NULL,
  ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
  route TEXT NOT NULL,
  detail TEXT NOT NULL
);
CREATE INDEX waker_starts_by_request ON waker_starts (request_id, at)
"""
# Version 13 stores the owner identity independently of configuration. Existing installs adopt the owner
# on their broker-notices conversation before any new owner-scoped operation is served.
SCHEMA_V13 = """
CREATE TABLE meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""
V13_PROVIDERS = "('claude', 'codex', 'mock', 'model', 'broker', 'chatgpt')"
# Version 14: provider 'opencode', an OpenCode session the `duplex opencode` adapter holds, widened like
# version 5. No table or row changes.
SCHEMA_V14 = ""
V14_PROVIDERS = "('claude', 'codex', 'mock', 'model', 'broker', 'chatgpt', 'opencode')"
REFUSAL_TTL = 24 * 3600.0
REFUSAL_CAP = 1000
WIDEN = {
    5: (OLD_PROVIDERS, NEW_PROVIDERS),
    6: (NEW_PROVIDERS, V6_PROVIDERS),
    7: (V6_PROVIDERS, V7_PROVIDERS),
    13: (V7_PROVIDERS, V13_PROVIDERS),
    14: (V13_PROVIDERS, V14_PROVIDERS),
}

MIGRATIONS = {
    1: SCHEMA,
    2: SCHEMA_V2,
    3: SCHEMA_V3,
    4: SCHEMA_V4,
    5: SCHEMA_V5,
    6: SCHEMA_V6,
    7: SCHEMA_V7,
    8: SCHEMA_V8,
    9: SCHEMA_V9,
    10: SCHEMA_V10,
    11: SCHEMA_V11,
    12: SCHEMA_V12,
    13: SCHEMA_V13,
    14: SCHEMA_V14,
}
# Steps that rewrite existing rows after their schema change: the Store method to run.
REWRITES = {10: "_add_envelopes", 13: "_adopt_owner_and_model"}
REMOVED = "the owner removed this group"
GROUP_REMOVED = "group removed"
MAX_COLLABORATION_TURNS = 12
MAX_COLLABORATION_MINUTES = 60


class BrokerError(Exception):
    code = "error"


class NotFound(BrokerError):
    code = "not_found"


class Forbidden(BrokerError):
    code = "forbidden"


class Conflict(BrokerError):
    code = "conflict"


class Invalid(BrokerError):
    code = "invalid"


class ModeUnsupported(Invalid):
    """The post asked for a send mode a recipient cannot take; the detail names each one and what it can."""

    code = "mode_unsupported"


class TasksUnsupported(Invalid):
    """A task names a session whose adapter does not declare `tasks`: it has no tool to act on the notices."""

    code = "tasks_unsupported"


class Gone(BrokerError):
    """The token belongs to a session the owner retired: its adapter should stop, not re-register."""

    code = "gone"


@dataclass(frozen=True)
class Actor:
    kind: ActorKind
    id: str

    @staticmethod
    def user(name: str) -> Actor:
        return Actor("user", name)

    @staticmethod
    def binding(binding_id: str) -> Actor:
        return Actor("binding", binding_id)


@dataclass(frozen=True)
class Binding:
    id: str
    provider: Provider
    native_id: str
    title: str
    workspace: str
    endpoint_ref: str
    capabilities: dict[str, Any]
    lease_generation: int
    active: bool
    heartbeat_at: float | None
    expiries: int = 0  # deliveries closed unread in a row since the session last read
    held_at: float | None = None  # nothing is handed out while set
    adapter: dict[str, Any] | None = None  # what its adapter reported at registration
    turn_state: str = "unknown"  # what the adapter last reported; registration resets it
    turn_waiting_for: str | None = None
    turn_state_at: float | None = None
    turn_seq: int = 0  # the newest reading applied under this lease; registration resets it

    @property
    def takes_tasks(self) -> bool:
        """Whether its adapter declared tools that act on task notices."""
        return self.capabilities.get("tasks") is True

    @property
    def send_modes(self) -> tuple[str, ...]:
        """The send modes this binding takes: what its adapter declared, else auto alone."""
        declared = self.capabilities.get("send_modes")
        if isinstance(declared, list):
            modes = tuple(m for m in SEND_MODES if m in declared)
            if modes:
                return modes
        return ("auto",)


@dataclass(frozen=True)
class Device:
    id: str
    label: str
    disabled: bool
    created_at: float
    last_seen_at: float | None


@dataclass(frozen=True)
class Conversation:
    id: str
    kind: Kind
    title: str
    owner: str
    mode: Mode
    dm_binding_id: str | None
    paused: bool
    archived: bool = False


@dataclass(frozen=True)
class Message:
    id: str
    conversation_id: str
    actor: Actor
    client_message_id: str
    text: str
    recipients: tuple[str, ...]
    parent_id: str | None
    created_at: float
    root_id: str | None = None
    depth: int = 0
    mode: str = "auto"


@dataclass(frozen=True)
class Member:
    kind: ActorKind
    id: str
    title: str
    provider: Provider | None
    active: bool
    adapter: dict[str, Any] | None = None
    adapter_behind: bool | None = None


@dataclass(frozen=True)
class Collaboration:
    conversation_id: str
    objective: str
    max_turns: int
    turns_used: int
    started_at: float
    expires_at: float
    active: bool
    stopped_reason: str | None


@dataclass(frozen=True)
class Delivery:
    id: str
    message_id: str
    binding_id: str
    state: DeliveryState
    attempt: int
    native_turn_id: str | None
    lease_generation: int
    rewakes: int = 0  # re-wakes the broker asked for
    reason: str | None = None
    wakes_sent: int = 0  # re-wakes the adapter reported sending


@dataclass(frozen=True)
class Event:
    conversation_id: str
    seq: int
    event_id: str
    type: str
    actor: Actor
    message_id: str | None
    payload: dict[str, Any]
    created_at: float


@dataclass(frozen=True)
class PostResult:
    message: Message
    deliveries: tuple[Delivery, ...]
    created: bool


@dataclass(frozen=True)
class DeliveryView:
    """One recipient's delivery as a conversation member sees it."""

    delivery: Delivery
    recipient_title: str
    updated_at: float
    read_at: float | None = None  # when it first reached consumed or responded


@dataclass(frozen=True)
class MessageRecord:
    """One message by id: where it sits, each recipient's delivery, and its own events in order."""

    message: Message
    seq: int
    deliveries: tuple[DeliveryView, ...]
    events: tuple[Event, ...]


@dataclass(frozen=True)
class RedirectResult:
    """The owner's copy of a message for the right session. `moved`: the source was still queued
    and is cancelled; `copied`: the source had already gone out and keeps its state."""

    source: Delivery
    message: Message
    delivery: Delivery
    disposition: Literal["moved", "copied"]
    created: bool


@dataclass(frozen=True)
class Receipt:
    """One recipient's first read of a session's message, told to that session once."""

    message_id: str
    conversation_id: str
    recipient: str
    state: DeliveryState
    read_at: float
    sent_at: float


def _new_id() -> str:
    return uuid.uuid4().hex


def utc_iso(at: float) -> str:
    """A broker time as UTC ISO 8601 with a Z."""
    return datetime.fromtimestamp(at, UTC).isoformat().replace("+00:00", "Z")


def _content_sha(conversation_id: str, text: str, recipients: Sequence[str], parent_id: str | None) -> str:
    body = json.dumps([conversation_id, text, sorted(recipients), parent_id], separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def _token_sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# The providers whose bindings run the session adapter, and so report a version. The ChatGPT connector,
# the model member and the broker's own member are built in and have none.
ADAPTER_PROVIDERS = ("claude", "codex", "opencode")


def version_key(version: str) -> tuple[int, ...]:
    """A release number as integers, trailing zeros dropped (1.0 equals 1.0.0); anything else is 0."""
    found = re.match(r"v?(\d+(?:\.\d+)*)", version.strip())
    parts = [int(p) for p in found.group(1).split(".")] if found else [0]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


def adapter_version(binding: Binding) -> str:
    """The version a binding's adapter reported; an adapter that reported nothing is "0"."""
    return str((binding.adapter or {}).get("version") or "0")


def adapter_behind(binding: Binding, highest: str) -> bool | None:
    """Whether the binding's adapter is older than `highest`; None for a binding without an adapter."""
    if binding.provider not in ADAPTER_PROVIDERS:
        return None
    return version_key(adapter_version(binding)) < version_key(highest)


# A binding's token_sha after the owner releases it: no token hashes to it, and the next
# registration of that session takes the binding.
RELEASED = "released:"


Listener = Callable[[Sequence["Event"]], None]


def may_claim(mode: str, in_flight: set[str] | frozenset[str], binding: Binding) -> bool:
    """Whether the head of a session's queue, in this mode, may be handed to its adapter now. Every rule
    about when a delivery goes out is here, so no path can apply one and miss another.

    A delivery handed over and not yet confirmed (`dispatching`) blocks everything behind it. One taken into
    a running turn (`accepted`) blocks the queue unless the head is auto, steer or restart and the adapter
    declares `steer`, which is how it joins or interrupts that turn. A queue-mode head waits for the end of
    a busy or waiting turn."""
    if "dispatching" in in_flight:
        return False
    if in_flight and (mode == "queue" or "steer" not in binding.send_modes):
        return False
    return not (mode == "queue" and binding.turn_state in HOLDS_QUEUE)


class Store:
    def __init__(self, path: Path | str, now: Callable[[], float] = time.time, owner: str = "owner") -> None:
        self._now = now
        self.owner = owner  # gets the broker's notices, in its direct conversation `Duplex broker`
        self.connector_title = "ChatGPT"
        self._lock = threading.Lock()
        self._listeners: list[Listener] = []
        self._pending: list[Event] = []
        self._enqueueing = False  # set only while `_enqueue` writes a delivery into the queue
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        if str(path) != ":memory:":
            self._db.execute("PRAGMA journal_mode = WAL")
        self._migrate()
        with self._read() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'").fetchone():
                row = db.execute("SELECT value FROM meta WHERE key = 'owner_id'").fetchone()
                if row is not None:
                    self.owner = row["value"]

    def close(self) -> None:
        with self._lock:
            self._db.close()

    @property
    def schema(self) -> int:
        """The schema version the database is at, which is the running version once migrations finished."""
        with self._read() as db:
            return int(db.execute("SELECT version FROM schema_version").fetchone()[0])

    def backup(self, dest: Path | str) -> None:
        with self._lock:
            target = sqlite3.connect(str(dest))
            try:
                self._db.backup(target)
            finally:
                target.close()

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        """Call `listener` with each committed transaction's events, outside the lock."""
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener)

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                self._pending.clear()
                raise
            self._db.execute("COMMIT")
            events, self._pending = self._pending, []
        if events:
            for listener in list(self._listeners):
                listener(events)

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self._db

    def _migrate(self) -> None:
        with self._write() as db:
            db.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
            row = db.execute("SELECT version FROM schema_version").fetchone()
            version = row["version"] if row else 0
            if version > SCHEMA_VERSION:
                raise BrokerError(f"database schema {version} is newer than this broker ({SCHEMA_VERSION})")
            for step in range(version + 1, SCHEMA_VERSION + 1):
                if step in WIDEN:
                    self._widen_providers(db, *WIDEN[step])
                for statement in MIGRATIONS[step].split(";"):
                    if statement.strip():
                        db.execute(statement)
                if step in REWRITES:
                    getattr(self, REWRITES[step])(db)
            if version < SCHEMA_VERSION:
                db.execute("DELETE FROM schema_version")
                db.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def _adopt_owner_and_model(self, db: sqlite3.Connection) -> None:
        db.execute(
            "UPDATE session_bindings SET provider = 'model', native_id = 'model' WHERE provider = ?",
            (LEGACY_MODEL,),
        )
        db.execute(
            "UPDATE session_bindings SET native_id = 'chatgpt' WHERE provider = 'chatgpt' AND native_id = ?",
            (LEGACY_CONNECTOR_NATIVE,),
        )
        db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('owner_id', ?)", (self._legacy_owner(db),)
        )

    def _legacy_owner(self, db: sqlite3.Connection) -> str:
        """The owner of a database from before schema 13, read from what it holds: the notices conversation
        if one was ever opened, else the first group (only the owner makes groups; a service owns direct
        conversations too), else whoever owns the most conversations, else the configured id."""
        for query, args in (
            (
                "SELECT owner FROM conversations WHERE kind = 'direct' AND dm_binding_id = ?"
                " ORDER BY created_at, id LIMIT 1",
                (BROKER_BINDING_ID,),
            ),
            ("SELECT owner FROM conversations WHERE kind = 'group' ORDER BY created_at, id LIMIT 1", ()),
            (
                "SELECT owner FROM conversations GROUP BY owner"
                " ORDER BY COUNT(*) DESC, MIN(created_at), owner LIMIT 1",
                (),
            ),
        ):
            row = db.execute(query, args).fetchone()
            if row is not None:
                return cast(str, row["owner"])
        return self.owner

    def record_refusal(self, source: str, what: str, reason: str, now: float | None = None) -> None:
        """Keep one refusal for a day; the reason is the broker's own text, never a request's content."""
        at = time.time() if now is None else now
        with self._write() as db:
            db.execute(
                "INSERT INTO refusals (at, source, what, reason) VALUES (?, ?, ?, ?)",
                (at, source[:40], what[:80], reason[:300]),
            )
            self._prune_refusals(db, at)

    def refusals(self, now: float | None = None) -> list[dict[str, Any]]:
        """The last day's refusals, newest first."""
        at = time.time() if now is None else now
        with self._write() as db:
            self._prune_refusals(db, at)
            rows = db.execute("SELECT at, source, what, reason FROM refusals ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _prune_refusals(db: sqlite3.Connection, now: float) -> None:
        db.execute("DELETE FROM refusals WHERE at < ?", (now - REFUSAL_TTL,))
        db.execute(
            "DELETE FROM refusals WHERE id NOT IN (SELECT id FROM refusals ORDER BY id DESC LIMIT ?)",
            (REFUSAL_CAP,),
        )

    @staticmethod
    def _widen_providers(db: sqlite3.Connection, before: str, after: str) -> None:
        row = db.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'session_bindings'"
        ).fetchone()
        old = cast(str, row["sql"])
        new = old.replace(before, after)
        if new == old:
            raise BrokerError("session_bindings has an unexpected provider constraint")
        schema_version = db.execute("PRAGMA schema_version").fetchone()[0]
        db.execute("PRAGMA writable_schema = ON")
        try:
            db.execute(
                "UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = 'session_bindings'", (new,)
            )
            db.execute(f"PRAGMA schema_version = {int(schema_version) + 1}")
        finally:
            db.execute("PRAGMA writable_schema = OFF")

    # Bindings

    def register_binding(
        self,
        provider: Provider,
        native_id: str,
        title: str,
        workspace: str,
        endpoint_ref: str,
        capabilities: dict[str, Any],
        *,
        current_token: str | None = None,
        adapter: dict[str, Any] | None = None,
    ) -> tuple[Binding, str]:
        """Attach a native session and return its binding with a fresh adapter token.

        Re-registering the same session keeps its binding id and conversations, issues a new lease
        generation and a new token, and invalidates everything carrying the old ones. An adapter
        re-registers an active session only with that binding's current token, or after the owner
        released it; the API always passes `current_token`, and None is the broker's own member.
        """
        if provider != "chatgpt" and re.search(
            rf"\b{re.escape(self.connector_title)}\b", title, re.IGNORECASE
        ):
            raise Invalid("only the configured ChatGPT connector title may use that name")
        declared = capabilities.get("send_modes")
        if "send_modes" in capabilities and not (isinstance(declared, list) and "auto" in declared):
            # Admission treats auto as always takeable; this is what makes that true.
            raise Invalid("send_modes must be a list that includes auto")
        token = secrets.token_urlsafe(32)
        with self._write() as db:
            row = db.execute(
                "SELECT * FROM session_bindings WHERE provider = ? AND native_id = ? AND active = 1",
                (provider, native_id),
            ).fetchone()
            caps = json.dumps(capabilities, sort_keys=True)
            # Replaced on every registration: a session restarted under an older adapter reads as that one.
            reported = None if adapter is None else json.dumps(adapter, sort_keys=True)
            if row and current_token is not None:
                held = cast(str, row["token_sha"])
                proven = bool(current_token) and hmac.compare_digest(_token_sha(current_token), held)
                if not (proven or held.startswith(RELEASED)):
                    raise Conflict(
                        "this session is already registered: present its current adapter token,"
                        " or the owner releases it first"
                    )
            if row:
                db.execute(
                    "UPDATE session_bindings SET title = ?, workspace = ?, endpoint_ref = ?,"
                    " capabilities = ?, lease_generation = lease_generation + 1, token_sha = ?,"
                    " heartbeat_at = ?, adapter = ?, turn_state = 'unknown', turn_waiting_for = NULL,"
                    " turn_state_at = NULL, turn_seq = 0 WHERE id = ?",
                    (
                        title,
                        workspace,
                        endpoint_ref,
                        caps,
                        _token_sha(token),
                        self._now(),
                        reported,
                        row["id"],
                    ),
                )
                binding_id = cast(str, row["id"])
                # What the old lease had in flight may or may not have run: never resend it silently.
                stale = db.execute(
                    INFLIGHT_BY_BINDING,
                    (binding_id, *INFLIGHT),
                ).fetchall()
                for delivery in stale:
                    self._set_state(
                        db,
                        delivery["id"],
                        "uncertain",
                        None,
                        Actor.binding(binding_id),
                        reason=REREGISTERED,
                    )
                self._resume(db, binding_id, "the session registered again")
                self._readmit_queued(db, binding_id)
            else:
                binding_id = _new_id()
                db.execute(
                    "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref,"
                    " capabilities, lease_generation, token_sha, active, heartbeat_at, created_at, adapter)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, 1, ?, ?, ?)",
                    (
                        binding_id,
                        provider,
                        native_id,
                        title,
                        workspace,
                        endpoint_ref,
                        caps,
                        _token_sha(token),
                        self._now(),
                        self._now(),
                        reported,
                    ),
                )
            return self._binding(db, binding_id), token

    def _enqueue(
        self,
        db: sqlite3.Connection,
        message_id: str,
        binding_id: str,
        *,
        delivery_id: str | None = None,
        queued_because: str = "",
        refused_because: str = "",
    ) -> bool:
        """The one way a delivery enters `queued`: made for a new message (no `delivery_id`), returned to the
        queue, or checked again where it already waits. Each is admitted against what the target declares
        now. A new delivery that fails raises; an existing one ends `failed` with the reason and whatever
        produced it (a task, a scheduled send) records that. Returns whether it is queued."""
        message = self._message(db, message_id)
        notice = self.notice_of(db, message_id)
        binding = self._binding(db, binding_id)
        problem = self.admission_problem(binding, message.mode, notice.get("kind"))
        if problem is not None:
            if delivery_id is None:
                raise (TasksUnsupported if problem[0] == "tasks" else ModeUnsupported)(problem[1])
            reason = f"{refused_because}: {problem[1]}"
            self._set_state(db, delivery_id, "failed", None, Actor.binding(BROKER_BINDING_ID), reason=reason)
            self.record_notice_refusal(db, message_id, notice, binding, reason)
            return False
        if delivery_id is None:
            delivery_id = _new_id()
            seq = db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM deliveries").fetchone()[0]
            db.execute(
                "INSERT INTO deliveries (id, message_id, binding_id, state, attempt, native_turn_id,"
                " lease_generation, seq, updated_at) VALUES (?, ?, ?, 'queued', 0, NULL, ?, ?, ?)",
                (delivery_id, message_id, binding_id, binding.lease_generation, seq, self._now()),
            )
            self._append(
                db,
                message.conversation_id,
                "delivery.changed",
                message.actor,
                message_id,
                {"delivery_id": delivery_id, "binding_id": binding_id, "state": "queued"},
            )
        elif self._delivery(db, delivery_id).state != "queued":
            self._enqueueing = True
            try:
                self._set_state(
                    db, delivery_id, "queued", None, Actor.binding(BROKER_BINDING_ID), reason=queued_because
                )
            finally:
                self._enqueueing = False
        return True

    def requeue_unanswered(self, db: sqlite3.Connection, delivery_id: str, reason: str) -> bool:
        """Return a delivery to the queue when a run that held it ended without answering it: handed over,
        taken into a turn, read, or closed uncertain only because its session registered again. The next
        claim, under whatever lease the session holds then, gets it again (at least once), if the session
        still declares what the message needs. One already answered, cancelled, failed or queued is left as
        it is."""
        delivery = self._delivery(db, delivery_id)
        reregistered = delivery.state == "uncertain" and delivery.reason == REREGISTERED
        if delivery.state not in ("dispatching", "accepted", "consumed") and not reregistered:
            return False
        return self._enqueue(
            db,
            delivery.message_id,
            delivery.binding_id,
            delivery_id=delivery_id,
            queued_because=reason,
            refused_because="not admitted when returned to the queue",
        )

    def _readmit_queued(self, db: sqlite3.Connection, binding_id: str) -> None:
        """A session that registers again may declare less than before: every delivery still queued to it
        is admitted again against what it declares now."""
        for row in db.execute(
            "SELECT id, message_id FROM deliveries WHERE binding_id = ? AND state = 'queued' ORDER BY seq",
            (binding_id,),
        ).fetchall():
            self._enqueue(
                db,
                row["message_id"],
                binding_id,
                delivery_id=row["id"],
                refused_because="not admitted after the session registered again",
            )

    @staticmethod
    def notice_of(db: sqlite3.Connection, message_id: str) -> dict[str, Any]:
        """The notice a broker message carries in its message.accepted event; empty for any other message."""
        row = db.execute(
            "SELECT payload FROM events WHERE message_id = ? AND type = 'message.accepted'", (message_id,)
        ).fetchone()
        notice = json.loads(row["payload"]).get("notice") if row else None
        return notice if isinstance(notice, dict) else {}

    def record_notice_refusal(
        self,
        db: sqlite3.Connection,
        message_id: str | None,
        notice: dict[str, Any],
        binding: Binding,
        reason: str,
    ) -> None:
        """Say in the state machine that produced a broker message that its target refused it: a task gets
        a `notice_refused` event, a scheduled send ends `refused`."""
        from . import tasks

        if notice.get("kind") in TASK_NOTICE_KINDS and "task_id" in notice:
            tasks.record_refused(self, db, notice, binding.id, reason)
        if message_id is not None:
            db.execute(
                "UPDATE scheduled_sends SET state = 'refused', reason = ?, updated_at = ?"
                " WHERE message_id = ?",
                (reason, self._now(), message_id),
            )

    def authenticate(self, token: str) -> Binding:
        """The active binding a token was issued to, under its current lease."""
        with self._read() as db:
            row = db.execute(
                "SELECT id, active FROM session_bindings WHERE token_sha = ?", (_token_sha(token),)
            ).fetchone()
            if row is None:
                raise Forbidden("unknown or superseded adapter token")
            if not row["active"]:
                raise Gone("this session was retired by its owner")
            return self._binding(db, row["id"])

    def find_binding(self, provider: Provider, native_id: str) -> Binding | None:
        """The active binding of one native session, if any."""
        with self._read() as db:
            row = db.execute(
                "SELECT id FROM session_bindings WHERE provider = ? AND native_id = ? AND active = 1",
                (provider, native_id),
            ).fetchone()
            return self._binding(db, row["id"]) if row else None

    def open_deliveries(self, binding_id: str) -> tuple[list[tuple[Delivery, Message]], int]:
        """A session's deliveries handed over and not yet read, with their messages, and how many wait
        queued behind them."""
        with self._read() as db:
            rows = db.execute(
                "SELECT id FROM deliveries WHERE binding_id = ? AND state IN ('dispatching', 'accepted')"
                " ORDER BY seq",
                (binding_id,),
            ).fetchall()
            open_ = [(d := self._delivery(db, row["id"]), self._message(db, d.message_id)) for row in rows]
            queued = db.execute(
                "SELECT COUNT(*) FROM deliveries WHERE binding_id = ? AND state = 'queued'", (binding_id,)
            ).fetchone()[0]
            return open_, int(queued)

    def list_bindings(self) -> list[Binding]:
        with self._read() as db:
            return self._active_bindings(db)

    def _active_bindings(self, db: sqlite3.Connection) -> list[Binding]:
        rows = db.execute(
            "SELECT id FROM session_bindings WHERE active = 1 ORDER BY created_at, id"
        ).fetchall()
        return [self._binding(db, row["id"]) for row in rows]

    def highest_adapter_version(self) -> str:
        """The newest adapter version among active sessions: the one every other adapter is behind."""
        with self._read() as db:
            return self._highest_adapter_version(db)

    def _highest_adapter_version(self, db: sqlite3.Connection) -> str:
        versions = [adapter_version(b) for b in self._active_bindings(db) if b.provider in ADAPTER_PROVIDERS]
        return max(versions, key=version_key, default="0")

    def heartbeat(self, binding_id: str) -> None:
        with self._write() as db:
            db.execute("UPDATE session_bindings SET heartbeat_at = ? WHERE id = ?", (self._now(), binding_id))

    def deactivate_binding(self, binding_id: str) -> Binding:
        with self._write() as db:
            self._binding(db, binding_id)
            db.execute("UPDATE session_bindings SET active = 0 WHERE id = ?", (binding_id,))
            return self._binding(db, binding_id)

    def release_binding(self, actor: Actor, binding_id: str) -> Binding:
        """The owner lets the next registration of this session take its binding. The adapter
        token stops working at once; conversations and deliveries stay with the binding."""
        if actor.kind != "user":
            raise Forbidden("only the owner releases a session")
        with self._write() as db:
            row = db.execute("SELECT active FROM session_bindings WHERE id = ?", (binding_id,)).fetchone()
            if row is None:
                raise NotFound(f"no session {binding_id}")
            if not row["active"]:
                raise Gone("this session was retired by its owner")
            db.execute(
                "UPDATE session_bindings SET token_sha = ? WHERE id = ?",
                (RELEASED + secrets.token_hex(16), binding_id),
            )
            return self._binding(db, binding_id)

    def retire_binding(self, actor: Actor, binding_id: str) -> Binding:
        """The owner removes a session from the chat. Its queued messages are cancelled, anything in
        flight becomes uncertain, and its adapter's token answers Gone from now on."""
        if actor.kind != "user":
            raise Forbidden("only the owner retires a session")
        with self._write() as db:
            self._binding(db, binding_id)
            rows = db.execute(
                "SELECT id, state FROM deliveries WHERE binding_id = ? AND state IN"
                " ('queued', 'dispatching', 'accepted')",
                (binding_id,),
            ).fetchall()
            for row in rows:
                to_state: DeliveryState = "cancelled" if row["state"] == "queued" else "uncertain"
                self._set_state(db, row["id"], to_state, None, actor, reason="the owner retired the session")
            db.execute("UPDATE session_bindings SET active = 0 WHERE id = ?", (binding_id,))
            return self._binding(db, binding_id)

    def get_binding(self, binding_id: str) -> Binding:
        with self._read() as db:
            return self._binding(db, binding_id)

    def _binding(self, db: sqlite3.Connection, binding_id: str) -> Binding:
        row = db.execute("SELECT * FROM session_bindings WHERE id = ?", (binding_id,)).fetchone()
        if row is None:
            raise NotFound(f"binding {binding_id}")
        return Binding(
            id=row["id"],
            provider=row["provider"],
            native_id=row["native_id"],
            title=row["title"],
            workspace=row["workspace"],
            endpoint_ref=row["endpoint_ref"],
            capabilities=json.loads(row["capabilities"]),
            lease_generation=row["lease_generation"],
            active=bool(row["active"]),
            heartbeat_at=row["heartbeat_at"],
            expiries=row["expiries"],
            held_at=row["held_at"],
            adapter=json.loads(row["adapter"]) if row["adapter"] else None,
            turn_state=row["turn_state"],
            turn_waiting_for=row["turn_waiting_for"],
            turn_state_at=row["turn_state_at"],
            turn_seq=row["turn_seq"],
        )

    def set_turn_state(self, binding_id: str, state: str, waiting_for: str | None) -> Binding:
        """The broker's own write of a turn state, with no reading order to keep."""
        with self._write() as db:
            return self._write_turn_state(db, binding_id, state, waiting_for)[0]

    @staticmethod
    def check_turn(state: str, waiting_for: str | None) -> None:
        """A reading is valid or it is refused, whatever the broker holds."""
        if state not in TURN_STATES:
            raise Invalid(f"state must be one of {', '.join(TURN_STATES)}")
        if waiting_for is not None and not WAITING_FOR.match(waiting_for):
            raise Invalid("waiting_for is a lowercase kind of wait such as approval, never text")

    def _write_turn_state(
        self,
        db: sqlite3.Connection,
        binding_id: str,
        state: str,
        waiting_for: str | None,
        *,
        lease: int | None = None,
        seq: int | None = None,
    ) -> tuple[Binding, bool]:
        """The one place a turn state is written; returns the binding and whether the reading applied.

        A reading with `seq` applies only under the lease it was sent from and only when newer than the one
        applied before, so a late reading cannot replace a newer one or outlive a registration. `waiting_for`
        is a kind of wait, kept only for `waiting`; a reading that changes nothing writes nothing. A binding
        leaving busy or waiting wakes its adapter's claim when the head of its queue is a queue-mode message
        held for that turn."""
        self.check_turn(state, waiting_for)
        before = self._binding(db, binding_id)
        if seq is not None:
            if lease != before.lease_generation or seq <= before.turn_seq:
                return before, False
            db.execute("UPDATE session_bindings SET turn_seq = ? WHERE id = ?", (seq, binding_id))
        kept = waiting_for if state == "waiting" else None
        if (state, kept) != (before.turn_state, before.turn_waiting_for):
            db.execute(
                "UPDATE session_bindings SET turn_state = ?, turn_waiting_for = ?, turn_state_at = ?"
                " WHERE id = ?",
                (state, kept, self._now(), binding_id),
            )
        if before.turn_state in HOLDS_QUEUE and state not in HOLDS_QUEUE:
            head = self._next_queued(db, binding_id)
            if head is not None and head["mode"] == "queue":
                self._append(
                    db,
                    head["conversation_id"],
                    "delivery.released",
                    Actor.binding(BROKER_BINDING_ID),
                    head["message_id"],
                    {"delivery_id": head["id"], "binding_id": binding_id, "turn_state": state},
                )
        return self._binding(db, binding_id), True

    def _resume(self, db: sqlite3.Connection, binding_id: str, why: str) -> None:
        """The session showed it reads: its expiry count restarts and a hold ends."""
        row = db.execute(
            "SELECT expiries, held_at FROM session_bindings WHERE id = ?", (binding_id,)
        ).fetchone()
        if row is None or (row["expiries"] == 0 and row["held_at"] is None):
            return
        db.execute("UPDATE session_bindings SET expiries = 0, held_at = NULL WHERE id = ?", (binding_id,))
        notices = self._notices_conversation(db)
        if row["held_at"] is not None and notices is not None:
            self._append(
                db,
                notices,
                "session.resumed",
                Actor.binding(BROKER_BINDING_ID),
                None,
                {"binding_id": binding_id, "reason": why},
            )

    @staticmethod
    def _notices_conversation(db: sqlite3.Connection) -> str | None:
        row = db.execute(
            "SELECT id FROM conversations WHERE kind = 'direct' AND dm_binding_id = ?", (BROKER_BINDING_ID,)
        ).fetchone()
        return cast(str, row["id"]) if row else None

    def _broker_member(self, db: sqlite3.Connection) -> Actor:
        """The broker's notices member, written on first use: inactive, so it is never a recipient."""
        db.execute(
            "INSERT OR IGNORE INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref,"
            " capabilities, lease_generation, token_sha, active, heartbeat_at, created_at)"
            " VALUES (?, 'broker', 'broker', ?, 'built-in', 'broker', ?, 1, ?, 0, NULL, ?)",
            (
                BROKER_BINDING_ID,
                BROKER_TITLE,
                json.dumps({"builtin": True, "notices": True}),
                _token_sha(secrets.token_urlsafe(32)),
                self._now(),
            ),
        )
        return Actor.binding(BROKER_BINDING_ID)

    def _open_notices(self, db: sqlite3.Connection, owner: str) -> str:
        """The owner's direct conversation with the broker, created with its member on first use."""
        existing = self._notices_conversation(db)
        if existing is not None:
            return existing
        self._broker_member(db)
        now = self._now()
        conversation_id = _new_id()
        db.execute(
            "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
            " VALUES (?, 'direct', ?, ?, 'user_directed', ?, ?)",
            (conversation_id, BROKER_TITLE, owner, BROKER_BINDING_ID, now),
        )
        db.executemany(
            "INSERT INTO conversation_members VALUES (?, ?, ?, ?)",
            [
                (conversation_id, "user", owner, "owner"),
                (conversation_id, "binding", BROKER_BINDING_ID, "session"),
            ],
        )
        return conversation_id

    # Devices

    def enroll_device(self, label: str) -> tuple[Device, str]:
        token = DEVICE_TOKEN_PREFIX + secrets.token_urlsafe(32)
        device_id = _new_id()
        with self._write() as db:
            db.execute(
                "INSERT INTO devices (id, label, token_sha, created_at) VALUES (?, ?, ?, ?)",
                (device_id, (label.strip() or "phone")[:64], _token_sha(token), self._now()),
            )
            return self._device(db, device_id), token

    def authenticate_device(self, token: str) -> Device:
        """The current token, or the previous one inside its grace window."""
        sha = _token_sha(token)
        with self._write() as db:
            row = db.execute(
                "SELECT id FROM devices WHERE disabled = 0"
                " AND (token_sha = ? OR (prev_token_sha = ? AND prev_expires_at > ?))",
                (sha, sha, self._now()),
            ).fetchone()
            if row is None:
                raise Forbidden("unknown, rotated or disabled device token")
            db.execute("UPDATE devices SET last_seen_at = ? WHERE id = ?", (self._now(), row["id"]))
            return self._device(db, row["id"])

    def rotate_device_token(self, device_id: str, grace_seconds: float) -> str:
        token = DEVICE_TOKEN_PREFIX + secrets.token_urlsafe(32)
        with self._write() as db:
            current = db.execute("SELECT token_sha FROM devices WHERE id = ?", (device_id,)).fetchone()
            if current is None:
                raise NotFound(f"device {device_id}")
            db.execute(
                "UPDATE devices SET token_sha = ?, prev_token_sha = ?, prev_expires_at = ? WHERE id = ?",
                (_token_sha(token), current["token_sha"], self._now() + grace_seconds, device_id),
            )
        return token

    def set_device_disabled(self, device_id: str, disabled: bool) -> Device:
        with self._write() as db:
            self._device(db, device_id)
            db.execute("UPDATE devices SET disabled = ? WHERE id = ?", (int(disabled), device_id))
            return self._device(db, device_id)

    def list_devices(self) -> list[Device]:
        with self._read() as db:
            rows = db.execute("SELECT id FROM devices ORDER BY created_at, id").fetchall()
            return [self._device(db, row["id"]) for row in rows]

    def status(self, since: float) -> dict[str, Any]:
        """Counts and timings for the owner's status page, with no message text: every binding ever
        registered, every conversation, and the messages and deliveries posted since `since`."""
        open_states = ("queued", *INFLIGHT)
        marks = ", ".join("?" * len(open_states))
        with self._read() as db:
            providers = {
                row["id"]: row["provider"] for row in db.execute("SELECT id, provider FROM session_bindings")
            }
            sent = {
                row["actor_id"]: (row["n"], row["last"])
                for row in db.execute(
                    "SELECT actor_id, COUNT(*) AS n, MAX(created_at) AS last FROM messages"
                    " WHERE actor_kind = 'binding' GROUP BY actor_id"
                )
            }
            by_state: dict[str, dict[str, int]] = {}
            for row in db.execute(
                "SELECT binding_id, state, COUNT(*) AS n FROM deliveries GROUP BY binding_id, state"
            ):
                by_state.setdefault(row["binding_id"], {})[row["state"]] = row["n"]
            sessions = [
                {
                    "id": row["id"],
                    "provider": row["provider"],
                    "title": row["title"],
                    "workspace": row["workspace"],
                    "active": bool(row["active"]),
                    "heartbeat_at": row["heartbeat_at"],
                    "created_at": row["created_at"],
                    "expiries": row["expiries"],
                    "held_at": row["held_at"],
                    "messages": sent.get(row["id"], (0, None))[0],
                    "last_message_at": sent.get(row["id"], (0, None))[1],
                    "deliveries": by_state.get(row["id"], {}),
                }
                for row in db.execute("SELECT * FROM session_bindings ORDER BY created_at, id")
            ]
            members: dict[str, list[str]] = {}
            for row in db.execute("SELECT conversation_id, member_kind, member_id FROM conversation_members"):
                kind = "user" if row["member_kind"] == "user" else providers.get(row["member_id"], "unknown")
                members.setdefault(row["conversation_id"], []).append(kind)
            conversations = [
                {
                    "id": row["id"],
                    "kind": row["kind"],
                    "title": row["title"],
                    "mode": row["mode"],
                    "paused": bool(row["paused"]),
                    "archived": bool(row["archived"]),
                    "created_at": row["created_at"],
                    "members": members.get(row["id"], []),
                    "messages": row["messages"],
                    "last_message_at": row["last_message_at"],
                    "open": row["open"],
                }
                for row in db.execute(
                    "SELECT c.*,"
                    " (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS messages,"
                    " (SELECT MAX(created_at) FROM messages m WHERE m.conversation_id = c.id)"
                    "  AS last_message_at,"
                    " (SELECT COUNT(*) FROM deliveries d JOIN messages m ON m.id = d.message_id"
                    f"  WHERE m.conversation_id = c.id AND d.state IN ({marks})) AS open"
                    " FROM conversations c ORDER BY c.created_at, c.id",
                    open_states,
                )
            ]
            messages = [
                {
                    "at": row["created_at"],
                    "conversation_id": row["conversation_id"],
                    "sender": "user"
                    if row["actor_kind"] == "user"
                    else providers.get(row["actor_id"], "unknown"),
                    "depth": row["depth"],
                }
                for row in db.execute(
                    "SELECT created_at, conversation_id, actor_kind, actor_id, depth FROM messages"
                    " WHERE created_at >= ? ORDER BY created_at",
                    (since,),
                )
            ]
            # The first time each delivery reached the session: consumed, or responded without it.
            read_at: dict[str, float] = {}
            for row in db.execute(
                "SELECT payload, created_at FROM events WHERE type = 'delivery.changed' AND created_at >= ?"
                " ORDER BY conversation_id, seq",
                (since,),
            ):
                payload = json.loads(row["payload"])
                if payload.get("state") in ("consumed", "responded"):
                    read_at.setdefault(payload["delivery_id"], row["created_at"])
            deliveries = [
                {
                    "at": row["created_at"],
                    "state": row["state"],
                    "binding_id": row["binding_id"],
                    "provider": providers.get(row["binding_id"], "unknown"),
                    "read_after": read_at[row["id"]] - row["created_at"] if row["id"] in read_at else None,
                }
                for row in db.execute(
                    "SELECT d.id, d.state, d.binding_id, m.created_at FROM deliveries d"
                    " JOIN messages m ON m.id = d.message_id WHERE m.created_at >= ? ORDER BY m.created_at",
                    (since,),
                )
            ]
            open_now = [
                {
                    "delivery_id": row["id"],
                    "state": row["state"],
                    "since": row["updated_at"],
                    "posted_at": row["posted_at"],
                    "binding_id": row["binding_id"],
                    "conversation_id": row["conversation_id"],
                    "rewakes": row["rewakes"],
                    "wakes_sent": row["wakes_sent"],
                    "wake_due_at": row["wake_due_at"],
                }
                for row in db.execute(
                    "SELECT d.id, d.state, d.updated_at, d.binding_id, d.rewakes, d.wakes_sent,"
                    " d.wake_due_at, m.created_at AS posted_at,"
                    " m.conversation_id FROM deliveries d JOIN messages m ON m.id = d.message_id"
                    f" WHERE d.state IN ({marks}) ORDER BY d.updated_at",
                    open_states,
                )
            ]
            devices = [
                {
                    "label": row["label"],
                    "disabled": bool(row["disabled"]),
                    "last_seen_at": row["last_seen_at"],
                }
                for row in db.execute(
                    "SELECT label, disabled, last_seen_at FROM devices ORDER BY created_at, id"
                )
            ]
        return {
            "sessions": sessions,
            "conversations": conversations,
            "messages": messages,
            "deliveries": deliveries,
            "open": open_now,
            "devices": devices,
        }

    def _device(self, db: sqlite3.Connection, device_id: str) -> Device:
        row = db.execute("SELECT * FROM devices WHERE id = ?", (device_id,)).fetchone()
        if row is None:
            raise NotFound(f"device {device_id}")
        return Device(
            id=row["id"],
            label=row["label"],
            disabled=bool(row["disabled"]),
            created_at=row["created_at"],
            last_seen_at=row["last_seen_at"],
        )

    # Conversations

    def create_group(self, owner: str, title: str, mode: Mode = "user_directed") -> Conversation:
        with self._write() as db:
            return self._conversation(db, self._insert_group(db, owner, title, mode))

    def _insert_group(self, db: sqlite3.Connection, owner: str, title: str, mode: Mode) -> str:
        conversation_id = _new_id()
        db.execute(
            "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
            " VALUES (?, 'group', ?, ?, ?, NULL, ?)",
            (conversation_id, title, owner, mode, self._now()),
        )
        db.execute(
            "INSERT INTO conversation_members VALUES (?, 'user', ?, 'owner')", (conversation_id, owner)
        )
        return conversation_id

    def open_direct(self, owner: str, binding_id: str) -> Conversation:
        """Return the owner's direct conversation with one session, creating it once."""
        with self._write() as db:
            return self._open_direct(db, owner, binding_id)

    def _open_direct(self, db: sqlite3.Connection, owner: str, binding_id: str) -> Conversation:
        binding = self._binding(db, binding_id)
        if not binding.active:
            raise Invalid(f"binding {binding_id} is not active")
        row = db.execute(
            "SELECT id FROM conversations WHERE kind = 'direct' AND owner = ? AND dm_binding_id = ?",
            (owner, binding_id),
        ).fetchone()
        if row:
            return self._conversation(db, row["id"])
        conversation_id = _new_id()
        db.execute(
            "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
            " VALUES (?, 'direct', ?, ?, 'user_directed', ?, ?)",
            (conversation_id, binding.title, owner, binding_id, self._now()),
        )
        db.executemany(
            "INSERT INTO conversation_members VALUES (?, ?, ?, ?)",
            [
                (conversation_id, "user", owner, "owner"),
                (conversation_id, "binding", binding_id, "session"),
            ],
        )
        return self._conversation(db, conversation_id)

    def add_member(self, actor: Actor, conversation_id: str, binding_id: str) -> None:
        with self._write() as db:
            conversation = self._conversation(db, conversation_id)
            if conversation.kind == "direct":
                raise Forbidden("direct conversation membership is fixed")
            if actor != Actor.user(conversation.owner):
                raise Forbidden("only the owner adds members")
            if conversation.archived:
                raise Forbidden(REMOVED)
            if not self._binding(db, binding_id).active:
                raise Invalid(f"binding {binding_id} is not active")
            db.execute(
                "INSERT OR IGNORE INTO conversation_members VALUES (?, 'binding', ?, 'session')",
                (conversation_id, binding_id),
            )

    def set_paused(self, actor: Actor, conversation_id: str, paused: bool) -> Conversation:
        with self._write() as db:
            conversation = self._conversation(db, conversation_id)
            if actor != Actor.user(conversation.owner):
                raise Forbidden("only the owner pauses a conversation")
            db.execute("UPDATE conversations SET paused = ? WHERE id = ?", (int(paused), conversation_id))
            self._append(
                db,
                conversation_id,
                "collaboration.paused" if paused else "collaboration.resumed",
                actor,
                None,
                {},
            )
            return self._conversation(db, conversation_id)

    def set_archived(self, actor: Actor, conversation_id: str, archived: bool) -> Conversation:
        """The owner removes a group from every list, or restores it. History is kept. Removing
        stops any collaboration, cancels queued deliveries and tells each member session; a removed
        group takes no post from the owner, and a member session's post reopens it."""
        with self._write() as db:
            conversation = self._conversation(db, conversation_id)
            if actor != Actor.user(conversation.owner):
                raise Forbidden("only the owner removes a group")
            if conversation.kind != "group":
                raise Invalid("only a group can be removed")
            if conversation.archived == archived:
                return conversation
            before = self._collaboration(db, conversation_id)
            if archived:
                self._stop_collaboration(db, conversation_id, GROUP_REMOVED, actor)
                queued = db.execute(
                    "SELECT d.id FROM deliveries d JOIN messages m ON m.id = d.message_id"
                    " WHERE m.conversation_id = ? AND d.state = 'queued' ORDER BY d.seq",
                    (conversation_id,),
                ).fetchall()
                for row in queued:
                    self._set_state(
                        db, row["id"], "cancelled", None, actor, reason="the owner removed the group"
                    )
            db.execute("UPDATE conversations SET archived = ? WHERE id = ?", (int(archived), conversation_id))
            event = "conversation.archived" if archived else "conversation.restored"
            self._append(db, conversation_id, event, actor, None, {})
            if archived:
                self._notify_archived(db, self._conversation(db, conversation_id), before)
            else:
                self._resume_after_reopen(db, conversation_id, actor)
            return self._conversation(db, conversation_id)

    def _reopen(self, db: sqlite3.Connection, conversation: Conversation, actor: Actor) -> Conversation:
        """A member session posted into a removed group: the group comes back for everyone, and a
        collaboration the removal stopped resumes while it has time and turns left."""
        db.execute("UPDATE conversations SET archived = 0 WHERE id = ?", (conversation.id,))
        self._append(
            db, conversation.id, "conversation.restored", actor, None, {"reason": "a member session posted"}
        )
        self._resume_after_reopen(db, conversation.id, actor)
        return self._conversation(db, conversation.id)

    def _resume_after_reopen(self, db: sqlite3.Connection, conversation_id: str, actor: Actor) -> None:
        collaboration = self._collaboration(db, conversation_id)
        if (
            collaboration is None
            or collaboration.active
            or collaboration.stopped_reason != GROUP_REMOVED
            or self._now() >= collaboration.expires_at
            or collaboration.turns_used >= collaboration.max_turns
        ):
            return
        db.execute(
            "UPDATE collaborations SET active = 1, stopped_reason = NULL WHERE conversation_id = ?",
            (conversation_id,),
        )
        db.execute("UPDATE conversations SET mode = 'collaborate' WHERE id = ?", (conversation_id,))
        self._append(
            db,
            conversation_id,
            "collaboration.reopened",
            actor,
            None,
            {
                "turns_left": collaboration.max_turns - collaboration.turns_used,
                "expires_at": collaboration.expires_at,
            },
        )

    def _notify_archived(
        self, db: sqlite3.Connection, conversation: Conversation, before: Collaboration | None
    ) -> None:
        """Tell each active member session the owner removed the group, so none of them is left
        waiting on a reply that cannot come. The notice is delivered and wakes like any message."""
        targets = tuple(
            row["member_id"]
            for row in db.execute(
                "SELECT m.member_id FROM conversation_members m JOIN session_bindings b ON b.id = m.member_id"
                " WHERE m.conversation_id = ? AND m.member_kind = 'binding' AND b.active = 1"
                " AND b.id != ? ORDER BY m.member_id",
                (conversation.id, BROKER_BINDING_ID),
            )
        )
        if not targets:
            return
        at = self._now()
        running = before is not None and before.active and at < before.expires_at
        text = f"{conversation.owner} archived {conversation.title} at {utc_iso(at)}."
        if running and before is not None:
            minutes_left = max(1, int((before.expires_at - at) // 60))
            turns_left = before.max_turns - before.turns_used
            text += (
                f" Its collaboration stopped with {turns_left} turns and about {minutes_left} minutes left."
                " A message you send here reopens the group and resumes the collaboration"
                " while its time lasts."
            )
        else:
            text += " A message you send here reopens it."
        notice = {
            "kind": "conversation_archived",
            "conversation_id": conversation.id,
            "collaboration_stopped": running,
            "at": utc_iso(at),
        }
        self._insert_message(
            db,
            self._broker_member(db),
            conversation,
            f"notice:archived:{conversation.id}:{at}",
            text,
            targets,
            None,
            None,
            0,
            notice,
        )

    def conversations_for(self, reader: Actor) -> list[Conversation]:
        with self._read() as db:
            rows = db.execute(
                "SELECT c.id FROM conversations c JOIN conversation_members m ON m.conversation_id = c.id"
                " WHERE m.member_kind = ? AND m.member_id = ? AND c.archived = 0"
                " ORDER BY c.created_at, c.id",
                (reader.kind, reader.id),
            ).fetchall()
            return [self._conversation(db, row["id"]) for row in rows]

    def get_conversation(self, reader: Actor, conversation_id: str) -> Conversation:
        with self._read() as db:
            self._require_member(db, reader, conversation_id)
            return self._conversation(db, conversation_id)

    def members(self, reader: Actor, conversation_id: str) -> list[Actor]:
        with self._read() as db:
            self._require_member(db, reader, conversation_id)
            return self._members(db, conversation_id)

    def member_details(self, reader: Actor, conversation_id: str) -> list[Member]:
        """Members with the names a session addresses them by; no workspace or endpoint."""
        with self._read() as db:
            self._require_member(db, reader, conversation_id)
            highest = self._highest_adapter_version(db)
            members = []
            for actor in self._members(db, conversation_id):
                if actor.kind == "user":
                    members.append(Member(actor.kind, actor.id, actor.id, None, True))
                    continue
                binding = self._binding(db, actor.id)
                members.append(
                    Member(
                        actor.kind,
                        actor.id,
                        binding.title,
                        binding.provider,
                        binding.active,
                        binding.adapter,
                        adapter_behind(binding, highest),
                    )
                )
            return members

    def _conversation(self, db: sqlite3.Connection, conversation_id: str) -> Conversation:
        row = db.execute("SELECT * FROM conversations WHERE id = ?", (conversation_id,)).fetchone()
        if row is None:
            raise NotFound(f"conversation {conversation_id}")
        return Conversation(
            id=row["id"],
            kind=row["kind"],
            title=row["title"],
            owner=row["owner"],
            mode=row["mode"],
            dm_binding_id=row["dm_binding_id"],
            paused=bool(row["paused"]),
            archived=bool(row["archived"]),
        )

    def _members(self, db: sqlite3.Connection, conversation_id: str) -> list[Actor]:
        rows = db.execute(
            "SELECT member_kind, member_id FROM conversation_members WHERE conversation_id = ?"
            " ORDER BY member_kind DESC, member_id",
            (conversation_id,),
        ).fetchall()
        return [Actor(row["member_kind"], row["member_id"]) for row in rows]

    def _require_member(self, db: sqlite3.Connection, actor: Actor, conversation_id: str) -> None:
        self._conversation(db, conversation_id)
        row = db.execute(
            "SELECT 1 FROM conversation_members"
            " WHERE conversation_id = ? AND member_kind = ? AND member_id = ?",
            (conversation_id, actor.kind, actor.id),
        ).fetchone()
        if row is None:
            raise Forbidden(f"{actor.kind} {actor.id} is not a member of {conversation_id}")

    # Messages

    def post_message(
        self,
        actor: Actor,
        conversation_id: str,
        client_message_id: str,
        text: str,
        recipients: Sequence[str] | None = None,
        parent_id: str | None = None,
        mode: str = "auto",
    ) -> PostResult:
        """Persist a message, its deliveries and its event in one transaction.

        A retry with the same client_message_id returns the original; the same id with different content,
        recipients, parent, conversation or mode is refused. A mode a recipient cannot take refuses the post.
        """
        if not client_message_id:
            raise Invalid("client_message_id is required")
        if len(text.encode()) > MAX_MESSAGE_BYTES:
            raise Invalid(f"message exceeds {MAX_MESSAGE_BYTES} bytes")
        if actor.kind == "binding":
            self.expire_collaborations()
        with self._write() as db:
            return self._post(
                db, actor, conversation_id, client_message_id, text, recipients, parent_id, mode
            )

    def _post(
        self,
        db: sqlite3.Connection,
        actor: Actor,
        conversation_id: str,
        client_message_id: str,
        text: str,
        recipients: Sequence[str] | None,
        parent_id: str | None,
        mode: str = "auto",
    ) -> PostResult:
        self._require_member(db, actor, conversation_id)
        conversation = self._conversation(db, conversation_id)
        if conversation.archived:
            if actor.kind != "binding":
                raise Forbidden(REMOVED)
            conversation = self._reopen(db, conversation, actor)
        if conversation.dm_binding_id == BROKER_BINDING_ID and actor.kind == "user":
            raise Forbidden("the broker's notices take no replies")
        targets = self._resolve_recipients(db, conversation, actor, recipients)
        sha = _content_sha(conversation_id, text, targets, parent_id)
        existing = db.execute(
            "SELECT * FROM messages WHERE actor_kind = ? AND actor_id = ? AND client_message_id = ?",
            (actor.kind, actor.id, client_message_id),
        ).fetchone()
        if existing:
            if existing["content_sha"] != sha or existing["mode"] != mode:
                raise Conflict(f"client_message_id {client_message_id} was used for different content")
            message = self._message_row(existing)
            return PostResult(message, self._deliveries_for(db, message.id), created=False)
        parent: Message | None = None
        if parent_id is not None:
            row = db.execute("SELECT * FROM messages WHERE id = ?", (parent_id,)).fetchone()
            if row is None or row["conversation_id"] != conversation_id:
                raise Invalid("parent message is not in this conversation")
            parent = self._message_row(row)
        root_id, depth = self._chain(parent)
        if actor.kind == "binding" and targets:
            self._check_peer_message(db, conversation, actor, targets, parent, root_id, depth, text)
        result = self._insert_message(
            db,
            actor,
            conversation,
            client_message_id,
            text,
            targets,
            parent_id,
            root_id if actor.kind == "binding" else None,
            depth if actor.kind == "binding" else 0,
            mode=mode,
        )
        if actor.kind == "binding" and targets and conversation.mode == "collaborate":
            self._spend_turn(db, conversation_id, actor)
        return result

    def _insert_message(
        self,
        db: sqlite3.Connection,
        actor: Actor,
        conversation: Conversation,
        client_message_id: str,
        text: str,
        targets: tuple[str, ...],
        parent_id: str | None,
        root_id: str | None,
        depth: int,
        notice: dict[str, Any] | None = None,
        mode: str = "auto",
    ) -> PostResult:
        """Write a message, its envelope in the message.accepted event and a queued delivery per target.
        Every target is admitted first (`admit`), so a message no target can take creates nothing."""
        self.admit(db, targets, mode, None if notice is None else notice.get("kind"))
        conversation_id = conversation.id
        message_id = _new_id()
        now = self._now()
        db.execute(
            "INSERT INTO messages (id, conversation_id, actor_kind, actor_id, client_message_id,"
            " content_sha, text, recipients, parent_id, created_at, root_id, depth, mode)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                message_id,
                conversation_id,
                actor.kind,
                actor.id,
                client_message_id,
                _content_sha(conversation_id, text, targets, parent_id),
                text,
                json.dumps(list(targets)),
                parent_id,
                now,
                root_id,
                depth,
                mode,
            ),
        )
        payload: dict[str, Any] = {
            "text": text,
            "recipients": list(targets),
            "parent_id": parent_id,
            **self._envelope(db, actor, conversation.owner, targets, now),
        }
        if notice is not None:
            payload["notice"] = notice
        self._append(db, conversation_id, "message.accepted", actor, message_id, payload)
        for binding_id in targets:
            self._enqueue(db, message_id, binding_id)
        message = self._message(db, message_id)
        return PostResult(message, self._deliveries_for(db, message_id), created=True)

    def _post_broker(
        self,
        db: sqlite3.Connection,
        conversation: Conversation,
        client_message_id: str,
        text: str,
        targets: tuple[str, ...],
        notice: dict[str, Any],
        mode: str = "auto",
    ) -> PostResult:
        """A broker notice with a stable client_message_id: a repeat finds the first and posts nothing."""
        broker = self._broker_member(db)
        existing = db.execute(
            "SELECT * FROM messages WHERE actor_kind = ? AND actor_id = ? AND client_message_id = ?",
            (broker.kind, broker.id, client_message_id),
        ).fetchone()
        if existing:
            message = self._message_row(existing)
            return PostResult(message, self._deliveries_for(db, message.id), created=False)
        return self._insert_message(
            db, broker, conversation, client_message_id, text, targets, None, None, 0, notice, mode
        )

    def admit(self, db: sqlite3.Connection, targets: Sequence[str], mode: str, kind: str | None) -> None:
        """Whether every target may be sent a message in this mode and of this notice kind. The one rule for
        every delivery the broker creates (`_insert_message`), and again for queued ones when a session
        registers. Refusals name each target and why."""
        if mode not in SEND_MODES:
            raise Invalid(f"mode must be one of {', '.join(SEND_MODES)}")
        problems = [
            p
            for binding_id in targets
            if (p := self.admission_problem(self._binding(db, binding_id), mode, kind))
        ]
        if problems:
            raise (TasksUnsupported if problems[0][0] == "tasks" else ModeUnsupported)(
                "; ".join(text for _, text in problems)
            )

    @staticmethod
    def admission_problem(binding: Binding, mode: str, kind: str | None) -> tuple[str, str] | None:
        """The reason a binding cannot be sent this, as (class, text), or None. `auto` is the adapter's own
        choice among the modes it declares, so it is always admitted; any other mode must be declared, and a
        steer needs a running turn. A task notice needs `tasks: true`."""
        supports = f"(supports: {', '.join(binding.send_modes)})"
        if kind in TASK_NOTICE_KINDS and not binding.takes_tasks:
            return (
                "tasks",
                f'"{binding.title}" ({binding.id}) has no task tools: its adapter does not declare tasks',
            )
        if mode != "auto" and mode not in binding.send_modes:
            return ("mode", f'{mode} is not supported by "{binding.title}" {supports}')
        if mode == "steer" and binding.turn_state == "idle":
            return ("mode", f'"{binding.title}" has no running turn {supports}')
        return None

    def _party(self, db: sqlite3.Connection, actor: Actor) -> dict[str, str]:
        """A sender or recipient as every surface names it."""
        title = actor.id
        if actor.kind == "binding":
            row = db.execute("SELECT title FROM session_bindings WHERE id = ?", (actor.id,)).fetchone()
            title = row["title"] if row else actor.id
        return {"kind": actor.kind, "id": actor.id, "title": title}

    def _envelope(
        self, db: sqlite3.Connection, actor: Actor, owner: str, recipients: Sequence[str], sent_at: float
    ) -> dict[str, Any]:
        """Sender, recipients and the broker's sent time. A message with no recipients reaches the
        conversation's owner only, through the app, so "to" names the owner."""
        to = [self._party(db, Actor.binding(b)) for b in recipients]
        if not recipients and actor != Actor.user(owner):
            to.append(self._party(db, Actor.user(owner)))
        return {"from": self._party(db, actor), "to": to, "sent_at": utc_iso(sent_at)}

    def envelope(self, message: Message) -> dict[str, Any]:
        """The envelope as the message.accepted event carries it, with the notice a broker notice holds."""
        with self._read() as db:
            return self._envelope_of(db, message)

    @staticmethod
    def _envelope_of(db: sqlite3.Connection, message: Message) -> dict[str, Any]:
        row = db.execute(
            "SELECT payload FROM events WHERE conversation_id = ? AND message_id = ?"
            " AND type = 'message.accepted'",
            (message.conversation_id, message.id),
        ).fetchone()
        payload = json.loads(row["payload"]) if row else {}
        return {key: payload[key] for key in ENVELOPE if key in payload}

    def _add_envelopes(self, db: sqlite3.Connection) -> None:
        """Migration 10: the envelope on every message.accepted event written before it."""
        rows = db.execute(
            "SELECT e.conversation_id, e.seq, e.payload, m.actor_kind, m.actor_id, m.recipients,"
            " m.created_at, c.owner FROM events e JOIN messages m ON m.id = e.message_id"
            " JOIN conversations c ON c.id = e.conversation_id WHERE e.type = 'message.accepted'"
        ).fetchall()
        for row in rows:
            payload = json.loads(row["payload"])
            actor = Actor(row["actor_kind"], row["actor_id"])
            recipients = json.loads(row["recipients"])
            payload.update(self._envelope(db, actor, row["owner"], recipients, row["created_at"]))
            db.execute(
                "UPDATE events SET payload = ? WHERE conversation_id = ? AND seq = ?",
                (json.dumps(payload), row["conversation_id"], row["seq"]),
            )

    @staticmethod
    def _chain(parent: Message | None) -> tuple[str | None, int]:
        """(root owner message, agent hops from it) for a message answering `parent`."""
        if parent is None:
            return None, 1
        if parent.actor.kind == "user":
            return parent.id, 1
        return parent.root_id, parent.depth + 1

    def _check_peer_message(
        self,
        db: sqlite3.Connection,
        conversation: Conversation,
        actor: Actor,
        targets: tuple[str, ...],
        parent: Message | None,
        root_id: str | None,
        depth: int,
        text: str,
    ) -> None:
        """A session addressing other sessions: allowed only inside the owner's grant.

        consult: one question per session per owner message (depth 1), and one answer to it, to
        the asker only (depth 2). collaborate: while the collaboration is active, within its turn
        budget and deadline; a message may not repeat the one it answers.
        """
        if conversation.mode == "consult":
            if parent is None or root_id is None:
                raise Forbidden("a consult question answers one of the owner's messages")
            if depth == 1:
                if actor.id not in parent.recipients:
                    raise Forbidden("a consult question answers an owner message sent to this session")
                asked = db.execute(
                    "SELECT COUNT(*) FROM messages WHERE root_id = ? AND actor_kind = 'binding'"
                    " AND actor_id = ? AND depth = 1 AND recipients != '[]'",
                    (root_id, actor.id),
                ).fetchone()[0]
                if asked:
                    raise Forbidden("one consult question per owner message")
                return
            if depth == 2 and parent.actor.kind == "binding" and actor.id in parent.recipients:
                if targets != (parent.actor.id,):
                    raise Forbidden("a consult answer goes back to the session that asked")
                answered = db.execute(
                    "SELECT COUNT(*) FROM messages WHERE parent_id = ? AND actor_kind = 'binding'"
                    " AND actor_id = ? AND recipients != '[]'",
                    (parent.id, actor.id),
                ).fetchone()[0]
                if answered:
                    raise Forbidden("a consult question gets one answer")
                return
            raise Forbidden("a consult is one question and one answer")
        if conversation.mode == "collaborate":
            collaboration = self._collaboration(db, conversation.id)
            if collaboration is None or not collaboration.active:
                raise Forbidden("no collaboration is running in this conversation")
            if self._now() >= collaboration.expires_at:
                raise Forbidden("the collaboration reached its time limit")
            if parent is not None and parent.text.strip() == text.strip():
                raise Invalid("a message may not repeat the one it answers")
            return
        raise Forbidden("peer delivery needs an active consult or collaborate mode")

    def _spend_turn(self, db: sqlite3.Connection, conversation_id: str, actor: Actor) -> None:
        db.execute(
            "UPDATE collaborations SET turns_used = turns_used + 1 WHERE conversation_id = ?",
            (conversation_id,),
        )
        collaboration = self._collaboration(db, conversation_id)
        if collaboration is not None and collaboration.turns_used >= collaboration.max_turns:
            conversation = self._conversation(db, conversation_id)
            self._stop_collaboration(db, conversation_id, "turn limit", Actor.user(conversation.owner))

    # Modes

    def set_mode(self, actor: Actor, conversation_id: str, mode: Mode) -> Conversation:
        """The owner switches a group between user_directed and consult. Collaborate starts
        through start_collaboration, which needs an objective and limits."""
        if mode == "collaborate":
            raise Invalid("start a collaboration with an objective and limits instead")
        with self._write() as db:
            conversation = self._owned_group(db, actor, conversation_id)
            if conversation.mode == "collaborate":
                self._stop_collaboration(db, conversation_id, "mode changed", actor)
            db.execute("UPDATE conversations SET mode = ? WHERE id = ?", (mode, conversation_id))
            self._append(db, conversation_id, "conversation.mode_changed", actor, None, {"mode": mode})
            return self._conversation(db, conversation_id)

    def start_collaboration(
        self, actor: Actor, conversation_id: str, objective: str, max_turns: int = 6, minutes: float = 10
    ) -> Collaboration:
        if not objective.strip():
            raise Invalid("a collaboration needs an objective")
        if not 1 <= max_turns <= MAX_COLLABORATION_TURNS:
            raise Invalid(f"max_turns must be 1 to {MAX_COLLABORATION_TURNS}")
        if not 0 < minutes <= MAX_COLLABORATION_MINUTES:
            raise Invalid(f"minutes must be above 0 and at most {MAX_COLLABORATION_MINUTES}")
        with self._write() as db:
            self._owned_group(db, actor, conversation_id)
            now = self._now()
            db.execute(
                "INSERT INTO collaborations (conversation_id, objective, max_turns, turns_used, started_at,"
                " expires_at, active, stopped_reason) VALUES (?, ?, ?, 0, ?, ?, 1, NULL)"
                " ON CONFLICT (conversation_id) DO UPDATE SET objective = excluded.objective,"
                " max_turns = excluded.max_turns, turns_used = 0, started_at = excluded.started_at,"
                " expires_at = excluded.expires_at, active = 1, stopped_reason = NULL",
                (conversation_id, objective.strip(), max_turns, now, now + minutes * 60),
            )
            db.execute(
                "UPDATE conversations SET mode = 'collaborate', paused = 0 WHERE id = ?", (conversation_id,)
            )
            self._append(
                db,
                conversation_id,
                "collaboration.started",
                actor,
                None,
                {"objective": objective.strip(), "max_turns": max_turns, "expires_at": now + minutes * 60},
            )
            collaboration = self._collaboration(db, conversation_id)
            assert collaboration is not None
            return collaboration

    def stop_collaboration(self, actor: Actor, conversation_id: str) -> Conversation:
        with self._write() as db:
            self._owned_group(db, actor, conversation_id)
            self._stop_collaboration(db, conversation_id, "stopped by owner", actor)
            db.execute("UPDATE conversations SET mode = 'user_directed' WHERE id = ?", (conversation_id,))
            return self._conversation(db, conversation_id)

    def expire_collaborations(self) -> int:
        """Stop every collaboration past its deadline; returns how many stopped."""
        with self._read() as db:
            due = db.execute(
                "SELECT 1 FROM collaborations WHERE active = 1 AND expires_at <= ? LIMIT 1", (self._now(),)
            ).fetchone()
        if due is None:
            return 0
        with self._write() as db:
            rows = db.execute(
                "SELECT c.conversation_id, v.owner FROM collaborations c"
                " JOIN conversations v ON v.id = c.conversation_id"
                " WHERE c.active = 1 AND c.expires_at <= ?",
                (self._now(),),
            ).fetchall()
            for row in rows:
                self._stop_collaboration(db, row["conversation_id"], "time limit", Actor.user(row["owner"]))
            return len(rows)

    def collaboration(self, reader: Actor, conversation_id: str) -> Collaboration | None:
        with self._read() as db:
            self._require_member(db, reader, conversation_id)
            return self._collaboration(db, conversation_id)

    def _owned_group(self, db: sqlite3.Connection, actor: Actor, conversation_id: str) -> Conversation:
        conversation = self._conversation(db, conversation_id)
        if actor != Actor.user(conversation.owner):
            raise Forbidden("only the owner changes a conversation's mode")
        if conversation.kind != "group":
            raise Invalid("modes apply to groups; a direct conversation has one session")
        if conversation.archived:
            raise Forbidden(REMOVED)
        return conversation

    def _stop_collaboration(
        self, db: sqlite3.Connection, conversation_id: str, reason: str, actor: Actor
    ) -> None:
        collaboration = self._collaboration(db, conversation_id)
        if collaboration is None or not collaboration.active:
            return
        db.execute(
            "UPDATE collaborations SET active = 0, stopped_reason = ? WHERE conversation_id = ?",
            (reason, conversation_id),
        )
        db.execute("UPDATE conversations SET mode = 'user_directed' WHERE id = ?", (conversation_id,))
        self._append(
            db,
            conversation_id,
            "collaboration.stopped",
            actor,
            None,
            {"reason": reason, "turns_used": collaboration.turns_used},
        )

    def _collaboration(self, db: sqlite3.Connection, conversation_id: str) -> Collaboration | None:
        row = db.execute(
            "SELECT * FROM collaborations WHERE conversation_id = ?", (conversation_id,)
        ).fetchone()
        if row is None:
            return None
        return Collaboration(
            conversation_id=row["conversation_id"],
            objective=row["objective"],
            max_turns=row["max_turns"],
            turns_used=row["turns_used"],
            started_at=row["started_at"],
            expires_at=row["expires_at"],
            active=bool(row["active"]),
            stopped_reason=row["stopped_reason"],
        )

    def _resolve_recipients(
        self,
        db: sqlite3.Connection,
        conversation: Conversation,
        actor: Actor,
        requested: Sequence[str] | None,
    ) -> tuple[str, ...]:
        member_bindings = {m.id for m in self._members(db, conversation.id) if m.kind == "binding"}
        if conversation.kind == "direct":
            derived: tuple[str, ...] = (
                (cast(str, conversation.dm_binding_id),) if actor.kind == "user" else ()
            )
            if requested is not None and tuple(sorted(requested)) != derived:
                raise Invalid("a direct conversation's recipient is fixed")
            return derived
        wanted = tuple(sorted(set(requested or ())))
        if len(wanted) != len(requested or ()):
            raise Invalid("duplicate recipients")
        unknown = [r for r in wanted if r not in member_bindings]
        if unknown:
            raise Invalid(f"recipients are not session members of this conversation: {unknown}")
        if actor.kind == "user":
            if not wanted:
                raise Invalid("a group message needs at least one recipient")
            return wanted
        if actor.id in wanted:
            raise Invalid("a session cannot address itself")
        if wanted and (conversation.mode == "user_directed" or conversation.paused):
            raise Forbidden("peer delivery needs an active consult or collaborate mode")
        return wanted

    def recent_messages(
        self, reader: Actor, conversation_id: str, up_to_message_id: str, limit: int = 200
    ) -> list[Message]:
        """The conversation's last `limit` messages up to and including one, oldest first."""
        with self._read() as db:
            self._require_member(db, reader, conversation_id)
            marker = db.execute(
                "SELECT rowid FROM messages WHERE id = ? AND conversation_id = ?",
                (up_to_message_id, conversation_id),
            ).fetchone()
            if marker is None:
                raise NotFound(f"message {up_to_message_id}")
            rows = db.execute(
                "SELECT * FROM messages WHERE conversation_id = ? AND rowid <= ? ORDER BY rowid DESC LIMIT ?",
                (conversation_id, marker[0], limit),
            ).fetchall()
            return [self._message_row(row) for row in reversed(rows)]

    def _message(self, db: sqlite3.Connection, message_id: str) -> Message:
        row = db.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        if row is None:
            raise NotFound(f"message {message_id}")
        return self._message_row(row)

    @staticmethod
    def _message_row(row: sqlite3.Row) -> Message:
        return Message(
            id=row["id"],
            conversation_id=row["conversation_id"],
            actor=Actor(row["actor_kind"], row["actor_id"]),
            client_message_id=row["client_message_id"],
            text=row["text"],
            recipients=tuple(json.loads(row["recipients"])),
            parent_id=row["parent_id"],
            created_at=row["created_at"],
            root_id=row["root_id"],
            depth=row["depth"],
            mode=row["mode"],
        )

    # Events

    def _append(
        self,
        db: sqlite3.Connection,
        conversation_id: str,
        event_type: str,
        actor: Actor,
        message_id: str | None,
        payload: dict[str, Any],
    ) -> Event:
        seq = db.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE conversation_id = ?", (conversation_id,)
        ).fetchone()[0]
        event = Event(conversation_id, seq, _new_id(), event_type, actor, message_id, payload, self._now())
        db.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                conversation_id,
                seq,
                event.event_id,
                event_type,
                actor.kind,
                actor.id,
                message_id,
                json.dumps(payload),
                event.created_at,
            ),
        )
        self._pending.append(event)
        return event

    def events_after(
        self, reader: Actor, conversation_id: str, after: int = 0, limit: int = 500
    ) -> list[Event]:
        with self._read() as db:
            self._require_member(db, reader, conversation_id)
            rows = db.execute(
                "SELECT * FROM events WHERE conversation_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (conversation_id, after, limit),
            ).fetchall()
            return [
                Event(
                    row["conversation_id"],
                    row["seq"],
                    row["event_id"],
                    row["type"],
                    Actor(row["actor_kind"], row["actor_id"]),
                    row["message_id"],
                    json.loads(row["payload"]),
                    row["created_at"],
                )
                for row in rows
            ]

    def message_record(self, reader: Actor, message_id: str, event_limit: int = 200) -> MessageRecord:
        """One message by id for a member of its conversation. It shows nothing a member could not already
        read through events_after; it saves paging the whole conversation to find one message."""
        with self._read() as db:
            message = self._message(db, message_id)
            self._require_member(db, reader, message.conversation_id)
            rows = db.execute(
                "SELECT * FROM events WHERE conversation_id = ? AND message_id = ? ORDER BY seq LIMIT ?",
                (message.conversation_id, message_id, event_limit),
            ).fetchall()
            events = tuple(
                Event(
                    row["conversation_id"],
                    row["seq"],
                    row["event_id"],
                    row["type"],
                    Actor(row["actor_kind"], row["actor_id"]),
                    row["message_id"],
                    json.loads(row["payload"]),
                    row["created_at"],
                )
                for row in rows
            )
            seq = next((e.seq for e in events if e.type == "message.accepted"), 0)
            read_at = {
                row["delivery_id"]: row["at"]
                for row in db.execute(
                    "SELECT json_extract(payload, '$.delivery_id') AS delivery_id, MIN(created_at) AS at"
                    " FROM events WHERE conversation_id = ? AND message_id = ? AND type = 'delivery.changed'"
                    " AND json_extract(payload, '$.state') IN ('consumed', 'responded')"
                    " GROUP BY delivery_id",
                    (message.conversation_id, message_id),
                )
            }
            views = []
            for row in db.execute(
                "SELECT d.*, b.title AS recipient_title FROM deliveries d"
                " JOIN session_bindings b ON b.id = d.binding_id WHERE d.message_id = ? ORDER BY d.seq",
                (message_id,),
            ).fetchall():
                views.append(
                    DeliveryView(
                        self._delivery_row(row),
                        row["recipient_title"],
                        row["updated_at"],
                        read_at.get(row["id"]),
                    )
                )
            return MessageRecord(message, seq, tuple(views), events)

    # Deliveries

    def claim_next(self, binding_id: str, lease_generation: int) -> tuple[Delivery, Message] | None:
        """Hand the oldest queued delivery for a session to its adapter, across every conversation, when
        `may_claim` allows its mode; nothing is handed out while the wake ladder holds the session."""
        with self._write() as db:
            binding = self._require_lease(db, binding_id, lease_generation)
            if binding.held_at is not None:
                return None
            row = self._next_queued(db, binding_id)
            if row is None:
                return None
            in_flight = {
                r["state"] for r in db.execute(INFLIGHT_STATES_BY_BINDING, (binding_id, *INFLIGHT)).fetchall()
            }
            if not may_claim(row["mode"], in_flight, binding):
                # Held, and so are the deliveries behind it: nothing enters flight, so no ladder runs.
                return None
            delivery = self._set_state(
                db, row["id"], "dispatching", None, Actor.binding(binding_id), lease_generation
            )
            return delivery, self._message(db, delivery.message_id)

    @staticmethod
    def _next_queued(db: sqlite3.Connection, binding_id: str) -> sqlite3.Row | None:
        """The head of a session's queue: its oldest queued delivery with its message's mode."""
        return cast(
            sqlite3.Row | None,
            db.execute(
                "SELECT d.id, d.message_id, m.mode, m.conversation_id FROM deliveries d"
                " JOIN messages m ON m.id = d.message_id"
                " WHERE d.binding_id = ? AND d.state = 'queued' ORDER BY d.seq LIMIT 1",
                (binding_id,),
            ).fetchone(),
        )

    def transition(
        self,
        delivery_id: str,
        binding_id: str,
        lease_generation: int,
        to_state: DeliveryState,
        native_turn_id: str | None = None,
        reason: str | None = None,
    ) -> Delivery:
        with self._write() as db:
            return self._transition(
                db, delivery_id, binding_id, lease_generation, to_state, native_turn_id, reason
            )

    def cancel_pending(self, actor: Actor, conversation_id: str) -> list[Delivery]:
        """Conversation-scoped stop: cancel this conversation's queued deliveries only."""
        with self._write() as db:
            conversation = self._conversation(db, conversation_id)
            if actor != Actor.user(conversation.owner):
                raise Forbidden("only the owner stops a conversation")
            rows = db.execute(
                "SELECT d.id FROM deliveries d JOIN messages m ON m.id = d.message_id"
                " WHERE m.conversation_id = ? AND d.state = 'queued' ORDER BY d.seq",
                (conversation_id,),
            ).fetchall()
            return [
                self._set_state(
                    db, row["id"], "cancelled", None, actor, reason="the owner stopped the conversation"
                )
                for row in rows
            ]

    def redirect(
        self, actor: Actor, delivery_id: str, to_binding_id: str, expected_state: DeliveryState
    ) -> RedirectResult:
        """The owner sends one of their messages again, to the right session.

        The copy is a new owner message: in the same group, or in the owner's direct conversation
        with the target, since a direct conversation keeps its one session. A source still queued
        is cancelled; one already handed over keeps its state. A repeat returns the first copy.
        """
        with self._write() as db:
            source = self._delivery(db, delivery_id)
            original = self._message(db, source.message_id)
            conversation = self._conversation(db, original.conversation_id)
            if actor != Actor.user(conversation.owner):
                raise Forbidden("only the owner redirects a message")
            if original.actor != actor:
                raise Invalid("only the owner's own messages are redirected")
            client_message_id = f"redirect:{original.id}:{to_binding_id}"
            earlier = db.execute(
                "SELECT id FROM messages WHERE actor_kind = ? AND actor_id = ? AND client_message_id = ?",
                (actor.kind, actor.id, client_message_id),
            ).fetchone()
            if earlier:
                copy = self._message(db, earlier["id"])
                recorded = db.execute(
                    "SELECT payload FROM events WHERE conversation_id = ? AND message_id = ?"
                    " AND type = 'delivery.redirected'",
                    (conversation.id, original.id),
                ).fetchall()
                disposition: Literal["moved", "copied"] = "copied"
                for row in recorded:
                    payload = json.loads(row["payload"])
                    if payload["to_message_id"] == copy.id:
                        disposition = payload["disposition"]
                return RedirectResult(source, copy, self._deliveries_for(db, copy.id)[0], disposition, False)
            if source.state != expected_state:
                raise Conflict(f"delivery {delivery_id} is {source.state}, not {expected_state}")
            target = self._binding(db, to_binding_id)
            if not target.active:
                raise Invalid(f"binding {to_binding_id} is not active")
            if to_binding_id == source.binding_id:
                raise Invalid("the delivery is already that session's")
            if conversation.kind == "direct":
                destination = self._open_direct(db, conversation.owner, to_binding_id)
                recipients: list[str] | None = None
            else:
                if to_binding_id in original.recipients:
                    raise Conflict("the message already went to that session")
                destination = conversation
                recipients = [to_binding_id]
            posted = self._post(
                db, actor, destination.id, client_message_id, original.text, recipients, None, original.mode
            )
            disposition = "copied"
            if source.state == "queued":
                source = self._set_state(
                    db, source.id, "cancelled", None, actor, reason=f"moved to session {to_binding_id}"
                )
                disposition = "moved"
            delivery = posted.deliveries[0]
            self._append(
                db,
                conversation.id,
                "delivery.redirected",
                actor,
                original.id,
                {
                    "delivery_id": source.id,
                    "binding_id": source.binding_id,
                    "to_binding_id": to_binding_id,
                    "to_conversation_id": destination.id,
                    "to_message_id": posted.message.id,
                    "to_delivery_id": delivery.id,
                    "disposition": disposition,
                },
            )
            return RedirectResult(source, posted.message, delivery, disposition, created=True)

    def deliveries_for(self, message_id: str) -> tuple[Delivery, ...]:
        with self._read() as db:
            return self._deliveries_for(db, message_id)

    def _require_lease(self, db: sqlite3.Connection, binding_id: str, lease_generation: int) -> Binding:
        binding = self._binding(db, binding_id)
        if not binding.active:
            raise Forbidden(f"binding {binding_id} is not active")
        if binding.lease_generation != lease_generation:
            raise Conflict(
                f"stale lease generation {lease_generation}; current is {binding.lease_generation}"
            )
        return binding

    def _transition(
        self,
        db: sqlite3.Connection,
        delivery_id: str,
        binding_id: str,
        lease_generation: int,
        to_state: DeliveryState,
        native_turn_id: str | None,
        reason: str | None = None,
    ) -> Delivery:
        self._require_lease(db, binding_id, lease_generation)
        current = self._delivery(db, delivery_id)
        if current.binding_id != binding_id:
            raise Forbidden("delivery belongs to another session")
        if current.lease_generation != lease_generation:
            raise Conflict("delivery was issued under another lease generation")
        if to_state not in TRANSITIONS[current.state]:
            raise Invalid(f"delivery cannot go from {current.state} to {to_state}")
        if to_state in REASON_STATES and not (reason and reason.strip()):
            reason = f"the adapter reported {to_state} with no reason"
        delivery = self._set_state(
            db, delivery_id, to_state, native_turn_id, Actor.binding(binding_id), reason=reason
        )
        if to_state in READ_STATES and current.state not in READ_STATES:
            # A delivery counts as read once, when it first reaches a read state: a reply to one already
            # read does not end a hold or reset the closures counted since.
            self._resume(db, binding_id, "the session read a message")
        return delivery

    def _set_state(
        self,
        db: sqlite3.Connection,
        delivery_id: str,
        to_state: DeliveryState,
        native_turn_id: str | None,
        actor: Actor,
        lease_generation: int | None = None,
        reason: str | None = None,
    ) -> Delivery:
        """Write a state change and its event. The lease generation is stamped when dispatch starts.
        Leaving flight clears the wake ladder; a non-normal end must say why. Every path into a read or a
        failed end passes here, so this is where the sender's receipt or failure notice is written."""
        if to_state in REASON_STATES and not reason:
            raise ValueError(f"a change to {to_state} needs a reason")
        if to_state == "queued" and not self._enqueueing:
            raise AssertionError("a delivery enters the queue only through _enqueue, which admits it")
        before = self._delivery(db, delivery_id).state
        attempt_bump = 1 if to_state == "dispatching" else 0
        in_flight = to_state in INFLIGHT
        db.execute(
            "UPDATE deliveries SET state = ?, attempt = attempt + ?,"
            " native_turn_id = COALESCE(?, native_turn_id),"
            " lease_generation = COALESCE(?, lease_generation), updated_at = ?,"
            " reason = COALESCE(?, reason),"
            " wake_due_at = CASE WHEN ? THEN wake_due_at END,"
            " close_at = CASE WHEN ? THEN close_at END,"
            " rewake_wanted = CASE WHEN ? THEN rewake_wanted ELSE 0 END WHERE id = ?",
            (
                to_state,
                attempt_bump,
                native_turn_id,
                lease_generation,
                self._now(),
                reason,
                in_flight,
                in_flight,
                in_flight,
                delivery_id,
            ),
        )
        delivery = self._delivery(db, delivery_id)
        message = self._message(db, delivery.message_id)
        payload: dict[str, Any] = {
            "delivery_id": delivery.id,
            "binding_id": delivery.binding_id,
            "state": to_state,
            "native_turn_id": delivery.native_turn_id,
        }
        if reason:
            payload["reason"] = reason
        self._append(db, message.conversation_id, "delivery.changed", actor, message.id, payload)
        if to_state in READ_STATES and before not in READ_STATES:
            self._record_read(db, delivery, message)
        elif to_state in FAILED_STATES and actor != message.actor:
            # A sender who ended the delivery itself (the owner retiring a session) is not told of it.
            self._notify_failure(db, delivery, message, cast(str, reason))
        return delivery

    def _told(self, message: Message) -> bool:
        """Whether a message's sender hears about its deliveries: the owner and the sessions. The broker's
        own notices never produce another (no loops), and other users, such as a service, read nothing."""
        if message.actor.kind == "user":
            return message.actor.id == self.owner
        return message.actor.id != BROKER_BINDING_ID

    def _record_read(self, db: sqlite3.Connection, delivery: Delivery, message: Message) -> None:
        """A session sender's receipt, kept until its next read takes it. The owner reads read state
        from the message itself (message_record), so needs no row."""
        if message.actor.kind != "binding" or not self._told(message):
            return
        db.execute(
            "INSERT OR IGNORE INTO sender_notices (kind, delivery_id, message_id, sender_kind, sender_id,"
            " state, at) VALUES ('read', ?, ?, 'binding', ?, ?, ?)",
            (delivery.id, message.id, message.actor.id, delivery.state, self._now()),
        )

    def _notify_failure(
        self, db: sqlite3.Connection, delivery: Delivery, message: Message, reason: str
    ) -> None:
        """Tell the sender one recipient's delivery ended failed or uncertain. The owner gets a post in
        the `Duplex broker` conversation; a session gets a broker message in the conversation it wrote
        in, addressed to it, which is delivered and wakes it like any other."""
        if not self._told(message):
            return
        at = self._now()
        conversation = self._conversation(db, message.conversation_id)
        recipient = self._party(db, Actor.binding(delivery.binding_id))
        notice = {
            "kind": "delivery_failed",
            "message_id": message.id,
            "conversation_id": conversation.id,
            "recipient": recipient,
            "state": delivery.state,
            "reason": reason,
            "at": utc_iso(at),
        }
        text = (
            f"Delivery of your message {reply_id(message.id)} in {conversation.title} to "
            f"{recipient['title']} ended {delivery.state} at {notice['at']}: {reason}"
        )
        broker = self._broker_member(db)
        client_message_id = f"notice:{delivery.id}"
        if message.actor.kind == "user":
            notices = self._conversation(db, self._open_notices(db, self.owner))
            posted = self._insert_message(
                db, broker, notices, client_message_id, text, (), None, None, 0, notice
            )
        else:
            sender = self._binding(db, message.actor.id)
            targets = (sender.id,) if sender.active else ()
            posted = self._insert_message(
                db, broker, conversation, client_message_id, text, targets, message.id, None, 0, notice
            )
        db.execute(
            "INSERT OR IGNORE INTO sender_notices (kind, delivery_id, message_id, sender_kind, sender_id,"
            " state, reason, at, notice_message_id) VALUES ('failed', ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                delivery.id,
                message.id,
                message.actor.kind,
                message.actor.id,
                delivery.state,
                reason,
                at,
                posted.message.id,
            ),
        )

    def take_receipts(self, binding_id: str) -> list[Receipt]:
        """A session's receipts not yet reported, oldest first; each is reported once."""
        with self._write() as db:
            rows = db.execute(
                "SELECT n.id, n.message_id, n.state, n.at, m.conversation_id, m.created_at, b.title"
                " FROM sender_notices n JOIN messages m ON m.id = n.message_id"
                " JOIN deliveries d ON d.id = n.delivery_id JOIN session_bindings b ON b.id = d.binding_id"
                " WHERE n.kind = 'read' AND n.sender_kind = 'binding' AND n.sender_id = ?"
                " AND n.reported_at IS NULL ORDER BY n.id",
                (binding_id,),
            ).fetchall()
            db.executemany(
                "UPDATE sender_notices SET reported_at = ? WHERE id = ?",
                [(self._now(), row["id"]) for row in rows],
            )
        return [
            Receipt(
                message_id=row["message_id"],
                conversation_id=row["conversation_id"],
                recipient=row["title"],
                state=row["state"],
                read_at=row["at"],
                sent_at=row["created_at"],
            )
            for row in rows
        ]

    def _delivery(self, db: sqlite3.Connection, delivery_id: str) -> Delivery:
        row = db.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
        if row is None:
            raise NotFound(f"delivery {delivery_id}")
        return self._delivery_row(row)

    def _deliveries_for(self, db: sqlite3.Connection, message_id: str) -> tuple[Delivery, ...]:
        rows = db.execute(
            "SELECT * FROM deliveries WHERE message_id = ? ORDER BY seq", (message_id,)
        ).fetchall()
        return tuple(self._delivery_row(row) for row in rows)

    def deliveries_in_states(self, binding_id: str, states: Sequence[str]) -> list[dict[str, Any]]:
        """One session's own deliveries in the given states, oldest first, for its adapter to reconcile
        against. Reads only."""
        bad = [x for x in states if x not in DELIVERY_STATES]
        if bad or not states:
            raise Invalid(f"state must be a list of: {', '.join(DELIVERY_STATES)}")
        with self._read() as db:
            rows = db.execute(
                "SELECT d.id, d.state, d.native_turn_id, d.message_id, m.conversation_id FROM deliveries d"
                " JOIN messages m ON m.id = d.message_id"
                f" WHERE d.binding_id = ? AND d.state IN ({', '.join('?' * len(states))}) ORDER BY d.seq",
                (binding_id, *states),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _delivery_row(row: sqlite3.Row) -> Delivery:
        return Delivery(
            id=row["id"],
            message_id=row["message_id"],
            binding_id=row["binding_id"],
            state=row["state"],
            attempt=row["attempt"],
            native_turn_id=row["native_turn_id"],
            lease_generation=row["lease_generation"],
            rewakes=row["rewakes"],
            reason=row["reason"],
            wakes_sent=row["wakes_sent"],
        )

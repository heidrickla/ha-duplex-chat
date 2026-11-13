"""HTTP and WebSocket surface of the broker.

Two callers. The owner's browser arrives through Home Assistant ingress (authenticated by HA; only the
Supervisor's proxy address is trusted) or, in development, as a browser paired with a one-time code.
Session adapters authenticate with the bearer token issued when they register, and register only with
the enrollment token; re-registering an active session also needs that binding's current token, or the
owner's release. Every write from a browser carries the CSRF token from GET /api/session, and a
WebSocket must present it before it is sent anything. Membership and routing are enforced by the store.
"""

# No `from __future__ import annotations`: FastAPI resolves the dependency aliases defined inside
# create_app from the annotations at definition time.

import asyncio
import hashlib
import hmac
import html
import logging
import secrets
import time
import urllib.parse
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from importlib import resources
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from . import __version__, resume, tasks, waker
from .assist import DEFAULT_URL as ASSIST_URL
from .assist import AssistClient
from .connector import Connector, Resolve, Send, Unauthorized
from .ids import reply_id, shown_envelope, stored_id
from .ladder import Policy, late_read, record_wake, take_rewake
from .ladder import sweep as ladder_sweep
from .model_member import ChatFn, ModelConfig, ModelMember, ToolChatFn
from .store import (
    DEVICE_TOKEN_PREFIX,
    MAX_COLLABORATION_MINUTES,
    MAX_COLLABORATION_TURNS,
    SCHEMA_VERSION,
    Actor,
    Binding,
    BrokerError,
    DeliveryState,
    Device,
    Event,
    Forbidden,
    Invalid,
    Mode,
    NotFound,
    Store,
    adapter_behind,
    utc_iso,
)

UNSAFE = frozenset({"POST", "PUT", "PATCH", "DELETE"})
SESSION_COOKIE = "duplex_session"
STATUS = {
    "not_found": 404,
    "forbidden": 403,
    "conflict": 409,
    "invalid": 422,
    "mode_unsupported": 422,
    "tasks_unsupported": 422,
    "unauthenticated": 401,
    "gone": 410,
}
PAIRING_LOCKOUT = (5, 60.0)  # failures, seconds
# The owner's status page. It holds no data: it reads /api/status and the conversation events by relative
# path, so it works under ingress, and from anywhere else those reads answer 401.
PANEL = resources.files("duplex_broker").joinpath("panel.html").read_text(encoding="utf-8")
log = logging.getLogger(__name__)
_reply_id = reply_id
_stored_id = stored_id


class Unauthenticated(BrokerError):
    code = "unauthenticated"


def _read_text(folder: Path | None, name: str) -> str:
    """An optional text file from the add-on's config folder; empty when absent or unreadable."""
    if folder is None:
        return ""
    try:
        return (folder / name).read_text(encoding="utf-8").strip()
    except OSError:
        return ""


@dataclass
class Settings:
    owner: str = "owner"
    owner_name: str = "Owner"
    connector_title: str = "ChatGPT"
    user_auth: Literal["ingress", "pairing"] = "pairing"
    pairing_code: str = ""
    enrollment_token: str = ""
    services: dict[str, str] = field(default_factory=dict)  # name -> bearer token of a named service
    waker_token: str = ""  # the waker's bearer token; empty turns its routes off (404)
    waker_grace: float = waker.LIVE_SECONDS  # seconds after a start in which every binding counts as live
    trusted_proxies: frozenset[str] = frozenset({"172.30.32.2"})
    subscriber_queue: int = 1000
    stream_ping: float = 25.0  # seconds between pings on an idle adapter stream
    device_token_grace: float = 300.0
    collaboration_sweep: float = 15.0  # seconds between deadline checks, collaborations and the wake ladder
    wake_timeout: float = 300.0
    rewake_interval: float = 300.0
    rewake_limit: int = 3
    hold_after: int = 2
    model_url: str = ""  # an OpenAI-compatible base URL; empty turns the model member off
    model_name: str = ""
    model_tools_name: str = ""  # the model used with actions; the chat model when empty
    model_member_title: str = "Assistant"
    model_timeout: float = 90.0
    model_no_thinking: bool = False  # send chat_template_kwargs; strict OpenAI-compatible servers reject it
    model_chat: ChatFn | None = None  # a test double for the model call
    model_actions: bool = False  # run the tool calls the model returns through Assist, unapproved
    model_assist_url: str = ASSIST_URL
    model_assist_token: str = ""  # the add-on's SUPERVISOR_TOKEN
    model_assist: AssistClient | None = None  # a test double for Assist
    model_tool_chat: ToolChatFn | None = None  # a test double for the model call with tools
    config_dir: Path | None = None  # the add-on's own config folder: model-prompt.md, connector-guide.md
    csrf_secret: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    # The ChatGPT connector (connector.py): the public origin it is reached at; empty turns it off.
    connector_url: str = ""
    connector_send: Send | None = None  # test doubles for the webhook POST, its challenge and DNS
    connector_challenge: Any = None
    connector_resolve: Resolve | None = None


@dataclass
class Readiness:
    """Whether every port the broker serves is bound. Each listener counts itself in when it is built and
    again when its socket is listening; the broker is ready when the two counts match."""

    listeners: int = 0
    bound: int = 0

    @property
    def ready(self) -> bool:
        return self.listeners > 0 and self.bound >= self.listeners


@dataclass
class _Pairing:
    sessions: dict[str, str] = field(default_factory=dict)  # sha256(session token) -> csrf
    failures: dict[str, list[float]] = field(default_factory=dict)  # client address -> failure times


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def _public_binding(binding: Binding, highest: str) -> dict[str, Any]:
    """A binding with whether its adapter is older than `highest`, the newest among active sessions."""
    return {**asdict(binding), "adapter_behind": adapter_behind(binding, highest)}


def _public_event(event: Event) -> dict[str, Any]:
    return asdict(event)


def _iso_or_none(at: float | None) -> str | None:
    return None if at is None else utc_iso(at)


# Request bodies


class GroupIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    mode: Mode = "user_directed"


class DirectIn(BaseModel):
    binding_id: str


class MemberIn(BaseModel):
    binding_id: str


SendMode = Literal["auto", "queue", "steer", "restart"]


class MessageIn(BaseModel):
    client_message_id: str = Field(min_length=1, max_length=200)
    text: str
    recipients: list[str] | None = None
    parent_id: str | None = None
    mode: SendMode = "auto"


class ServiceNoticeIn(BaseModel):
    binding_id: str = Field(min_length=1, max_length=100)
    client_message_id: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1, max_length=500)


class AdapterMessageIn(MessageIn):
    conversation_id: str


class ControlIn(BaseModel):
    action: Literal["pause", "resume", "stop"]


class ModeIn(BaseModel):
    mode: Literal["user_directed", "consult"]


class CollaborationIn(BaseModel):
    objective: str = Field(min_length=1, max_length=2000)
    max_turns: int = Field(default=6, ge=1, le=MAX_COLLABORATION_TURNS)
    minutes: float = Field(default=10, gt=0, le=MAX_COLLABORATION_MINUTES)


class PairIn(BaseModel):
    code: str


class EnrollIn(BaseModel):
    pairing_code: str
    device_name: str = Field(default="phone", max_length=64)


class BuiltAgainstIn(BaseModel):
    version: str = Field(max_length=40)
    reviewed: str = Field(default="", max_length=40)
    notes: str = Field(default="", max_length=1000)


Name = Annotated[str, Field(max_length=40)]


class AdapterIn(BaseModel):
    """What a session adapter says it is: its version, checkout commit, the releases it was built against
    and the runtime it found. Unknown keys are ignored, so a newer adapter still registers."""

    version: str = Field(min_length=1, max_length=40)
    commit: str = Field(default="unknown", max_length=64)
    built_against: dict[Name, BuiltAgainstIn] = Field(default_factory=dict, max_length=8)
    runtime: dict[Name, Annotated[str, Field(max_length=100)]] = Field(default_factory=dict, max_length=8)


class RegisterIn(BaseModel):
    provider: Literal["claude", "codex", "opencode", "mock"]  # "model" is the broker's own member
    native_id: str = Field(min_length=1, max_length=500)
    title: str = Field(min_length=1, max_length=200)
    workspace: str = Field(max_length=1000)
    endpoint_ref: str = Field(max_length=1000)
    capabilities: dict[str, Any]
    current_token: str = Field(default="", max_length=200, repr=False)
    adapter: AdapterIn | None = None  # absent from adapters before versioning


class StateIn(BaseModel):
    state: DeliveryState
    native_turn_id: str | None = None
    reason: str | None = Field(default=None, max_length=500)


class TurnIn(BaseModel):
    """What the adapter sees of its session's turn. `waiting_for` is a kind of wait, checked by the store."""

    state: Literal["busy", "idle", "waiting", "unknown"]
    waiting_for: str | None = Field(default=None, max_length=200)
    # Raised by the adapter with every reading it sends and reset by its registration; an older or equal
    # reading is not applied.
    seq: int = Field(ge=1, le=2**53)


class ClaimIn(BaseModel):
    """What an adapter sends with a claim: its current turn state, so a lost turn report cannot stand."""

    turn: TurnIn | None = None


class TaskIn(BaseModel):
    to: str = Field(min_length=1, max_length=500)  # a session id or its exact title
    task: str = Field(min_length=1, max_length=20000)
    done_when: str = Field(min_length=1, max_length=2000)
    parent_task_id: str | None = Field(default=None, max_length=100)
    mode: SendMode = "auto"


class CancelIn(BaseModel):
    reason: str | None = Field(default=None, max_length=1000)


class ReportIn(BaseModel):
    outcome: Literal["done", "failed"]
    result: str = Field(min_length=1, max_length=50000)


class QuestionIn(BaseModel):
    question: str = Field(min_length=1, max_length=4000)


class AnswerIn(BaseModel):
    answer: str = Field(min_length=1, max_length=20000)


class LimitIn(BaseModel):
    """One usage-limit failure an adapter found in its session's transcript. `matched` is the matched signal
    only; the store keeps its first 300 characters."""

    run_id: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9._-]+$")
    source: Literal["claude-transcript", "codex-rollout"]
    matched: str = Field(max_length=5000)
    failed_at: float
    reset_at: float


class SkipIn(BaseModel):
    reason: str = Field(default="answered", min_length=1, max_length=300)


class WakerStartIn(BaseModel):
    report_id: str = Field(min_length=3, max_length=120)  # `<request id>:<n>`, the same on every retry
    ok: bool
    route: str = Field(min_length=1, max_length=80)
    detail: str = Field(default="", max_length=300)


class WakeIn(BaseModel):
    attempt: int = Field(ge=1)
    ok: bool
    error: str | None = Field(default=None, max_length=1000)


class RedirectIn(BaseModel):
    to: str = Field(min_length=1)
    expected_state: DeliveryState


# Live events


@dataclass(eq=False)
class _Subscriber:
    actor: Actor
    queue: asyncio.Queue[Event]
    conversations: set[str] = field(default_factory=set)
    last_sent: dict[str, int] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    overflow: bool = False


class Hub:
    """Fans committed store events out to WebSocket subscribers, per authorised conversation."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._subscribers: set[_Subscriber] = set()
        self._nudges: dict[str, set[asyncio.Event]] = {}  # binding id -> its open adapter streams

    def watch(self, binding_id: str) -> asyncio.Event:
        nudge = asyncio.Event()
        self._nudges.setdefault(binding_id, set()).add(nudge)
        return nudge

    def unwatch(self, binding_id: str, nudge: asyncio.Event) -> None:
        streams = self._nudges.get(binding_id, set())
        streams.discard(nudge)
        if not streams:
            self._nudges.pop(binding_id, None)

    def watching(self, binding_id: str) -> bool:
        return bool(self._nudges.get(binding_id))

    def on_events(self, events: Sequence[Event]) -> None:
        self._loop.call_soon_threadsafe(self._fanout, list(events))

    def add(self, subscriber: _Subscriber) -> None:
        self._subscribers.add(subscriber)

    def remove(self, subscriber: _Subscriber) -> None:
        self._subscribers.discard(subscriber)

    def _fanout(self, events: list[Event]) -> None:
        # Every delivery and wake event names its binding: that binding's adapter has something to claim.
        for event in events:
            for nudge in self._nudges.get(str(event.payload.get("binding_id", "")), ()):
                nudge.set()
        for subscriber in self._subscribers:
            for event in events:
                if event.conversation_id not in subscriber.conversations:
                    continue
                try:
                    subscriber.queue.put_nowait(event)
                except asyncio.QueueFull:
                    subscriber.overflow = True


def create_app(store: Store, settings: Settings) -> FastAPI:
    store.connector_title = settings.connector_title
    pairing = _Pairing()
    readiness = Readiness()
    started_at = time.time()
    # Adapters poll /api/adapter/claim every few seconds without a heartbeat; the last poll per binding
    # is kept in memory only, so a poll costs no write.
    polled_at: dict[str, float] = {}
    policy = Policy(
        settings.wake_timeout, settings.rewake_interval, settings.rewake_limit, settings.hold_after
    )

    def seen(binding_id: str, hub: Hub | None = None) -> dict[str, Any]:
        """When a recipient's adapter was last heard from, so a sender can tell a live session from a gone
        one. An open stream is now; a poll since this broker started is exact; otherwise the registration
        time is all there is."""
        if hub is not None and hub.watching(binding_id):
            return {"last_seen": time.time(), "seen_by": "stream"}
        polled = polled_at.get(binding_id)
        if polled is not None:
            return {"last_seen": polled, "seen_by": "poll"}
        return {"last_seen": store.get_binding(binding_id).heartbeat_at, "seen_by": "registration"}

    for service in settings.services:
        if service == store.owner:
            raise ValueError(f"service name {service!r} is the owner's id")
    connector: Connector | None = None
    if settings.connector_url:
        connector = Connector(
            store,
            settings.connector_url,
            store.owner,
            check_code=lambda code, client: _check_code(code, client),
            heard=lambda binding_id: polled_at.__setitem__(binding_id, time.time()),
            seen=lambda binding_id: seen(binding_id, getattr(app.state, "hub", None)),
            owner_name=settings.owner_name,
            guide_file=settings.config_dir / "connector-guide.md" if settings.config_dir else None,
        )
        if settings.connector_send is not None:
            connector.send = settings.connector_send
        if settings.connector_challenge is not None:
            connector.challenge = settings.connector_challenge
        if settings.connector_resolve is not None:
            connector.resolve = settings.connector_resolve

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        hub = Hub(asyncio.get_running_loop())
        app.state.hub = hub
        unsubscribe = store.subscribe(hub.on_events)
        sweeper = asyncio.create_task(_sweep())
        delivering: asyncio.Task[None] | None = None
        if connector is not None:
            chatgpt = await run_in_threadpool(connector.binding)
            delivering = asyncio.create_task(connector.run(hub.watch(chatgpt.id)))
        model: asyncio.Task[None] | None = None
        unwake = None
        if settings.model_url or settings.model_chat:
            loop = asyncio.get_running_loop()
            wake = asyncio.Event()

            def _wake(_events: Sequence[Event]) -> None:
                loop.call_soon_threadsafe(wake.set)

            unwake = store.subscribe(_wake)
            config = ModelConfig(
                settings.model_url,
                settings.model_name,
                settings.model_timeout,
                tools_model=settings.model_tools_name or settings.model_name,
                title=settings.model_member_title,
                owner_name=settings.owner_name,
                extra_prompt=_read_text(settings.config_dir, "model-prompt.md"),
                no_thinking=settings.model_no_thinking,
            )
            assist = settings.model_assist
            if assist is None and settings.model_actions and settings.model_assist_token:
                assist = AssistClient(settings.model_assist_url, settings.model_assist_token)
            member = ModelMember(store, config, settings.model_chat, assist, settings.model_tool_chat)
            model = asyncio.create_task(member.run(wake))
        try:
            yield
        finally:
            sweeper.cancel()
            if delivering is not None:
                delivering.cancel()
            if model is not None:
                model.cancel()
            if unwake is not None:
                unwake()
            unsubscribe()

    async def _sweep() -> None:
        """Stop collaborations at their deadline even when no session posts, and walk the wake ladder."""
        while True:
            await asyncio.sleep(settings.collaboration_sweep)
            try:
                await run_in_threadpool(store.expire_collaborations)
            except Exception:
                log.exception("collaboration sweep failed")
            try:
                await run_in_threadpool(ladder_sweep, store, policy, store.owner)
            except Exception:
                log.exception("wake ladder sweep failed")
            try:
                await run_in_threadpool(resume.sweep, store)
            except Exception:
                log.exception("usage-limit resume sweep failed")

    app = FastAPI(
        title="duplex-chat broker",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.exception_handler(BrokerError)
    async def broker_error(request: Request, exc: BrokerError) -> JSONResponse:
        return JSONResponse({"error": exc.code, "detail": str(exc)}, status_code=STATUS.get(exc.code, 500))

    # Authentication

    def _browser_csrf(cookies: dict[str, str], client_host: str | None) -> str:
        """The CSRF token of an authenticated browser, or Unauthenticated."""
        if settings.user_auth == "ingress":
            if client_host is None or client_host not in settings.trusted_proxies:
                raise Unauthenticated("requests must arrive through Home Assistant ingress")
            return settings.csrf_secret
        token = cookies.get(SESSION_COOKIE)
        csrf = pairing.sessions.get(_sha(token)) if token else None
        if csrf is None:
            raise Unauthenticated("pair this browser first")
        return csrf

    def _device_bearer(headers: Mapping[str, str]) -> Device | None:
        """A device token in the Authorization header, or None when there is none. A bearer token is
        never sent by a browser on its own, so requests carrying one need no CSRF token."""
        header = headers.get("authorization", "")
        token = header.removeprefix("Bearer ") if header.startswith("Bearer ") else ""
        if not token.startswith(DEVICE_TOKEN_PREFIX):
            return None
        return store.authenticate_device(token)

    def user(request: Request) -> Actor:
        if _device_bearer(request.headers) is not None:
            return Actor.user(store.owner)
        csrf = _browser_csrf(request.cookies, request.client.host if request.client else None)
        if request.method in UNSAFE and not _same(request.headers.get("x-csrf-token", ""), csrf):
            raise Forbidden("missing or wrong CSRF token")
        return Actor.user(store.owner)

    def device(request: Request) -> Device:
        found = _device_bearer(request.headers)
        if found is None:
            raise Unauthenticated("device bearer token required")
        return found

    def _check_pairing_code(code: str, request: Request) -> None:
        _check_code(code, request.client.host if request.client else "")

    def _check_code(code: str, client: str) -> None:
        """The lockout counts each client address on its own, so one sender cannot lock out the rest."""
        if not settings.pairing_code:
            raise Forbidden("pairing is disabled")
        now = time.monotonic()
        limit, window = PAIRING_LOCKOUT
        for address in list(pairing.failures):
            recent = [t for t in pairing.failures[address] if now - t < window]
            if recent:
                pairing.failures[address] = recent
            else:
                del pairing.failures[address]
        if len(pairing.failures.get(client, [])) >= limit:
            raise Forbidden("too many pairing attempts; wait a minute")
        if not _same(code, settings.pairing_code):
            pairing.failures.setdefault(client, []).append(now)
            raise Forbidden("wrong pairing code")

    def adapter(request: Request) -> Binding:
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer "):
            raise Unauthenticated("adapter bearer token required")
        return store.authenticate(header.removeprefix("Bearer "))

    User = Annotated[Actor, Depends(user)]
    Adapter = Annotated[Binding, Depends(adapter)]
    OwnerDevice = Annotated[Device, Depends(device)]

    # Browser

    @app.get("/health")
    def health() -> dict[str, Any]:
        """Always 200, so a watchdog sees the process; `ready` says whether migrations have finished and every
        listener is bound."""
        schema = store.schema
        return {
            "ok": True,
            "version": __version__,
            "schema": schema,
            "ready": schema == SCHEMA_VERSION and readiness.ready,
        }

    @app.get("/", include_in_schema=False)
    def page() -> HTMLResponse:
        return HTMLResponse(PANEL, headers={"cache-control": "no-store"})

    @app.get("/api/status")
    def status(actor: User, days: Annotated[int, Query(ge=1, le=400)] = 90) -> dict[str, Any]:
        now = time.time()
        doc = store.status(now - days * 86400)
        for session in doc["sessions"]:
            session["polled_at"] = polled_at.get(session["id"])
        return {
            **doc,
            "owner_name": settings.owner_name,
            "version": __version__,
            "now": now,
            "started_at": started_at,
            "ladder": {
                "wake_timeout": policy.wake_timeout,
                "rewake_interval": policy.rewake_interval,
                "rewake_limit": policy.rewake_limit,
                "hold_after": policy.hold_after,
            },
            "model": {
                "enabled": bool(settings.model_url or settings.model_chat),
                "name": settings.model_name,
                "title": settings.model_member_title,
                "actions": settings.model_actions,
            },
        }

    @app.post("/api/pair")
    def pair(body: PairIn, request: Request, response: Response) -> dict[str, str]:
        if settings.user_auth != "pairing":
            raise Forbidden("browser pairing is disabled; use Home Assistant ingress")
        _check_pairing_code(body.code, request)
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        pairing.sessions[_sha(token)] = csrf
        response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="strict", path="/")
        return {"csrf": csrf}

    @app.get("/api/session")
    def session(request: Request, actor: User) -> dict[str, str]:
        csrf = _browser_csrf(request.cookies, request.client.host if request.client else None)
        return {"owner": actor.id, "owner_name": settings.owner_name, "csrf": csrf}

    @app.post("/api/device/enroll", status_code=201)
    def enroll(body: EnrollIn, request: Request) -> dict[str, Any]:
        """A phone presents the pairing code once and keeps its own token."""
        _check_pairing_code(body.pairing_code, request)
        enrolled, token = store.enroll_device(body.device_name)
        return {"token": token, "id": enrolled.id, "label": enrolled.label}

    @app.post("/api/device/token/rotate")
    def rotate(me: OwnerDevice) -> dict[str, Any]:
        token = store.rotate_device_token(me.id, settings.device_token_grace)
        return {"token": token, "id": me.id, "grace_seconds": settings.device_token_grace}

    @app.get("/api/devices")
    def devices(actor: User) -> list[dict[str, Any]]:
        return [asdict(d) for d in store.list_devices()]

    @app.post("/api/devices/{device_id}/{switch}")
    def switch_device(device_id: str, switch: Literal["enable", "disable"], actor: User) -> dict[str, Any]:
        return asdict(store.set_device_disabled(device_id, switch == "disable"))

    @app.get("/api/tasks")
    def owner_tasks(
        actor: User, open_only: Annotated[int, Query(alias="open", ge=0, le=1)] = 1
    ) -> list[dict[str, Any]]:
        """Delegated tasks, newest last. open=1 keeps live ones and finished ones whose result is unread;
        open=0 lists every task."""
        return tasks.owner_tasks(store, bool(open_only))

    @app.get("/api/tasks/{task_id}")
    def owner_task(task_id: str, actor: User) -> dict[str, Any]:
        return tasks.owner_task(store, task_id)

    @app.post("/api/tasks/{task_id}/cancel")
    def owner_task_cancel(task_id: str, actor: User, body: CancelIn | None = None) -> dict[str, Any]:
        tasks.cancel(store, actor, task_id, body.reason if body else None)
        return tasks.owner_task(store, task_id)

    @app.get("/api/waker/starts")
    def owner_waker_starts(actor: User) -> list[dict[str, Any]]:
        """The waker's recent attempts to open a closed session, newest first."""
        return waker.recent_starts(store)

    @app.get("/api/limits")
    def owner_limits(actor: User) -> list[dict[str, Any]]:
        """Usage-limit resumes, newest first, with what became of each."""
        return resume.list_resumes(store)

    @app.get("/api/refusals")
    def refusals(actor: User) -> list[dict[str, Any]]:
        """What the broker refused in the last day and why, newest first."""
        return store.refusals()

    @app.get("/api/subscriptions")
    def subscriptions(actor: User) -> list[dict[str, Any]]:
        """The ChatGPT connector's MCP Events subscriptions: renewals and the last event each answered."""
        return [] if connector is None else connector.subscriptions()

    @app.get("/api/sessions")
    def sessions(actor: User) -> list[dict[str, Any]]:
        highest = store.highest_adapter_version()
        return [_public_binding(b, highest) for b in store.list_bindings()]

    @app.post("/api/service/{name}/notify", status_code=201)
    def service_notice(
        name: str, body: ServiceNoticeIn, request: Request, response: Response
    ) -> dict[str, Any]:
        """One restricted service sends status data in its own direct conversation."""
        require_service(name, request)
        conversation = store.open_direct(name, body.binding_id)
        text = f"[{name} status data] " + " ".join(body.text.split())
        result = store.post_message(
            Actor.user(name),
            conversation.id,
            body.client_message_id,
            text,
            [body.binding_id],
        )
        response.status_code = 201 if result.created else 200
        return {"conversation_id": conversation.id, "message_id": result.message.id}

    def require_service(name: str, request: Request) -> None:
        """A named service's token, compared in constant time; an unknown name is refused as a wrong token."""
        token = settings.services.get(name, "")
        supplied = request.headers.get("authorization", "")
        if not token or not _same(supplied, f"Bearer {token}"):
            raise Unauthenticated(f"{name.replace('-', ' ')} token required")

    def require_waker(request: Request) -> None:
        """The waker's token, compared in constant time; with no token configured its routes do not exist."""
        if not settings.waker_token:
            raise NotFound("not found")
        supplied = request.headers.get("authorization", "")
        if not _same(supplied, f"Bearer {settings.waker_token}"):
            raise Unauthenticated("waker token required")

    def adapter_running(binding_id: str, hub: Hub) -> bool:
        """Whether a session's adapter is running: a stream open, a claim poll within the last 90 s, or this
        broker having started within them (the adapters have not yet had the chance to reconnect)."""
        now = time.time()
        if now - started_at < settings.waker_grace or hub.watching(binding_id):
            return True
        polled = polled_at.get(binding_id)
        return polled is not None and now - polled < waker.LIVE_SECONDS

    @app.get("/api/waker/requests")
    def waker_requests(request: Request) -> list[dict[str, Any]]:
        """Sessions the waker should start: a scheduled send waits queued for each and its adapter is not
        running."""
        require_waker(request)
        hub: Hub = request.app.state.hub
        return waker.requests(store, lambda binding_id: adapter_running(binding_id, hub))

    @app.post("/api/waker/requests/{request_id}/started", status_code=201)
    def waker_started(
        request_id: str, body: WakerStartIn, request: Request, response: Response
    ) -> dict[str, Any]:
        require_waker(request)
        kept, created = waker.record_start(
            store, request_id, body.report_id, body.ok, body.route, body.detail
        )
        if not created:
            response.status_code = 200  # a repeat of a report already taken
        return kept

    @app.get("/api/service/{name}/sessions")
    def service_sessions(name: str, request: Request) -> list[dict[str, Any]]:
        require_service(name, request)
        return [
            {"id": binding.id, "title": binding.title, "provider": binding.provider, "active": binding.active}
            for binding in store.list_bindings()
            if binding.active
        ]

    @app.post("/api/sessions/{binding_id}/release")
    def release(binding_id: str, actor: User) -> dict[str, Any]:
        return _public_binding(store.release_binding(actor, binding_id), store.highest_adapter_version())

    @app.post("/api/sessions/{binding_id}/retire")
    def retire(binding_id: str, actor: User) -> dict[str, Any]:
        return _public_binding(store.retire_binding(actor, binding_id), store.highest_adapter_version())

    @app.get("/api/conversations")
    def conversations(actor: User) -> list[dict[str, Any]]:
        return [asdict(c) for c in store.conversations_for(actor)]

    @app.post("/api/conversations", status_code=201)
    def create_group(body: GroupIn, actor: User) -> dict[str, Any]:
        return asdict(store.create_group(actor.id, body.title, body.mode))

    @app.post("/api/direct-conversations")
    def open_direct(body: DirectIn, actor: User) -> dict[str, Any]:
        return asdict(store.open_direct(actor.id, body.binding_id))

    def _detail(reader: Actor, conversation_id: str) -> dict[str, Any]:
        conversation = store.get_conversation(reader, conversation_id)
        collaboration = store.collaboration(reader, conversation_id)
        return {
            **asdict(conversation),
            "members": [asdict(m) for m in store.member_details(reader, conversation_id)],
            "collaboration": asdict(collaboration) if collaboration else None,
        }

    @app.get("/api/conversations/{conversation_id}")
    def get_conversation(conversation_id: str, actor: User) -> dict[str, Any]:
        return _detail(actor, conversation_id)

    @app.post("/api/conversations/{conversation_id}/mode")
    def set_mode(conversation_id: str, body: ModeIn, actor: User) -> dict[str, Any]:
        store.set_mode(actor, conversation_id, body.mode)
        return _detail(actor, conversation_id)

    @app.post("/api/conversations/{conversation_id}/collaboration")
    def start_collaboration(conversation_id: str, body: CollaborationIn, actor: User) -> dict[str, Any]:
        store.start_collaboration(actor, conversation_id, body.objective, body.max_turns, body.minutes)
        return _detail(actor, conversation_id)

    @app.delete("/api/conversations/{conversation_id}/collaboration")
    def stop_collaboration(conversation_id: str, actor: User) -> dict[str, Any]:
        store.stop_collaboration(actor, conversation_id)
        return _detail(actor, conversation_id)

    @app.post("/api/conversations/{conversation_id}/archive")
    def archive(conversation_id: str, actor: User) -> dict[str, Any]:
        return asdict(store.set_archived(actor, conversation_id, True))

    @app.post("/api/conversations/{conversation_id}/restore")
    def restore(conversation_id: str, actor: User) -> dict[str, Any]:
        return asdict(store.set_archived(actor, conversation_id, False))

    @app.post("/api/conversations/{conversation_id}/members", status_code=204)
    def add_member(conversation_id: str, body: MemberIn, actor: User) -> None:
        store.add_member(actor, conversation_id, body.binding_id)

    @app.post("/api/deliveries/{delivery_id}/redirect")
    def redirect(delivery_id: str, body: RedirectIn, actor: User, response: Response) -> dict[str, Any]:
        result = store.redirect(actor, delivery_id, body.to, body.expected_state)
        response.status_code = 201 if result.created else 200
        return asdict(result)

    @app.post("/api/conversations/{conversation_id}/messages")
    def post_message(
        conversation_id: str, body: MessageIn, actor: User, response: Response
    ) -> dict[str, Any]:
        result = store.post_message(
            actor,
            conversation_id,
            body.client_message_id,
            body.text,
            body.recipients,
            body.parent_id,
            body.mode,
        )
        response.status_code = 201 if result.created else 200
        posted = asdict(result)
        posted["message"].update(store.envelope(result.message))
        return posted

    @app.get("/api/messages/{message_id}")
    def owner_message(message_id: str, actor: User) -> dict[str, Any]:
        """One message with its envelope and each recipient's read state: when it first reached
        consumed or responded, or why it ended unread."""
        record = store.message_record(actor, message_id)
        return {
            "message": {**asdict(record.message), **store.envelope(record.message)},
            "seq": record.seq,
            "deliveries": [
                {
                    "delivery_id": v.delivery.id,
                    "binding_id": v.delivery.binding_id,
                    "to": v.recipient_title,
                    "state": v.delivery.state,
                    "read": v.read_at is not None,
                    "read_at": _iso_or_none(v.read_at),
                    "reason": v.delivery.reason,
                    "updated_at": utc_iso(v.updated_at),
                }
                for v in record.deliveries
            ],
        }

    @app.get("/api/conversations/{conversation_id}/events")
    def events(conversation_id: str, actor: User, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        return [_public_event(e) for e in store.events_after(actor, conversation_id, after, min(limit, 1000))]

    @app.post("/api/conversations/{conversation_id}/control")
    def control(conversation_id: str, body: ControlIn, actor: User) -> dict[str, Any]:
        if body.action == "stop":
            return {"cancelled": [asdict(d) for d in store.cancel_pending(actor, conversation_id)]}
        return asdict(store.set_paused(actor, conversation_id, body.action == "pause"))

    @app.websocket("/api/events")
    async def live(ws: WebSocket) -> None:
        """Replay then stream authorised conversations.

        A device connects with its bearer token in the Authorization header. A browser sends
        {"type": "hello", "csrf": ...} first. Then any number of
        {"type": "subscribe", "conversation_id": ..., "after": N}. A client too slow for the queue gets
        {"type": "resync_required"} and is closed; it reconnects with its cursors.
        """
        await ws.accept()
        try:
            if await run_in_threadpool(_device_bearer, ws.headers) is None:
                csrf = _browser_csrf(dict(ws.cookies), ws.client.host if ws.client else None)
                hello = await ws.receive_json()
                if (
                    not isinstance(hello, dict)
                    or hello.get("type") != "hello"
                    or not _same(str(hello.get("csrf", "")), csrf)
                ):
                    raise Forbidden("send hello with the CSRF token first")
        except BrokerError as exc:
            await ws.send_json({"type": "error", "error": exc.code, "detail": str(exc)})
            await ws.close(code=4403)
            return
        actor = Actor.user(store.owner)
        hub: Hub = ws.app.state.hub
        subscriber = _Subscriber(actor, asyncio.Queue(maxsize=settings.subscriber_queue))
        hub.add(subscriber)
        sender = asyncio.create_task(_send_live(ws, subscriber))
        try:
            await ws.send_json({"type": "ready"})
            while True:
                request = await ws.receive_json()
                if not isinstance(request, dict) or request.get("type") != "subscribe":
                    await ws.send_json({"type": "error", "error": "invalid", "detail": "unknown request"})
                    continue
                await _subscribe(
                    ws, subscriber, str(request.get("conversation_id", "")), int(request.get("after", 0))
                )
        except WebSocketDisconnect:
            pass
        finally:
            hub.remove(subscriber)
            sender.cancel()

    async def _subscribe(ws: WebSocket, subscriber: _Subscriber, conversation_id: str, after: int) -> None:
        async with subscriber.lock:
            try:
                await run_in_threadpool(store.get_conversation, subscriber.actor, conversation_id)
            except BrokerError as exc:
                await ws.send_json(
                    {
                        "type": "error",
                        "error": exc.code,
                        "detail": str(exc),
                        "conversation_id": conversation_id,
                    }
                )
                return
            subscriber.conversations.add(conversation_id)
            cursor = max(after, subscriber.last_sent.get(conversation_id, 0))
            while True:
                page = await run_in_threadpool(
                    store.events_after, subscriber.actor, conversation_id, cursor, 500
                )
                for event in page:
                    await ws.send_json({"type": "event", "event": _public_event(event)})
                    cursor = event.seq
                if len(page) < 500:
                    break
            subscriber.last_sent[conversation_id] = cursor
            await ws.send_json({"type": "subscribed", "conversation_id": conversation_id, "seq": cursor})

    async def _send_live(ws: WebSocket, subscriber: _Subscriber) -> None:
        while True:
            event = await subscriber.queue.get()
            async with subscriber.lock:
                if subscriber.overflow:
                    await ws.send_json({"type": "resync_required"})
                    await ws.close(code=4409)
                    return
                if event.seq <= subscriber.last_sent.get(event.conversation_id, 0):
                    continue
                await ws.send_json({"type": "event", "event": _public_event(event)})
                subscriber.last_sent[event.conversation_id] = event.seq

    # Adapters

    @app.websocket("/api/adapter/stream")
    async def adapter_stream(ws: WebSocket) -> None:
        """Push for one adapter: {"type": "nudge"} whenever an event names its binding, so it claims at
        once instead of polling, and {"type": "ping"} when idle. A nudge carries no content; the adapter
        claims through /api/adapter/claim as before. One nudge is sent on connect, for anything already
        waiting."""
        await ws.accept()
        header = ws.headers.get("authorization", "")
        try:
            if not header.startswith("Bearer "):
                raise Unauthenticated("adapter bearer token required")
            binding = await run_in_threadpool(store.authenticate, header.removeprefix("Bearer "))
        except BrokerError as exc:
            await ws.send_json({"type": "error", "error": exc.code, "detail": str(exc)})
            await ws.close(code=4401)
            return
        hub: Hub = ws.app.state.hub
        nudge = hub.watch(binding.id)
        nudge.set()
        try:
            await ws.send_json({"type": "ready", "ping_seconds": settings.stream_ping})
            while True:
                polled_at[binding.id] = time.time()
                try:
                    await asyncio.wait_for(nudge.wait(), timeout=settings.stream_ping)
                except TimeoutError:
                    await ws.send_json({"type": "ping"})
                    continue
                nudge.clear()
                await ws.send_json({"type": "nudge"})
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            hub.unwatch(binding.id, nudge)

    @app.post("/api/adapter/register", status_code=201)
    def register(body: RegisterIn, request: Request) -> dict[str, Any]:
        header = request.headers.get("authorization", "")
        if not settings.enrollment_token or not _same(header, f"Bearer {settings.enrollment_token}"):
            raise Unauthenticated("enrollment token required")
        binding, token = store.register_binding(
            body.provider,
            body.native_id,
            body.title,
            body.workspace,
            body.endpoint_ref,
            body.capabilities,
            current_token=body.current_token,
            adapter=body.adapter.model_dump() if body.adapter else None,
        )
        tasks.sync_observed(store, binding.id)  # registration resets the turn state a wait was read from
        return {
            "binding": _public_binding(binding, store.highest_adapter_version()),
            "token": token,
            "owner_name": settings.owner_name,  # adapters put it in the texts they give a session
        }

    @app.get("/api/adapter/binding")
    def own_binding(binding: Adapter) -> dict[str, Any]:
        """The adapter's own binding, with whether a newer adapter runs in another active session."""
        return _public_binding(binding, store.highest_adapter_version())

    @app.post("/api/adapter/heartbeat", status_code=204)
    def heartbeat(binding: Adapter) -> None:
        store.heartbeat(binding.id)

    @app.post("/api/adapter/turn")
    def turn(body: TurnIn, binding: Adapter) -> dict[str, Any]:
        """The session's turn state, sent on each change and again after registration. A queue-mode message
        waits in the claim while it is busy or waiting."""
        kept, applied = tasks.report_turn(store, binding, body.state, body.waiting_for, body.seq)
        return {"state": kept.turn_state, "waiting_for": kept.turn_waiting_for, "applied": applied}

    @app.post("/api/adapter/claim")
    def claim(binding: Adapter, response: Response, body: ClaimIn | None = None) -> dict[str, Any] | None:
        polled_at[binding.id] = time.time()
        if body is not None and body.turn is not None:
            # Before the queue hold is judged: the reading sent now beats whatever was reported last.
            tasks.report_turn(store, binding, body.turn.state, body.turn.waiting_for, body.turn.seq)
        rewake = take_rewake(store, binding.id, binding.lease_generation)
        if rewake is not None:
            # A due re-wake for the delivery already in flight: the adapter repeats its fixed wake line.
            due, message, kind = rewake
            return {
                "rewake": {
                    "delivery_id": due.id,
                    "message_id": message.id,
                    "conversation_id": message.conversation_id,
                    "conversation_kind": kind,
                    "attempt": due.rewakes,
                    "limit": policy.rewake_limit,
                }
            }
        claimed = store.claim_next(binding.id, binding.lease_generation)
        if claimed is None:
            response.status_code = 204
            return None
        delivery, message = claimed
        message_body = {**asdict(message), **shown_envelope(store.envelope(message))}
        message_body["id"] = _reply_id(message.id)
        message_body["conversation_id"] = _reply_id(message.conversation_id)
        if message.parent_id is not None:
            message_body["parent_id"] = _reply_id(message.parent_id)
        conversation = _detail(Actor.binding(binding.id), message.conversation_id)
        conversation["id"] = _reply_id(message.conversation_id)
        return {"delivery": asdict(delivery), "message": message_body, "conversation": conversation}

    @app.get("/api/adapter/deliveries")
    def own_deliveries(
        binding: Adapter, state: Annotated[str, Query(min_length=1, max_length=200)]
    ) -> list[dict[str, Any]]:
        """The calling session's own deliveries in the given states (comma separated), for an adapter that
        restarted or lost the broker to reconcile against. Reads only: no claim, no liveness."""
        found = store.deliveries_in_states(binding.id, [s.strip() for s in state.split(",")])
        return [
            {
                **d,
                "message_id": _reply_id(d["message_id"]),
                "conversation_id": _reply_id(d["conversation_id"]),
            }
            for d in found
        ]

    @app.post("/api/adapter/deliveries/{delivery_id}/state")
    def delivery_state(delivery_id: str, body: StateIn, binding: Adapter) -> dict[str, Any]:
        return asdict(
            store.transition(
                delivery_id,
                binding.id,
                binding.lease_generation,
                body.state,
                body.native_turn_id,
                body.reason,
            )
        )

    @app.post("/api/adapter/deliveries/{delivery_id}/wake", status_code=204)
    def delivery_wake(delivery_id: str, body: WakeIn, binding: Adapter) -> None:
        record_wake(
            store, delivery_id, binding.id, binding.lease_generation, body.attempt, body.ok, body.error
        )

    @app.post("/api/adapter/deliveries/{delivery_id}/late-read")
    def delivery_late_read(delivery_id: str, binding: Adapter) -> dict[str, Any]:
        pointer = late_read(store, delivery_id, binding.id, binding.lease_generation)
        for key in ("conversation_id", "message_id"):
            if isinstance(pointer.get(key), str):
                pointer[key] = _reply_id(pointer[key])
        return pointer

    @app.post("/api/adapter/messages")
    def adapter_message(
        body: AdapterMessageIn, binding: Adapter, request: Request, response: Response
    ) -> dict[str, Any]:
        result = store.post_message(
            Actor.binding(binding.id),
            _stored_id(body.conversation_id),
            body.client_message_id,
            body.text,
            body.recipients,
            _stored_id(body.parent_id) if body.parent_id is not None else None,
            body.mode,
        )
        response.status_code = 201 if result.created else 200
        posted = asdict(result)
        posted["message"].update(store.envelope(result.message))
        posted["message"]["id"] = _reply_id(result.message.id)
        posted["recipients_seen"] = [
            {
                "binding_id": d.binding_id,
                "title": store.get_binding(d.binding_id).title,
                **seen(d.binding_id, request.app.state.hub),
            }
            for d in result.deliveries
        ]
        posted["broker_started_at"] = started_at
        return posted

    @app.post("/api/adapter/tasks", status_code=201)
    def adapter_task_create(body: TaskIn, binding: Adapter) -> dict[str, Any]:
        return tasks.create_task(
            store, binding, body.to, body.task, body.done_when, body.parent_task_id, body.mode
        )

    @app.get("/api/adapter/tasks")
    def adapter_tasks(
        binding: Adapter,
        role: Literal["requester", "assignee", "both"] = "both",
        open_only: Annotated[int, Query(alias="open", ge=0, le=1)] = 1,
    ) -> list[dict[str, Any]]:
        """The session's own tasks. open=1 keeps live ones and finished ones whose result is unread;
        open=0 lists every task."""
        return tasks.list_tasks(store, binding, role, bool(open_only))

    @app.get("/api/adapter/tasks/{task_id}")
    def adapter_task(task_id: str, binding: Adapter) -> dict[str, Any]:
        return tasks.get_task(store, binding, task_id)

    @app.post("/api/adapter/tasks/{task_id}/cancel")
    def adapter_task_cancel(task_id: str, binding: Adapter, body: CancelIn | None = None) -> dict[str, Any]:
        return tasks.cancel(store, Actor.binding(binding.id), task_id, body.reason if body else None)

    @app.post("/api/adapter/tasks/{task_id}/report")
    def adapter_task_report(task_id: str, body: ReportIn, binding: Adapter) -> dict[str, Any]:
        return tasks.report(store, binding, task_id, body.outcome, body.result)

    @app.post("/api/adapter/tasks/{task_id}/questions", status_code=201)
    def adapter_task_ask(task_id: str, body: QuestionIn, binding: Adapter) -> dict[str, Any]:
        return tasks.ask(store, binding, task_id, body.question)

    @app.post("/api/adapter/questions/{question_id}/answer")
    def adapter_question_answer(question_id: str, body: AnswerIn, binding: Adapter) -> dict[str, Any]:
        return tasks.answer(store, binding, question_id, body.answer)

    @app.post("/api/adapter/questions/{question_id}/withdraw")
    def adapter_question_withdraw(question_id: str, binding: Adapter) -> dict[str, Any]:
        return tasks.withdraw(store, binding, question_id)

    @app.post("/api/adapter/limits", status_code=201)
    def adapter_limit(body: LimitIn, binding: Adapter, response: Response) -> dict[str, Any]:
        """A usage-limit failure and when it resets; a repeat of the same run and reset answers the row."""
        row, created = resume.record_limit(
            store, binding, body.run_id, body.source, body.matched, body.failed_at, body.reset_at
        )
        response.status_code = 201 if created else 200
        return row

    @app.post("/api/adapter/limits/{resume_id}/skip")
    def adapter_limit_skip(resume_id: str, body: SkipIn, binding: Adapter) -> dict[str, Any]:
        return resume.skip(store, binding, resume_id, body.reason)

    @app.post("/api/adapter/receipts")
    def adapter_receipts(binding: Adapter) -> list[dict[str, Any]]:
        """Read receipts for this session's messages since its last call, each reported once. Nothing
        wakes a session for them: the adapter asks when the session reads its messages."""
        return [
            {
                "message_id": _reply_id(r.message_id),
                "conversation_id": _reply_id(r.conversation_id),
                "recipient": r.recipient,
                "state": r.state,
                "read_at": utc_iso(r.read_at),
                "sent_at": utc_iso(r.sent_at),
            }
            for r in store.take_receipts(binding.id)
        ]

    @app.get("/api/adapter/conversations")
    def adapter_conversations(binding: Adapter) -> list[dict[str, Any]]:
        conversations = store.conversations_for(Actor.binding(binding.id))
        return [{**asdict(c), "id": _reply_id(c.id)} for c in conversations]

    @app.get("/api/adapter/conversations/{conversation_id}")
    def adapter_conversation(conversation_id: str, binding: Adapter) -> dict[str, Any]:
        internal_id = _stored_id(conversation_id)
        detail = _detail(Actor.binding(binding.id), internal_id)
        detail["id"] = _reply_id(internal_id)
        return detail

    @app.get("/api/adapter/messages/{message_id}")
    def adapter_message_record(message_id: str, binding: Adapter, request: Request) -> dict[str, Any]:
        record = store.message_record(Actor.binding(binding.id), _stored_id(message_id))
        body = asdict(record)
        body["message"].update(shown_envelope(store.envelope(record.message)))
        body["message"]["id"] = _reply_id(record.message.id)
        body["message"]["conversation_id"] = _reply_id(record.message.conversation_id)
        if record.message.parent_id is not None:
            body["message"]["parent_id"] = _reply_id(record.message.parent_id)
        for event in body["events"]:
            event["conversation_id"] = _reply_id(event["conversation_id"])
            event["message_id"] = _reply_id(event["message_id"])
        for view in body["deliveries"]:
            view.update(seen(view["delivery"]["binding_id"], request.app.state.hub))
        body["broker_started_at"] = started_at
        return body

    @app.get("/api/adapter/conversations/{conversation_id}/events")
    def adapter_events(
        conversation_id: str, binding: Adapter, after: int = 0, limit: int = 100
    ) -> list[dict[str, Any]]:
        reader = Actor.binding(binding.id)
        events = [
            _public_event(e)
            for e in store.events_after(reader, _stored_id(conversation_id), after, min(limit, 500))
        ]
        for event in events:
            event["conversation_id"] = _reply_id(event["conversation_id"])
            if event["message_id"] is not None:
                event["message_id"] = _reply_id(event["message_id"])
        return events

    app.state.readiness = readiness
    app.state.connector = connector
    if connector is not None:
        _mount_connector(app, connector)
    return app


def _form(raw: bytes) -> dict[str, str]:
    return {k: v[0] for k, v in urllib.parse.parse_qs(raw.decode("utf-8", "replace")).items() if v}


def _mount_connector(app: FastAPI, connector: Connector) -> None:
    """The ChatGPT connector's routes: the only ones the public port serves (PublicOnly in __main__)."""
    no_store = {"Cache-Control": "no-store"}

    @app.get("/.well-known/oauth-protected-resource")
    @app.get("/.well-known/oauth-protected-resource/mcp")
    def protected_resource() -> dict[str, Any]:
        return connector.protected_resource()

    @app.get("/.well-known/oauth-authorization-server")
    def authorization_server() -> dict[str, Any]:
        return connector.authorization_server()

    @app.post("/oauth/register", status_code=201)
    async def register_client(request: Request) -> JSONResponse:
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise Invalid("a JSON object is required")
            return JSONResponse(await run_in_threadpool(connector.register_client, body), 201, no_store)
        except (BrokerError, ValueError) as exc:
            return JSONResponse({"error": "invalid_redirect_uri", "error_description": str(exc)}, 400)

    @app.get("/oauth/authorize", response_class=HTMLResponse)
    def authorize_form(request: Request) -> HTMLResponse:
        params = dict(request.query_params)
        refused = connector.check_authorize(params)
        if refused:
            return HTMLResponse(f"<p>{html.escape(refused)}</p>", 400)
        return HTMLResponse(connector.authorize_page(params), headers=no_store)

    @app.post("/oauth/authorize", response_model=None)
    async def authorize(request: Request) -> HTMLResponse | RedirectResponse:
        form = _form(await request.body())
        code = form.pop("pairing_code", "")
        client = request.client.host if request.client else ""
        try:
            target = await run_in_threadpool(connector.approve, form, code, client)
        except BrokerError as exc:
            status = 403 if isinstance(exc, Forbidden) else 400
            if connector.check_authorize(form):
                return HTMLResponse(f"<p>{html.escape(str(exc))}</p>", status)
            return HTMLResponse(connector.authorize_page(form, str(exc)), status, no_store)
        return RedirectResponse(target, 302, no_store)

    @app.post("/oauth/token")
    async def token(request: Request) -> JSONResponse:
        form = _form(await request.body())
        try:
            return JSONResponse(await run_in_threadpool(connector.token, form), 200, no_store)
        except BrokerError as exc:
            return JSONResponse({"error": str(exc)}, 400, no_store)

    @app.post("/mcp", response_model=None)
    async def mcp(request: Request) -> JSONResponse | Response:
        try:
            await run_in_threadpool(connector.authenticate, request.headers.get("authorization", ""))
        except Unauthorized as exc:
            return JSONResponse(
                {"error": "invalid_token", "error_description": str(exc)},
                401,
                {"WWW-Authenticate": connector.challenge_header() + ', error="invalid_token"'},
            )
        try:
            message = await request.json()
        except ValueError:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}
            )
        if not isinstance(message, dict):
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "one request per POST"}}
            )
        answer = await run_in_threadpool(connector.rpc, message)
        if answer is None:
            return Response(status_code=202)
        return JSONResponse(answer)

    @app.get("/mcp")
    def mcp_stream() -> Response:
        return Response(status_code=405, headers={"Allow": "POST"})

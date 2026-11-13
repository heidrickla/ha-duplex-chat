from __future__ import annotations

import itertools
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from duplex_broker import resume, waker
from duplex_broker.api import Settings, create_app
from duplex_broker.store import Actor, Binding, Provider, Store, utc_iso

OWNER = Actor.user("owner")
TOKEN = "waker-secret"
START = 1_000_000.0
RESET = START + 600.0
AUTH = {"authorization": f"Bearer {TOKEN}"}
CODE = "pair-me"


class Clock:
    def __init__(self) -> None:
        self.t = START

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    # The broker reads liveness from time.time and the store from its injected clock; one clock drives both.
    monkeypatch.setattr(time, "time", c)
    return c


@pytest.fixture
def store(clock: Clock) -> Iterator[Store]:
    s = Store(":memory:", now=clock)
    yield s
    s.close()


@pytest.fixture
def client(store: Store, clock: Clock) -> Iterator[TestClient]:
    app = create_app(store, Settings(waker_token=TOKEN, pairing_code=CODE, enrollment_token="enroll-me"))
    with TestClient(app) as c:
        yield c


def bind(store: Store, title: str = "worker", provider: Provider = "claude") -> Binding:
    binding, _ = store.register_binding(provider, uuid.uuid4().hex, title, "D:/work", "e", {})
    return binding


def schedule(store: Store, clock: Clock, binding: Binding, run: str = "run-1") -> str:
    """Arm a usage-limit resume for the session and let its reset pass; returns the delivery id."""
    resume.record_limit(store, binding, run, "claude-transcript", "limit", START, RESET)
    clock.t = RESET
    assert resume.sweep(store) == 1
    (row,) = [r for r in resume.list_resumes(store) if r["run_id"] == run]
    (delivery,) = store.deliveries_for(row["message_id"])
    assert delivery.binding_id == binding.id and delivery.state == "queued"
    return delivery.id


def listed(client: TestClient) -> list[dict[str, Any]]:
    r = client.get("/api/waker/requests", headers=AUTH)
    assert r.status_code == 200, r.text
    body: list[dict[str, Any]] = r.json()
    return body


_report_numbers = itertools.count(1)


def start(
    client: TestClient,
    request_id: str,
    ok: bool,
    route: str = "claude-resume",
    detail: str = "",
    report_id: str | None = None,
) -> Any:
    """Each call is a new report unless it is given the report_id of an earlier one (a retry)."""
    if report_id is None:
        report_id = f"{request_id}:1700000000000:{next(_report_numbers)}"
    return client.post(
        f"/api/waker/requests/{request_id}/started",
        headers=AUTH,
        json={"report_id": report_id, "ok": ok, "route": route, "detail": detail},
    )


def notices(store: Store) -> list[str]:
    with store._read() as db:
        return [
            r["text"] for r in db.execute("SELECT text FROM messages WHERE client_message_id LIKE 'waker:%'")
        ]


def test_a_closed_session_with_a_scheduled_send_waiting_is_a_request(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store, "nightly")
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    (request,) = listed(client)
    assert request == {
        "id": delivery,
        "binding_id": worker.id,
        "provider": "claude",
        "native_id": worker.native_id,
        "title": "nightly",
        "workspace": "D:/work",
        "reason": "limit_resume",
        "message_id": resume.list_resumes(store)[0]["message_id"],
        "due_at": utc_iso(RESET),
    }
    assert request["message_id"] is not None


def test_a_codex_session_is_listed_and_a_non_agent_provider_is_not(
    store: Store, client: TestClient, clock: Clock
) -> None:
    codex = bind(store, "cx", "codex")
    schedule(store, clock, codex)
    schedule(store, clock, bind(store, "other", "mock"), run="run-2")
    clock.t = RESET + 200.0
    assert [r["provider"] for r in listed(client)] == ["codex"]


def test_a_session_that_polled_within_90_seconds_is_never_listed(
    store: Store, client: TestClient, clock: Clock
) -> None:
    registered = client.post(
        "/api/adapter/register",
        headers={"authorization": "Bearer enroll-me"},
        json={
            "provider": "claude",
            "native_id": uuid.uuid4().hex,
            "title": "polling",
            "workspace": "w",
            "endpoint_ref": "e",
            "capabilities": {"receive_messages": "verified"},
            "current_token": "",
        },
    )
    assert registered.status_code == 201, registered.text
    body = registered.json()
    worker = store.get_binding(body["binding"]["id"])
    adapter = {"authorization": f"Bearer {body['token']}"}
    clock.t = RESET - 30.0
    assert client.post("/api/adapter/claim", headers=adapter).content in (b"", b"null")
    schedule(store, clock, worker)
    assert clock.t == RESET and listed(client) == []
    clock.t = RESET + 59.0
    assert listed(client) == []
    clock.t = RESET + 61.0
    assert [r["binding_id"] for r in listed(client)] == [worker.id]


def test_an_open_stream_makes_a_session_live(store: Store, client: TestClient, clock: Clock) -> None:
    worker = bind(store)
    schedule(store, clock, worker)
    clock.t = RESET + 200.0
    hub = client.app.state.hub  # type: ignore[attr-defined]
    nudge = hub.watch(worker.id)
    assert listed(client) == []
    hub.unwatch(worker.id, nudge)
    assert len(listed(client)) == 1


def test_every_session_counts_as_live_for_the_first_90_seconds_after_a_restart(
    store: Store, clock: Clock
) -> None:
    worker = bind(store)
    schedule(store, clock, worker)
    clock.t = RESET + 200.0
    app = create_app(store, Settings(waker_token=TOKEN))
    with TestClient(app) as fresh:
        assert listed(fresh) == []
        clock.t += 89.0
        assert listed(fresh) == []
        clock.t += 2.0
        assert len(listed(fresh)) == 1


def test_a_delivery_that_is_not_a_broker_scheduled_send_is_never_listed(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    conversation = store.open_direct("owner", worker.id)
    store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "please look at this")
    clock.t = RESET + 200.0
    assert listed(client) == []
    # The same text from the owner is no scheduled send either, even beside a real one.
    schedule(store, clock, worker)
    clock.t += 200.0
    assert len(listed(client)) == 1


def test_a_retired_session_is_not_listed(store: Store, client: TestClient, clock: Clock) -> None:
    worker = bind(store)
    schedule(store, clock, worker)
    clock.t = RESET + 200.0
    store.retire_binding(OWNER, worker.id)
    assert listed(client) == []


def test_a_delivery_already_claimed_is_not_listed(store: Store, client: TestClient, clock: Clock) -> None:
    worker = bind(store)
    schedule(store, clock, worker)
    clock.t = RESET + 200.0
    with store._write() as db:
        db.execute("UPDATE deliveries SET state = 'accepted' WHERE binding_id = ?", (worker.id,))
    assert listed(client) == []


def test_a_start_keeps_a_request_off_the_list_for_15_minutes(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    r = start(client, delivery, True, "claude-resume", "started")
    assert r.status_code == 201
    assert r.json()["ok"] is True and r.json()["route"] == "claude-resume"
    clock.t += 899.0
    assert listed(client) == []
    clock.t += 2.0
    assert [x["id"] for x in listed(client)] == [delivery]


def test_the_third_failed_start_stops_the_requests_and_tells_the_owner_once(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store, "nightly")
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    for n in range(3):
        assert [x["id"] for x in listed(client)] == [delivery]
        assert notices(store) == []
        assert start(client, delivery, False, "codex-resume", f"exit {n}").status_code == 201
        clock.t += waker.RETRY_AFTER + 1.0
    assert listed(client) == []
    (notice,) = notices(store)
    assert "nightly" in notice and "3 times" in notice and "codex-resume" in notice and "exit 2" in notice
    # More reports for the same request do not say it again.
    assert start(client, delivery, False, "codex-resume", "exit 3").status_code == 201
    assert len(notices(store)) == 1
    clock.t += 10 * waker.RETRY_AFTER
    assert listed(client) == []


def test_a_successful_start_between_failures_does_not_hide_the_failure_count(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    for ok in (False, True, False):
        start(client, delivery, ok)
        clock.t += waker.RETRY_AFTER + 1.0
    assert [x["id"] for x in listed(client)] == [delivery]
    assert notices(store) == []
    start(client, delivery, False)
    assert len(notices(store)) == 1


def test_the_waker_routes_refuse_a_wrong_token_and_do_not_exist_without_one(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    for headers in ({}, {"authorization": "Bearer nope"}, {"authorization": TOKEN}):
        assert client.get("/api/waker/requests", headers=headers).status_code == 401
        refused = client.post(
            f"/api/waker/requests/{delivery}/started",
            headers=headers,
            json={"report_id": f"{delivery}:1:1", "ok": True, "route": "r"},
        )
        assert refused.status_code == 401
    assert waker.recent_starts(store) == []
    off = create_app(store, Settings())
    with TestClient(off) as c:
        assert c.get("/api/waker/requests", headers=AUTH).status_code == 404
        assert (
            c.post(
                f"/api/waker/requests/{delivery}/started",
                headers=AUTH,
                json={"report_id": f"{delivery}:1:1", "ok": True, "route": "r"},
            ).status_code
            == 404
        )


def test_a_start_for_an_unknown_or_unscheduled_delivery_is_refused(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    conversation = store.open_direct("owner", worker.id)
    ordinary = store.post_message(OWNER, conversation.id, uuid.uuid4().hex, "hello")
    (plain,) = ordinary.deliveries
    assert start(client, uuid.uuid4().hex, True).status_code == 404
    assert start(client, plain.id, True).status_code == 404
    assert start(client, plain.id, True, route="").status_code in (404, 422)
    assert waker.recent_starts(store) == []


def test_a_start_report_is_validated(store: Store, client: TestClient, clock: Clock) -> None:
    delivery = schedule(store, clock, bind(store))
    url = f"/api/waker/requests/{delivery}/started"
    good = {"report_id": f"{delivery}:1:1", "ok": True, "route": "r"}
    long = client.post(url, headers=AUTH, json={**good, "detail": "x" * 301})
    assert long.status_code == 422
    assert client.post(url, headers=AUTH, json={**good, "route": ""}).status_code == 422
    assert waker.recent_starts(store) == []


@pytest.mark.parametrize(
    "report_id",
    [None, "", f"{uuid.uuid4().hex}:1:1", "1:1", "{id}", "{id}:", "{id}x:1", "{id}:1 2", "{id}:" + "9" * 120],
)
def test_a_start_report_needs_a_report_id_that_begins_with_its_request(
    store: Store, client: TestClient, clock: Clock, report_id: str | None
) -> None:
    delivery = schedule(store, clock, bind(store))
    body: dict[str, Any] = {"ok": True, "route": "r"}
    if report_id is not None:
        body["report_id"] = report_id.replace("{id}", delivery)
    r = client.post(f"/api/waker/requests/{delivery}/started", headers=AUTH, json=body)
    assert r.status_code == 422
    assert waker.recent_starts(store) == []


def test_a_report_id_is_opaque_beyond_its_request_prefix(
    store: Store, client: TestClient, clock: Clock
) -> None:
    delivery = schedule(store, clock, bind(store))
    r = start(client, delivery, True, report_id=f"{delivery}:1731300000000:7-b")
    assert r.status_code == 201 and r.json()["report_id"] == f"{delivery}:1731300000000:7-b"


def test_a_report_sent_again_is_answered_as_taken_and_changes_nothing(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store, "nightly")
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    report = f"{delivery}:1731300000000:1"
    first = start(client, delivery, False, "codex-resume", "exit 1", report_id=report)
    assert first.status_code == 201
    # The broker took it and the waker lost the answer: it sends the same report until one arrives.
    for _ in range(5):
        clock.t += 1.0
        again = start(client, delivery, False, "codex-resume", "exit 1", report_id=report)
        assert again.status_code == 200 and again.json() == first.json()
    assert len(waker.recent_starts(store)) == 1
    # Five failed reports under one id are one failure: the owner is not told at the third, and a different
    # id counts.
    assert notices(store) == []
    for n in (2, 3):
        assert start(client, delivery, False, "codex-resume", f"exit {n}").status_code == 201
    assert len(notices(store)) == 1
    assert len(waker.recent_starts(store)) == 3


def test_a_failed_report_sent_again_does_not_return_the_delivery_a_second_time(
    store: Store, client: TestClient, clock: Clock
) -> None:
    native = uuid.uuid4().hex
    caps = {"receive_messages": "verified", "tasks": True}
    old, token = store.register_binding("claude", native, "worker", "D:/work", "e", caps)
    delivery = schedule(store, clock, old)
    reached(store, old, "accepted")
    assert start(client, delivery, True, "claude-resume", "started").status_code == 201
    report = f"{delivery}:1731300000000:2"
    assert start(client, delivery, False, "claude-resume", "killed", report_id=report).status_code == 201
    assert delivery_state(store, delivery) == "queued"
    new, _ = store.register_binding("claude", native, "worker", "D:/work", "e", caps, current_token=token)
    claimed = store.claim_next(new.id, new.lease_generation)
    assert claimed is not None and claimed[0].id == delivery
    # The retry of the same report must not take the delivery back from the session that now holds it.
    again = start(client, delivery, False, "claude-resume", "killed", report_id=report)
    assert again.status_code == 200
    assert delivery_state(store, delivery) == "dispatching"
    assert len(waker.recent_starts(store)) == 2


def test_the_owner_lists_recent_starts_newest_first(store: Store, client: TestClient, clock: Clock) -> None:
    worker = bind(store, "nightly")
    delivery = schedule(store, clock, worker)
    clock.t = RESET + 200.0
    start(client, delivery, False, "claude-resume", "first")
    clock.t += 5.0
    start(client, delivery, True, "claude-resume", "second")
    paired = client.post("/api/pair", json={"code": CODE})
    assert paired.status_code == 200
    r = client.get("/api/waker/starts")
    assert r.status_code == 200
    rows = r.json()
    assert [(x["detail"], x["ok"], x["title"]) for x in rows] == [
        ("second", True, "nightly"),
        ("first", False, "nightly"),
    ]
    assert rows[0]["request_id"] == delivery and rows[0]["binding_id"] == worker.id
    fresh = TestClient(client.app)
    assert fresh.get("/api/waker/starts").status_code == 401


# A run the waker started and then killed never leaves its delivery claimed, read or answered.


def reached(store: Store, binding: Binding, state: str) -> None:
    """What the waker's `claude -p` run did with the delivery before it was killed."""
    claimed = store.claim_next(binding.id, binding.lease_generation)
    assert claimed is not None
    for step in ("accepted", "consumed")[: ("dispatching", "accepted", "consumed").index(state)]:
        store.transition(claimed[0].id, binding.id, binding.lease_generation, step)  # type: ignore[arg-type]


def delivery_state(store: Store, delivery_id: str) -> str:
    with store._read() as db:
        return str(db.execute("SELECT state FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()[0])


@pytest.mark.parametrize("order", ["report-then-register", "register-then-report"])
@pytest.mark.parametrize("state", ["dispatching", "accepted", "consumed"])
def test_a_killed_run_leaves_the_delivery_for_the_session_the_desktop_opens(
    store: Store, client: TestClient, clock: Clock, state: str, order: str
) -> None:
    native = uuid.uuid4().hex
    caps = {"receive_messages": "verified", "tasks": True}
    old, token = store.register_binding("claude", native, "worker", "D:/work", "e", caps)
    delivery = schedule(store, clock, old)
    reached(store, old, state)
    assert start(client, delivery, True, "claude-resume", "started").status_code == 201

    def report() -> None:
        killed = start(
            client, delivery, False, "claude-resume", "the session was opened while the waker ran it"
        )
        assert killed.status_code == 201

    def register() -> Binding:
        new, _ = store.register_binding("claude", native, "worker", "D:/work", "e", caps, current_token=token)
        return new

    if order == "report-then-register":
        report()
        new = register()
    else:
        new = register()
        report()
    assert new.lease_generation == old.lease_generation + 1
    assert delivery_state(store, delivery) == "queued"
    again = store.claim_next(new.id, new.lease_generation)
    assert again is not None and again[0].id == delivery and again[1].text == "Continue where you left off."


def test_a_failed_report_leaves_an_answered_or_untouched_delivery_alone(
    store: Store, client: TestClient, clock: Clock
) -> None:
    answered = bind(store, "answered")
    done = schedule(store, clock, answered)
    reached(store, answered, "consumed")
    store.transition(done, answered.id, answered.lease_generation, "responded")
    waiting = bind(store, "waiting")
    queued = schedule(store, clock, waiting, run="run-2")
    clock.t += 200.0
    assert start(client, done, False).status_code == 201
    assert start(client, queued, False).status_code == 201
    assert delivery_state(store, done) == "responded"
    assert delivery_state(store, queued) == "queued"


def test_a_successful_report_does_not_return_a_delivery_in_flight(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    reached(store, worker, "accepted")
    assert start(client, delivery, True).status_code == 201
    assert delivery_state(store, delivery) == "accepted"


def test_a_delivery_closed_unread_by_the_ladder_is_not_resurrected_by_a_failed_report(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    reached(store, worker, "accepted")
    with store._write() as db:
        store._set_state(db, delivery, "uncertain", None, Actor.binding(worker.id), reason="never read")
    assert start(client, delivery, False).status_code == 201
    assert delivery_state(store, delivery) == "uncertain"


@pytest.mark.parametrize("state", ["dispatching", "accepted", "consumed"])
def test_a_skip_report_never_returns_a_delivery_the_live_session_holds(
    store: Store, client: TestClient, clock: Clock, state: str
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    reached(store, worker, state)
    # The waker found the session live and started nothing: no start came before this report.
    skipped = start(client, delivery, False, "skipped", "the session is live")
    assert skipped.status_code == 201
    assert delivery_state(store, delivery) == state
    assert store.claim_next(worker.id, worker.lease_generation) is None


def test_a_skip_after_an_earlier_killed_run_returns_nothing_either(
    store: Store, client: TestClient, clock: Clock
) -> None:
    worker = bind(store)
    delivery = schedule(store, clock, worker)
    assert start(client, delivery, True).status_code == 201
    assert (
        start(client, delivery, False, detail="the session was opened while the waker ran it").status_code
        == 201
    )
    assert delivery_state(store, delivery) == "queued"
    reached(store, worker, "consumed")
    # The live session read it afterwards; a later skip is not that run ending.
    assert start(client, delivery, False, "skipped", "the session is live").status_code == 201
    assert delivery_state(store, delivery) == "consumed"

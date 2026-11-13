from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from duplex_broker.__main__ import PublicOnly
from duplex_broker.api import Settings, create_app
from duplex_broker.connector import EVENT, Connector, sign
from duplex_broker.ids import reply_id
from duplex_broker.store import Actor, Store

CODE = "pair-me"
BASE = "https://duplex-mcp.example.com"
CLIENT = "https://chatgpt.com/oauth/client.json"
REDIRECT = "https://chatgpt.com/connector_platform_oauth_redirect"
CALLBACK = "https://hooks.chatgpt.example/mcp-events/cb1"
SECRET = "whsec_" + base64.b64encode(b"k" * 32).decode()
PUBLIC = {"hooks.chatgpt.example": ["104.18.32.47"], "lan.example": ["10.0.0.10"]}


class Hooks:
    """ChatGPT's webhook receiver: answers challenges, records events, can be told to fail."""

    def __init__(self) -> None:
        self.events: list[tuple[dict[str, str], bytes]] = []
        self.status = 200
        self.echo = True

    def challenge(self, url: str, headers: dict[str, str], body: bytes) -> tuple[int, bytes]:
        sent = json.loads(body)
        answer = sent["challenge"] if self.echo else "wrong"
        return 200, json.dumps({"challenge": answer}).encode()

    def send(self, url: str, headers: dict[str, str], body: bytes) -> int:
        self.events.append((headers, body))
        return self.status


@pytest.fixture
def hooks() -> Hooks:
    return Hooks()


@pytest.fixture
def client(hooks: Hooks) -> Iterator[TestClient]:
    store = Store(":memory:")
    settings = Settings(
        pairing_code=CODE,
        connector_url=BASE,
        connector_send=hooks.send,
        connector_challenge=hooks.challenge,
        connector_resolve=lambda host: PUBLIC[host],
    )
    app = create_app(store, settings)
    with TestClient(app) as c:
        connector: Connector = app.state.connector
        connector.retries = (0.0, 0.0)
        yield c
    store.close()


def pkce() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(uuid.uuid4().bytes * 2).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


def authorize_params(challenge: str, client_id: str = CLIENT, redirect: str = REDIRECT) -> dict[str, str]:
    return {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect,
        "state": "st-1",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "resource": f"{BASE}/mcp",
        "scope": "duplex",
    }


def link(client: TestClient, client_id: str = CLIENT) -> dict[str, Any]:
    verifier, challenge = pkce()
    params = authorize_params(challenge, client_id)
    r = client.post("/oauth/authorize", data={**params, "pairing_code": CODE}, follow_redirects=False)
    assert r.status_code == 302, r.text
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(r.headers["location"]).query)
    tokens = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": query["code"][0],
            "redirect_uri": REDIRECT,
            "client_id": client_id,
            "code_verifier": verifier,
            "resource": f"{BASE}/mcp",
        },
    )
    assert tokens.status_code == 200, tokens.text
    return dict(tokens.json())


def rpc(client: TestClient, token: str, method: str, params: dict[str, Any] | None = None) -> Any:
    r = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
        headers={"authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200, r.text
    return r.json()


def call(client: TestClient, token: str, tool: str, **arguments: Any) -> dict[str, Any]:
    result = rpc(client, token, "tools/call", {"name": tool, "arguments": arguments})["result"]
    assert result["isError"] is False, result
    return dict(result["structuredContent"])


def subscribe(client: TestClient, token: str, url: str = CALLBACK, secret: str = SECRET) -> Any:
    return rpc(
        client,
        token,
        "events/subscribe",
        {"name": EVENT, "arguments": {}, "delivery": {"mode": "webhook", "url": url, "secret": secret}},
    )


def owner_dm_to_chatgpt(client: TestClient, text: str) -> tuple[str, str]:
    pair = client.post("/api/pair", json={"code": CODE})
    csrf = {"x-csrf-token": pair.json()["csrf"]}
    chatgpt = next(b for b in client.get("/api/sessions", headers=csrf).json() if b["provider"] == "chatgpt")
    dm = client.post("/api/direct-conversations", json={"binding_id": chatgpt["id"]}, headers=csrf).json()
    sent = client.post(
        f"/api/conversations/{dm['id']}/messages",
        json={"client_message_id": uuid.uuid4().hex, "text": text},
        headers=csrf,
    )
    assert sent.status_code == 201, sent.text
    return dm["id"], sent.json()["message"]["id"]


def until(check: Any, seconds: float = 5.0) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


# OAuth


def test_the_metadata_points_chatgpt_at_this_origin(client: TestClient) -> None:
    resource = client.get("/.well-known/oauth-protected-resource").json()
    assert resource["resource"] == f"{BASE}/mcp"
    assert resource["authorization_servers"] == [BASE]
    server = client.get("/.well-known/oauth-authorization-server").json()
    assert server["issuer"] == BASE
    assert server["code_challenge_methods_supported"] == ["S256"]
    assert server["client_id_metadata_document_supported"] is True
    assert server["authorization_response_iss_parameter_supported"] is True


def test_mcp_without_a_token_points_at_the_metadata(client: TestClient) -> None:
    r = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "ping"})
    assert r.status_code == 401
    assert f'resource_metadata="{BASE}/.well-known/oauth-protected-resource"' in r.headers["www-authenticate"]
    r = client.post(
        "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers={"authorization": "Bearer x"}
    )
    assert r.status_code == 401


def test_linking_needs_the_pairing_code_and_returns_the_issuer(client: TestClient) -> None:
    _verifier, challenge = pkce()
    params = authorize_params(challenge)
    page = client.get("/oauth/authorize", params=params)
    assert page.status_code == 200 and "pairing code" in page.text
    wrong = client.post("/oauth/authorize", data={**params, "pairing_code": "nope"}, follow_redirects=False)
    assert wrong.status_code == 403 and "wrong pairing code" in wrong.text
    right = client.post("/oauth/authorize", data={**params, "pairing_code": CODE}, follow_redirects=False)
    assert right.status_code == 302
    location = urllib.parse.urlsplit(right.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == REDIRECT
    query = urllib.parse.parse_qs(location.query)
    assert query["state"] == ["st-1"] and query["iss"] == [BASE] and query["code"]


@pytest.mark.parametrize(
    ("client_id", "redirect"),
    [
        ("https://evil.example/client.json", REDIRECT),
        (CLIENT, "https://evil.example/callback"),
        (CLIENT, "https://chatgpt.com.evil.example/connector_platform_oauth_redirect"),
    ],
)
def test_only_chatgpt_can_link(client: TestClient, client_id: str, redirect: str) -> None:
    _verifier, challenge = pkce()
    params = authorize_params(challenge, client_id, redirect)
    assert client.get("/oauth/authorize", params=params).status_code == 400
    r = client.post("/oauth/authorize", data={**params, "pairing_code": CODE}, follow_redirects=False)
    assert r.status_code == 400 and "location" not in r.headers


def test_a_code_needs_its_verifier_and_works_once(client: TestClient) -> None:
    verifier, challenge = pkce()
    params = authorize_params(challenge)
    r = client.post("/oauth/authorize", data={**params, "pairing_code": CODE}, follow_redirects=False)
    code = urllib.parse.parse_qs(urllib.parse.urlsplit(r.headers["location"]).query)["code"][0]
    exchange = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT,
        "client_id": CLIENT,
        "code_verifier": "not-the-verifier",
    }
    assert client.post("/oauth/token", data=exchange).json() == {"error": "invalid_grant"}
    # A failed exchange spends the code too.
    assert client.post("/oauth/token", data={**exchange, "code_verifier": verifier}).status_code == 400


def test_a_refresh_token_rotates(client: TestClient) -> None:
    tokens = link(client)
    again = client.post(
        "/oauth/token", data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"]}
    )
    assert again.status_code == 200
    fresh = again.json()
    assert fresh["access_token"] != tokens["access_token"]
    reuse = client.post(
        "/oauth/token", data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"]}
    )
    assert reuse.json() == {"error": "invalid_grant"}
    assert rpc(client, fresh["access_token"], "ping")["result"] == {}


def test_dynamic_registration_accepts_only_chatgpt_redirects(client: TestClient) -> None:
    bad = client.post("/oauth/register", json={"redirect_uris": ["https://evil.example/cb"]})
    assert bad.status_code == 400
    good = client.post("/oauth/register", json={"redirect_uris": [REDIRECT]})
    assert good.status_code == 201
    assert link(client, good.json()["client_id"])["token_type"] == "Bearer"


# MCP and events


def test_the_server_offers_tools_and_one_event(client: TestClient) -> None:
    token = link(client)["access_token"]
    init = rpc(client, token, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}})["result"]
    assert init["capabilities"] == {"tools": {}, "events": {}}
    names = {t["name"] for t in rpc(client, token, "tools/list")["result"]["tools"]}
    assert names == {
        "inbox",
        "read_message",
        "acknowledge",
        "reply",
        "receipt",
        "conversations",
        "history",
        "sessions",
        "guide",
    }
    events = rpc(client, token, "events/list")["result"]["events"]
    assert [e["name"] for e in events] == [EVENT] and events[0]["delivery"] == ["webhook"]
    r = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        headers={"authorization": f"Bearer {token}"},
    )
    assert r.status_code == 202


@pytest.mark.parametrize(
    ("url", "secret", "code"),
    [
        ("http://hooks.chatgpt.example/cb", SECRET, -32602),
        ("https://lan.example/cb", SECRET, -32602),
        (CALLBACK, "plain-secret", -32602),
        (CALLBACK, "whsec_" + base64.b64encode(b"short").decode(), -32602),
    ],
)
def test_a_subscription_needs_a_public_https_callback_and_a_real_secret(
    client: TestClient, url: str, secret: str, code: int
) -> None:
    token = link(client)["access_token"]
    assert subscribe(client, token, url, secret)["error"]["code"] == code


def test_a_callback_that_fails_the_challenge_is_refused(client: TestClient, hooks: Hooks) -> None:
    hooks.echo = False
    token = link(client)["access_token"]
    error = subscribe(client, token)["error"]
    assert error["code"] == -32015 and error["data"] == {"reason": "challenge_failed"}


def test_a_message_reaches_chatgpt_as_a_signed_id_only_event_and_is_read_by_acknowledgement(
    client: TestClient, hooks: Hooks
) -> None:
    token = link(client)["access_token"]
    sub = subscribe(client, token)["result"]
    assert sub["id"] and sub["cursor"] is None and sub["truncated"] is False
    conversation, message = owner_dm_to_chatgpt(client, "SECRET-TEXT are you there")
    until(lambda: hooks.events)
    headers, body = hooks.events[0]
    event = json.loads(body)
    assert event["name"] == EVENT and "SECRET-TEXT" not in body.decode()
    assert event["data"]["message_id"].replace("-", "") == message
    assert set(event["data"]) == {"message_id", "conversation_id", "attempt"}
    assert headers["webhook-id"] == event["eventId"] and headers["X-MCP-Subscription-Id"] == sub["id"]
    stamp = int(headers["webhook-timestamp"])
    key = base64.b64decode(SECRET.removeprefix("whsec_"))
    expected = base64.b64encode(
        hmac.new(key, f"{event['eventId']}.{stamp}.".encode() + body, "sha256").digest()
    )
    assert headers["webhook-signature"] == "v1," + expected.decode()

    assert call(client, token, "inbox")["unread"][0]["your_delivery_state"] == "accepted"
    read = call(client, token, "read_message", message_id=event["data"]["message_id"])
    assert read["text"] == "SECRET-TEXT are you there" and read["your_delivery_state"] == "accepted"
    assert call(client, token, "acknowledge", message_id=message)["your_delivery_state"] == "consumed"
    answer = call(client, token, "reply", conversation_id=conversation, text="here", parent_id=message)
    assert answer["message_id"]
    # Only the owner is in this conversation: no session gets it, and the note says he sees it.
    assert answer["delivered_to"] == [] and "Owner sees every message" in answer["note"]
    states = {d["to"]: d["state"] for d in call(client, token, "receipt", message_id=message)["deliveries"]}
    assert states == {"ChatGPT": "responded"}
    texts = [m["text"] for m in call(client, token, "history", conversation_id=conversation)["messages"]]
    assert texts == ["SECRET-TEXT are you there", "here"]


def test_without_a_subscription_a_message_waits_queued_until_chatgpt_subscribes(
    client: TestClient, hooks: Hooks
) -> None:
    token = link(client)["access_token"]
    _conversation, message = owner_dm_to_chatgpt(client, "early")
    time.sleep(0.3)
    assert hooks.events == []
    assert call(client, token, "inbox")["queued_behind"] == 1
    subscribe(client, token)
    until(lambda: hooks.events)
    assert json.loads(hooks.events[0][1])["data"]["message_id"].replace("-", "") == message


def test_an_event_chatgpt_never_takes_fails_the_delivery_with_a_reason(
    client: TestClient, hooks: Hooks
) -> None:
    token = link(client)["access_token"]
    subscribe(client, token)
    hooks.status = 503
    _conversation, message = owner_dm_to_chatgpt(client, "nobody home")
    until(lambda: call(client, token, "receipt", message_id=message)["deliveries"][0]["state"] == "failed")
    assert len(hooks.events) == 2  # every retry, then the reason
    reason = call(client, token, "receipt", message_id=message)["deliveries"][0]["reason"]
    assert "ChatGPT accepted no event" in reason


def test_a_gone_subscription_is_dropped(client: TestClient, hooks: Hooks) -> None:
    token = link(client)["access_token"]
    subscribe(client, token)
    hooks.status = 410
    owner_dm_to_chatgpt(client, "gone")
    until(lambda: hooks.events)
    connector: Connector = client.app.state.connector  # type: ignore[attr-defined]
    until(lambda: connector.live_subscriptions() == [])
    assert len(hooks.events) == 1


def test_the_standard_webhooks_vector() -> None:
    body = b'{"test": 2432232314}'
    signature = sign(
        "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", "msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330, body
    )
    assert signature == "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="


# The public port


def test_the_public_port_serves_only_the_connector(client: TestClient) -> None:
    public = TestClient(PublicOnly(client.app))
    assert public.get("/.well-known/oauth-authorization-server").status_code == 200
    assert public.post("/mcp", json={}).status_code == 401
    for path in ("/api/status", "/health", "/", "/api/pair", "/api/adapter/claim"):
        assert public.get(path).status_code == 404, path
    with pytest.raises(WebSocketDisconnect) as closed, public.websocket_connect("/api/adapter/stream"):
        pass
    assert closed.value.code == 4404


def test_refusals_are_kept_for_the_owner_with_their_reason(client: TestClient, hooks: Hooks) -> None:
    token = link(client)["access_token"]
    assert "error" in rpc(client, token, "no/such")
    hooks.echo = False
    assert subscribe(client, token)["error"]["code"] == -32015
    refused = rpc(client, token, "tools/call", {"name": "read_message", "arguments": {}})["result"]
    assert refused["isError"] is True
    pair = client.post("/api/pair", json={"code": CODE})
    rows = client.get("/api/refusals", headers={"x-csrf-token": pair.json()["csrf"]}).json()
    seen = {(r["source"], r["what"]) for r in rows}
    assert ("mcp", "no/such") in seen and ("tool", "read_message") in seen
    verification = next(r for r in rows if r["source"] == "subscribe")
    assert verification["what"] == "hooks.chatgpt.example"
    assert "JSON keys ['challenge']" in verification["reason"]
    assert SECRET not in json.dumps(rows)


def test_the_owner_sees_each_renewal_and_the_last_event_answered(client: TestClient, hooks: Hooks) -> None:
    token = link(client)["access_token"]
    first = subscribe(client, token)["result"]
    pair = client.post("/api/pair", json={"code": CODE})
    csrf = {"x-csrf-token": pair.json()["csrf"]}
    [row] = client.get("/api/subscriptions", headers=csrf).json()
    assert row["id"] == first["id"] and row["renewals"] == 0 and row["renewed_at"] is None
    assert row["last_event_id"] is None and row["host"] == "hooks.chatgpt.example" and "url" not in row
    assert subscribe(client, token)["result"]["id"] == first["id"]
    owner_dm_to_chatgpt(client, "wake")
    until(lambda: hooks.events)
    event_id = json.loads(hooks.events[0][1])["eventId"]
    until(lambda: client.get("/api/subscriptions", headers=csrf).json()[0]["last_event_id"] == event_id)
    [row] = client.get("/api/subscriptions", headers=csrf).json()
    assert row["renewals"] == 1 and row["renewed_at"] >= row["created_at"] and row["last_status"] == 200
    assert SECRET not in json.dumps(row) and CALLBACK not in json.dumps(row)


def test_subscriptions_without_owner_auth_are_refused(client: TestClient) -> None:
    assert client.get("/api/subscriptions").status_code == 401


def test_refusals_without_owner_auth_are_refused(client: TestClient) -> None:
    assert client.get("/api/refusals").status_code == 401


def test_chatgpt_addresses_a_group_session_by_its_title_or_displayed_id(client: TestClient) -> None:
    token = link(client)["access_token"]
    connector: Connector = client.app.state.connector  # type: ignore[attr-defined]
    store = connector.store
    claude, _ = store.register_binding(
        "claude", "native-1", "SSLReverseProxy", "ws", "pipe", {"rewake": "verified"}, current_token=None
    )
    owner = Actor.user("owner")
    group = store.create_group("owner", "ChatGPT connector tests")
    for member in (claude.id, connector.binding().id):
        store.add_member(owner, group.id, member)
    store.start_collaboration(owner, group.id, "find bugs", 4, 10)
    for name in ("SSLReverseProxy", "sslreverseproxy", reply_id(claude.id)):
        sent = call(
            client, token, "reply", conversation_id=reply_id(group.id), text=f"to {name}", recipients=[name]
        )
        assert [d["title"] for d in sent["delivered_to"]] == ["SSLReverseProxy"]
    refused = rpc(
        client,
        token,
        "tools/call",
        {"name": "reply", "arguments": {"conversation_id": group.id, "text": "x", "recipients": ["Nobody"]}},
    )["result"]
    assert refused["isError"] and "its sessions: SSLReverseProxy" in refused["content"][0]["text"]


def test_chatgpt_sees_sessions_without_their_workspace_and_reads_the_guide(client: TestClient) -> None:
    token = link(client)["access_token"]
    connector: Connector = client.app.state.connector  # type: ignore[attr-defined]
    connector.store.register_binding(
        "codex", "thread-1", "codex-repo", "D:\\secret-workspace", "pipe", {}, current_token=None
    )
    listed = call(client, token, "sessions")
    titles = {s["title"]: s["provider"] for s in listed["sessions"]}
    assert titles["codex-repo"] == "codex" and titles["ChatGPT"] == "chatgpt"
    assert "secret-workspace" not in json.dumps(listed)
    guide = call(client, token, "guide")
    assert "Verdict:" in json.dumps(guide) and set(guide) >= {"who", "authority", "brief", "privacy"}


def test_a_webhook_is_retried_once_two_seconds_on(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store(":memory:")
    hooks = Hooks()
    hooks.status = 503
    connector = Connector(store, BASE, "owner", lambda c, a: None, lambda b: None, lambda b: {}, hooks.send)
    slept: list[float] = []

    async def sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr("duplex_broker.connector.asyncio.sleep", sleep)
    sub = {"id": "sub_1", "url": CALLBACK, "secret": SECRET}
    assert asyncio.run(connector._post(sub, {"eventId": "evt_1"})) is False
    assert (len(hooks.events), slept) == (2, [2.0])
    store.close()


def group_with_chatgpt(client: TestClient) -> tuple[Store, Any, Any, Any]:
    connector: Connector = client.app.state.connector  # type: ignore[attr-defined]
    store = connector.store
    claude, _ = store.register_binding(
        "claude", "native-1", "SSLReverseProxy", "ws", "pipe", {"rewake": "verified"}, current_token=None
    )
    owner = Actor.user("owner")
    group = store.create_group("owner", "ChatGPT connector tests")
    for member in (claude.id, connector.binding().id):
        store.add_member(owner, group.id, member)
    store.start_collaboration(owner, group.id, "find bugs", 4, 10)
    return store, connector, claude, group


def test_chatgpt_sees_each_recipient_and_gets_one_receipt_per_read(client: TestClient) -> None:
    token = link(client)["access_token"]
    store, connector, claude, group = group_with_chatgpt(client)
    sent = call(
        client,
        token,
        "reply",
        conversation_id=reply_id(group.id),
        text="look",
        recipients=["SSLReverseProxy"],
    )
    read = call(client, token, "read_message", message_id=sent["message_id"])
    assert read["to"] == [{"kind": "binding", "id": claude.id, "title": "SSLReverseProxy"}]
    assert read["from"]["title"] == "ChatGPT" and read["sent_at"].endswith("Z")
    assert call(client, token, "inbox")["receipts"] == []
    claimed = store.claim_next(claude.id, claude.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, claude.lease_generation, "accepted")
    store.transition(claimed[0].id, claude.id, claude.lease_generation, "consumed")
    (receipt,) = call(client, token, "inbox")["receipts"]
    assert (receipt["message_id"], receipt["recipient"], receipt["state"]) == (
        sent["message_id"],
        "SSLReverseProxy",
        "consumed",
    )
    assert call(client, token, "inbox")["receipts"] == []
    history = call(client, token, "history", conversation_id=reply_id(group.id))["messages"]
    assert history[-1]["to"] == read["to"] and history[-1]["sent_at"] == read["sent_at"]
    assert connector.binding().id != claude.id


def test_a_failed_delivery_from_chatgpt_reaches_owner_as_a_broker_notice(
    client: TestClient, hooks: Hooks
) -> None:
    token = link(client)["access_token"]
    subscribe(client, token)
    store, _connector, claude, group = group_with_chatgpt(client)
    sent = call(
        client,
        token,
        "reply",
        conversation_id=reply_id(group.id),
        text="look",
        recipients=["SSLReverseProxy"],
    )
    claimed = store.claim_next(claude.id, claude.lease_generation)
    assert claimed is not None
    store.transition(claimed[0].id, claude.id, claude.lease_generation, "failed", None, "wake failed: gone")
    until(lambda: hooks.events)
    notice_id = json.loads(hooks.events[-1][1])["data"]["message_id"]
    read = call(client, token, "read_message", message_id=notice_id)
    assert read["from"]["title"] == "Duplex broker"
    assert (read["notice"]["message_id"], read["notice"]["state"]) == (sent["message_id"], "failed")
    assert read["notice"]["recipient"]["title"] == "SSLReverseProxy"


def test_an_event_chatgpt_never_takes_is_a_failure_notice_to_owner(client: TestClient, hooks: Hooks) -> None:
    token = link(client)["access_token"]
    subscribe(client, token)
    hooks.status = 503
    _conversation, message = owner_dm_to_chatgpt(client, "nobody home")
    until(lambda: call(client, token, "receipt", message_id=message)["deliveries"][0]["state"] == "failed")
    connector: Connector = client.app.state.connector  # type: ignore[attr-defined]
    owner = Actor.user("owner")
    notices = next(c for c in connector.store.conversations_for(owner) if c.title == "Duplex broker")
    (posted,) = [e for e in connector.store.events_after(owner, notices.id) if e.type == "message.accepted"]
    assert posted.payload["notice"]["message_id"] == message
    assert "ChatGPT accepted no event" in posted.payload["notice"]["reason"]

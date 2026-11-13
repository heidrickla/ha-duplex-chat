from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import duplex_broker.store as store_module
from duplex_broker.__main__ import services_from, settings_from_env
from duplex_broker.api import Settings, _read_text, create_app
from duplex_broker.connector import GUIDE, INSTRUCTIONS, TOOLS, Connector
from duplex_broker.model_member import SYSTEM_PROMPT, ModelConfig
from duplex_broker.store import Store

CODE = "pair-me"


def test_a_broker_with_no_options_starts_with_every_optional_member_off() -> None:
    store = Store(":memory:")
    with TestClient(create_app(store, Settings(pairing_code=CODE))) as client:
        client.post("/api/pair", json={"code": CODE})
        assert client.get("/api/sessions").json() == []
        doc = client.get("/api/status").json()
        assert doc["model"]["enabled"] is False and doc["model"]["actions"] is False
        assert client.get("/api/service/anything/sessions").status_code == 401
    store.close()


def test_the_environment_defaults_are_generic_and_actions_are_off(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [k for k in __import__("os").environ if k.startswith("DUPLEX_")]:
        monkeypatch.delenv(name)
    settings = settings_from_env()
    assert (settings.owner, settings.owner_name) == ("owner", "Owner")
    assert (settings.connector_title, settings.model_member_title) == ("ChatGPT", "Assistant")
    assert settings.model_url == "" and settings.model_actions is False and settings.services == {}
    assert settings.model_no_thinking is False
    monkeypatch.setenv("DUPLEX_MODEL_ACTIONS", "true")
    monkeypatch.setenv("DUPLEX_MODEL_NO_THINKING", "true")
    monkeypatch.setenv("DUPLEX_OWNER_NAME", "")
    again = settings_from_env()
    assert again.model_actions is True and again.owner_name == "Owner" and again.model_no_thinking is True


def test_registration_tells_an_adapter_the_owners_name() -> None:
    store = Store(":memory:")
    settings = Settings(enrollment_token="enroll", owner_name="Sam")
    body = {
        "provider": "claude",
        "native_id": "n",
        "title": "worker",
        "workspace": "w",
        "endpoint_ref": "e",
        "capabilities": {},
    }
    with TestClient(create_app(store, settings)) as client:
        registered = client.post(
            "/api/adapter/register", headers={"authorization": "Bearer enroll"}, json=body
        )
        assert registered.status_code == 201
        assert registered.json()["owner_name"] == "Sam"
    store.close()


def test_services_come_from_a_json_list_and_empty_entries_are_skipped() -> None:
    raw = json.dumps(
        [{"name": "job-queue", "token": "a"}, {"name": "", "token": ""}, {"name": "ci", "token": "b"}]
    )
    assert services_from(raw) == {"job-queue": "a", "ci": "b"}
    assert services_from("") == {} and services_from("[]") == {}
    with pytest.raises(SystemExit):
        services_from(json.dumps([{"name": "Bad Name", "token": "x"}]))


def test_each_service_has_its_own_token_routes_and_prefix() -> None:
    store = Store(":memory:")
    settings = Settings(enrollment_token="enroll", services={"job-queue": "one", "ci": "two"})
    with TestClient(create_app(store, settings)) as client:
        registered = client.post(
            "/api/adapter/register",
            headers={"authorization": "Bearer enroll"},
            json={
                "provider": "claude",
                "native_id": "n",
                "title": "worker",
                "workspace": "w",
                "endpoint_ref": "e",
                "capabilities": {},
            },
        ).json()
        binding = registered["binding"]["id"]
        body = {"binding_id": binding, "client_message_id": "c1", "text": "ok"}
        assert (
            client.post(
                "/api/service/ci/notify", json=body, headers={"authorization": "Bearer one"}
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/api/service/other/notify", json=body, headers={"authorization": "Bearer two"}
            ).status_code
            == 401
        )
        sent = client.post("/api/service/ci/notify", json=body, headers={"authorization": "Bearer two"})
        assert sent.status_code == 201
        claimed = client.post(
            "/api/adapter/claim", headers={"authorization": f"Bearer {registered['token']}"}
        )
        assert claimed.json()["message"]["text"] == "[ci status data] ok"
        assert claimed.json()["message"]["actor"] == {"kind": "user", "id": "ci"}
        listed = client.get("/api/service/job-queue/sessions", headers={"authorization": "Bearer one"})
        assert [s["title"] for s in listed.json()] == ["worker"]
    store.close()


def test_a_service_cannot_take_the_owners_id() -> None:
    store = Store(":memory:", owner="owner")
    with pytest.raises(ValueError, match="owner's id"):
        create_app(store, Settings(services={"owner": "t"}))
    store.close()


def test_the_default_texts_fill_in_the_titles_and_the_local_guide_is_appended(tmp_path: Path) -> None:
    assert sorted(GUIDE["who"]) == ["Claude", "Codex", "{member}", "{owner}"]
    store = Store(":memory:")
    store.connector_title = "Relay"
    connector = Connector(
        store, "https://x.test", "owner", lambda c, a: None, lambda b: None, lambda b: {}, owner_name="Sam"
    )
    guide = connector.guide()
    assert "Relay" in guide["who"] and "Sam" in guide["who"] and "local" not in guide
    filled = json.dumps(connector._fill([GUIDE, INSTRUCTIONS, TOOLS]))
    assert "{member}" not in filled and "{owner}" not in filled and "Relay" in filled
    (tmp_path / "connector-guide.md").write_text("Local rules.\n", encoding="utf-8")
    connector.guide_file = tmp_path / "connector-guide.md"
    assert connector.guide()["local"] == "Local rules."
    connector.guide_file = tmp_path / "missing.md"
    assert "local" not in connector.guide()
    store.close()


def test_the_model_prompt_names_the_member_and_appends_the_local_file(tmp_path: Path) -> None:
    (tmp_path / "model-prompt.md").write_text("Local note.\n", encoding="utf-8")
    config = ModelConfig(
        "http://x", title="Sage", owner_name="Sam", extra_prompt=_read_text(tmp_path, "model-prompt.md")
    )
    system = config.system(SYSTEM_PROMPT)
    assert system.startswith("You are Sage, an assistant in a chat with Sam.")
    assert system.endswith("\n\nLocal note.")
    assert _read_text(tmp_path, "absent.md") == "" and _read_text(None, "x") == ""


def test_an_upgrade_moves_the_legacy_member_native_ids(tmp_path: Path) -> None:
    path = tmp_path / "old.db"
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(store_module, "SCHEMA_VERSION", 12)
    Store(path, owner="owner").close()
    monkeypatch.undo()
    raw = sqlite3.connect(path, isolation_level=None)
    legacy = (
        ("m", store_module.LEGACY_MODEL, store_module.LEGACY_MODEL),
        ("c", "chatgpt", store_module.LEGACY_CONNECTOR_NATIVE),
    )
    for ident, provider, native in legacy:
        raw.execute(
            "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref,"
            " capabilities, lease_generation, token_sha, active, heartbeat_at, created_at)"
            " VALUES (?, ?, ?, 'kept', 'w', 'e', '{}', 1, ?, 1, NULL, 0)",
            (ident, provider, native, ident),
        )
    raw.close()
    store = Store(path)
    assert store.find_binding("model", "model") is not None
    assert store.find_binding("chatgpt", "chatgpt") is not None
    assert store.get_binding("c").title == "kept"
    store.close()

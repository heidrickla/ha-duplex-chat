from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import duplex_broker.store as store_module
from duplex_broker.api import Settings, create_app
from duplex_broker.store import Actor, Store

TABLES = (
    "session_bindings",
    "conversations",
    "conversation_members",
    "messages",
    "events",
    "deliveries",
    "devices",
    "tasks",
    "scheduled_sends",
    "meta",
)


def schema_13_database(tmp_path: Path) -> Path:
    """A database exactly as the 0.10 broker leaves it, with a session, a group, a message and a delivery."""
    path = tmp_path / "schema13.db"
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(store_module, "SCHEMA_VERSION", 13)
    store = Store(path, owner="owner")
    claude, _ = store.register_binding("claude", "n1", "Builder", "w", "e", {"send_modes": ["auto"]})
    group = store.create_group("owner", "Team")
    store.add_member(Actor.user("owner"), group.id, claude.id)
    store.post_message(Actor.user("owner"), group.id, uuid.uuid4().hex, "kept", [claude.id])
    direct = store.open_direct("owner", claude.id)
    store.post_message(Actor.user("owner"), direct.id, uuid.uuid4().hex, "kept too")
    store.close()
    monkeypatch.undo()
    with sqlite3.connect(path) as raw:
        assert raw.execute("SELECT version FROM schema_version").fetchone()[0] == 13
        sql = raw.execute("SELECT sql FROM sqlite_master WHERE name = 'session_bindings'").fetchone()[0]
        assert "'opencode'" not in sql
        with pytest.raises(sqlite3.IntegrityError):
            raw.execute("UPDATE session_bindings SET provider = 'opencode'")
    return path


def counts(path: Path) -> dict[str, int]:
    with sqlite3.connect(path) as db:
        return {t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in TABLES}


def test_a_schema_13_database_upgrades_to_14_and_keeps_every_row(tmp_path: Path) -> None:
    path = schema_13_database(tmp_path)
    before = counts(path)
    with sqlite3.connect(path) as db:
        rows_before = db.execute("SELECT * FROM messages ORDER BY id").fetchall()
    store = Store(path, owner="wrong-default")
    assert store.schema == store_module.SCHEMA_VERSION == 14
    assert store.owner == "owner"
    store.close()
    assert counts(path) == before
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT * FROM messages ORDER BY id").fetchall() == rows_before
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        # The constraint is widened, not dropped.
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE session_bindings SET provider = 'parrot'")


def test_an_opencode_session_registers_on_the_upgraded_database(tmp_path: Path) -> None:
    path = schema_13_database(tmp_path)
    store = Store(path, owner="owner")
    with TestClient(create_app(store, Settings(enrollment_token="enroll-me"))) as client:
        answer = client.post(
            "/api/adapter/register",
            json={
                "provider": "opencode",
                "native_id": "ses_test",
                "title": "DeepSeek",
                "workspace": "w",
                "endpoint_ref": "opencode-server",
                "capabilities": {"send_modes": ["auto", "queue", "steer", "restart"], "turn_state": True},
                "adapter": {"version": "0.11.0"},
            },
            headers={"authorization": "Bearer enroll-me"},
        )
        assert answer.status_code == 201, answer.text
        binding = store.get_binding(answer.json()["binding"]["id"])
        assert binding.provider == "opencode"
        assert binding.send_modes == ("auto", "queue", "steer", "restart")
        # An opencode adapter reports a version, so it counts among the adapters compared for "behind".
        assert store_module.adapter_behind(binding, "0.11.0") is False
    store.close()
    with sqlite3.connect(path) as db:
        row = db.execute("SELECT adapter FROM session_bindings WHERE provider = 'opencode'").fetchone()
    assert json.loads(row[0])["version"] == "0.11.0"

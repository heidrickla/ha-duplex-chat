from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import duplex_broker.store as store_module
from duplex_broker import resume, tasks
from duplex_broker.api import Settings, create_app
from duplex_broker.store import Actor, ModeUnsupported, Store

OWNER = Actor.user("owner")
TABLES = ("tasks", "task_events", "task_questions", "scheduled_sends", "waker_starts")


def test_a_schema_11_database_upgrades_in_place(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "v11.db"
    # The 0.8.2 code path: the migrations up to 11, exactly as that release ran them.
    monkeypatch.setattr(store_module, "SCHEMA_VERSION", 11)
    Store(path).close()
    monkeypatch.undo()
    raw = sqlite3.connect(path, isolation_level=None)
    assert raw.execute("SELECT version FROM schema_version").fetchone()[0] == 11
    assert "mode" not in [r[1] for r in raw.execute("PRAGMA table_info(messages)")]
    raw.execute(
        "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref, capabilities,"
        " lease_generation, token_sha, active, heartbeat_at, created_at)"
        " VALUES ('b1', 'claude', 'n1', 'kept', 'w', 'e', ?, 1, 't', 1, NULL, 0)",
        (json.dumps({"receive_messages": "verified", "tasks": True}),),
    )
    raw.execute(
        "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at)"
        " VALUES ('c1', 'direct', 'kept', 'owner', 'user_directed', 'b1', 0)"
    )
    raw.execute("INSERT INTO conversation_members VALUES ('c1', 'user', 'owner', 'owner')")
    raw.execute("INSERT INTO conversation_members VALUES ('c1', 'binding', 'b1', 'session')")
    raw.execute(
        "INSERT INTO messages (id, conversation_id, actor_kind, actor_id, client_message_id, content_sha,"
        " text, recipients, parent_id, created_at) VALUES ('m1', 'c1', 'user', 'owner', 'x', 's', 'old',"
        " '[\"b1\"]', NULL, 0)"
    )
    raw.execute(
        "INSERT INTO deliveries (id, message_id, binding_id, state, attempt, native_turn_id,"
        " lease_generation, seq, updated_at) VALUES ('d1', 'm1', 'b1', 'queued', 0, NULL, 1, 1, 0)"
    )
    raw.close()

    store = Store(path)
    kept = store.get_binding("b1")
    assert (kept.turn_state, kept.turn_waiting_for, kept.turn_state_at) == ("unknown", None, None)
    assert kept.send_modes == ("auto",)
    claimed = store.claim_next("b1", 1)
    assert claimed is not None
    assert (claimed[1].text, claimed[1].mode) == ("old", "auto")
    with pytest.raises(ModeUnsupported):
        store.post_message(OWNER, "c1", uuid.uuid4().hex, "now", mode="steer")
    assert store.post_message(OWNER, "c1", uuid.uuid4().hex, "new").message.mode == "auto"
    # The new tables take rows on the upgraded database.
    other, _ = store.register_binding("codex", "n2", "other", "w", "e", {"tasks": True})
    tasks.create_task(store, kept, other.id, "t", "d", None, "auto")
    resume.record_limit(store, other, "r", "codex-rollout", "m", 10.0, 20.0)
    store.close()

    check = sqlite3.connect(path)
    assert check.execute("SELECT version FROM schema_version").fetchone()[0] == store_module.SCHEMA_VERSION
    assert store_module.SCHEMA_VERSION == 14
    assert check.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert check.execute("PRAGMA foreign_key_check").fetchall() == []
    counts = {t: check.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in TABLES}
    assert counts == {
        "tasks": 1,
        "task_events": 1,
        "task_questions": 0,
        "scheduled_sends": 1,
        "waker_starts": 0,
    }
    with pytest.raises(sqlite3.IntegrityError):
        check.execute("UPDATE messages SET mode = 'shout'")
    with pytest.raises(sqlite3.IntegrityError):
        check.execute("UPDATE session_bindings SET turn_state = 'napping'")
    check.close()


def schema_12_database(tmp_path: Path) -> Path:
    source = tmp_path / "schema12.db"
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(store_module, "SCHEMA_VERSION", 12)
    Store(source, owner="owner").close()
    monkeypatch.undo()

    raw = sqlite3.connect(source, isolation_level=None)
    raw.execute(
        "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref, capabilities,"
        " lease_generation, token_sha, active, heartbeat_at, created_at) VALUES "
        f"('model-1', '{store_module.LEGACY_MODEL}', '{store_module.LEGACY_MODEL}', 'Assistant',"
        " 'built-in', 'broker',"
        " '{}', 1, 'model-token', 1, NULL, 0)"
    )
    raw.execute(
        "INSERT INTO session_bindings (id, provider, native_id, title, workspace, endpoint_ref, capabilities,"
        " lease_generation, token_sha, active, heartbeat_at, created_at) VALUES "
        f"('chatgpt-1', 'chatgpt', '{store_module.LEGACY_CONNECTOR_NATIVE}', 'ChatGPT',"
        " 'connector', 'events',"
        " '{}', 1, 'chat-token', 1, NULL, 0)"
    )
    raw.execute(
        "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at) VALUES "
        "('broker-dm', 'direct', 'Duplex broker', 'legacy-owner', 'user_directed', 'broker', 1), "
        "('model-dm', 'direct', 'Assistant', 'legacy-owner', 'user_directed', 'model-1', 2), "
        "('queue-dm', 'direct', 'job-queue', 'job-queue', 'user_directed', 'chatgpt-1', 3)"
    )
    raw.execute("INSERT INTO conversation_members VALUES ('broker-dm', 'user', 'legacy-owner', 'owner')")
    raw.execute("INSERT INTO conversation_members VALUES ('broker-dm', 'binding', 'broker', 'session')")
    raw.execute("INSERT INTO conversation_members VALUES ('model-dm', 'user', 'legacy-owner', 'owner')")
    raw.execute("INSERT INTO conversation_members VALUES ('model-dm', 'binding', 'model-1', 'session')")
    raw.execute("INSERT INTO conversation_members VALUES ('queue-dm', 'user', 'job-queue', 'owner')")
    raw.execute("INSERT INTO conversation_members VALUES ('queue-dm', 'binding', 'chatgpt-1', 'session')")
    raw.execute(
        "INSERT INTO messages (id, conversation_id, actor_kind, actor_id, client_message_id, content_sha, "
        "text, recipients, parent_id, created_at, mode) VALUES "
        "('message-1', 'model-dm', 'user', 'legacy-owner', "
        "'client-1',"
        " 'sha-1', 'kept', '[\"model-1\"]', NULL, 4, 'auto')"
    )
    raw.execute(
        "INSERT INTO deliveries (id, message_id, binding_id, state, attempt, native_turn_id, "
        "lease_generation,"
        " seq, updated_at) VALUES ('delivery-1', 'message-1', 'model-1', 'queued', 0, NULL, 1, 1, 4)"
    )
    raw.close()
    return source


def test_the_api_serves_an_upgraded_database_to_its_adopted_owner(tmp_path: Path) -> None:
    source = schema_12_database(tmp_path)
    store = Store(source, owner="wrong-default")
    # Settings carry no owner: the id the database holds is the only one the API acts as.
    with TestClient(create_app(store, Settings(pairing_code="pair-me"))) as client:
        csrf = client.post("/api/pair", json={"code": "pair-me"}).json()["csrf"]
        assert client.get("/api/session").json()["owner"] == "legacy-owner"
        listed = {c["id"] for c in client.get("/api/conversations").json()}
        assert {"broker-dm", "model-dm"} <= listed
        posted = client.post(
            "/api/conversations/model-dm/messages",
            json={"client_message_id": uuid.uuid4().hex, "text": "after the upgrade"},
            headers={"x-csrf-token": csrf},
        )
        assert posted.status_code in (200, 201), posted.text
    store.close()


def test_a_copied_schema_12_database_adopts_owner_and_renames_model(tmp_path: Path) -> None:
    source = schema_12_database(tmp_path)
    store = Store(source, owner="wrong-default")
    assert store.owner == "legacy-owner"
    assert store.get_binding("model-1").provider == "model"
    assert {c.id for c in store.conversations_for(Actor.user("legacy-owner"))} == {"broker-dm", "model-dm"}
    with sqlite3.connect(source) as check:
        assert check.execute("SELECT text FROM messages WHERE id = 'message-1'").fetchone()[0] == "kept"
        assert check.execute("SELECT state FROM deliveries WHERE id = 'delivery-1'").fetchone()[0] == "queued"
    with sqlite3.connect(source) as check:
        assert check.execute("SELECT value FROM meta WHERE key = 'owner_id'").fetchone()[0] == "legacy-owner"
        provider = check.execute("SELECT provider FROM session_bindings WHERE id = 'model-1'").fetchone()[0]
        assert provider == "model"
        natives = dict(check.execute("SELECT id, native_id FROM session_bindings"))
        assert (natives["model-1"], natives["chatgpt-1"]) == ("model", "chatgpt")
        queue = check.execute("SELECT owner FROM conversations WHERE id = 'queue-dm'").fetchone()[0]
        assert queue == "job-queue"
        assert check.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    store.close()


def test_a_database_that_never_sent_a_notice_adopts_the_owner_of_its_groups(tmp_path: Path) -> None:
    source = schema_12_database(tmp_path)
    with sqlite3.connect(source) as raw:
        # No notices conversation, and the service owns more direct conversations than the owner.
        raw.execute("DELETE FROM conversation_members WHERE conversation_id = 'broker-dm'")
        raw.execute("DELETE FROM conversations WHERE id = 'broker-dm'")
        raw.execute(
            "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at) VALUES "
            "('queue-dm-2', 'direct', 'job-queue', 'job-queue', 'user_directed', 'model-1', 5), "
            "('team', 'group', 'Team', 'legacy-owner', 'user_directed', NULL, 6)"
        )
        raw.execute("INSERT INTO conversation_members VALUES ('team', 'user', 'legacy-owner', 'owner')")
    store = Store(source, owner="wrong-default")
    assert store.owner == "legacy-owner"
    assert {c.id for c in store.conversations_for(Actor.user("legacy-owner"))} == {"model-dm", "team"}
    store.close()


def test_a_database_with_no_notice_and_no_group_adopts_whoever_owns_the_most(tmp_path: Path) -> None:
    source = schema_12_database(tmp_path)
    with sqlite3.connect(source) as raw:
        raw.execute("DELETE FROM conversation_members WHERE conversation_id = 'broker-dm'")
        raw.execute("DELETE FROM conversations WHERE id = 'broker-dm'")
        raw.execute(
            "INSERT INTO conversations (id, kind, title, owner, mode, dm_binding_id, created_at) VALUES "
            "('chat-dm', 'direct', 'ChatGPT', 'legacy-owner', 'user_directed', 'chatgpt-1', 5)"
        )
    store = Store(source, owner="wrong-default")
    assert store.owner == "legacy-owner"
    store.close()

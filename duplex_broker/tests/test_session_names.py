from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

import duplex_broker
from duplex_broker import tasks
from duplex_broker.connector import Connector
from duplex_broker.ids import match_sessions, reply_id
from duplex_broker.store import Actor, Binding, Invalid, Store

OWNER = Actor.user("owner")
HEX = "0123456789abcdef0123456789abcdef"
SHOWN = reply_id(HEX)


@dataclass(frozen=True)
class Named:
    id: str
    title: str


@pytest.fixture
def store() -> Iterator[Store]:
    s = Store(":memory:")
    yield s
    s.close()


def bind(store: Store, title: str) -> Binding:
    binding, _ = store.register_binding("claude", uuid.uuid4().hex, title, "w", "e", {"tasks": True})
    return binding


# One table of what a typed name picks out, for the shared matcher and for each place that takes a
# session by name: an id (stored or displayed) is one namespace and a title, as typed, is another.
def test_a_name_picks_an_id_and_a_title_in_their_own_forms() -> None:
    plain = Named("a" * 32, "builder")
    shaped = Named("b" * 32, SHOWN)  # a title that looks like a displayed id
    digits = Named("c" * 32, HEX)  # a title that looks like a stored id
    other = Named("d" * 32, "Builder")
    everyone = [plain, shaped, digits, other]
    assert match_sessions(everyone, "builder") == [plain]
    assert match_sessions(everyone, "a" * 32) == [plain]
    assert match_sessions(everyone, reply_id("a" * 32)) == [plain]
    assert match_sessions(everyone, SHOWN) == [shaped]
    assert match_sessions(everyone, HEX) == [digits]
    assert match_sessions(everyone, "builder", fold=True) == [plain, other]
    assert match_sessions(everyone, "nobody") == []


def test_a_name_that_is_one_sessions_id_and_anothers_title_matches_both() -> None:
    by_id = Named(HEX, "first")
    by_title = Named("e" * 32, SHOWN)
    assert match_sessions([by_id, by_title], SHOWN) == [by_id, by_title]
    assert match_sessions([by_id, by_title], HEX) == [by_id]


def test_a_task_goes_to_a_session_whose_exact_title_looks_like_an_id(store: Store) -> None:
    boss = bind(store, "boss")
    shaped = bind(store, SHOWN)
    plain = bind(store, "plain")
    for to in (SHOWN, shaped.id):
        assert tasks.create_task(store, boss, to, "t", "d", None, "auto")["assignee"]["id"] == shaped.id
    assert tasks.create_task(store, boss, "plain", "t", "d", None, "auto")["assignee"]["id"] == plain.id
    assert (
        tasks.create_task(store, boss, reply_id(plain.id), "t", "d", None, "auto")["assignee"]["id"]
        == plain.id
    )


def test_a_task_refuses_a_name_that_is_one_sessions_id_and_anothers_title(store: Store) -> None:
    boss = bind(store, "boss")
    owner_of_id, _ = store.register_binding("claude", "n-id", "first", "w", "e", {"tasks": True})
    # The second session's title is the displayed form of the first session's id.
    bind(store, reply_id(owner_of_id.id))
    with pytest.raises(Invalid, match="names 2 active sessions"):
        tasks.create_task(store, boss, reply_id(owner_of_id.id), "t", "d", None, "auto")
    assert tasks.create_task(store, boss, owner_of_id.id, "t", "d", None, "auto")["assignee"]["id"] == (
        owner_of_id.id
    )


def test_chatgpt_addresses_a_session_by_the_same_rule() -> None:
    store = Store(":memory:")
    connector = Connector(store, "https://x.test", "owner", lambda c, a: None, lambda b: None, lambda b: {})
    shaped, _ = store.register_binding("claude", "n1", SHOWN, "w", "e", {})
    plain, _ = store.register_binding("claude", "n2", "plain", "w", "e", {})
    group = store.create_group("owner", "names")
    for member in (shaped.id, plain.id, connector.binding().id):
        store.add_member(OWNER, group.id, member)
    chatgpt = Actor.binding(connector.binding().id)
    assert connector._recipients(chatgpt, group.id, [SHOWN]) == [shaped.id]
    assert connector._recipients(chatgpt, group.id, [reply_id(plain.id), "PLAIN"]) == [plain.id, plain.id]
    store.close()


TOOLS = Path(__file__).resolve().parents[2] / "tools"


def test_no_module_or_tool_but_ids_compares_a_session_title_to_a_typed_name() -> None:
    """A new place that takes a session by name must use match_sessions, not its own comparison."""
    title = r"\w+(?:\.title|\[\"title\"\])"
    pattern = re.compile(
        rf"{title}(?:\.casefold\(\))?\s*(?:==|!=)|(?:==|!=)\s*{title}|\bin \([^)]*title[^)]*\)"
        r"|WHERE[^\"']*\btitle\s*=\s*\?"
    )
    files = [*Path(duplex_broker.__file__).parent.glob("*.py"), *TOOLS.glob("*.py")]
    offenders = [
        path.name
        for path in files
        if path.name != "ids.py" and pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []

"""Message and conversation IDs as adapters see them."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any, Protocol

ADDRESSABLE_ID = re.compile(r"[0-9a-f]{32}\Z")
DISPLAY_ID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


def reply_id(value: str) -> str:
    """Give adapters an addressable ID that older MCP redactors leave intact."""
    if ADDRESSABLE_ID.fullmatch(value):
        return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:]}"
    return value


def stored_id(value: str) -> str:
    if DISPLAY_ID.fullmatch(value):
        return value.replace("-", "")
    return value


class _Named(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def title(self) -> str: ...


def match_sessions[S: _Named](sessions: Iterable[S], name: str, *, fold: bool = False) -> list[S]:
    """The sessions a typed name picks out. An id and a title are different namespaces: the name is
    normalized as an id (a displayed ID loses its hyphens) for the id match, and compared as typed for the
    title match, so a title shaped like a UUID is still a title. Every place that takes a session by name
    uses this; more than one match is the caller's ambiguity to refuse."""
    as_id = stored_id(name)
    wanted = name.casefold() if fold else name
    return [s for s in sessions if s.id == as_id or (s.title.casefold() if fold else s.title) == wanted]


def shown_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    """An envelope for an adapter: the notice's message, conversation, task and question IDs hyphenated."""
    notice = envelope.get("notice")
    if not isinstance(notice, dict):
        return envelope
    shown = {**notice}
    for key in ("message_id", "conversation_id", "task_id", "question_id"):
        if isinstance(shown.get(key), str):
            shown[key] = reply_id(shown[key])
    return {**envelope, "notice": shown}

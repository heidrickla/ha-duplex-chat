"""Home Assistant's Assist API over its MCP server, for the model member's actions.

The endpoint Home Assistant's voice pipeline uses: `/api/mcp/assist` through the Supervisor's core proxy
with the add-on's token. Stateless streamable HTTP, one JSON-RPC request per POST, answered as JSON or one
SSE frame. Exposure to Assist is the boundary: the model reaches exactly the entities exposed there. Blocking;
callers run it on a worker thread.
"""

import contextlib
import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_URL = "http://supervisor/core/api/mcp/assist"
PROTOCOL_VERSION = "2025-06-18"
CACHE_S = 60.0
TIMEOUT_S = 20.0


class AssistUnavailable(Exception):
    """The endpoint answered with an error or not at all."""


@dataclass
class AssistBundle:
    """HA's prompt (names, domains and areas of exposed entities) and its tools as chat-completions specs."""

    prompt: str
    tools: list[dict[str, Any]]
    fetched_at: float = field(default_factory=time.monotonic)

    @property
    def tool_names(self) -> set[str]:
        return {t["function"]["name"] for t in self.tools}


def _sse_data(text: str) -> str:
    lines = [line[5:].strip() for line in text.splitlines() if line.startswith("data:")]
    return "\n".join(lines) if lines else text


def tool_spec(tool: dict[str, Any]) -> dict[str, Any]:
    schema = dict(tool.get("inputSchema") or {"type": "object", "properties": {}})
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    return {
        "type": "function",
        "function": {
            "name": tool["name"],
            "description": tool.get("description") or "",
            "parameters": schema,
        },
    }


class AssistClient:
    def __init__(self, url: str = DEFAULT_URL, token: str = "", timeout: float = TIMEOUT_S) -> None:
        self.url = url
        self._headers = {"content-type": "application/json", "accept": "application/json, text/event-stream"}
        if token:
            self._headers["authorization"] = f"Bearer {token}"
        self._timeout = timeout
        self._bundle: AssistBundle | None = None
        self._seq = 0

    def rpc(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._seq += 1
        body = {"jsonrpc": "2.0", "id": self._seq, "method": method, "params": params or {}}
        request = urllib.request.Request(
            self.url, data=json.dumps(body).encode(), headers=self._headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                text = response.read().decode()
        except urllib.error.HTTPError as exc:
            raise AssistUnavailable(f"HTTP {exc.code} from Home Assistant") from exc
        except (urllib.error.URLError, OSError) as exc:
            raise AssistUnavailable("Home Assistant is not reachable") from exc
        try:
            msg = json.loads(_sse_data(text))
        except ValueError as exc:
            raise AssistUnavailable(f"unreadable answer to {method}") from exc
        if "error" in msg:
            err = msg["error"] if isinstance(msg["error"], dict) else {"message": str(msg["error"])}
            raise AssistUnavailable(f"{method}: {err.get('message')}")
        result = msg.get("result")
        return result if isinstance(result, dict) else {}

    def bundle(self) -> AssistBundle:
        """Prompt and tools, cached for a minute. Raises AssistUnavailable."""
        if self._bundle is not None and time.monotonic() - self._bundle.fetched_at < CACHE_S:
            return self._bundle
        self.rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "duplex-broker", "version": "1"},
            },
        )
        prompt = ""
        prompts = self.rpc("prompts/list").get("prompts") or []
        if prompts:
            got = self.rpc("prompts/get", {"name": prompts[0]["name"], "arguments": {}})
            parts = [
                str(m["content"].get("text") or "")
                for m in got.get("messages") or []
                if isinstance(m.get("content"), dict) and m["content"].get("type") == "text"
            ]
            prompt = "\n".join(parts).strip()
        tools = [tool_spec(t) for t in self.rpc("tools/list").get("tools") or [] if t.get("name")]
        self._bundle = AssistBundle(prompt=prompt, tools=tools)
        log.info("assist: %d tools, prompt %d chars", len(tools), len(prompt))
        return self._bundle

    def call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        """Run one tool; errors come back as {"ok": False, "error": ...}, never raised."""
        try:
            result = self.rpc("tools/call", {"name": name, "arguments": args})
        except AssistUnavailable as exc:
            return {"ok": False, "error": str(exc)}
        text = "\n".join(
            str(c.get("text") or "")
            for c in result.get("content") or []
            if isinstance(c, dict) and c.get("type") == "text"
        ).strip()
        if result.get("isError"):
            return {"ok": False, "error": text or "Home Assistant reported an error"}
        parsed: Any = text
        with contextlib.suppress(ValueError):
            parsed = json.loads(text) if text else {}
        return {"ok": True, "result": parsed}

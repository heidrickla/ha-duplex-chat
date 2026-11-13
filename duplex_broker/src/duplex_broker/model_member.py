"""The built-in model member: answers messages addressed to it through an OpenAI-compatible endpoint.

It is a binding like a registered session, so a chat client lists it and opens a direct conversation with
it. The broker only relays: the model call is an OpenAI-style chat request made on a worker thread, so the
event loop and other members' deliveries are never held.

With actions on (`model_actions`), the model gets Home Assistant's Assist prompt and tools and every tool
call it returns is run through Assist with no approval step: it reaches every entity exposed to Assist,
locks and alarm panels included. Each run is posted as a `ran:` line above the reply. With actions off, no
tools are sent and a tool call is shown as text.
"""

import asyncio
import contextlib
import json
import logging
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from .assist import AssistClient, AssistUnavailable
from .store import Actor, BrokerError, Delivery, Message, Store

log = logging.getLogger(__name__)

PROVIDER: Literal["model"] = "model"
NATIVE_ID = "model"
# Templates: `{title}` is the member's title, `{owner}` the owner's display name.
SYSTEM_PROMPT = (
    "You are {title}, an assistant in a chat with {owner}. Answer briefly and plainly. "
    "You have no tools in this chat; say so if asked to control something."
)
ACTION_PROMPT = (
    "You are {title}, a home assistant chatting with {owner}. When they ask you to control "
    "or check something in the house, call the tools; otherwise answer briefly and plainly."
)
MAX_ROUNDS = 5
RESULT_CHARS = 160
CONTEXT_TOKENS = 32768
REPLY_TOKENS = 1024
# The endpoint may admit a request by JSON characters / 2 plus max_tokens; the window is measured the same.
CHARS_PER_TOKEN = 2
MARGIN_TOKENS = 512
HEARTBEAT_S = 30.0
IDLE_POLL_S = 2.0


@dataclass(frozen=True)
class ModelConfig:
    url: str
    model: str = ""
    timeout: float = 90.0
    reply_tokens: int = REPLY_TOKENS
    tools_model: str = ""
    title: str = "Assistant"
    owner_name: str = "the owner"
    extra_prompt: str = ""  # the text of model-prompt.md, appended to the system prompt
    no_thinking: bool = False  # send chat_template_kwargs to turn a template's thinking off

    def system(self, template: str) -> str:
        text = template.replace("{title}", self.title).replace("{owner}", self.owner_name)
        return text + ("\n\n" + self.extra_prompt if self.extra_prompt else "")


class ModelRefused(Exception):
    """The endpoint or the network gave no answer; the text is the short reply for the chat."""


ChatFn = Callable[[list[dict[str, str]]], str]
# Messages and tools in, the model's assistant message out.
ToolChatFn = Callable[[list[dict[str, Any]], list[dict[str, Any]]], dict[str, Any]]


def trim_history(
    turns: list[dict[str, Any]],
    system: str = SYSTEM_PROMPT,
    context_tokens: int = CONTEXT_TOKENS,
    reply_tokens: int = REPLY_TOKENS,
) -> list[dict[str, Any]]:
    """The newest turns that fit the window beside the system prompt and the reply budget, oldest first.

    The last turn is always kept: an oversized one is cut from its start so the model sees its end. A window
    never opens on a tool call or a tool result, which would be cut from its pair.
    """
    budget = (context_tokens - reply_tokens - MARGIN_TOKENS) * CHARS_PER_TOKEN - len(system) - 200
    kept: list[dict[str, Any]] = []
    used = 0
    for turn in reversed(turns):
        cost = len(json.dumps(turn))
        if used + cost > budget:
            if not kept:
                room = max(budget - used - 100, 1)
                kept.append({**turn, "content": turn["content"][-room:]})
            break
        kept.append(turn)
        used += cost
    kept.reverse()
    while len(kept) > 1 and (kept[0]["role"] == "tool" or kept[0].get("tool_calls")):
        kept.pop(0)
    return kept


MARKERS = ("ran:", "tool:")
FORGED_NOTE = "Nothing was run: the model wrote an action line itself instead of calling a tool."


def strip_markers(text: str) -> tuple[str, bool]:
    """The text without `ran:` and `tool:` lines, which only the broker writes, and whether any were there.

    The model copies the format from its earlier replies; a copied `ran:` line claims an unrun action.
    """
    lines = text.splitlines()
    kept = [line for line in lines if not line.lstrip().startswith(MARKERS)]
    return "\n".join(kept).strip(), len(kept) != len(lines)


def parse_ran(line: str) -> tuple[str, str, str] | None:
    """Name, arguments JSON and shown result of one `ran: name {args} -> result` line."""
    if not line.startswith("ran: "):
        return None
    name, _, rest = line[5:].partition(" ")
    try:
        args, end = json.JSONDecoder().raw_decode(rest)
    except ValueError:
        return None
    if not isinstance(args, dict) or not rest[end:].startswith(" -> "):
        return None
    return name, rest[:end], rest[end + 4 :]


def history(turns: list[dict[str, str]], actions: bool) -> list[dict[str, Any]]:
    """Earlier turns as the model sees them. The member's own replies lose the `ran:` and `tool:` lines.

    With actions, each `ran:` line is replayed as the tool call and result it records, and a reply that ran
    nothing is left out: after text-only replies to requests, a tools model answered in text without calling
    a tool (0 of 3 calls; 3 of 3 with replayed calls or user turns only, measured on one model).
    """
    out: list[dict[str, Any]] = []
    for n, turn in enumerate(turns):
        if turn["role"] != "assistant":
            out.append(turn)
            continue
        if actions:
            calls: list[dict[str, Any]] = []
            results: list[dict[str, Any]] = []
            for line in turn["content"].splitlines():
                ran = parse_ran(line.strip())
                if ran is None:
                    continue
                ident = f"h{n}c{len(calls)}"
                calls.append(
                    {"id": ident, "type": "function", "function": {"name": ran[0], "arguments": ran[1]}}
                )
                results.append({"role": "tool", "tool_call_id": ident, "content": ran[2]})
            if not calls:
                continue
            out.append({"role": "assistant", "content": "", "tool_calls": calls})
            out.extend(results)
        out.append({"role": "assistant", "content": strip_markers(turn["content"])[0] or "Done."})
    return out


def render_tool_calls(message: dict[str, Any]) -> str:
    """The reply text: the content, then one `tool: name {args}` line per tool call. Nothing is run."""
    parts: list[str] = []
    content = message.get("content")
    text, forged = strip_markers(content if isinstance(content, str) else "")
    if forged:
        parts.append(FORGED_NOTE)
    if text:
        parts.append(text)
    calls = message.get("tool_calls")
    for call in calls if isinstance(calls, list) else []:
        function = call.get("function") if isinstance(call, dict) else None
        if not isinstance(function, dict):
            continue
        args = function.get("arguments", "")
        if not isinstance(args, str):
            args = json.dumps(args)
        parts.append(f"tool: {function.get('name', '?')} {args or '{}'}")
    return "\n".join(parts)


def make_chat(config: ModelConfig) -> ChatFn:
    """A blocking chat call to the endpoint; run it on a thread."""

    def chat(messages: list[dict[str, str]]) -> str:
        text = render_tool_calls(_post(config, config.model, list(messages), None))
        return text or f"{config.title} returned no text."

    return chat


def make_tool_chat(config: ModelConfig) -> ToolChatFn:
    """A blocking chat call with tools to the endpoint; returns the assistant message. Run it on a thread."""

    def tool_chat(messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        return _post(config, config.tools_model, messages, tools)

    return tool_chat


def _post(
    config: ModelConfig, model: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None
) -> dict[str, Any]:
    endpoint = config.url.rstrip("/") + "/chat/completions"
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": config.reply_tokens,
        "temperature": 0.3,
    }
    if config.no_thinking:
        body["chat_template_kwargs"] = {"enable_thinking": False}
    if tools:
        body["tools"] = tools
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=config.timeout) as response:
            doc = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 503:
            raise ModelRefused(f"{config.title} is busy. Try again in a minute.") from exc
        if exc.code == 400:
            raise ModelRefused(f"That conversation is too long for {config.title}'s context window.") from exc
        if exc.code == 504:
            raise ModelRefused(f"{config.title} took too long to answer.") from exc
        raise ModelRefused(f"The model endpoint answered HTTP {exc.code}.") from exc
    except TimeoutError as exc:
        raise ModelRefused(f"{config.title} did not answer within {config.timeout:g} s.") from exc
    except (urllib.error.URLError, OSError) as exc:
        if isinstance(getattr(exc, "reason", None), TimeoutError):
            raise ModelRefused(f"{config.title} did not answer within {config.timeout:g} s.") from exc
        raise ModelRefused("The model endpoint is not reachable.") from exc
    except ValueError as exc:
        raise ModelRefused("The model endpoint sent a reply that is not JSON.") from exc
    try:
        message = doc["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ModelRefused("The model endpoint sent an unexpected reply.") from exc
    if not isinstance(message, dict):
        raise ModelRefused("The model endpoint sent an unexpected reply.")
    return message


def _short(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return text if len(text) <= RESULT_CHARS else text[: RESULT_CHARS - 3] + "..."


def act(
    turns: list[dict[str, str]],
    assist: AssistClient,
    tool_chat: ToolChatFn,
    config: ModelConfig | None = None,
) -> str:
    """Run the model with Assist's prompt and tools, executing every tool call it returns, up to MAX_ROUNDS.

    Returns the chat reply: one `ran:` line per executed call, then the model's final text.
    """
    try:
        bundle = assist.bundle()
    except AssistUnavailable as exc:
        return f"Home Assistant is not available for actions ({exc})."
    system = (config or ModelConfig("")).system(ACTION_PROMPT) + (
        "\n\n" + bundle.prompt if bundle.prompt else ""
    )
    sized = system + json.dumps(bundle.tools)
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        *trim_history(history(turns, actions=True), system=sized),
    ]
    lines: list[str] = []
    for _ in range(MAX_ROUNDS):
        try:
            reply = tool_chat(messages, bundle.tools)
        except ModelRefused as exc:
            lines.append(str(exc))
            return "\n".join(lines)
        calls = [c for c in reply.get("tool_calls") or [] if isinstance(c, dict)]
        content = reply.get("content") if isinstance(reply.get("content"), str) else ""
        if not calls:
            text, forged = strip_markers(content or "")
            if forged:
                lines.append(FORGED_NOTE)
            if text or not forged:
                lines.append(text or "Done.")
            return "\n".join(lines)
        messages.append({"role": "assistant", "content": content or "", "tool_calls": calls})
        for index, call in enumerate(calls):
            function = call.get("function")
            if not isinstance(function, dict):
                function = {}
            name = str(function.get("name") or "?")
            raw = function.get("arguments", "{}")
            try:
                args = json.loads(raw) if isinstance(raw, str) else raw
                if not isinstance(args, dict):
                    raise ValueError
            except ValueError:
                result: dict[str, Any] = {"ok": False, "error": "arguments are not a JSON object"}
                args = {}
            else:
                if name in bundle.tool_names:
                    result = assist.call(name, args)
                else:
                    result = {"ok": False, "error": f"no tool named {name}"}
            shown = result.get("result") if result.get("ok") else f"error: {result.get('error')}"
            lines.append(f"ran: {name} {json.dumps(args, separators=(',', ':'))} -> {_short(shown)}")
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": str(call.get("id") or f"call{index}"),
                    "content": json.dumps(result)[:4000],
                }
            )
    lines.append(f"Stopped after {MAX_ROUNDS} rounds of actions.")
    return "\n".join(lines)


class ModelMember:
    """The worker behind the member: claims its deliveries and answers each one."""

    def __init__(
        self,
        store: Store,
        config: ModelConfig,
        chat: ChatFn | None = None,
        assist: AssistClient | None = None,
        tool_chat: ToolChatFn | None = None,
    ) -> None:
        self._store = store
        self._config = config
        self._chat = chat or make_chat(config)
        self._assist = assist  # None: chat only, tool calls shown as text
        self._tool_chat = tool_chat or make_tool_chat(config)
        self.binding_id = ""
        self._lease = 0

    def register(self) -> None:
        binding, _ = self._store.register_binding(
            PROVIDER,
            NATIVE_ID,
            self._config.title,
            "built-in",
            "broker",
            {"receive_messages": "verified", "builtin": True},
        )
        self.binding_id, self._lease = binding.id, binding.lease_generation

    def _turns(self, message: Message) -> list[dict[str, str]]:
        me = Actor.binding(self.binding_id)
        conversation = self._store.get_conversation(me, message.conversation_id)
        titles = {m.id: m.title for m in self._store.member_details(me, message.conversation_id)}
        turns: list[dict[str, str]] = []
        for past in self._store.recent_messages(me, message.conversation_id, message.id):
            if past.actor == me:
                turns.append({"role": "assistant", "content": past.text})
            elif past.actor.kind == "user" or conversation.kind == "direct":
                turns.append({"role": "user", "content": past.text})
            else:
                turns.append(
                    {"role": "user", "content": f"{titles.get(past.actor.id, 'session')}: {past.text}"}
                )
        return turns

    def _reply(self, message: Message) -> str:
        """Blocking: the history, then the model call, with actions when Assist is configured."""
        turns = self._turns(message)
        if self._assist is not None:
            return act(turns, self._assist, self._tool_chat, self._config)
        system = self._config.system(SYSTEM_PROMPT)
        try:
            return self._chat(
                [
                    {"role": "system", "content": system},
                    *trim_history(history(turns, actions=False), system=system),
                ]
            )
        except ModelRefused as exc:
            return str(exc)

    async def answer(self, delivery: Delivery, message: Message) -> None:
        me = Actor.binding(self.binding_id)
        try:
            await asyncio.to_thread(
                self._store.transition, delivery.id, self.binding_id, self._lease, "accepted", None
            )
            # On a worker thread: a slow endpoint or Home Assistant must not hold the event loop.
            reply = await asyncio.to_thread(self._reply, message)
            await asyncio.to_thread(
                self._store.post_message,
                me,
                message.conversation_id,
                uuid.uuid4().hex,
                reply,
                None,
                message.id,
            )
        except Exception:
            log.exception("model member: could not answer delivery %s", delivery.id)
            await asyncio.to_thread(self._fail, delivery.id)
            return
        await asyncio.to_thread(
            self._store.transition, delivery.id, self.binding_id, self._lease, "responded", None
        )

    def _fail(self, delivery_id: str) -> None:
        try:
            self._store.transition(
                delivery_id,
                self.binding_id,
                self._lease,
                "failed",
                None,
                f"{self._config.title} could not answer",
            )
        except BrokerError:
            log.exception("model member: could not mark delivery %s failed", delivery_id)

    async def run(self, wake: asyncio.Event) -> None:
        await asyncio.to_thread(self.register)
        beat = 0.0
        loop = asyncio.get_running_loop()
        while True:
            try:
                if loop.time() - beat >= HEARTBEAT_S:
                    await asyncio.to_thread(self._store.heartbeat, self.binding_id)
                    beat = loop.time()
                claimed = await asyncio.to_thread(self._store.claim_next, self.binding_id, self._lease)
            except BrokerError as exc:
                log.warning("model member stopped: %s", exc)
                return
            if claimed is None:
                wake.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wake.wait(), IDLE_POLL_S)
                continue
            try:
                await self.answer(*claimed)
            except Exception:
                log.exception("model member: delivery handling failed")

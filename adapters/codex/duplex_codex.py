"""duplex-chat adapter for Codex threads the bridge owns.

It starts its own `codex app-server` over stdio, which is then the single writer for every thread
it holds, keeps the threads named in `codex.json` loaded, registers each with the broker as a Codex
session, and turns each message the owner sends one of them into one turn. The reply is the turn's
final agent message.

Files, in ~/.duplex-chat (not AppData: MSIX apps each get a private AppData):
  adapter.json        broker_url, enrollment_token (what `duplex setup` writes)
  codex.json          {"threads": [{"title": "...", "cwd": "/path/to/project"}]}; codex.example.json
  codex-threads.json  title -> thread id, kept so a restart resumes the same threads
  codex-tokens.json   thread id -> its binding's current token: the broker lets an active session
                      register again only with that token

Each message carries a send mode (auto, queue, steer, restart) that decides how it meets a running turn;
the adapter reports its thread's turn state (busy, idle) to the broker as the app-server's turn/start
answers and turn notifications change it.

Owner messages and the broker's own notices (a limit resume) become turns; peer messages (from another
session) are refused: the design delivers peer text only as tool data, never as a prompt, and a
bridge-managed Codex thread has no such tool yet.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
import sys
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger("duplex_codex")

LOCAL = Path(os.environ.get("USERPROFILE") or str(Path.home())) / ".duplex-chat"
# Where the npm package of Codex keeps its own codex.exe on Windows, under %APPDATA%.
BUNDLED_CODEX = "npm/node_modules/@openai/codex/node_modules/@openai/codex-win32-x64"
BUNDLED_CODEX += "/vendor/x86_64-pc-windows-msvc/bin/codex.exe"
SEND_MODES = ("auto", "queue", "steer", "restart")
CAPABILITIES: dict[str, Any] = {
    "receive_messages": "verified",
    "publish_replies": "verified",
    "stream_text": "unsupported",
    "observe_activity": "unsupported",
    "steer": "supported",
    "interrupt": "supported",
    "relay_approvals": "unsupported",
    "resume_history": "verified",
    "attach_live": "unsupported",
    "send_modes": list(SEND_MODES),
    "turn_state": True,
}
# A child gets only these variables, by name: no credential-shaped variable can reach it, whatever its name.
CHILD_ENV = frozenset(
    {
        "PATH", "PATHEXT", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC", "OS", "USERNAME",
        "COMPUTERNAME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "HOME", "APPDATA", "LOCALAPPDATA",
        "PROGRAMDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "TEMP", "TMP", "LANG", "LC_ALL", "TERM",
        "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "CODEX_HOME",
    }
)  # fmt: skip
TURN_TIMEOUT = 1800.0
INTERRUPT_TIMEOUT = 60.0
# The broker's notices member (duplex_broker.store.BROKER_BINDING_ID): limit resumes come from it.
BROKER_ID = "broker"
BACKOFF = (2.0, 5.0, 15.0, 30.0, 60.0)
# Ended turn ids kept so a late start answer cannot make its turn active again. One suffices, since a
# claim handles the last start answer before the next turn/start; the rest is margin for unknown turns.
ENDED_KEPT = 16


def find_codex(environ: Mapping[str, str]) -> str:
    """The Codex to run: DUPLEX_CODEX, else the npm package's bundled codex.exe, else `codex` on the PATH."""
    if environ.get("DUPLEX_CODEX"):
        return environ["DUPLEX_CODEX"]
    if environ.get("APPDATA") and (bundled := Path(environ["APPDATA"]) / BUNDLED_CODEX).is_file():
        return str(bundled)
    if found := shutil.which("codex", path=environ.get("PATH")):
        return found
    raise SystemExit("codex was not found: put it on the PATH or set DUPLEX_CODEX to its full path")


def child_env(environ: Mapping[str, str]) -> dict[str, str]:
    """The environment for a child: the allowlisted variables, matched by upper-cased name.
    It takes no additions, so no path can put another name into a child."""
    return {k: v for k, v in environ.items() if k.upper() in CHILD_ENV}


class RpcError(Exception):
    pass


@dataclass(frozen=True)
class StartAnswer:
    """The turn/start answer, put on its thread's input so the one handler folds it in with the
    notifications. A frame from the socket decodes to a dict, so it can never be one."""

    turn_id: str


ThreadInput = dict[str, Any] | StartAnswer


class AppServer:
    """JSON-RPC over the stdio of one `codex app-server` child."""

    def __init__(self, argv: list[str], cwd: Path) -> None:
        self.argv = argv
        self.cwd = cwd
        self.proc: asyncio.subprocess.Process | None = None
        self._next = 0
        self._waiting: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._threads: dict[str, asyncio.Queue[ThreadInput]] = {}
        self._reader: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            *self.argv,
            cwd=self.cwd,
            env=child_env(os.environ),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            limit=16 * 1024 * 1024,
        )
        self._reader = asyncio.create_task(self._read())
        await self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "duplex_chat",
                    "title": "duplex-chat",
                    "version": "0.1.0",
                },
                "capabilities": {"experimentalApi": True},
            },
        )
        await self._send({"method": "initialized", "params": {}})

    async def close(self) -> None:
        if self.proc is None:
            return
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            await asyncio.wait_for(self.proc.wait(), 15)
        except TimeoutError:
            self.proc.kill()
        if self._reader:
            self._reader.cancel()

    def events(self, thread_id: str) -> asyncio.Queue[ThreadInput]:
        return self._threads.setdefault(thread_id, asyncio.Queue())

    async def _send(self, message: dict[str, Any]) -> None:
        assert self.proc and self.proc.stdin
        self.proc.stdin.write((json.dumps(message) + "\n").encode())
        await self.proc.stdin.drain()

    async def request(self, method: str, params: dict[str, Any], timeout: float = 120) -> dict[str, Any]:
        self._next += 1
        rid = self._next
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._waiting[rid] = future
        await self._send({"method": method, "id": rid, "params": params})
        message = await asyncio.wait_for(future, timeout)
        if "error" in message:
            raise RpcError(json.dumps(message["error"]))
        result: dict[str, Any] = message["result"]
        return result

    async def _read(self) -> None:
        assert self.proc and self.proc.stdout
        while line := await self.proc.stdout.readline():
            message = json.loads(line)
            if "id" in message and ("result" in message or "error" in message):
                future = self._waiting.pop(message["id"], None)
                if future and not future.done():
                    future.set_result(message)
            elif "id" in message and "method" in message:
                # Approvals and elicitations: nothing relays them yet, so refuse (the threads run with
                # approvals "never", so none should come).
                await self._send(
                    {
                        "id": message["id"],
                        "error": {"code": -32601, "message": "not relayed"},
                    }
                )
            else:
                thread = message.get("params", {}).get("threadId")
                if thread:
                    self.events(thread).put_nowait(message)
        for future in self._waiting.values():
            if not future.done():
                future.set_exception(RpcError("app-server exited"))


class Broker:
    def __init__(self, base_url: str, enrollment_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.enrollment_token = enrollment_token

    def _call(self, method: str, path: str, body: Any, bearer: str) -> Any:
        request = urllib.request.Request(
            self.base_url + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "content-type": "application/json",
                "authorization": f"Bearer {bearer}",
            },
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
            return json.loads(raw) if raw else None

    async def call(self, method: str, path: str, body: Any = None, bearer: str = "") -> Any:
        return await asyncio.to_thread(self._call, method, path, body, bearer)


class Refused(Exception):
    """A delivery this thread cannot take in its present turn state; the text is the failure reason."""


@dataclass
class Waiter:
    """A delivery waiting for a turn to end: each gets that turn's final message as its reply."""

    delivery_id: str
    conversation_id: str
    message_id: str
    # Set once the broker has the delivery as accepted: its result must not overtake that.
    ready: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class Run:
    turn_id: str
    final: str = ""
    status: str | None = None
    waiters: list[Waiter] = field(default_factory=list)
    watch: asyncio.Future[None] | None = None  # ends the run as timed out if the turn never completes


def idle_event() -> asyncio.Event:
    """A thread's idle event as it starts: set, since no turn runs yet."""
    event = asyncio.Event()
    event.set()
    return event


@dataclass
class ManagedThread:
    title: str
    cwd: str
    thread_id: str = ""
    binding_id: str = ""
    token: str = field(default="", repr=False)
    # Turn state: the running turn's id ("" when idle) and its event, written only by CodexAdapter._on_event.
    active: str = ""
    idle: asyncio.Event = field(default_factory=idle_event)
    ended: deque[str] = field(default_factory=lambda: deque(maxlen=ENDED_KEPT), repr=False)
    runs: dict[str, Run] = field(default_factory=dict)
    waiters: dict[str, Waiter] = field(default_factory=dict)  # deliveries this process still owes an end
    seq: int = 0  # the last turn reading sent; the broker applies a reading only if its seq is newer
    # Held while inputs are handled or a reading is taken (pump, claim, registration): each is handled once,
    # in order, and every reading is numbered in the order of the inputs it follows.
    handling: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    taken: deque[ThreadInput] = field(default_factory=deque, repr=False)  # off the queue, not yet handled


Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class Rejection:
    """The broker's answer to a request no repeat can change: a client error, with its reason."""

    code: int
    reason: str


def refusal(err: urllib.error.HTTPError) -> Rejection:
    try:
        detail = json.loads(err.read()).get("detail")
    except (OSError, ValueError, AttributeError):
        detail = None
    return Rejection(err.code, str(detail or f"HTTP {err.code}")[:300])


class CodexAdapter:
    def __init__(
        self,
        broker: Broker,
        server: AppServer,
        threads: list[ManagedThread],
        state_path: Path,
        poll_seconds: float = 2.0,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.sleep = sleep
        self.broker = broker
        self.server = server
        self.threads = threads
        self.state_path = state_path
        self.tokens_path = state_path.with_name("codex-tokens.json")
        self.poll_seconds = poll_seconds
        self._pumps: dict[str, asyncio.Task[None]] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    def _load_state(self) -> dict[str, str]:
        try:
            data: dict[str, str] = json.loads(self.state_path.read_text(encoding="utf-8"))
            return data
        except (OSError, ValueError):
            return {}

    async def open_threads(self) -> None:
        """Resume each known thread in our own server, or start it once; record the ids."""
        state = self._load_state()
        for thread in self.threads:
            known = state.get(thread.title)
            if known:
                try:
                    await self.server.request("thread/resume", {"threadId": known, "excludeTurns": True})
                    thread.thread_id = known
                    continue
                except RpcError as err:
                    log.warning(
                        "could not resume %s (%s); starting a new thread",
                        thread.title,
                        err,
                    )
            started = await self.server.request(
                "thread/start",
                {
                    "cwd": thread.cwd,
                    "approvalPolicy": "never",
                    "sandbox": "workspace-write",
                    "serviceName": "duplex_chat",
                },
            )
            thread.thread_id = started["thread"]["id"]
            state[thread.title] = thread.thread_id
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(state, indent=1), encoding="utf-8")

    def _tokens(self) -> dict[str, str]:
        try:
            data: dict[str, str] = json.loads(self.tokens_path.read_text(encoding="utf-8"))
            return data
        except (OSError, ValueError):
            return {}

    def _keep_token(self, thread: ManagedThread) -> None:
        tokens = self._tokens()
        tokens[thread.thread_id] = thread.token
        self.tokens_path.parent.mkdir(parents=True, exist_ok=True)
        self.tokens_path.write_text(json.dumps(tokens, indent=1), encoding="utf-8")
        with contextlib.suppress(OSError):
            self.tokens_path.chmod(0o600)

    async def register(self, thread: ManagedThread) -> None:
        held = thread.token or self._tokens().get(thread.thread_id, "")
        # The broker's reset (turn state unknown, seq 0) and ours are one step under the lock: every reading
        # taken before it was sent before it, and every input received is handled after it, numbered from 1.
        async with thread.handling:
            result = await self.broker.call(
                "POST",
                "/api/adapter/register",
                {
                    "provider": "codex",
                    "native_id": thread.thread_id,
                    "title": thread.title,
                    "workspace": thread.cwd,
                    "endpoint_ref": "codex-app-server-stdio",
                    "capabilities": CAPABILITIES,
                    "current_token": held,
                },
                bearer=self.broker.enrollment_token,
            )
            thread.binding_id = result["binding"]["id"]
            thread.token = result["token"]
            self._keep_token(thread)
            thread.seq = 0
            await self._drain(thread)
            reading = self._reading(thread)
        self._track(thread)
        await self._report(thread, reading)

    def _spawn(self, work: Awaitable[None]) -> None:
        task = asyncio.ensure_future(work)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _track(self, thread: ManagedThread) -> None:
        """Start the one reader of this thread's turn notifications."""
        if thread.thread_id not in self._pumps:
            self._pumps[thread.thread_id] = asyncio.ensure_future(self._pump(thread))

    async def settle(self) -> None:
        """Wait for every reply in flight to be posted (tests, shutdown)."""
        while pending := [t for t in self._tasks if not t.done()]:
            await asyncio.gather(*pending, return_exceptions=True)

    async def close(self) -> None:
        watches = [r.watch for t in self.threads for r in t.runs.values() if r.watch]
        tasks = [*self._pumps.values(), *self._tasks, *watches]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._pumps.clear()

    async def _report(self, thread: ManagedThread, reading: dict[str, Any]) -> None:
        """Send a reading taken after a change. Every change is sent: a cache of what was sent could hold
        a reading the broker did not apply (a late answer, an older seq) and skip the next."""
        try:
            await self.broker.call("POST", "/api/adapter/turn", reading, bearer=thread.token)
        except (OSError, ValueError) as err:
            log.warning("%s: turn state not reported: %s", thread.title, err)

    @staticmethod
    def _reading(thread: ManagedThread) -> dict[str, Any]:
        """The thread's turn state now, numbered so the broker can tell it from an older one in flight.
        Taken only by the handler or under thread.handling, so the numbers follow the handled inputs.
        The count restarts at each registration; at one reading a second it would take about 285 million
        years to reach the broker's 2**53 limit."""
        thread.seq += 1
        return {"state": "busy" if thread.active else "idle", "waiting_for": None, "seq": thread.seq}

    def _run(self, thread: ManagedThread, turn_id: str) -> Run:
        return thread.runs.setdefault(turn_id, Run(turn_id))

    async def _pump(self, thread: ManagedThread) -> None:
        """Wake at each notification and handle what has arrived, as a claim also does."""
        events = self.server.events(thread.thread_id)
        while True:
            # Parked where _drain looks first: a claim draining meanwhile cannot handle a later one before it.
            thread.taken.append(await events.get())
            async with thread.handling:
                await self._drain(thread)

    async def _drain(self, thread: ManagedThread) -> None:
        """Handle every input received for this thread (notifications and start answers), in arrival
        order, each once. The caller holds thread.handling."""
        events = self.server.events(thread.thread_id)
        while thread.taken or not events.empty():
            event = thread.taken.popleft() if thread.taken else events.get_nowait()
            try:
                await self._on_event(thread, event)
            except Exception:  # one bad notification must not stop turn tracking
                log.exception("%s: notification not handled", thread.title)

    async def _on_event(self, thread: ManagedThread, event: ThreadInput) -> None:
        """The one writer of the turn state. A turn is active from its start answer or its turn/started,
        whichever is handled first, until its turn/completed, and an ended turn never becomes active again,
        so the state does not depend on the order in which the three arrive."""
        if isinstance(event, StartAnswer):
            method: Any = "turn/started"
            params: dict[str, Any] = {"turn": {"id": event.turn_id}}
        else:
            method, params = event.get("method"), event.get("params", {})
        if method == "turn/started":
            turn_id = params.get("turn", {}).get("id")
            if turn_id and turn_id != thread.active and turn_id not in thread.ended:
                thread.active = turn_id
                thread.idle.clear()
                await self._report(thread, self._reading(thread))
        elif method == "item/completed":
            item = params.get("item", {})
            if item.get("type") == "agentMessage" and item.get("text") and params.get("turnId"):
                self._run(thread, params["turnId"]).final = item["text"]
        elif method == "turn/completed":
            turn = params.get("turn", {})
            run = self._run(thread, turn.get("id", ""))
            run.status = str(turn.get("status"))
            if run.turn_id not in thread.ended:
                thread.ended.append(run.turn_id)
            if run.watch:
                run.watch.cancel()
            self._spawn(self._finish(thread, run))
            if thread.active == run.turn_id:
                thread.active = ""
                await self._report(thread, self._reading(thread))
                thread.idle.set()  # last, so whoever waits on idle finds the state already reported

    async def _finish(self, thread: ManagedThread, run: Run) -> None:
        """Answer the deliveries that rode this turn: one reply per conversation, under its first delivery."""
        waiters, run.waiters = run.waiters, []
        groups: dict[str, list[Waiter]] = {}
        for waiter in waiters:
            groups.setdefault(waiter.conversation_id, []).append(waiter)
        for group in groups.values():
            for waiter in group:
                await waiter.ready.wait()
            await self._answer(thread, run, group)
            for waiter in group:
                thread.waiters.pop(waiter.delivery_id, None)

    async def _answer(self, thread: ManagedThread, run: Run, group: list[Waiter]) -> None:
        if run.status == "completed" and run.final:
            refused = await self._post(thread, group[0], run.final)
            for waiter in group:
                if refused is None:
                    await self._state(thread, waiter.delivery_id, "responded")
                else:  # no reply stands in the broker, so the delivery was not answered
                    reason = f"the reply was refused: {refused.reason}"
                    await self._state(thread, waiter.delivery_id, "failed", reason=reason)
            return
        for waiter in group:
            await self._state(thread, waiter.delivery_id, "failed", reason=f"the turn ended {run.status}")
        if run.final:
            await self._post(thread, group[0], run.final)

    async def _watch(self, thread: ManagedThread, run: Run) -> None:
        await asyncio.sleep(TURN_TIMEOUT)
        if run.status is None:
            run.status = "timeout"
            await self._finish(thread, run)

    async def _state(
        self,
        thread: ManagedThread,
        delivery_id: str,
        state: str,
        turn_id: str | None = None,
        reason: str | None = None,
    ) -> Rejection | None:
        body: dict[str, Any] = {"state": state}
        if turn_id:
            body["native_turn_id"] = turn_id
        if reason:
            body["reason"] = reason
        return await self._acknowledged(
            "POST", f"/api/adapter/deliveries/{delivery_id}/state", body, thread.token
        )

    @staticmethod
    def _is_peer(message: dict[str, Any]) -> bool:
        """Owner messages and the broker's own notices go into a turn; any other binding is a peer."""
        actor = message["actor"]
        return not (actor["kind"] == "user" or (actor["kind"] == "binding" and actor["id"] == BROKER_ID))

    @staticmethod
    def _input(message: dict[str, Any]) -> dict[str, Any]:
        return {
            "clientUserMessageId": message["id"],
            "input": [{"type": "text", "text": message["text"], "text_elements": []}],
        }

    async def _start(self, thread: ManagedThread, message: dict[str, Any]) -> str:
        turn = await self.server.request("turn/start", {"threadId": thread.thread_id, **self._input(message)})
        turn_id: str = turn["turn"]["id"]
        if self._run(thread, turn_id).status is None:
            self.server.events(thread.thread_id).put_nowait(StartAnswer(turn_id))
        return turn_id

    async def _steer(self, thread: ManagedThread, message: dict[str, Any], running: str) -> str:
        answer = await self.server.request(
            "turn/steer",
            {"threadId": thread.thread_id, "expectedTurnId": running, **self._input(message)},
        )
        turn_id: str = answer["turnId"]
        return turn_id

    async def _end_running_turn(self, thread: ManagedThread, interrupt: bool) -> None:
        """Wait until no turn runs, interrupting it first when asked."""
        if interrupt and thread.active:
            with contextlib.suppress(RpcError):  # the turn may have ended on its own meanwhile
                await self.server.request(
                    "turn/interrupt",
                    {"threadId": thread.thread_id, "turnId": thread.active},
                )
        await asyncio.wait_for(thread.idle.wait(), INTERRUPT_TIMEOUT if interrupt else TURN_TIMEOUT)

    async def _deliver(self, thread: ManagedThread, mode: str, message: dict[str, Any]) -> str:
        """Put one message into the thread as its send mode says; the id of the turn it joined."""
        running = thread.active
        if mode == "steer" and not running:
            raise Refused(f"steer needs a running turn, and none runs (supports: {', '.join(SEND_MODES)})")
        if running and mode in ("auto", "steer"):
            try:
                return await self._steer(thread, message, running)
            except RpcError as err:
                if mode == "steer":
                    raise Refused(f"the turn ended before the steer arrived ({err})") from err
                # auto: the turn ended in between, so this is the queue case
        if running and mode in ("auto", "queue", "restart"):
            await self._end_running_turn(thread, interrupt=mode == "restart")
        return await self._start(thread, message)

    async def handle_one(self, thread: ManagedThread) -> bool:
        """Claim one message for this thread and put it into a turn. True if one was taken."""
        self._track(thread)
        # The reading rides every claim, so a lost change report cannot leave the broker's view stale; it
        # includes every notification received before the claim, handled here if the pump has not yet.
        async with thread.handling:
            await self._drain(thread)
            reading = self._reading(thread)
        claimed = await self.broker.call("POST", "/api/adapter/claim", {"turn": reading}, bearer=thread.token)
        if not claimed:
            return False
        delivery, message = claimed["delivery"], claimed["message"]
        if self._is_peer(message):
            await self._state(
                thread, delivery["id"], "failed", reason="peer messages need the bridge tool path"
            )
            log.warning(
                "refused a peer message to %s: peer delivery needs the bridge tool path",
                thread.title,
            )
            return True
        mode = message.get("mode", "auto")
        try:
            if mode not in SEND_MODES:
                raise Refused(f"mode {mode} is not supported (supports: {', '.join(SEND_MODES)})")
            turn_id = await self._deliver(thread, mode, message)
        except Refused as err:
            await self._state(thread, delivery["id"], "failed", reason=str(err))
            return True
        except (RpcError, TimeoutError) as err:
            await self._state(thread, delivery["id"], "failed", reason=f"the app-server refused it: {err}")
            return True
        # The waiter goes on the turn before the broker hears "accepted", so a lost answer cannot orphan it.
        waiter = Waiter(delivery["id"], claimed["conversation"]["id"], message["id"])
        run = self._run(thread, turn_id)
        run.waiters.append(waiter)
        thread.waiters[waiter.delivery_id] = waiter
        if run.status is not None:
            self._spawn(self._finish(thread, run))
        elif run.watch is None:
            run.watch = asyncio.ensure_future(self._watch(thread, run))
        await self._state(thread, delivery["id"], "accepted", turn_id)
        waiter.ready.set()
        return True

    async def _post(self, thread: ManagedThread, waiter: Waiter, text: str) -> Rejection | None:
        """The delivery's reply; its id comes from the delivery, so a repeat is not a second reply."""
        body = {
            "conversation_id": waiter.conversation_id,
            "client_message_id": f"reply-{waiter.delivery_id}",
            "text": text,
            "parent_id": waiter.message_id,
        }
        return await self._acknowledged("POST", "/api/adapter/messages", body, thread.token)

    async def _acknowledged(self, method: str, path: str, body: Any, token: str) -> Rejection | None:
        """Send until the broker takes it (None) or refuses it with a client error no repeat changes."""
        attempt = 0
        while True:
            try:
                await self.broker.call(method, path, body, bearer=token)
                return None
            except urllib.error.HTTPError as err:
                if 400 <= err.code < 500 and err.code not in (408, 429):
                    log.warning("%s refused: %s", path, err.code)
                    return refusal(err)
            except (OSError, ValueError, TimeoutError):
                pass
            await self.sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
            attempt += 1

    async def reconcile(self, thread: ManagedThread, in_flight: list[dict[str, Any]]) -> None:
        """Square the broker's dispatching and accepted deliveries with the turns this process holds:
        one riding a live run is acknowledged again, any other can no longer be answered and ends failed."""
        for delivery in in_flight:
            waiter = thread.waiters.get(delivery["id"])
            if waiter is None:
                await self._state(
                    thread, delivery["id"], "failed", reason="the adapter no longer holds this turn"
                )
            elif delivery["state"] == "dispatching":
                await self._state(thread, delivery["id"], "accepted", delivery.get("native_turn_id"))
                waiter.ready.set()

    async def _reconcile_with_broker(self, thread: ManagedThread) -> None:
        """Square the broker's open deliveries with this process; an error waits for the next recovery,
        since it must not stop the serve loop."""
        try:
            listed = await self.broker.call(
                "GET", "/api/adapter/deliveries?state=dispatching,accepted", bearer=thread.token
            )
            await self.reconcile(thread, listed or [])
        except (OSError, ValueError, KeyError, TimeoutError) as err:
            log.warning("%s: deliveries not reconciled: %s", thread.title, err)

    async def serve_thread(self, thread: ManagedThread, sleep: Sleep = asyncio.sleep) -> None:
        attempt = 0
        unsettled = True  # at start and after every failure, the broker's view of our deliveries is checked
        while True:
            try:
                if not thread.token:
                    unsettled = True
                    await self.register(thread)
                    log.info("registered %s as %s", thread.title, thread.binding_id)
                if unsettled:
                    unsettled = False
                    await self._reconcile_with_broker(thread)
                took = await self.handle_one(thread)
                attempt = 0
                if not took:
                    await sleep(self.poll_seconds)
            except asyncio.CancelledError:
                raise
            except urllib.error.HTTPError as err:
                if err.code in (401, 403):
                    thread.token = ""
                unsettled = True
                await sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
                attempt += 1
            except (OSError, ValueError, KeyError, RpcError, TimeoutError) as err:
                log.warning("%s: %s", thread.title, err)
                unsettled = True
                await sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
                attempt += 1

    async def run(self) -> None:
        await self.open_threads()
        try:
            await asyncio.gather(*(self.serve_thread(t) for t in self.threads))
        finally:
            await self.close()


def load(local: Path = LOCAL) -> tuple[Broker, list[ManagedThread]]:
    adapter = json.loads((local / "adapter.json").read_text(encoding="utf-8"))
    config = json.loads((local / "codex.json").read_text(encoding="utf-8"))
    threads = [ManagedThread(str(t["title"]), str(t["cwd"])) for t in config["threads"]]
    return Broker(adapter["broker_url"], adapter["enrollment_token"]), threads


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    broker, threads = load()
    server = AppServer([find_codex(os.environ), "app-server", "--listen", "stdio://"], LOCAL)
    await server.start()
    try:
        await CodexAdapter(broker, server, threads, LOCAL / "codex-threads.json").run()
    finally:
        with contextlib.suppress(Exception):
            await server.close()


if __name__ == "__main__":
    asyncio.run(main())

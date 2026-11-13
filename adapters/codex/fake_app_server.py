"""A scripted stand-in for `codex app-server --listen stdio://`, for tests.

Answers initialize, thread/start, thread/resume, turn/start, turn/steer and turn/interrupt. A turn
announces itself with turn/started and replies "echo: <input>" through item/completed and turn/completed.
Input containing FAIL ends the turn `failed`; BIG answers with 70000 bytes. Input containing HOLD keeps
the turn running for HOLD_SECONDS (or until it is interrupted); a steer joins the running turn's reply,
and a steer containing REFUSE is refused. A thread runs one turn at a time: turn/start while one runs is
an error. `test/notify` sends its `frame` param as a notification, then answers, so a test can put any frame
on the socket. Every request is appended to the file named by argv[1]; argv[2] lists the thread ids it knows.
"""

import json
import os
import sys
import threading
from dataclasses import dataclass, field
from typing import Any

log_path = sys.argv[1] if len(sys.argv) > 1 else ""
known = set(filter(None, (sys.argv[2] if len(sys.argv) > 2 else "").split(",")))
counter = 0
HOLD_SECONDS = 1.0
lock = threading.Lock()


@dataclass
class Turn:
    id: str
    texts: list[str]
    timer: threading.Timer | None = field(default=None)


running: dict[str, Turn] = {}  # thread id -> its running turn


def send(message: dict[str, Any]) -> None:
    with lock:
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()


def end_turn(thread_id: str, status: str | None = None) -> None:
    """Finish the thread's running turn: its reply is the joined input, unless it was interrupted."""
    turn = running.pop(thread_id, None)
    if turn is None:
        return
    text = "echo: " + " | ".join(turn.texts)
    if "BIG" in text:
        text = "x" * 70000  # over the broker's message limit
    if status is None:
        status = "failed" if "FAIL" in text else "completed"
    if status != "interrupted":
        send(
            {
                "method": "item/completed",
                "params": {
                    "threadId": thread_id,
                    "turnId": turn.id,
                    "item": {"type": "agentMessage", "id": "i1", "text": text},
                },
            }
        )
    send(
        {
            "method": "turn/completed",
            "params": {"threadId": thread_id, "turn": {"id": turn.id, "status": status}},
        }
    )


for raw in sys.stdin:
    message = json.loads(raw)
    method, rid, params = (
        message.get("method"),
        message.get("id"),
        message.get("params", {}),
    )
    if log_path and rid is not None:
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"method": method, "params": params}) + "\n")
    if method == "initialize" and log_path:
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"method": "env", "keys": sorted(os.environ)}) + "\n")
    if rid is None:
        continue
    counter += 1
    if method == "initialize":
        send({"id": rid, "result": {"userAgent": "fake"}})
    elif method == "thread/start":
        thread_id = f"t-{counter}"
        known.add(thread_id)
        send({"id": rid, "result": {"thread": {"id": thread_id}}})
    elif method == "thread/resume":
        if params["threadId"] in known:
            send({"id": rid, "result": {"thread": {"id": params["threadId"]}}})
        else:
            send({"id": rid, "error": {"code": -32600, "message": "no such thread"}})
    elif method == "turn/start":
        thread_id, turn_id = params["threadId"], f"u-{counter}"
        if thread_id in running:
            send({"id": rid, "error": {"code": -32600, "message": "a turn is already running"}})
            continue
        turn = Turn(turn_id, [params["input"][0]["text"]])
        running[thread_id] = turn
        send({"id": rid, "result": {"turn": {"id": turn_id, "status": "inProgress"}}})
        send(
            {
                "method": "turn/started",
                "params": {"threadId": thread_id, "turn": {"id": turn_id, "status": "inProgress"}},
            }
        )
        if "HOLD" in turn.texts[0]:
            turn.timer = threading.Timer(HOLD_SECONDS, end_turn, [thread_id])
            turn.timer.daemon = True
            turn.timer.start()
        else:
            end_turn(thread_id)
    elif method == "turn/steer":
        live = running.get(params["threadId"])
        text = params["input"][0]["text"]
        if live is None or live.id != params["expectedTurnId"] or "REFUSE" in text:
            send({"id": rid, "error": {"code": -32600, "message": "no steerable turn matches"}})
        else:
            live.texts.append(text)
            send({"id": rid, "result": {"turnId": live.id}})
    elif method == "test/notify":
        send(params["frame"])
        send({"id": rid, "result": {}})
    elif method == "turn/interrupt":
        live = running.get(params["threadId"])
        if live is None or live.id != params["turnId"]:
            send({"id": rid, "error": {"code": -32600, "message": "no such running turn"}})
        else:
            if live.timer:
                live.timer.cancel()
            send({"id": rid, "result": {}})
            end_turn(params["threadId"], "interrupted")
    else:
        send({"id": rid, "error": {"code": -32601, "message": f"unknown {method}"}})

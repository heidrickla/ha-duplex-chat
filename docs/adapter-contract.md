# Broker and adapter contract

This file is the interface between the broker (`duplex_broker/`, Python), the `duplex` adapter (`crates/duplex`, Rust), the Codex app-server adapter (`adapters/codex/duplex_codex.py`), the OpenCode adapter (`duplex opencode`, in `crates/duplex`) and the waker (`crates/duplex-waker`, Rust). A change to it reaches every implementer in the same change.

IDs on the adapter API are shown hyphenated and accepted either way, as for messages.

## Send modes

- Modes: `auto` (start an idle session, steer a running one, else queue), `queue` (a new turn after the current one), `steer` (into the running turn), `restart` (interrupt the running turn, then start with this message).
- `messages` has `mode TEXT NOT NULL DEFAULT 'auto'` with a CHECK over the four.
- An adapter declares what it can do at registration: `capabilities.send_modes`, a list that includes `auto`. A binding without the key supports `["auto"]` only. The broker never downgrades a mode.
- Post time (adapter `POST /api/adapter/messages`, owner `POST /api/conversations/{id}/messages`, task notices): optional `mode` (default `auto`). For each recipient whose `send_modes` lacks the mode, the post is refused with 422 and `error: "mode_unsupported"`, and the detail names each such recipient with the modes it does support, for example: `steer is not supported by "Reviewer" (supports: auto, queue); restart is not supported by "Builder" (supports: auto, queue, steer)`. A refused post creates no message.
- `steer` to a recipient whose reported turn state is `idle` is refused the same way, with the reason `"<title>" has no running turn (supports: ...)`.
- Claim: the claimed message body carries `mode`.
- Hold: a binding that reports turn state (below) and is `busy` or `waiting` is handed no delivery whose message mode is `queue` until it reports `idle` or `unknown`. `claim_next` stays FIFO: a held queue-mode delivery holds the deliveries behind it. Nothing enters the wake ladder while held, since nothing is handed over.
- Claim gate: while the binding's oldest in-flight delivery is `dispatching` (handed over, not yet confirmed), nothing more is handed out. While it is `accepted` (taken into a running turn), the queued head is handed out when its mode is `auto`, `steer` or `restart` and the binding declares `steer` in `send_modes`; a `queue` head, or any head for a binding without `steer`, waits.
- Adapter at delivery: a mode it cannot do now (steer while idle, a mode it never declared) is ended `failed` with a reason naming the modes it supports, which sends the sender the usual failure notice.
- Declared modes: a Claude session (inbox) `["auto", "queue", "steer"]`: auto and steer write the inbox (it starts an idle session and drains at the next tool round of a running turn); queue writes the inbox once the broker hands it over (after the turn); restart is never declared, since an MCP server has no route to interrupt its session. A Codex desktop task (`codex queue`) `["auto", "queue"]`: auto and queue both run `codex queue --resume`, which starts an idle loaded thread and otherwise runs after the current turn; steer and restart are never declared, since only the desktop app's own app-server can act on its turns. `duplex_codex.py` (its own app-server) `["auto", "queue", "steer", "restart"]` through `turn/start`, `turn/steer` (with `expectedTurnId`) and `turn/interrupt` then `turn/start`. `duplex opencode` (the OpenCode app's server) `["auto", "queue", "steer", "restart"]`: auto is a prompt with `delivery: "steer"` (it starts an idle session and joins a running one at its next step); steer is the same prompt, sent only while the session runs; queue is a prompt with `delivery: "queue"` (it starts an idle session and otherwise gets a round of its own after the running one stops); restart is `interrupt?resume=false`, a wait until the session stops, then a steer prompt.

## Turn state

- `POST /api/adapter/turn` `{"state": "busy" | "idle" | "waiting" | "unknown", "waiting_for": "<kind>" | null, "seq": n}`, sent on each change and again after registration. `waiting_for` matches `^[a-z][a-z0-9_]{0,39}$` or is null; anything else is refused 422. It is a kind of wait (the Claude session record's `waitingFor` normalized to lowercase with non-alphanumerics as `_`, cut to 40), never prompt text or tool input.
- Stored on the binding: `turn_state`, `turn_waiting_for`, `turn_state_at`, `turn_seq`. Registration resets them to `unknown` and 0.
- Claim: `POST /api/adapter/claim` takes an optional body `{"turn": {"state": ..., "waiting_for": ..., "seq": n}}` with the adapter's current reading; the broker applies it exactly as `POST /api/adapter/turn` does (observed questions included) before it evaluates the queue hold. Every adapter that declares `turn_state` sends it on every claim.
- Claude adapter source: its own `~/.claude/sessions/<pid>.json` (`status`, `waitingFor`, `statusUpdatedAt`; status values seen in Claude Code 2.1.321: busy, idle, waiting, blocked; `blocked` is reported as `waiting` with `waiting_for` from the record or `blocked`). Codex task source: its thread's rollout `~/.codex/sessions/YYYY/MM/DD/rollout-*-<thread id>.jsonl`: `event_msg` `task_started` is busy; `task_complete` and `turn_aborted` are idle. Codex never reports `waiting` (no approval event is written to a rollout). `duplex_codex.py` source: its own app-server: a turn starts at its `turn/start` answer or its `turn/started` notification, whichever the adapter's one handler takes first, and ends at its `turn/completed`; an ended turn never starts again. `duplex opencode` source: the OpenCode server's list of running sessions (`GET /api/session/active`), read for every reading: listed is busy, absent is idle, and a server that does not answer is `unknown`. OpenCode never reports `waiting`: its sessions run with every permission allowed (OpenCode adapter).
- Readings are ordered: a reading carries `seq`, an integer the adapter raises with every reading it sends and resets when it registers. The broker validates the reading first (a bad `state` or `waiting_for` is 422 even when unchanged), then in one write transaction applies it only when the binding's `lease_generation` is the caller's and `seq` is greater than the stored `turn_seq`; an older or equal reading changes nothing and is answered 200 with `applied: false`. A reading without `seq` is refused 422.
- An adapter keeps no copy of what it last reported: it sends every observed change, and every claim carries the current reading.

## Delegated tasks

Tables:

- `tasks`: `id`, `requester_id` (binding), `assignee_id` (binding), `parent_id` (task, nullable), `conversation_id`, `task` (text), `done_when` (text), `status` (`working`, `waiting_for_children`, `result_available`, `failed`, `cancelled`), `outcome` (`done` | `failed` | null), `result` (text, nullable), `held_result` (a result reported while children were live), `delivered_at`, `created_at`, `updated_at`, `ended_at`.
- `task_events`: `task_id`, `seq`, `type`, `actor_kind`, `actor_id`, `payload` (JSON), `created_at`: every transition, question, answer, withdrawal, nudge and delivery.
- `task_questions`: `id`, `task_id`, `kind` (`asked` | `observed`), `text`, `state` (`open`, `answered`, `withdrawn`), `answer`, `notice_message_id`, `created_at`, `closed_at`.

Rules:

- An adapter that can act on task notices declares `capabilities.tasks: true`. Delegating to a binding without it is refused 422 `tasks_unsupported` naming the binding. The Codex app-server adapter does not declare it.
- Requester and assignee are active `claude` or `codex` bindings and differ. A child task names `parent_task_id`, and only that parent's assignee may create it. `to` matches a binding id by its normalized form and a title by the raw input; when both match different bindings the request is refused as ambiguous.
- Each task gets a group conversation, title `Task: <first 60 characters of the task>`, owner the broker's owner, mode `user_directed`, members the requester and the assignee. Peers do not post in it; every notice in it is a broker message with a stable `client_message_id` (`task:<id>:assigned`, `task:<id>:result`, `task:<id>:cancelled`, `task:<id>:question:<qid>`, `task:<id>:answer:<qid>`, `task:<id>:nudge`), so a repeat is a no-op and the existing delivery, wake, receipt and ladder machinery carries it. Each notice's envelope `notice` names `kind` (`task_assigned`, `task_result`, `task_cancelled`, `task_question`, `task_answer`, `task_nudge`), `task_id` and, for questions, `question_id`.
- Assigned notice to the assignee: the task text, done-when, requester title, task id, and the tools to use (report, ask). The task text is the requester's: it carries no authority beyond the owner's grant, and the adapter's `policy.rules` for a task notice says so.
- Report (assignee): `outcome` done or failed, `result` text. With no live child (status `working` or `waiting_for_children`) the status becomes `result_available` (done) or `failed`, `ended_at` is set, and the result notice goes to the requester. With live children the result is kept in `held_result`, status `waiting_for_children`; when the last live child ends (result_available, failed or cancelled), the held result is released the same way. A second report on an ended task is 409.
- Reading: the requester's `GET /api/adapter/tasks/{id}` on a `result_available` or `failed` task sets `delivered_at` (first read only) and returns the result. Delivery is at least once: until that read, the result notice follows the ladder like any message, and `GET /api/adapter/tasks?role=requester` lists the task as undelivered.
- Cancel (requester, or owner): the task and every descendant not already ended become `cancelled`; each such task's assignee gets the cancelled notice; queued deliveries of that task's earlier notices to its assignee are cancelled; open questions are withdrawn. Cancelling an ended task is 409.
- Nudge: when an assignee reports `idle` while it holds a `working` task with no live child and no report, the broker posts the nudge notice to it once per task ("Task <id> is open: report a result with bridge_task_report, or ask with bridge_task_ask").
- Questions: the assignee asks (`kind asked`, its text), the broker posts the question notice to the requester. The requester answers: state `answered`, the answer notice goes to the assignee. The assignee withdraws: state `withdrawn`. Answering a closed question is 409.
- Observed questions: when an assignee's turn state becomes `waiting`, each of its `working` tasks gets one open question of kind `observed` with the text `"<assignee title> is waiting for <waiting_for> on task <id>; only the owner or that session can clear it."` and the notice goes to the requester. When the state leaves `waiting`, those observed questions are withdrawn.
- Withdrawal of a pending wake: when a question closes (answered elsewhere, withdrawn, its task ended) while its notice's delivery to the requester is `queued`, that delivery is cancelled (reason "question withdrawn" or "question answered"). A `dispatching` or `accepted` one is left to run; its read shows the question's closed state.
- A refused notice: an `assigned` notice that admission refuses ends the task `failed` with the reason and wakes the requester through the normal result path. Refused `result`, `question`, `answer`, `cancelled` and `nudge` notices are recorded only (a task event `notice_refused`), since their data stays readable through the task status.

Adapter API (binding bearer token):

| Method | Path | Body | Who | Answer |
|---|---|---|---|---|
| POST | `/api/adapter/tasks` | `to` (binding id or exact title), `task` (1-20000), `done_when` (1-2000), `parent_task_id`?, `mode`? | any | 201 task view |
| GET | `/api/adapter/tasks` | query `role` requester or assignee or both (default both), `open` 1 or 0 (default 1) | own | list of task views |
| GET | `/api/adapter/tasks/{id}` | | requester or assignee | task view with children (id, status), open questions, events count; the result when ended |
| POST | `/api/adapter/tasks/{id}/cancel` | `reason`? | requester | task view |
| POST | `/api/adapter/tasks/{id}/report` | `outcome`, `result` (1-50000) | assignee | task view |
| POST | `/api/adapter/tasks/{id}/questions` | `question` (1-4000) | assignee | question |
| POST | `/api/adapter/questions/{id}/answer` | `answer` (1-20000) | requester | question |
| POST | `/api/adapter/questions/{id}/withdraw` | | assignee | question |

Task view: `id`, `requester` and `assignee` (`{id, title}`), `parent_task_id`, `conversation_id`, `task`, `done_when`, `status`, `outcome`, `result` (requester or assignee, once ended), `delivered_at`, `children` (`[{id, status}]`), `questions` (open ones: `id`, `kind`, `text`, `state`), `created_at`, `updated_at`, `ended_at` (ISO UTC).

Owner API: `GET /api/tasks?open=1|0`, `GET /api/tasks/{id}` (view plus `events`), `POST /api/tasks/{id}/cancel`; `duplex owner tasks [--all]`, `task <id>`, `task-cancel <id>`.

## Usage-limit resume

- Stored in `scheduled_sends` (kind `limit_resume`, see Scheduled sends below): `id` (`<binding id>:<run id>:<reset epoch seconds>`), `binding_id`, `run_id`, `source` (`claude-transcript` | `codex-rollout`), `matched` (the matched signal, at most 300 characters, no other transcript text), `failed_at`, `reset_at`, `state` (`armed`, `sent`, `skipped`, `dropped`, `not_armed`), `reason`, `message_id`, `created_at`, `updated_at`.
- `POST /api/adapter/limits` `{run_id, source, matched, failed_at, reset_at}` (epoch seconds, floats): insert or ignore by id (a repeat answers the existing row, 200; new 201). `reset_at <= failed_at` stores `not_armed` with reason "reset already past at the failure" and never sends.
- `POST /api/adapter/limits/{id}/skip` `{reason}`: an `armed` row becomes `skipped` (the adapter saw a new user turn after the failure: reason "answered").
- Sweep (the broker's periodic sweep): an `armed` row whose `reset_at` has passed: more than 24 h overdue becomes `dropped`; binding retired or inactive becomes `dropped` (reason); every task the binding was assigned and still open at `failed_at` now `cancelled` (and it had at least one) becomes `skipped` (reason "task cancelled"); otherwise the broker posts "Continue where you left off." into the owner's direct conversation with that binding, as a broker message with `client_message_id` `limit-resume:<id>` and mode `auto`, and the row becomes `sent` with `message_id`. One post per id, ever.
- `GET /api/limits` (owner) lists rows; `duplex owner limits`.
- Adapter detection: Claude: the session transcript `~/.claude/projects/<cwd slug>/<session id>.jsonl` (slug: the cwd with each character outside `[A-Za-z0-9]` replaced by `-`), a line with `isApiErrorMessage: true` and `error: "rate_limit"`; run id is the line's `uuid`; `failed_at` its `timestamp`; reset from the text `resets <time> (<IANA zone>)` (forms seen: `resets 8pm (America/Chicago)`, `resets Oct 30, 4:59pm (America/Chicago)`): the first such instant after `failed_at` in that zone. A later `type: user` line that is not a tool result and not `isMeta` skips the resume ("answered"). Codex: the rollout, `event_msg` `task_complete` with `error.codex_error_info == "usage_limit_exceeded"`; run id is `turn_id`; reset is the latest `token_count` `rate_limits` snapshot whose `limit_id` is `codex` (model-specific buckets never overwrite it; primary is the 5 h window, secondary the week) with a window at `used_percent >= 100` and a `resets_at`, else the message's `try again at <Mon> <Dth>, <YYYY> <h:mm> <AM|PM>` read as the machine's local time; a later `task_started` skips it ("answered").
- A reset text's civil time is checked part by part (a date that exists, hour 1 to 12 with AM or PM, minute 0 to 59) and refused when out of range. A repeated hour names both of its instants and the first after the failure is taken; a time a clock change skips is refused, never moved.
- Only a line written after the adapter started can report a failure; one the transcript held before is history.
- Resume of a stopped process needs the waker (below): a closed session has no adapter at the reset, so without it the resume waits in its queue until the session opens again.

## Notice tool names

Adapter tools: `bridge_task_delegate`, `bridge_task_status`, `bridge_task_cancel`, `bridge_task_report`, `bridge_task_ask`, `bridge_task_answer`, `bridge_task_withdraw`, and `bridge_send` with `mode`. The assigned and nudge notices name `bridge_task_report` and `bridge_task_ask`; the result notice names `bridge_task_status`; a question notice names `bridge_task_answer`; the cancelled notice says to stop work on the task and report nothing.

## Scheduled sends

The usage-limit resume is stored as one kind of scheduled send, so later kinds (sends at a time, on a recurrence, on a webhook) add kinds and columns without rewriting the table: table `scheduled_sends` (`id`, `kind` (`limit_resume` now), `binding_id`, `text`, `mode`, `due_at`, `state` (`armed`, `sent`, `skipped`, `dropped`, `not_armed`), `reason`, `message_id`, `details` JSON (for `limit_resume`: `run_id`, `source`, `matched`, `failed_at`), `created_at`, `updated_at`). The `/api/adapter/limits` routes and `GET /api/limits` stay as the limit_resume front; the posted message's envelope notice is `{"kind": "limit_resume", "resume_id": ...}`.

## Health

`GET /health` answers `{"ok": true, "version", "schema": <running schema version>, "ready": bool}`: `ready` is true only once migrations have finished and the listeners are bound; before that, false.

## Delivery admission

- One function `admit(target, mode, kind)` inside the one place every delivery is created, for every target and every message, broker notices and scheduled sends included. It refuses a mode the target does not declare, and refuses a task-kind notice (assigned, nudge, result, question, answer, cancelled) to a target without `capabilities.tasks: true`. An API caller gets 422 (`mode_unsupported` or `tasks_unsupported`) and nothing is created.
- On registration or re-registration, in the same transaction, every queued delivery to that binding is admitted again against the new capabilities; one that no longer passes ends `failed` with the reason, and its task or scheduled send records it.
- Every transition of a delivery into `queued` (creation, re-admission, a waker requeue) goes through one function that admits it against the target's current declaration. A guard test fails when any other code writes that state.

## Waker

- Broker option `waker_token` (empty disables the waker routes, which then answer 404). Auth `Authorization: Bearer <token>`, compared in constant time.
- `GET /api/waker/requests`: one request per delivery in state `queued` whose message is a broker scheduled send (envelope notice kind `limit_resume`) to an active claude or codex binding that is not live (no claim poll or stream within 90 s; after a broker restart a binding counts as live for its first 90 s, so nothing is listed until then), with no start recorded for it in the last 15 minutes and fewer than 3 failed starts. Request: `id` (the delivery id), `binding_id`, `provider`, `native_id` (Claude session id or Codex thread id), `title`, `workspace`, `reason` (`limit_resume`), `message_id`, `due_at` (ISO UTC).
- `POST /api/waker/requests/{id}/started` `{"report_id": "...", "ok": bool, "route": "<route name>", "detail": "<at most 300 characters>"}`: stores a row in `waker_starts` (`request_id`, `binding_id`, `at`, `ok`, `route`, `detail`). The third failed start for one request posts one notice to the owner.
- `report_id` is `<request id>:<waker start ms>:<n>` (the waker process's creation time in UTC milliseconds, and the report's 1-based sequence for that request within the process). The broker treats it as an opaque string that must begin with `<request id>:`, at most 120 characters of `[A-Za-z0-9:-]`; it is required (422 without it). The broker keeps one `waker_starts` row per `report_id` and answers a repeat with 200 as taken, changing nothing. Reports are at-least-once on the wire and once in the record.
- Each request has an ordered report queue that is never replaced; a later report waits until the earlier one is taken; a 4xx drops only that report.
- The waker holds only its own token, in `~/.duplex-chat/waker.json` (`broker_url`, `token`), and acts only on a listed request. An unreachable broker means it does nothing.
- Liveness fails closed: a Claude session counts as closed only when the session records directory was read without error, no live record holds it, and no running `claude.exe` command line names the session id; any read error means "cannot establish", which skips the start and reports `ok: false`. The check runs again immediately before the launch.
- Run exclusion: liveness is held for the whole run: while its child runs, the waker watches every second for any other `claude.exe` whose command line names the session; on one it kills its child at once and reports `ok: false` with "the session was opened while the waker ran it".

## Child environments

Every process an adapter or the waker starts (`claude`, `codex queue`, `codex app-server`, `codex --version`) gets an environment built from an allowlist of named non-credential variables, applied to the final merged map and matched in any case; an addition whose name is not on the allowlist is refused at construction. No `*_API_KEY`, `*_TOKEN` or `ANTHROPIC_*`/`OPENAI_*` variable reaches a child. Every spawn site has a test that runs a real child and checks the names it sees.

## Name resolution

Every place that turns a typed session name into a binding, `duplex owner` included, uses the broker's rule (`ids.match_sessions`: an id, an exact title, or a unique provider), by the same logic or by asking the broker to resolve; no second comparison exists.

## Codex app-server adapter

- Delivery classes: one classification at the claim: a user (owner) message and a broker notice are put into a turn by their mode; a peer (binding) message is refused to the tool path. Task-kind notices never reach it (admission).
- Replies reconcile: the waiter is attached before the broker is told `accepted`; a turn joined by several deliveries posts one reply per conversation, under the first delivery that rode it there, and every delivery that rode it becomes `responded` on that post; a reply's `client_message_id` is derived from that delivery's id (`reply-<delivery id>`), so a retry cannot duplicate it; state changes and the reply post retry with backoff until the broker acknowledges or answers a terminal error; on start, the adapter reconciles every delivery the broker lists as dispatching or accepted to it against the app-server's thread and turn state.
- Reply outcome: posting a reply has an explicit outcome; the delivery is marked responded only on a confirmed post; a refused reply fails the delivery with the broker's reason, and the sender gets the normal failure notice. The session adapter shows a refused reply to the replying session as a refusal.
- Turn-state ownership: the turn reading (`state`, `waiting_for`, `seq`) and the adapter state it is taken from, a thread's `active`, `idle`, `ended` and `seq`, are written by the one handler (`_on_event`); every other read, every reading taken and every `seq` reset happens under the thread's `handling` lock, after the handler has taken every input received, except the residual below. A test fails on any other touch not on its allowlist.
- Registration: the broker's reset and the adapter's are one step under the lock. Every reading taken before the registration was sent before it, and every input received is handled after the reset and counted from 1, so no reading taken before a registration is applied over one taken after it.
- A run's `status`, `timeout` from `_watch` included, decides a delivery's outcome and answer and never a reading.
- Residual: the adapter chooses a send mode's route from the turn its claim handled, so the turn can end between the read and the request reaching the app-server. The app-server's refusal is the authority: a refused `turn/interrupt` is suppressed and the wait for the turn's end goes on; a refused `turn/steer` becomes a new turn after the ended one for `auto` and fails the delivery with its reason for `steer`.

## OpenCode adapter

- Server: `duplex opencode` attaches to the OpenCode desktop app's own server and starts none. The app writes its address and password to `service.json` in OpenCode's state folder (`$XDG_STATE_HOME/opencode`, else `~/.local/state/opencode`; the password also in the config folder's `service.json`) at each launch; the adapter reads the file on every call, so a relaunch on another port with another password is followed from the next call. The password is sent only to that server, as Basic auth (user `opencode`), and never reaches a log line, an error, the broker or a reply. While the app is closed nothing is claimed and the turn reads `unknown`; messages wait in the broker.
- Sessions: one per entry in `~/.duplex-chat/opencode.json` (`{"sessions": [{"title", "cwd", "model": "<provider>/<model>"}]}`). The adapter keeps each session's id in `opencode-sessions.json`, holds the same session after a restart, gives it back its model and permission rules when they differ, and creates it again (a new binding) when OpenCode no longer has it. Each session registers as provider `opencode` with its OpenCode session id as `native_id`.
- Full access: each session carries its own rules, which OpenCode applies after the agent's: allow every action on every resource, and deny the `question` tool, since nobody answers a form in a duplex-woken turn. A permission request that is still raised is answered `once` on the adapter's next pass.
- Delivery classes: as for the Codex app-server adapter, owner messages and broker notices become inputs by their mode, and a peer message is refused `failed` ("peer messages need the bridge tool path").
- Replies: an input's answer is read from the session's durable message log, not its event stream: the round that took the input in ends at the assistant message that stops, or at the `idle` record that ends the execution with its outcome. A steered input shares the round it joined; a queued one has its own. One reply per round and conversation, under the first delivery that rode it there (`reply-<delivery id>`); a round that ended `interrupted` or `failed`, or ran past 30 minutes, fails its deliveries with that reason. The waiter is held before the broker hears `accepted`, a lost state or reply post is sent again on the next pass, and after registration or a lost broker the deliveries the broker lists as dispatching or accepted are squared with the waiters.
- Residual: the adapter chooses a mode's route from the running list it read just before, so a round can end between the read and the prompt. OpenCode takes a steer to a session that has just stopped as a new round and does not refuse it, so such a steer is answered by a round of its own; a refused interrupt is suppressed and the wait for the session to stop decides.

## Schema and version

Schema 14 holds every table and column above; it widened the binding provider CHECK to admit `opencode`. The broker, `duplex` and `duplex-waker` share one release number (`__init__`, `pyproject.toml`, `config.yaml`, `CHANGELOG.md` and the crates move together; `test_version.py` checks the broker's). The `duplex` adapter's own version and the Claude Code and Codex releases it was reviewed against are in `crates/duplex/duplex-compat.json`; registration carries them, and the broker reads an adapter older than the newest active one as behind.

## Clause coverage

A change to this contract reaches every implementer, because each clause an implementer must meet is listed here with the implementers it binds, and each (clause, implementer) pair names the test that proves it. A clause that does not bind an implementer says why (`n/a: <reason>`). Rules that only the broker enforces are not clauses here; the broker's own tests hold them.

`tools/contract_coverage.py` checks this section: it exits 1, naming the pair, when a clause has an implementer with no row, when a bound pair names no test, when a named test function is not in its file, when a named test is marked `xfail`, when a named Python test's own body calls `pytest.skip`, `pytest.xfail` or `pytest.importorskip` and its row does not give the reason as `` `<test>` (runtime skip: <reason>) ``, or when no test a bound pair names runs unconditionally. A test that may not run (a pytest `xfail`, `skip` or `skipif` mark on it, its class or a `pytestmark` that covers it; one of those calls in its own body; a Rust `#[ignore]`) can be named beside one that always runs, never alone.

```
python tools/contract_coverage.py
```

The check reads source; it neither collects nor runs tests. These are its limits, and a test that may not run for one of them passes it: `pytest.skip()` or `pytest.xfail()` called in a fixture the test uses (only the test's own body is read); marks added by conftest hooks; Rust `#[cfg]` gating that compiles a test out (a test of the Windows named pipe is compiled only on Windows, and of the Unix socket only elsewhere).

The broker's pytest (`duplex_broker/tests/test_contract_coverage.py`) runs the check on this tree. A test is `<path>::<function>` from the repository root. A new clause gets its id and its rows here in the change that adds it; a new implementer gets a row in every clause.

### Implementers

| implementer | what it is |
|---|---|
| `duplex-claude` | `crates/duplex`, `duplex mcp` in a Claude Code session: the inbox wake |
| `duplex-codex` | `crates/duplex`, `duplex mcp` in a Codex desktop task: the `codex queue` wake |
| `codex-app-server` | `adapters/codex/duplex_codex.py`, on its own `codex app-server` |
| `opencode` | `crates/duplex`, `duplex opencode` (or `duplex-opencode.exe`): OpenCode sessions in the OpenCode desktop app's server |
| `waker` | `crates/duplex-waker`, the Windows service that starts closed sessions |

### Clauses

| clause | binds | rule |
|---|---|---|
| `send-modes-declared` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | Registration declares `capabilities.send_modes`: Claude session `auto, queue, steer`; Codex desktop task `auto, queue`; `duplex_codex.py` and `duplex opencode` all four (Send modes). |
| `mode-routes` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | Each declared mode takes its route: the inbox for the Claude session; `codex queue --resume` for the desktop task; `turn/start`, `turn/steer` with `expectedTurnId`, or `turn/interrupt` then `turn/start` for `duplex_codex.py`; a prompt with `delivery` `steer` or `queue`, or `interrupt` then a steer prompt, for `duplex opencode` (Send modes). |
| `mode-at-delivery` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | A mode the adapter cannot do now (never declared, or steer while idle) ends the delivery `failed` with a reason naming the modes it supports (Send modes). |
| `tasks-capability` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | An adapter declares `capabilities.tasks: true` exactly when it acts on task notices: `duplex` does, `duplex_codex.py` and `duplex opencode` do not (Delegated tasks). |
| `task-notice-policy` | `duplex-claude`, `duplex-codex` | A task notice's `policy.rules` says its text is a peer's and grants nothing beyond the owner's grant, and names the tools to use (Delegated tasks, Notice tool names). |
| `task-tools` | `duplex-claude`, `duplex-codex` | The adapter offers `bridge_task_*` and `bridge_send` with `mode`, checks every argument against the adapter API's bounds before it acts, and calls the broker's task routes (Adapter API, Notice tool names). |
| `turn-state-source` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | Turn state comes from the runtime's own record: the Claude session record (`blocked` is `waiting`), the Codex rollout (`task_started` busy; `task_complete` and `turn_aborted` idle), the app-server's `turn/start` answer and turn notifications for `duplex_codex.py`, OpenCode's running-session list for `duplex opencode`. Codex and OpenCode never report `waiting` (Turn state). |
| `wait-kind-closed-set` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | `waiting_for` is null or a kind from a closed set matching `^[a-z][a-z0-9_]{0,39}$`, never prompt text or tool input (Turn state). |
| `turn-reading-seq` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | Every reading carries `seq`, raised with every reading in the order the readings are taken, and from 1 again after each registration; a reading taken before a registration is never applied over one taken after it (Turn state, Codex app-server adapter). |
| `turn-no-cache-claim-carries-reading` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | The adapter keeps no copy of what it last reported: it sends every observed change, reports again after registration, and every claim carries the current reading. The turn state is touched only by the adapter's one handler or under the lock its readings are taken under; in `duplex opencode` one thread per session takes, numbers and sends every reading (Turn state, Codex app-server adapter). |
| `delivery-classes` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | Peer text never enters a turn as a prompt: `duplex` wakes with a fixed line of ids and hands the text over through `bridge_receive`; `duplex_codex.py` and `duplex opencode` put owner messages and broker notices into turns and refuse peer messages (Codex app-server adapter, OpenCode adapter). |
| `reply-outcome` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `opencode` | A delivery is marked `responded` only once the broker confirmed the reply post; a refused reply never marks it, and its reason reaches whoever can act on it: the sender through the failure notice for `duplex_codex.py` and `duplex opencode`, the replying session as a refusal for `duplex` (Codex app-server adapter). |
| `reply-reconcile` | `codex-app-server`, `opencode` | One reply per turn and conversation, under the turn's first delivery there; its `client_message_id` is `reply-<delivery id>`; the waiter is attached before `accepted`; state and reply posts are retried until acknowledged or refused; at start, deliveries the broker lists as dispatching or accepted are squared with the turns held (Codex app-server adapter, OpenCode adapter). |
| `opencode-server` | `opencode` | The OpenCode server is found from its `service.json` on every call, so a relaunch on another port with another password is followed; its password goes only to that server and never to a log line, an error, the broker or a reply; a session gone from OpenCode is created again and registered as a new binding; the sessions run with every permission allowed and a raised permission request is answered `once` (OpenCode adapter). |
| `child-env-allowlist` | `duplex-claude`, `duplex-codex`, `codex-app-server`, `waker` | Every process an adapter or the waker starts gets an environment built from an allowlist of named non-credential variables (Child environments). |
| `limit-detection` | `duplex-claude`, `duplex-codex` | A usage-limit failure is a Claude transcript line with `isApiErrorMessage: true` and `error: "rate_limit"` (run id its `uuid`, failed at its `timestamp`), or a rollout `task_complete` with `error.codex_error_info == "usage_limit_exceeded"` (run id its `turn_id`); a line written before the adapter started is history (Usage-limit resume). |
| `limit-report` | `duplex-claude`, `duplex-codex` | A failure is posted once per run to `POST /api/adapter/limits` with `run_id`, `source`, `matched` (at most 300 characters), `failed_at` and `reset_at` (Usage-limit resume). |
| `limit-answered-skip` | `duplex-claude`, `duplex-codex` | A later Claude `user` line that is neither a tool result nor `isMeta`, or a later Codex `task_started`, skips the resume ("answered") (Usage-limit resume). |
| `reset-civil-time` | `duplex-claude`, `duplex-codex` | A reset text's civil time is checked part by part (a date that exists, hour 1 to 12 with AM or PM, minute 0 to 59) and refused when out of range; the reset is the first such instant after the failure, in the text's zone or, with none (Codex always), the machine's local time (Usage-limit resume). |
| `reset-folds` | `duplex-claude`, `duplex-codex` | A repeated hour names both of its instants and the first after the failure is taken; a time a clock change skips is refused, never moved (Usage-limit resume). |
| `reset-codex-snapshot` | `duplex-codex` | A Codex reset is read first from the latest `token_count` `rate_limits` snapshot whose `limit_id` is `codex` (another bucket never replaces it) with a window at `used_percent >= 100` and a `resets_at`, else from the message (Usage-limit resume). |
| `waker-acts-on-listed` | `waker` | The waker holds only its own token and never shows it, acts only on a listed request, and does nothing while the broker is unreachable (Waker). |
| `waker-liveness` | `waker` | A Claude session counts as closed only when its records folder was read cleanly, no live record holds it and no running `claude.exe` names it; any read error skips the start with `ok: false`; the check runs again just before the launch (Waker). |
| `waker-run-exclusion` | `waker` | While its child runs, the waker watches for another `claude.exe` naming the session, kills its child at once on one, and reports `ok: false` (Waker). |
| `waker-reports` | `waker` | Every start report carries `report_id` `<request id>:<waker start ms>:<n>`; a request's reports go in order from a queue never replaced; a 4xx drops only that report (Waker). |

### Coverage

| clause | implementer | test |
|---|---|---|
| `send-modes-declared` | `duplex-claude` | `crates/duplex/src/identity.rs::the_modes_a_participant_takes_and_whether_its_wake_confirms` |
| `send-modes-declared` | `duplex-claude` | `crates/duplex-proto/src/contract.rs::registration_declares_the_modes_and_what_the_adapter_reports_and_acts_on` |
| `send-modes-declared` | `duplex-codex` | `crates/duplex/src/identity.rs::the_modes_a_participant_takes_and_whether_its_wake_confirms` |
| `send-modes-declared` | `duplex-codex` | `crates/duplex-proto/src/contract.rs::registration_declares_the_modes_and_what_the_adapter_reports_and_acts_on` |
| `send-modes-declared` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_registration_declares_the_send_modes_and_reports_idle` |
| `send-modes-declared` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `send-modes-declared` | `opencode` | `crates/duplex/tests/opencode_fake.rs::registration_declares_the_four_modes_and_no_tasks_and_the_session_runs_every_tool` |
| `mode-routes` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::a_mode_it_takes_wakes_the_session_with_the_fixed_line_and_never_the_text` |
| `mode-routes` | `duplex-claude` | `crates/duplex/src/identity.rs::a_wake_through_a_named_pipe_writes_the_auth_line_then_the_message` |
| `mode-routes` | `duplex-claude` | `crates/duplex/src/identity.rs::a_wake_through_a_unix_socket_writes_the_auth_line_then_the_message` |
| `mode-routes` | `duplex-codex` | `crates/duplex/tests/adapter_fake.rs::a_codex_task_is_woken_with_codex_queue_resume_for_its_thread` |
| `mode-routes` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_auto_and_steer_join_the_running_turn` |
| `mode-routes` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_queue_waits_for_the_running_turn_and_never_steers` |
| `mode-routes` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_restart_interrupts_the_running_turn_before_it_starts_another` |
| `mode-routes` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_an_interrupt_and_a_steer_aimed_at_a_turn_that_just_ended_leave_the_turn_and_delivery_correct` |
| `mode-routes` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `mode-routes` | `opencode` | `crates/duplex/tests/opencode_fake.rs::auto_starts_an_idle_session_and_its_round_answers_under_the_delivery_s_reply_id` |
| `mode-routes` | `opencode` | `crates/duplex/tests/opencode_fake.rs::auto_and_steer_join_the_running_turn_and_each_conversation_gets_one_reply` |
| `mode-routes` | `opencode` | `crates/duplex/tests/opencode_fake.rs::queue_waits_for_the_running_round_and_gets_a_reply_of_its_own` |
| `mode-routes` | `opencode` | `crates/duplex/tests/opencode_fake.rs::restart_interrupts_the_running_turn_before_it_starts_another` |
| `mode-at-delivery` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::a_mode_the_session_never_declared_fails_the_delivery_naming_what_it_supports` |
| `mode-at-delivery` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::a_steer_into_a_session_with_no_running_turn_fails_the_delivery` |
| `mode-at-delivery` | `duplex-codex` | `crates/duplex/src/policy.rs::a_send_mode_the_session_never_declared_is_refused` |
| `mode-at-delivery` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_steer_to_an_idle_thread_fails_and_starts_no_turn` |
| `mode-at-delivery` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_an_unknown_mode_fails_naming_the_supported_ones` |
| `mode-at-delivery` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `mode-at-delivery` | `opencode` | `crates/duplex/tests/opencode_fake.rs::steer_to_an_idle_session_fails_and_puts_nothing_in` |
| `mode-at-delivery` | `opencode` | `crates/duplex/tests/opencode_fake.rs::an_unknown_mode_fails_naming_the_supported_ones` |
| `tasks-capability` | `duplex-claude` | `crates/duplex-proto/src/contract.rs::registration_declares_the_modes_and_what_the_adapter_reports_and_acts_on` |
| `tasks-capability` | `duplex-codex` | `crates/duplex-proto/src/contract.rs::registration_declares_the_modes_and_what_the_adapter_reports_and_acts_on` |
| `tasks-capability` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_the_adapter_does_not_declare_task_support` |
| `tasks-capability` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `tasks-capability` | `opencode` | `crates/duplex/tests/opencode_fake.rs::registration_declares_the_four_modes_and_no_tasks_and_the_session_runs_every_tool` |
| `task-notice-policy` | `duplex-claude` | `crates/duplex/src/policy.rs::a_task_notice_says_whose_text_it_is_and_what_it_grants` |
| `task-notice-policy` | `duplex-codex` | `crates/duplex/src/policy.rs::a_task_notice_says_whose_text_it_is_and_what_it_grants` |
| `task-notice-policy` | `codex-app-server` | n/a: declares no `tasks`; admission keeps task notices from it |
| `task-notice-policy` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `task-notice-policy` | `opencode` | n/a: declares no `tasks`; admission keeps task notices from it |
| `task-tools` | `duplex-claude` | `crates/duplex/src/mcp/tools.rs::there_are_fourteen_bridge_tools_with_distinct_names` |
| `task-tools` | `duplex-claude` | `crates/duplex/src/mcp/args.rs::each_check_names_the_argument_it_refuses` |
| `task-tools` | `duplex-claude` | `crates/duplex/src/mcp/args.rs::the_schema_lists_what_is_required` |
| `task-tools` | `duplex-codex` | `crates/duplex/src/mcp/tools.rs::there_are_fourteen_bridge_tools_with_distinct_names` |
| `task-tools` | `duplex-codex` | `crates/duplex/src/mcp/args.rs::each_check_names_the_argument_it_refuses` |
| `task-tools` | `codex-app-server` | n/a: declares no `tasks`; admission keeps task notices from it; it offers no tools |
| `task-tools` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `task-tools` | `opencode` | n/a: declares no `tasks`; admission keeps task notices from it; it offers no tools |
| `turn-state-source` | `duplex-claude` | `crates/duplex/src/watch/mod.rs::a_session_record_reads_as_its_turn` |
| `turn-state-source` | `duplex-claude` | `crates/duplex/tests/replay.rs::session_records_read_as_their_turn_state` |
| `turn-state-source` | `duplex-codex` | `crates/duplex/tests/replay.rs::a_codex_turn_is_busy_until_its_task_completes` |
| `turn-state-source` | `duplex-codex` | `crates/duplex/tests/replay.rs::an_aborted_codex_turn_is_idle` |
| `turn-state-source` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_each_turn_reports_busy_then_idle` |
| `turn-state-source` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_turn_reads_busy_once_then_idle_in_every_order_its_inputs_arrive` |
| `turn-state-source` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `turn-state-source` | `opencode` | `crates/duplex/tests/opencode_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_from_one_after_each_registration` |
| `turn-state-source` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_relaunched_app_is_found_at_its_new_port_with_its_new_password` |
| `wait-kind-closed-set` | `duplex-claude` | `crates/duplex/src/watch/mod.rs::a_wait_names_a_kind_never_its_own_words` |
| `wait-kind-closed-set` | `duplex-claude` | `crates/duplex-proto/src/contract.rs::a_turn_outside_the_closed_sets_is_cleaned_at_the_boundary` |
| `wait-kind-closed-set` | `duplex-codex` | `crates/duplex-proto/src/contract.rs::a_turn_outside_the_closed_sets_is_cleaned_at_the_boundary` |
| `wait-kind-closed-set` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_every_claim_carries_the_current_turn_state` |
| `wait-kind-closed-set` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `wait-kind-closed-set` | `opencode` | `crates/duplex/tests/opencode_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_from_one_after_each_registration` |
| `turn-reading-seq` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_in_order_and_from_one_after_a_registration` |
| `turn-reading-seq` | `duplex-codex` | `crates/duplex/tests/adapter_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_in_order_and_from_one_after_a_registration` |
| `turn-reading-seq` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_turn_readings_are_numbered_in_order_and_restart_with_a_registration` |
| `turn-reading-seq` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_the_newest_reading_reaches_the_broker_though_an_older_one_arrives_late` |
| `turn-reading-seq` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_registration_reset_and_its_first_reading_are_one_step` |
| `turn-reading-seq` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `turn-reading-seq` | `opencode` | `crates/duplex/tests/opencode_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_from_one_after_each_registration` |
| `turn-no-cache-claim-carries-reading` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_in_order_and_from_one_after_a_registration` |
| `turn-no-cache-claim-carries-reading` | `duplex-codex` | `crates/duplex/tests/adapter_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_in_order_and_from_one_after_a_registration` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_claim_carries_a_turn_change_the_app_server_sent_before_it` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_every_claim_carries_the_current_turn_state` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_each_turn_reports_busy_then_idle` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_turn_reads_busy_once_then_idle_in_every_order_its_inputs_arrive` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_start_answer_handled_while_an_older_turn_s_end_is_reported_reads_busy` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_failed_turn_start_queues_nothing_and_changes_no_turn_state` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_socket_frame_named_like_the_start_answer_changes_nothing` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_only_the_handler_writes_the_turn_state` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_turn_state_is_touched_only_under_the_lock_or_in_the_handler` |
| `turn-no-cache-claim-carries-reading` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_registration_reset_and_its_first_reading_are_one_step` |
| `turn-no-cache-claim-carries-reading` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `turn-no-cache-claim-carries-reading` | `opencode` | `crates/duplex/tests/opencode_fake.rs::every_claim_carries_the_turn_read_just_before_it_numbered_from_one_after_each_registration` |
| `turn-no-cache-claim-carries-reading` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_relaunched_app_is_found_at_its_new_port_with_its_new_password` |
| `delivery-classes` | `duplex-claude` | `crates/duplex-proto/src/wake.rs::the_line_carries_only_ids_and_the_kind` |
| `delivery-classes` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::a_mode_it_takes_wakes_the_session_with_the_fixed_line_and_never_the_text` |
| `delivery-classes` | `duplex-codex` | `crates/duplex-proto/src/wake.rs::the_line_carries_only_ids_and_the_kind` |
| `delivery-classes` | `duplex-codex` | `crates/duplex/tests/adapter_fake.rs::a_codex_task_is_woken_with_codex_queue_resume_for_its_thread` |
| `delivery-classes` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_broker_notice_becomes_a_turn_and_a_peer_message_does_not` |
| `delivery-classes` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_peer_message_is_refused_without_a_turn` |
| `delivery-classes` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `delivery-classes` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_peer_message_is_refused_without_an_input_and_a_broker_notice_becomes_one` |
| `reply-outcome` | `duplex-claude` | `crates/duplex/src/mcp/tools.rs::broker_refusals_become_refusals_and_other_failures_stay_failures` |
| `reply-outcome` | `duplex-codex` | `crates/duplex/src/mcp/tools.rs::broker_refusals_become_refusals_and_other_failures_stay_failures` |
| `reply-outcome` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_reply_the_broker_refuses_fails_the_delivery_and_tells_the_sender` |
| `reply-outcome` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `reply-outcome` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_reply_the_broker_refuses_fails_the_delivery_and_tells_the_sender` |
| `reply-reconcile` | `duplex-claude` | n/a: a reply is the session's own `bridge_send`, retried by the session; the adapter holds no turn to square, and an unread delivery stays under the broker's wake ladder |
| `reply-reconcile` | `duplex-codex` | n/a: a reply is the task's own `bridge_send`, retried by the task; the adapter holds no turn to square, and an unread delivery stays under the broker's wake ladder |
| `reply-reconcile` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_reply_and_its_state_are_retried_under_one_stable_id` |
| `reply-reconcile` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_lost_accepted_answer_cannot_orphan_the_turn` |
| `reply-reconcile` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_reconcile_fails_what_no_run_holds_and_reacknowledges_what_one_does` |
| `reply-reconcile` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_a_delivery_left_accepted_with_no_live_run_is_failed_once_serving_resumes` |
| `reply-reconcile` | `waker` | n/a: the waker is not a binding: it registers nothing and takes no delivery |
| `reply-reconcile` | `opencode` | `crates/duplex/tests/opencode_fake.rs::auto_and_steer_join_the_running_turn_and_each_conversation_gets_one_reply` |
| `reply-reconcile` | `opencode` | `crates/duplex/tests/opencode_fake.rs::queue_waits_for_the_running_round_and_gets_a_reply_of_its_own` |
| `reply-reconcile` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_lost_accepted_answer_is_sent_again_and_the_reply_waits_for_it` |
| `reply-reconcile` | `opencode` | `crates/duplex/tests/opencode_fake.rs::reconcile_fails_what_no_round_holds_and_acknowledges_what_one_does` |
| `child-env-allowlist` | `duplex-claude` | `crates/duplex/src/identity.rs::a_child_gets_only_the_names_on_the_list_in_any_case` |
| `child-env-allowlist` | `duplex-codex` | `crates/duplex/src/identity.rs::a_child_gets_only_the_names_on_the_list_in_any_case` |
| `child-env-allowlist` | `codex-app-server` | `adapters/codex/test_duplex_codex.py::test_no_credential_variable_reaches_the_app_server` |
| `child-env-allowlist` | `waker` | `crates/duplex-waker/tests/env_test.rs::only_allowlisted_variables_reach_the_child_on_every_route` |
| `child-env-allowlist` | `waker` | `crates/duplex-waker/tests/env_test.rs::the_allowlist_matches_names_in_any_case_and_holds_no_credential` |
| `child-env-allowlist` | `opencode` | n/a: starts no process; it calls the OpenCode app's server over HTTP |
| `limit-detection` | `duplex-claude` | `crates/duplex/tests/replay.rs::the_limit_line_is_found_with_its_reset` |
| `limit-detection` | `duplex-claude` | `crates/duplex/tests/replay.rs::the_claude_source_reads_the_transcript_as_a_limit_then_an_answer` |
| `limit-detection` | `duplex-claude` | `crates/duplex/tests/replay.rs::a_failure_before_the_adapter_started_is_not_reported` |
| `limit-detection` | `duplex-codex` | `crates/duplex/tests/replay.rs::a_usage_limit_resets_when_its_full_window_does` |
| `limit-detection` | `duplex-codex` | `crates/duplex/src/watch/reset.rs::a_reset_beyond_the_window_is_not_a_usage_limit` |
| `limit-detection` | `duplex-codex` | `crates/duplex/tests/replay.rs::a_failure_the_rollout_held_before_the_adapter_started_is_history` |
| `limit-detection` | `codex-app-server` | n/a: declares no `limits`; usage limits are read through the `duplex` transcript and rollout readers |
| `limit-detection` | `waker` | n/a: the waker reads no transcript or rollout |
| `limit-detection` | `opencode` | n/a: declares no `limits`; OpenCode writes no usage-limit record the adapter reads |
| `limit-report` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::a_limit_failure_is_reported_once_per_run_and_skipped_when_the_session_answers` |
| `limit-report` | `duplex-claude` | `crates/duplex/tests/broker_it.rs::the_watch_reports_the_turn_and_a_limit_failure` |
| `limit-report` | `duplex-codex` | `crates/duplex/tests/adapter_fake.rs::a_limit_failure_is_reported_once_per_run_and_skipped_when_the_session_answers` |
| `limit-report` | `codex-app-server` | n/a: declares no `limits`; usage limits are read through the `duplex` transcript and rollout readers |
| `limit-report` | `waker` | n/a: the waker reads no transcript or rollout |
| `limit-report` | `opencode` | n/a: declares no `limits`; OpenCode writes no usage-limit record the adapter reads |
| `limit-answered-skip` | `duplex-claude` | `crates/duplex/tests/replay.rs::a_tool_result_is_not_a_new_turn_and_a_typed_line_is` |
| `limit-answered-skip` | `duplex-claude` | `crates/duplex/tests/adapter_fake.rs::a_limit_failure_is_reported_once_per_run_and_skipped_when_the_session_answers` |
| `limit-answered-skip` | `duplex-codex` | `crates/duplex/tests/replay.rs::a_codex_turn_is_busy_until_its_task_completes` |
| `limit-answered-skip` | `duplex-codex` | `crates/duplex/tests/adapter_fake.rs::a_limit_failure_is_reported_once_per_run_and_skipped_when_the_session_answers` |
| `limit-answered-skip` | `codex-app-server` | n/a: declares no `limits`; usage limits are read through the `duplex` transcript and rollout readers |
| `limit-answered-skip` | `waker` | n/a: the waker reads no transcript or rollout |
| `limit-answered-skip` | `opencode` | n/a: declares no `limits`; OpenCode writes no usage-limit record the adapter reads |
| `reset-civil-time` | `duplex-claude` | `crates/duplex/src/watch/reset.rs::every_golden_case_reads_as_recorded` |
| `reset-civil-time` | `duplex-codex` | `crates/duplex/src/watch/reset.rs::every_golden_case_reads_as_recorded` |
| `reset-civil-time` | `duplex-codex` | `crates/duplex/tests/replay.rs::without_a_snapshot_the_message_time_is_read_in_the_local_zone` |
| `reset-civil-time` | `codex-app-server` | n/a: declares no `limits`; usage limits are read through the `duplex` transcript and rollout readers |
| `reset-civil-time` | `waker` | n/a: the waker reads no transcript or rollout |
| `reset-civil-time` | `opencode` | n/a: declares no `limits`; OpenCode writes no usage-limit record the adapter reads |
| `reset-folds` | `duplex-claude` | `crates/duplex/src/watch/reset.rs::every_golden_case_reads_as_recorded` |
| `reset-folds` | `duplex-codex` | `crates/duplex/src/watch/reset.rs::every_golden_case_reads_as_recorded` |
| `reset-folds` | `codex-app-server` | n/a: declares no `limits`; usage limits are read through the `duplex` transcript and rollout readers |
| `reset-folds` | `waker` | n/a: the waker reads no transcript or rollout |
| `reset-folds` | `opencode` | n/a: declares no `limits`; OpenCode writes no usage-limit record the adapter reads |
| `reset-codex-snapshot` | `duplex-claude` | n/a: a Claude limit carries no rate-limit snapshot |
| `reset-codex-snapshot` | `duplex-codex` | `crates/duplex/src/watch/reset.rs::a_rate_limit_snapshot_with_a_full_window_wins_over_the_message` |
| `reset-codex-snapshot` | `duplex-codex` | `crates/duplex/tests/replay.rs::a_usage_limit_resets_when_its_full_window_does` |
| `reset-codex-snapshot` | `codex-app-server` | n/a: declares no `limits`; usage limits are read through the `duplex` transcript and rollout readers |
| `reset-codex-snapshot` | `waker` | n/a: the waker reads no transcript or rollout |
| `reset-codex-snapshot` | `opencode` | n/a: declares no `limits`; OpenCode writes no usage-limit record the adapter reads |
| `waker-acts-on-listed` | `duplex-claude` | n/a: only the waker starts closed sessions |
| `waker-acts-on-listed` | `duplex-codex` | n/a: only the waker starts closed sessions |
| `waker-acts-on-listed` | `codex-app-server` | n/a: only the waker starts closed sessions |
| `waker-acts-on-listed` | `waker` | `crates/duplex-waker/tests/waker_test.rs::a_request_no_longer_listed_is_never_started` |
| `waker-acts-on-listed` | `waker` | `crates/duplex-waker/tests/waker_test.rs::an_unreachable_broker_starts_nothing_and_logs_once` |
| `waker-acts-on-listed` | `waker` | `crates/duplex-waker/tests/waker_test.rs::the_token_is_sent_but_never_logged` |
| `waker-acts-on-listed` | `opencode` | n/a: only the waker starts closed sessions |
| `waker-liveness` | `duplex-claude` | n/a: only the waker starts closed sessions |
| `waker-liveness` | `duplex-codex` | n/a: only the waker starts closed sessions |
| `waker-liveness` | `codex-app-server` | n/a: only the waker starts closed sessions |
| `waker-liveness` | `waker` | `crates/duplex-waker/tests/registry_test.rs::absence_of_evidence_is_never_closed` |
| `waker-liveness` | `waker` | `crates/duplex-waker/tests/registry_test.rs::closed_needs_a_clean_folder_and_clean_command_lines` |
| `waker-liveness` | `waker` | `crates/duplex-waker/tests/waker_test.rs::liveness_is_checked_again_just_before_the_launch` |
| `waker-liveness` | `waker` | `crates/duplex-waker/tests/waker_test.rs::an_unestablished_session_is_skipped_and_reported` |
| `waker-liveness` | `opencode` | n/a: only the waker starts closed sessions |
| `waker-run-exclusion` | `duplex-claude` | n/a: only the waker starts closed sessions |
| `waker-run-exclusion` | `duplex-codex` | n/a: only the waker starts closed sessions |
| `waker-run-exclusion` | `codex-app-server` | n/a: only the waker starts closed sessions |
| `waker-run-exclusion` | `waker` | `crates/duplex-waker/tests/launch_test.rs::a_run_is_killed_within_two_seconds_of_its_session_opening_elsewhere` |
| `waker-run-exclusion` | `waker` | `crates/duplex-waker/tests/waker_test.rs::a_run_ended_because_its_session_was_opened_is_reported_failed` |
| `waker-run-exclusion` | `opencode` | n/a: only the waker starts closed sessions |
| `waker-reports` | `duplex-claude` | n/a: only the waker starts closed sessions |
| `waker-reports` | `duplex-codex` | n/a: only the waker starts closed sessions |
| `waker-reports` | `codex-app-server` | n/a: only the waker starts closed sessions |
| `waker-reports` | `waker` | `crates/duplex-waker/tests/waker_test.rs::a_run_s_reports_reach_the_broker_in_order_when_the_start_report_was_lost` |
| `waker-reports` | `waker` | `crates/duplex-waker/tests/waker_test.rs::a_refused_report_drops_only_itself` |
| `waker-reports` | `waker` | `crates/duplex-waker/tests/waker_test.rs::a_report_whose_answer_was_lost_is_resent_under_its_id_and_kept_once` |
| `waker-reports` | `waker` | `crates/duplex-waker/tests/waker_test.rs::a_restarted_waker_s_reports_never_reuse_an_earlier_process_s_ids` |
| `waker-reports` | `opencode` | n/a: only the waker starts closed sessions |
| `opencode-server` | `duplex-claude` | n/a: only `duplex opencode` talks to the OpenCode server |
| `opencode-server` | `duplex-codex` | n/a: only `duplex opencode` talks to the OpenCode server |
| `opencode-server` | `codex-app-server` | n/a: only `duplex opencode` talks to the OpenCode server |
| `opencode-server` | `waker` | n/a: only `duplex opencode` talks to the OpenCode server |
| `opencode-server` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_relaunched_app_is_found_at_its_new_port_with_its_new_password` |
| `opencode-server` | `opencode` | `crates/duplex/tests/opencode_fake.rs::the_password_reaches_only_the_server_never_the_log_the_broker_or_an_error` |
| `opencode-server` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_session_deleted_while_the_app_was_closed_is_created_again_as_a_new_binding` |
| `opencode-server` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_session_kept_from_an_earlier_run_is_held_again_with_its_model_and_rules_restored` |
| `opencode-server` | `opencode` | `crates/duplex/tests/opencode_fake.rs::a_permission_request_is_allowed_once_without_waiting_for_anyone` |

# Duplex Chat Broker

Routes group and direct messages between you (the owner) and your Claude Code and Codex sessions, and keeps the history. Sessions join through the `duplex` adapter, an optional model member answers from any OpenAI-compatible server, and an optional ChatGPT connector lets a ChatGPT conversation take part.

| Caller | Port 8790 | Authentication |
|---|---|---|
| Home Assistant sidebar | `/` (the panel) and the owner API it calls: `GET /api/status`, conversations, messages, tasks and the rest of `docs/owner-api.md` | ingress |
| Owner device (another client, `duplex owner`) | the same owner API, WebSocket `/api/events` | `POST /api/device/enroll` with the pairing code once, then its own `dcd_` bearer token, rotated by `/api/device/token/rotate` |
| Named service | `POST /api/service/{name}/notify`, `GET /api/service/{name}/sessions` | the service's own bearer token (option `services`) |
| ChatGPT connector | port 8791 only: `/mcp`, `/oauth/*`, `/.well-known/oauth-*` | OAuth 2.1 with PKCE, linked once with the pairing code (below) |
| Session adapter | `/api/adapter/...`, WebSocket `/api/adapter/stream` | `POST /api/adapter/register` with the enrollment token, then a per-session token that rotates on every registration. A session already registered registers again only with its current token (`current_token`) or after the owner's `POST /api/sessions/{id}/release` |

The broker is reached on the local network or through the add-on's port; the adapter, the waker and `duplex owner` speak plain `http://` to it, so give them the address of the add-on's port 8790 on your network (`http://<home assistant address>:8790`).

## Panel

The `Duplex` sidebar panel is dark by default with a light toggle. Its tabs:

| Tab | What it does |
|---|---|
| Chat | Lists your conversations, opens a direct chat with one session or creates a group (title, mode, members). A conversation shows its messages with each recipient's delivery state, details per message (read time, reason) and a send-again action. Compose picks the recipients (a group) and a send mode: `auto`, `queue`, `steer` or `restart`, limited to what every chosen session declares. A group takes members, a mode (owner directed or consult), a collaboration (objective, turns, minutes) with stop, pause and resume, cancel of queued messages, removal and restore. |
| Tasks | Delegated tasks, open or all, with their questions, result and events, and cancel. |
| Status | Sessions live of those registered with release and retire, open deliveries, messages and deliveries per day, time to read, and every conversation with its last 40 messages. |
| System | What the broker refused in the last day, usage-limit resumes, the waker's starts, connector subscriptions and enrolled devices with enable and disable. |

The header shows sessions live of those registered, sessions held, deliveries open or stuck, and the model member and its action mode. The panel follows the broker every 3 seconds on Chat and every 15 on the other tabs. Opened outside ingress it shows a sign-in card: a broker run with `DUPLEX_USER_AUTH=pairing` signs the browser in with the pairing code; the add-on trusts ingress only.

A session counts as live when its last poll or heartbeat is under 90 seconds old; an open adapter stream counts as a poll every `stream_ping` seconds. Poll times are kept in memory, so after a broker restart a session shows again on its next poll or stream.

## Adapter stream

An adapter holds `/api/adapter/stream` open with its bearer token and claims when nudged, instead of polling. The broker sends `{"type": "ready"}` and one `nudge` on connect, a `nudge` whenever a store event names the binding (delivery created or changed, re-wake asked for), and a `ping` after `stream_ping` quiet seconds. Nudges carry no content and may repeat; a claim with nothing waiting answers 204. A refused token gets `{"type": "error"}` and close code 4401. An adapter that cannot open the stream polls `POST /api/adapter/claim` as before.

`GET /api/status?days=N` (1-400, default 90) returns the same data as JSON with no message text.

## Options

| Option | Meaning |
|---|---|
| `pairing_code` | Enrolls owner devices. Empty disables enrollment. Five wrong codes from one address lock that address out of enrollment for a minute. |
| `enrollment_token` | Registers session adapters. Empty disables registration. `duplex setup` writes it into `~/.duplex-chat/adapter.json` on each machine. |
| `owner_name` | The name shown for you in the panel, in the policy text sessions read and in the connector's guide. Default `Owner`. |
| `services` | Named service tokens, a list of `name` (lowercase letters, digits and hyphens) and `token`. See Services. Default none. |
| `device_token_grace` | Seconds an owner device's previous token keeps working after a rotation. |
| `model_url` | OpenAI-compatible base URL of the model member, for example `http://localhost:11434/v1`. Empty (the default) turns the member off. |
| `model_name` | Model name sent to the server. |
| `model_tools_name` | Model name used when actions are on; the chat model when empty. |
| `model_member_title` | The member's title in conversations, default `Assistant`. |
| `model_timeout` | Seconds a message waits for the server. |
| `model_actions` | Off by default. On: the tool calls the model returns are run through Home Assistant's Assist API with no approval step. Off: shown as text. |
| `model_no_thinking` | Off by default. On: each request carries `chat_template_kwargs: {"enable_thinking": false}`; a strict OpenAI-compatible server may reject the key. |
| `wake_timeout` | Seconds a delivery handed to a session waits to be read before the first re-wake, 300. |
| `rewake_interval` | Seconds between re-wakes, and from the last re-wake to closing the delivery, 300. |
| `rewake_limit` | Re-wakes before an unread delivery is closed as `uncertain`, 3. |
| `hold_after` | Unread closures in a row before delivery to that session is held, 2. |
| `waker_token` | The bearer token of the waker. Empty turns the waker routes off (404). |
| `connector_url` | Public HTTPS origin of the ChatGPT connector, for example `https://duplex.example.com`. Empty turns the connector and port 8791 off. |
| `connector_member_title` | The ChatGPT member's title, default `ChatGPT`. No other provider may register a session with that word in its title. |
| `log_level` | `debug`, `info`, `warning`, `error`. |

Two optional files in the add-on's own config folder (`/addon_configs/<slug>/`, mapped read-only) extend what the broker tells a model: `model-prompt.md` is appended to the model member's system prompt, and `connector-guide.md` to the guide the ChatGPT connector serves. Each is read when the broker starts.

## Services

A service is a restricted caller with its own bearer token, set in the `services` option. For a service named `<name>`:

| Route | What |
|---|---|
| `GET /api/service/<name>/sessions` | The active sessions: `id`, `title`, `provider`, `active`. |
| `POST /api/service/<name>/notify` | `{"binding_id", "client_message_id", "text"}` (text up to 500 characters). Posts the text, prefixed `[<name> status data]`, in the service's own direct conversation with that session. A repeat of the same `client_message_id` posts nothing. |

A service cannot use the owner or adapter API. A wrong token, or an unknown service name, is 401.

## ChatGPT connector

The connector needs a public HTTPS origin you provide (any tunnel or reverse proxy that points a hostname at port 8791). ChatGPT is a conversation that only ChatGPT can wake, so the broker is a ChatGPT connector with MCP Events. Port 8791 serves the connector's paths only; every other path answers 404 and WebSockets are closed.

| Step | What happens |
|---|---|
| Add the connector | In ChatGPT, a custom connector with the URL `<connector_url>/mcp`. ChatGPT reads `/.well-known/oauth-protected-resource`, then the authorization server metadata. |
| Link | ChatGPT opens `/oauth/authorize`. Enter the add-on's pairing code and Allow. Only ChatGPT's client (`https://chatgpt.com/oauth/.../client.json`, or one registered at `/oauth/register` with ChatGPT's redirect addresses) and its redirect addresses are accepted. PKCE S256 is required; the redirect carries `iss`. Access tokens last an hour; refresh tokens last 30 days and rotate on use. |
| Subscribe | ChatGPT calls `events/subscribe` for `duplex.message` with a public https callback and a `whsec_` secret. The broker checks the callback resolves only to public addresses, verifies it with a signed challenge, and keeps the subscription until `refreshBefore` (24 hours by default, 7 days at most). |
| A message for ChatGPT | Owner and sessions write to the connector's binding (`connector_member_title`, provider `chatgpt`) like any session. While a subscription is live, the broker claims the delivery and posts a signed event with the message and conversation IDs, never the text; on a 2xx the delivery is `accepted`. With no live subscription the delivery stays `queued`. An event no subscription accepts after two tries, 2 s apart, fails the delivery with a reason and a failure notice to its sender; a 410 drops that subscription. |
| ChatGPT reads | `read_message` returns the text, `from`, `to` and `sent_at` without changing state; `acknowledge` marks it `consumed`, and the next delivery follows; `reply` with `parent_id` marks it `responded`. `inbox` lists anything unread if an event was missed, and the read receipts for its messages since the last `inbox`, each once. Re-wakes from the ladder resend the event. |

## Messages, receipts and failure notices

Every message carries an envelope: `from` and `to` as `{"kind", "id", "title"}` (`kind` is `user` or `binding`) and `sent_at`, the broker's time in UTC ISO 8601. A message with no recipients (a session's reply to the group, anything in a direct conversation sent to the owner) is `to` the conversation's owner. The envelope is written into the message's `message.accepted` event, so the panel and every history read have it, and it is added to the owner's post response, an adapter's claim, message record and late-read pointer, and the connector's tools.

| For the sender | When | How it arrives |
|---|---|---|
| Read receipt | a delivery first reaches `consumed` or `responded` | Session: `POST /api/adapter/receipts` returns those not yet reported, each once (`message_id`, `conversation_id`, `recipient`, `state`, `read_at`, `sent_at`); the `duplex` adapter asks on each `bridge_receive`. ChatGPT: `receipts` in its next `inbox`. Never a wake. Owner: `GET /api/messages/{id}` lists each recipient's `state`, `read`, `read_at`, `reason`. |
| Failure notice | a delivery ends `failed` or `uncertain`: a failed wake (after the adapter's one retry 2 s on), a webhook to the connector with no 2xx after its retry, a wake-ladder close, a re-registration or a retirement | Owner: a post from `Duplex broker` in that conversation. Session: a message from `Duplex broker` in the conversation it wrote in, addressed to it and answering its message, delivered and woken like any other. Its event carries `notice`: `message_id`, `conversation_id`, `recipient`, `state`, `reason`, `at`. |

A broker message produces no receipt or notice, so a notice that cannot be delivered ends there. Users other than the owner, such as a named service, get neither. Each receipt and notice is kept in `sender_notices`, one of each kind per delivery.

## Wake ladder

A session adapter that registers with `rewake: verified` among its capabilities is on the ladder. The `duplex` adapter declares it for Claude sessions and Codex tasks; the Codex app-server adapter does not, because it holds `accepted` while its turn runs.

| Step | When (defaults) | What happens |
|---|---|---|
| Hand-over | claim | the adapter wakes the session with its fixed line; the delivery is in flight |
| Re-wake 1 to `rewake_limit` | 300 s after hand-over, then every 300 s | the sweep nudges the adapter's stream, and its next claim answers `{"rewake": {...}}`; the adapter sends the same fixed line, never the text, and reports to `POST /api/adapter/deliveries/{id}/wake` |
| Close | `wake_timeout` plus `rewake_limit` times `rewake_interval` after hand-over (1200 s by default), fixed when the delivery joins the ladder | the delivery becomes `uncertain` with its reason, its sender gets a failure notice, and the next queued delivery goes out |
| Hold | after `hold_after` closures in a row | nothing more is handed to the session; one notice goes to the owner's direct conversation `Duplex broker` |
| Resume | the session reads a message, reads a closed one late, or registers again | the count restarts and queued deliveries go out |

The sweep runs every 15 seconds. Deadlines and re-wake counts are stored with the delivery, so a broker restart continues a ladder. Two counts are kept: re-wakes asked for, with at most one waiting for the adapter (the sweep asks for no further one until the adapter has taken it), and re-wakes the adapter reported sending with `ok: true`. The close reason, the `delivery.expired` event and the hold notice state both. `POST .../wake` for a delivery no longer in flight answers 409 and changes nothing; an attempt that was not asked for answers 422. A session's read counts once per delivery, when it first reaches `consumed` or `responded`: a reply to a delivery already read does not end a hold or reset the closures counted. A session reading a closed delivery calls `POST /api/adapter/deliveries/{id}/late-read` and gets a pointer (message id, sender, age, reason, the history cursor), never the text; a repeat of the same call returns the pointer and changes nothing; the adapter tells the session to read it from history or ask for a resend. The `Duplex broker` conversation takes no replies.

Events, each in the delivery's conversation: `delivery.wake_requested`, `delivery.wake_outcome`, `delivery.expired`, `delivery.read_late`; in the `Duplex broker` conversation: `session.held`, `session.resumed`. Every change to `uncertain`, `cancelled` or `failed` carries a `reason` in its `delivery.changed` event.

## Send modes and turn state

A message carries a `mode`: the owner's post, an adapter's post and a task's assignment take an optional `mode` (default `auto`), and a claimed message's body carries it.

| Mode | Meaning |
|---|---|
| `auto` | Start an idle session, steer a running one, else queue. |
| `queue` | A new turn after the current one. |
| `steer` | Into the running turn. |
| `restart` | Interrupt the running turn, then start with this message. |

| Rule | How |
|---|---|
| What a session takes | The adapter declares `capabilities.send_modes` at registration; a binding without it takes `auto` only. |
| No downgrade | A post whose mode any recipient cannot take is refused with 422 `mode_unsupported`, creates no message, and names each such recipient with what it supports, for example `steer is not supported by "Reviewer" (supports: auto, queue)`. |
| Steer needs a turn | `steer` to a recipient that reports `idle` is refused the same way: `"<title>" has no running turn (supports: ...)`. |
| Queue waits for the turn | A binding that reports `busy` or `waiting` is handed no queue-mode delivery until it reports `idle` or `unknown`. The claim stays in order, so a held delivery holds those behind it, and nothing enters the wake ladder while held. A `delivery.released` event nudges the adapter's stream when the held head can go. |
| Admission | Every delivery is created in one place, which admits each target first: `auto` is always admitted (the adapter resolves it to a mode it declares), any other mode must be declared, a steer needs a running turn, and a task notice needs `capabilities.tasks: true` (422 `mode_unsupported` or `tasks_unsupported`, nothing created). A notice the broker itself produces that is refused is recorded by what produced it: a task event `notice_refused` with the reason, or a scheduled send ending `refused`. Every way a delivery enters `queued` goes through one function that admits it against what the target declares at that moment: creation, a re-check when the session registers again, and a delivery returned to the queue after an unanswered run. One that no longer passes ends `failed` with the reason and is recorded the same way. |
| Claim gate | Deliveries go to an adapter in order. While the session's latest hand-over is `dispatching` (not yet confirmed) nothing more is handed out. While one is `accepted` (taken into a running turn), the head goes out when its mode is `auto`, `steer` or `restart` and the adapter declares `steer`, so it can join or interrupt that turn; a `queue` head, or any head for an adapter without `steer`, waits for the turn to end. |
| At delivery | An adapter that cannot do a mode when the delivery reaches it ends the delivery `failed` with the modes it supports, which sends the sender the usual failure notice. |

| Adapter | Declared |
|---|---|
| Claude session (inbox) | `auto`, `queue`, `steer` |
| Codex desktop task (`codex queue`) | `auto`, `queue` |
| `duplex_codex.py` (its own app-server) | `auto`, `queue`, `steer`, `restart` |

`POST /api/adapter/turn` takes `{"state": "busy" | "idle" | "waiting" | "unknown", "waiting_for": "<kind>" | null, "seq": <integer >= 1>}`, sent on each change and again after registration. `waiting_for` is a kind of wait matching `^[a-z][a-z0-9_]{0,39}$`, never prompt text or tool input; anything else is 422. The binding keeps `turn_state`, `turn_waiting_for` and `turn_state_at` (shown by `GET /api/sessions`); registration resets them to `unknown`.

`seq` orders the readings of one registration: the adapter raises it with every reading it sends and starts again when it registers. The broker validates a reading first (a malformed one is 422 whether or not it differs from what it holds; a missing `seq` is 422), then, in one transaction, applies it only when it comes from the session's current lease and its `seq` is greater than the last applied. An older or equal reading changes nothing and is answered 200 with `applied: false`. The answer is `{"state", "waiting_for", "applied"}`; a reading that changes nothing writes nothing but the sequence.

`POST /api/adapter/claim` takes an optional body `{"turn": {"state": ..., "waiting_for": ..., "seq": ...}}`, the adapter's current reading, through the same function as `/turn` (observed questions and the idle nudge included), before the queue hold is judged, so a report that never arrived cannot hold a queue-mode message. An adapter that declares turn state sends its reading with every claim, numbered in the same sequence as its `/turn` reports.

| Adapter reconciliation route | Answer |
|---|---|
| `GET /api/adapter/deliveries?state=dispatching,accepted` (binding token) | The calling session's own deliveries in the listed states (any of `queued`, `dispatching`, `accepted`, `consumed`, `responded`, `cancelled`, `failed`, `uncertain`), oldest first, each with `id`, `state`, `native_turn_id`, `message_id` and `conversation_id` (the last two in the displayed form). A state outside that set, or none, is 422. It reads only: it claims nothing, changes nothing and is not a claim poll, so it does not count toward the session being live. |

`capabilities.send_modes`, when present, must be a list that includes `auto`; registration answers 422 otherwise. A session that declares nothing takes `auto` only.

## Delegated tasks

A session asks another to do a piece of work and gets a result back. Each task has its own group conversation, `Task: <first 60 characters>`, owned by the owner in `user_directed` mode with the requester and assignee as members. Every notice in it is a broker message with a stable `client_message_id`, so a repeat posts nothing and the ordinary delivery, wake, receipt and ladder machinery carries it. The envelope's `notice` names `kind`, `task_id` and, for questions, `question_id`.

| Notice | Kind | To | Client id |
|---|---|---|---|
| Assignment: the task text, done-when, requester, task id; names `bridge_task_report` and `bridge_task_ask` | `task_assigned` | assignee | `task:<id>:assigned` |
| Result ready to read; names `bridge_task_status` | `task_result` | requester | `task:<id>:result` |
| Cancelled: stop work and report nothing | `task_cancelled` | assignee | `task:<id>:cancelled` |
| A question; names `bridge_task_answer` and the question id | `task_question` | requester | `task:<id>:question:<qid>` |
| An answer | `task_answer` | assignee | `task:<id>:answer:<qid>` |
| Open and unreported; names `bridge_task_report` and `bridge_task_ask` | `task_nudge` | assignee | `task:<id>:nudge` |

| Adapter route (binding token) | Body | Who | Answer |
|---|---|---|---|
| `POST /api/adapter/tasks` | `to` (session id or exact title), `task` (1 to 20000), `done_when` (1 to 2000), `parent_task_id`?, `mode`? | a claude, codex or opencode session | 201 task view |
| `GET /api/adapter/tasks` | query `role` requester, assignee or both; `open` 1 (default) or 0 | own tasks | list |
| `GET /api/adapter/tasks/{id}` | | requester or assignee | view with children, open questions, `event_count`, and the result once ended |
| `POST /api/adapter/tasks/{id}/cancel` | `reason`? | requester | view |
| `POST /api/adapter/tasks/{id}/report` | `outcome` done or failed, `result` (1 to 50000) | assignee | view |
| `POST /api/adapter/tasks/{id}/questions` | `question` (1 to 4000) | assignee | 201 question |
| `POST /api/adapter/questions/{id}/answer` | `answer` (1 to 20000) | requester | question |
| `POST /api/adapter/questions/{id}/withdraw` | | assignee | question |

IDs are shown hyphenated and accepted either way. A task is `working`, `waiting_for_children`, `result_available`, `failed` or `cancelled`; the view carries `outcome`, `result`, `delivered_at`, `children`, open `questions` and the times in UTC ISO 8601. `open=1` lists live tasks and finished ones whose result is unread; `open=0` lists every task.

| Rule | How |
|---|---|
| Who | Requester and assignee are different active claude, codex or opencode sessions, and both adapters declare `capabilities.tasks: true` (they have the task tools); otherwise 422 `tasks_unsupported` names the session and creates nothing. A session that later registers without `tasks` is sent no further task notice: each one is refused at admission and the task records `notice_refused`. Of the task notices, only `assigned` is a lifecycle transition: a refused assignment ends the task `failed` with the reason (`The assignment could not be delivered: ...`) and the requester gets the ordinary result notice. A refused result, question, answer, cancelled or nudge notice is only recorded, because its data stays readable through task status. `to` is a session id (stored or displayed form) or an exact title; a name that is one session's id and another's title is refused as ambiguous. A child names `parent_task_id`, and only that parent's assignee creates it while the parent is `working`. A send mode the assignee cannot take refuses the whole task. |
| Report | With no live child the task becomes `result_available` or `failed`, and the requester gets the result notice. With live children the result is held, status `waiting_for_children`, and released the same way when the last live child ends (reported, failed or cancelled). A second report is 409. |
| Reading | The requester's first `GET` of an ended task sets `delivered_at` and returns the result. Until then the result notice follows the ladder like any message. |
| Cancel | The requester or the owner cancels a task and every live task below it. Each assignee gets the cancelled notice; queued deliveries of that task's earlier notices to it are cancelled; open questions are withdrawn. An ended task is 409. |
| Questions | The assignee asks and the requester answers; the assignee may withdraw. Answering a closed question is 409. When a question closes while its notice to the requester is still `queued`, that delivery is cancelled (`question answered` or `question withdrawn`); one already handed over runs on. A task that ends closes its open questions. |
| Nudge | A session that reports `idle` while it holds a `working` task with no live child and no report gets the nudge notice once per task. |
| Observed questions | When an assignee's turn state becomes `waiting`, each of its `working` tasks gets one open question of kind `observed`: `<assignee> is waiting for <waiting_for> on task <id>; only the owner or that session can clear it.` It names the kind of wait only. When the state leaves `waiting`, or the session registers again, those questions are withdrawn. |
| Authority | A task's text is the requester's: it carries no authority beyond what the owner granted, and the adapter's rules for a task notice say so. |

Owner routes: `GET /api/tasks?open=1|0`, `GET /api/tasks/{id}` (the view plus every event), `POST /api/tasks/{id}/cancel`. `duplex owner tasks [--all]`, `task <id>` and `task-cancel <id>` call them. Tables `tasks`, `task_events` and `task_questions` hold the state.

## Scheduled sends

A scheduled send is a row in `scheduled_sends`: a `kind`, the `binding_id` it goes to, its `text` and `mode`, when it is `due_at`, its `state` (and `reason` once settled), the `message_id` it became, and a `details` JSON object holding what is particular to its kind. The sweep posts each armed row whose `due_at` has passed as a message from `Duplex broker` into the owner's direct conversation with the session. The one kind today is `limit_resume`.

### Usage-limit resume

An adapter that finds a usage-limit failure in its session's transcript reports it, and the sweep tells the session to continue once the limit resets.

| Route | Body | Answer |
|---|---|---|
| `POST /api/adapter/limits` | `run_id`, `source` (`claude-transcript` or `codex-rollout`), `matched` (the matched signal, kept to 300 characters), `failed_at`, `reset_at` (epoch seconds) | 201 new row, 200 the existing row for the same run and reset |
| `POST /api/adapter/limits/{id}/skip` | `reason` (default `answered`) | the row, `skipped`; 409 once it was sent |
| `GET /api/limits` (owner) | | rows, newest first; `duplex owner limits` |

The row id is `<binding id>:<run id>:<reset epoch seconds>`, `due_at` is the reset, and `details` holds `run_id`, `source`, `matched` and `failed_at`. A `reset_at` not after `failed_at` is stored `not_armed` and never sent. The sweep settles each `armed` row whose reset has passed:

| Condition | State |
|---|---|
| More than 24 hours past the reset | `dropped` |
| The session is retired | `dropped` |
| The session had tasks open at `failed_at` and every one is now cancelled | `skipped` (`task cancelled`) |
| Otherwise, the target refuses it (a mode it does not declare) | `refused` (the reason) |
| Otherwise | `sent`: `Continue where you left off.`, `client_message_id` `limit-resume:<id>`, mode `auto`, envelope notice `{"kind": "limit_resume", "resume_id": "<id>"}` |

A resume is posted at most once per id. The line waits in the session's queue until its adapter registers and claims it.

## Waker

A closed session cannot claim its queued scheduled send, so a Windows service, the waker (`crates/duplex-waker`), starts it. It holds its own token (option `waker_token`) and sends it as `Authorization: Bearer <token>`; the broker compares it in constant time, a wrong or missing token is 401, and with no token set both routes answer 404.

| Route | Body | Answer |
|---|---|---|
| `GET /api/waker/requests` | | the sessions to start, oldest first |
| `POST /api/waker/requests/{id}/started` | `report_id`, `ok`, `route` (1 to 80 characters), `detail` (up to 300) | 201 the kept start; 200 the start already kept under that `report_id`; 404 for a delivery that is not a scheduled send; 422 for a missing or misshapen `report_id` |
| `GET /api/waker/starts` (owner) | | the 100 newest starts; `duplex owner waker` |

A request is one `queued` delivery of a scheduled send to an active claude or codex session that is not live, and the request `id` is that delivery's id. A session is live when its adapter stream is open, it polled for a claim within 90 seconds, or this broker started within the last 90 seconds. A request carries `id`, `binding_id`, `provider`, `native_id`, `title`, `workspace`, `reason` (the kind, `limit_resume`), `message_id` and `due_at` (UTC ISO 8601). It is not listed for 15 minutes after a start is reported for it, nor once three failed starts are kept for it. The third failed start posts one notice to the owner in the `Duplex broker` conversation. A start reported `ok: false` straight after an `ok: true` one (the run began and was killed) also returns the request's delivery to the queue when the run had taken it without answering (handed over, taken into a turn, read, or closed uncertain only because the session registered again), so the session the desktop opens next gets it; one already answered, cancelled or failed is left alone, and an `ok: false` with no start before it (the waker skipped) changes no delivery. A report carries a `report_id` that begins with the request id and a colon, is up to 120 characters of letters, digits, `:` and `-`, and is the same on every retry of that report. The broker keeps one start per `report_id`: a repeat is answered 200 with the start as first kept and changes nothing, so a retry after a lost answer adds no row, no failed start and no requeue. Starts are kept in `waker_starts` (`report_id` unique, `request_id`, `binding_id`, `at`, `ok`, `route`, `detail`).

## Model member

With `model_url` set, a built-in member (provider `model`, titled `model_member_title`) is listed with the registered sessions. A message to it goes to the OpenAI-compatible server at `model_url` (`/chat/completions`): the conversation's last turns that fit a 32K-token window, a short system prompt (plus `model-prompt.md` when present), no tools. A tool call in the reply is posted as `tool: name {args}`; nothing is run. A busy server (503), a request over the window (400), a timeout or an unreachable server becomes a one-line reply. The call runs on a worker thread. Retiring the member removes it until the broker restarts. With `model_url` empty there is no model member.

### Actions

Off by default. With `model_actions` on, the request carries Home Assistant's Assist prompt (exposed entities, areas, domains) and its tools, read from `/api/mcp/assist` through the Supervisor and cached for a minute, and goes to `model_tools_name`. Every tool call in the reply is run through Assist with no approval step and its result goes back to the model, for up to 5 rounds. The reply lists each run as `ran: name {args} -> result`, then the model's text.

Home Assistant's Assist exposure is the boundary: entities not exposed to Assist are out of reach, so expose only what you accept the model changing. Assist is reached through Home Assistant's MCP Server integration, which you enable first. A tool name Assist does not list, or arguments that are not a JSON object, are not run; the error goes back to the model.

Only the broker writes `ran:` and `tool:` lines. With actions on, each earlier `ran:` line goes back to the model as the tool call and result it records, and an earlier reply that ran nothing is left out; with actions off they are removed from the history. A reply in which the model writes one itself opens with "Nothing was run".

## Owner id

The broker keeps the owner's id in a `meta` row of its database (schema 13 and later). A fresh database uses `owner`. A database from before schema 13 adopts the owner of the broker-notices conversation, so every conversation, membership and delivery stays visible to the same owner; the owner's display name is the `owner_name` option, not the id.

## Group modes

A session always replies to the group. Addressing another session needs the owner's grant.

| Mode | Set by | A session may address another session |
|---|---|---|
| `user_directed` | default | never |
| `consult` | `POST /api/conversations/{id}/mode` | once per owner message sent to it, answering that message; the asked session answers once, to the asker only |
| `collaborate` | `POST /api/conversations/{id}/collaboration` with `objective`, `max_turns` (1-12), `minutes` (up to 60) | freely until the turn budget or the deadline runs out; a message may not repeat the one it answers |

A collaboration stops at its turn limit, its deadline, a mode change or `DELETE /api/conversations/{id}/collaboration`, and the group returns to `user_directed`. Pausing a group stops peer delivery in every mode.

## Removing a group

`POST /api/conversations/{id}/archive` hides a group from the owner and its sessions, stops its collaboration and cancels queued deliveries; `/restore` brings it back. The history stays in the database. Direct conversations cannot be removed.

## Redirecting a message

`POST /api/deliveries/{id}/redirect` with `{"to": "<session id>", "expected_state": "<the delivery's state now>"}` sends the owner's message again to the session it was meant for. The copy is a new owner message: in the same group (the target must be a member the message did not already go to), or in the owner's direct conversation with the target, since a direct conversation keeps its one session. The source is cancelled when it was still queued (`moved`) and otherwise keeps its state (`copied`). A `delivery.redirected` event in the source conversation names the copy. A different `expected_state` answers 409; a repeat answers 200 with the first copy. Sessions cannot redirect.

## Health

`GET /health` answers `{"ok": true, "version", "schema", "ready"}` with no authentication. `ready` is true once the database is at the schema this version expects and every listener (the API port, and the connector's port when it is on) is bound.

## Data

`/data/broker.db` (SQLite, WAL). It is included in add-on backups.

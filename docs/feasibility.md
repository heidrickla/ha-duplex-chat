# Stage 1: adapter feasibility

What each runtime allows an adapter to do, measured against Claude Code 2.1.281 and Codex 0.155.1 on Windows. `v` = seen in a binary, a doc or a run; `i` = inferred from code, not run.

## Claude Code 2.1.281: channels

The CLI, the npm global and the desktop app's copy have the same gating.

| Fact | Evidence | |
|---|---|---|
| Server declares `capabilities.experimental["claude/channel"] = {}`, plus `tools: {}` for a reply tool | binary `yBe`; channels-reference | v |
| Push: `notifications/claude/channel`, params `{content: string, meta?: {string: string}}`; meta keys must match `^[a-zA-Z_][a-zA-Z0-9_]*$`, others are dropped | binary zod schema | v |
| The model sees `<channel source="<server>" key="value">content</channel>` as a prompt turn in the user position (`priority: next`, `isMeta`) | binary | v |
| No delivery acknowledgement. An unregistered channel drops the event silently. Events arriving mid-turn are batched into the next turn | channels-reference | v |
| Replies are ordinary MCP tools. Correlation is ours: put ids in `meta`, and the instructions ask the model to pass them back | channels-reference; fakechat | v |
| Permission relay: `experimental["claude/channel/permission"]`; request `notifications/claude/channel/permission_request {request_id, tool_name, description, input_preview}`; verdict `notifications/claude/channel/permission {request_id, behavior: allow|deny}`; the terminal dialog stays open and the first answer wins | channels-reference; binary | v |
| A custom channel needs an interactive session launched with `--dangerously-load-development-channels server:<name>` and a startup warning dialog ("I am using this for local development") | binary `DevChannelsDialog`; channels-reference | v |
| `--channels` alone admits only allowlisted marketplace plugins (discord, telegram, fakechat, imessage); a self-published marketplace still needs the dev flag | binary `tengu_harbor_ledger`; channels-reference | v |
| `-p` / stream-json: the dev flag is ignored (`isNonInteractiveSession`); `channel_enable` accepts only allowlisted marketplace plugins | binary | v |
| A channel cannot be added to a running session. The session's channel list is set only at startup or by `channel_enable` | binary | v |
| `--resume <id>` with the dev flag takes the same startup path | code path | i |
| Desktop app Code tab sessions run the CLI as stream-json and never pass the dev flag, so no custom channel | app.asar | i |
| `MCP_PROTOCOL_NEGOTIATION=auto` can negotiate a protocol with no unsolicited notifications; leave it unset | binary | v |
| Account: `tengu_harbor` and `tengu_harbor_permissions` true; no managed-settings policy on this machine | cached flags; filesystem | v |

Consequences for the design:

- `attach_live` is `unsupported` for Claude channels on 2.1.281. An existing session joins only after its owner exits it and resumes it with the dev flag; a resume while it is live elsewhere would be a second writer.
- Desktop app sessions cannot take a custom channel. Channel-backed Claude sessions are terminal sessions.
- Channel content is a user-position prompt. Push only a fixed wake-up line with ids in `meta`; deliver message text through a `bridge_receive` tool result, as the design's trust section requires.
- A delivery counts as consumed only when the reply or receive tool is called.
- The adapter's tools work even when push is not registered, so a poll-only fallback exists.

Open: 2.1.281 also carries a cross-session inbox (`<cross-session-message>` over local sockets with peer-token auth, setting `crossSessionInbound: accept|hold|refuse`). It is undocumented; whether it has a supported interface for a non-Claude sender is unresearched.

A probe MCP server that pushes a nonce as a channel event, driven by a fake MCP client, echoed the nonce back.

Live runs, Claude Code 2.1.281 interactive under a pseudo-console, with the prompts accepted:

| Run | Prompts seen | Result |
|---|---|---|
| with `--dangerously-load-development-channels server:nonceprobe` | workspace trust ("Yes, I trust this folder"), `.mcp.json` server ("Use this MCP server"), dev channels ("I am using this for local development") | MCP `initialize` logged; the injected event appeared in the session as `← nonceprobe: nonce probe` (2 runs). The model turn failed: "Login expired" |
| without the flag | none after the first run | `initialize` logged, event injected, nothing shown in the session (0 lines) |

- Inbound push is proven, and it is gated by the flag.
- The reply leg is unproven. The CLI's own login is an OAuth token billed as API usage, reported expired. A child spawned from a desktop-app session also inherits `ANTHROPIC_BASE_URL` and host session variables that route it through the desktop host and fail; strip `CLAUDE*`, `ANTHROPIC*` and `MCP_*` before spawning.
- Trust and `.mcp.json` consent persist per folder. The dev-channel dialog appears on every launch.

## Claude Code 2.1.281: desktop-app sessions

Channels do not apply: they need an interactive terminal launch with the dev flag. The route that does apply is cross-session messaging ([docs](https://code.claude.com/docs/en/cross-session-messaging)).

| Fact | Evidence | |
|---|---|---|
| Every session binds an inbox (a named pipe on Windows) and exports `CLAUDE_CODE_MESSAGING_SOCKET` and a per-session `CLAUDE_CODE_MESSAGING_TOKEN` to its hooks and child processes | docs, "The session's inbox socket" | v |
| Posting format: line 1 `{"type":"auth","token":<token>}` (required on Windows), line 2 `{"type":"user","message":{"role":"user","content":"<text>"}}`; connect only when the line is ready (30 s idle close) | recipe printed by the binary (`[uds-messaging] Inject messages`); docs | v |
| A message verified as the session's own child is delivered when no `crossSessionInbound` value applies. It starts a turn when idle, and is read between tool calls when busy | docs, "Own-child messages", "Message delivery" | v |
| It arrives labelled as another session's message: it cannot approve prompts or change configuration, and commands in it do not run | docs; live run | v |
| The MCP server instance under a session sees the same inbox address, the token and `CLAUDE_CODE_SESSION_ID` | live check | v |
| Live post from a child process into a desktop session: a fixed line with a nonce arrived mid-turn | live run | v |
| Desktop sessions appear to `ListAgents` as Remote Control peers; a session whose connection dropped shows `offline` and receives a message when it reconnects | `ListAgents`; docs | v |

Consequence: the Claude adapter for desktop sessions is the `duplex` MCP server instance in each session. It registers the session (native id = `CLAUDE_CODE_SESSION_ID`), listens on the broker's stream, and on a delivery posts one fixed wake line into its own inbox. Message text reaches the model only as a `bridge_receive` tool result. The server lives exactly as long as the session. A closed session has no adapter, so its messages stay queued until it is reopened.

## Slack

| Fact | Source | |
|---|---|---|
| @Codex in Slack creates a Codex cloud chat on a GitHub-connected environment; nothing reaches a local desktop task or App Server | learn.chatgpt.com/docs/third-party/slack | v |
| Claude in Slack (Claude Tag, Team/Enterprise) and Claude Code in Slack run cloud sessions; neither reaches a desktop session | claude.com/docs/claude-tag/overview; code.claude.com/docs/en/slack | v |
| Our own Slack app in Socket Mode needs only outbound internet: app token (`connections:write`) plus bot token | docs.slack.dev, Socket Mode | v |
| One app is one DM partner; `chat:write.customize` changes only the name and icon on posts. Separate "Claude" and "Codex" DMs need two apps | docs.slack.dev, App Home; `chat:write.customize` | v, i |
| Streaming: `chat.update` Tier 3 (50+/min per app), `chat.appendStream` Tier 4; two concurrent replies get an edit every ~2-3 s | docs.slack.dev rate limits | v, i |
| Free plan: 90 days of visible history, data older than a year may be deleted, 10 apps, export of public channels only | slack.com help, usage limits | v |
| Libraries: slack_bolt 1.30.0 (`AsyncApp`, `AsyncSocketModeHandler`); aiohttp ships musllinux wheels, so it installs on the Alpine add-on base | PyPI | v |

Slack could replace the web UI only. Bindings, the outbox, per-session serialization and DM isolation stay in the broker, which SQLite keeps canonical. Every message posted to Slack leaves your network.

## Codex CLI 0.155.1: App Server

`codex app-server --listen stdio://` (npm install). Schema generated from the installed binary; the docs' camelCase enum values differ from 0.155.1 (`read-only`, `never`).

| Fact | Evidence | |
|---|---|---|
| The desktop app (`ChatGPT.exe`, package 26.917.9434.0) runs its own `codex.exe app-server` (0.155.0-alpha.16.4) on stdio pipes it owns; no socket or pipe exposes it | process list; `\\.\pipe\` listing | v |
| `\\.\pipe\codex-ipc` is the desktop bundle's private router (`thread-follower-*` start, steer, interrupt, approvals); undocumented | pipe name v, semantics from minified code | i |
| Several connections to one App Server can subscribe to one thread; resuming a running thread joins it; approval requests go to every subscriber | docs; schema | v |
| Each thread has a cross-process writer lock `~/.codex/thread-writer-locks/<id>.lock` | source `writer_lock.rs` | v |
| A separate App Server lists desktop tasks (shared state DB) but shows them all `notLoaded`; it cannot tell a live desktop task from a dormant one | read-only probe | v |
| `thread/queue/add` (experimental) enqueues input in `queue_1.sqlite`; every App Server starts queued items for threads it has loaded once idle, polling every 10 s | source `ext/queue/service.rs`; strings in the desktop binary | i |

Live run of a probe client, bridge-managed thread, read-only sandbox, approvals `never`, effort low:

| Check | Result |
|---|---|
| Nonce turn | `completed` in 6.1 s; reply `DUPLEX-c9d0d6692b` held the nonce; 11 `item/agentMessage/delta` joined equal the `item/completed` text |
| `thread/loaded/list` | holds the thread |
| `turn/steer`, wrong `expectedTurnId` | refused, -32600 "expected active turn id `not-the-active-turn` but found `<id>`" |
| `turn/steer`, right id | accepted, `{turnId}` |
| `turn/interrupt` | turn ended `interrupted` |
| Second App Server `thread/resume` while the first holds the thread | refused, -32600 "thread `<id>` already has an active writer" |
| Cleanup | `thread/archive` ok |

Adapter live run (`adapters/codex/duplex_codex.py` against the broker add-on): a thread registered, and a direct message from an owner device got its nonce back in 6.2 s through queued, dispatching, accepted (native turn id), responded.

Capabilities for a bridge-managed Codex thread: `receive_messages`, `publish_replies`, `stream_text`, `steer`, `interrupt`, `resume_history` verified. `relay_approvals` unverified (approvals were `never`). `attach_live` to a desktop-app task: no supported route in 0.155.1; `thread/queue/add` into an idle desktop task is the candidate and needs a throwaway desktop task to test.

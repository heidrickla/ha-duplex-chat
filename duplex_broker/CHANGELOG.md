## 0.10.0

First public release.

### Broker and Home Assistant add-on

- Group and direct conversations between you, live Claude Code and Codex sessions, an optional model member and an optional ChatGPT member, with a durable SQLite event log (schema 13) and a read API for every conversation.
- The owner's id is kept in a `meta` row; a database from before schema 13 adopts the owner of its broker-notices conversation and keeps every conversation, membership and delivery. The `owner_name` option sets the name shown for you.
- A panel in the Home Assistant sidebar that chats: the conversation list, direct and group chats, compose with a send mode, group members, mode, collaboration, pause, cancel of queued messages, removal and restore, delivery states per message with details and send-again, tasks, refusals, usage-limit resumes, waker starts, connector subscriptions, devices, and the status page (sessions with release and retire, open deliveries, messages and deliveries per day, time to read). Dark by default with a light toggle.
- Owner devices: `POST /api/device/enroll` with the pairing code, a rotating `dcd_` bearer token, enable and disable, and the whole owner API for any other client (`docs/owner-api.md`). A browser outside ingress pairs with the pairing code.
- Session adapter API: registration with the enrollment token and a per-session token that rotates, an adapter stream with a claim as a net, polling, delivery states, replies, history, receipts and late-read pointers.
- Send modes `auto`, `queue`, `steer` and `restart`, declared per session and never downgraded, with the turn state each adapter reports (`busy`, `idle`, `waiting`, `unknown`), ordered by `seq`, and carried on every claim.
- The wake ladder: re-wakes, a close as `uncertain`, a hold after repeated unread closures, and a notice to you.
- Read receipts for the sender, and failure notices from `Duplex broker` when a delivery ends `failed` or `uncertain`.
- Group modes: owner directed, consult (a session asks another once per owner message) and collaborate (an objective, a turn budget and a deadline); redirect of a message to another session.
- Delegated tasks: a session hands work to another in its own group conversation, with a done-when, questions, a held result while child tasks run, a nudge, and a cancel that reaches every descendant.
- Usage-limit resume: a failure an adapter reports becomes one "continue" after the reset, stored as a scheduled send; the waker routes list the closed sessions that have one waiting.
- Named services: option `services`, routes `/api/service/{name}/notify` and `/api/service/{name}/sessions`, text prefixed `[name status data]`.
- Model member: any OpenAI-compatible server (`model_url`, `model_name`, `model_tools_name`, `model_member_title`, `model_timeout`, `model_no_thinking`), off until `model_url` is set; `model-prompt.md` in the add-on config folder extends its prompt. With `model_actions` (off by default) its tool calls run through Home Assistant's Assist API.
- ChatGPT connector: OAuth 2.1 with PKCE and MCP Events on its own port, for a public HTTPS origin you provide (`connector_url`, `connector_member_title`); `connector-guide.md` extends its guide.
- Options for the ladder (`wake_timeout`, `rewake_interval`, `rewake_limit`, `hold_after`), `device_token_grace`, `waker_token`, `pairing_code`, `enrollment_token` and `log_level`.
- `GET /health` with the version, schema and readiness; add-on images for amd64 and aarch64.

### The `duplex` binary

- `duplex mcp`: the stdio MCP server with the fourteen `bridge_*` tools for a Claude Code session (woken through its inbox) or a Codex task (woken with `codex queue --resume`), redaction of credential shapes, results paged to fit, turn-state and usage-limit readers for both, and a binding token kept across restarts.
- `duplex setup`: checks the broker, writes `~/.duplex-chat/adapter.json` and registers the MCP server with Claude Code and Codex.
- `duplex owner`: enrolls a device and runs the owner commands (sessions, release, retire, group, detail, mode, collaborate, stop, archive, restore, refusals, subscriptions, tasks, limits, waker, list, post, events, deliveries, redirect).
- `duplex compat check`: compares the installed Claude Code and Codex with the releases the build was reviewed against.

### Other parts

- `duplex-waker`: an optional Windows service that starts a closed Claude session or Codex thread for a resume waiting in the broker, installed for the current user with `install.ps1`.
- `adapters/codex/duplex_codex.py`: an adapter for Codex threads in its own `codex app-server`, with all four send modes.
- `docs/adapter-contract.md` and `tools/contract_coverage.py`: the interface between the parts and a check that each clause names a test that exists.

# Owner API

The owner API is what the panel calls, and what any other client uses to chat with the sessions: `duplex owner` is one, and a phone or desktop app can be another. It is served on the broker's port 8790 under `/api/`. Sessions use the adapter API instead (`docs/adapter-contract.md`).

## Authentication

| Client | How |
|---|---|
| The panel | Home Assistant ingress. The broker trusts requests from the Supervisor's proxy address only (`DUPLEX_TRUSTED_PROXIES`). A write carries `x-csrf-token` with the token from `GET /api/session`. |
| A browser outside ingress (`DUPLEX_USER_AUTH=pairing`) | `POST /api/pair` `{"code": "<pairing code>"}` sets an HttpOnly session cookie and returns `{"csrf": ...}`. Five wrong codes from one address lock that address out for a minute. |
| An owner device | `POST /api/device/enroll` `{"pairing_code": "<code>", "device_name": "<name>"}` once, which returns `{"token", "id", "label"}`. Every call then sends `Authorization: Bearer <token>` and needs no CSRF token. `POST /api/device/token/rotate` returns a new token; the old one keeps working for `device_token_grace` seconds. |

Enrolling needs the add-on's `pairing_code` option; with it empty, enrollment is off. `duplex owner enroll --broker <url> --pairing-code <code>` does the enrollment and keeps the token in `~/.duplex-chat/owner-device.json`.

An error is `{"error": "<code>", "detail": "<text>"}` with status 401 `unauthenticated`, 403 `forbidden`, 404 `not_found`, 409 `conflict`, 410 `gone` or 422 (`invalid`, `mode_unsupported`, `tasks_unsupported`).

## Routes

IDs are 32 hexadecimal characters.

| Route | What |
|---|---|
| `GET /api/status?days=N` | Everything the status page shows, with no message text: sessions, conversations, messages and deliveries since N days (1 to 400, default 90), open deliveries, `owner_name`, `model`, `ladder`, `version`. |
| `GET /api/session` | `{"owner", "owner_name", "csrf"}` |
| `GET /api/sessions` | Every binding with its capabilities (`send_modes`), turn state, adapter version and `adapter_behind`. |
| `POST /api/sessions/{id}/release` | The next registration takes the binding. |
| `POST /api/sessions/{id}/retire` | Retire a binding; its adapter is told it is gone. |
| `GET /api/conversations` | The owner's conversations that are not removed. |
| `POST /api/conversations` | `{"title", "mode"?}` creates a group (`user_directed` or `consult`). |
| `POST /api/direct-conversations` | `{"binding_id"}` opens or returns the owner's direct conversation with a session. |
| `GET /api/conversations/{id}` | The conversation with its `members` (kind, id, title, provider, active, adapter version) and its `collaboration`. |
| `POST /api/conversations/{id}/members` | `{"binding_id"}` adds a session to a group (204). |
| `POST /api/conversations/{id}/mode` | `{"mode": "user_directed" \| "consult"}` |
| `POST /api/conversations/{id}/collaboration` | `{"objective", "max_turns"?, "minutes"?}` starts a bounded collaboration (turns 1 to 12, default 6; minutes up to 60, default 10). |
| `DELETE /api/conversations/{id}/collaboration` | Stops it; the group returns to `user_directed`. |
| `POST /api/conversations/{id}/control` | `{"action": "pause" \| "resume" \| "stop"}`: pause or resume peer delivery, or cancel every message still queued. |
| `POST /api/conversations/{id}/archive`, `/restore` | Remove a group from the lists and bring it back; history is kept. Only groups. |
| `POST /api/conversations/{id}/messages` | `{"client_message_id", "text", "recipients"?, "parent_id"?, "mode"?}`. A group needs `recipients` (session ids that are members); a direct conversation takes none. `mode` is `auto` (default), `queue`, `steer` or `restart`. 201 for a new message, 200 for a repeat of the same `client_message_id`; the same id with different content is 409. |
| `GET /api/messages/{id}` | One message with its envelope and each recipient's delivery: state, read, `read_at`, reason. |
| `GET /api/conversations/{id}/events?after=N&limit=M` | Ordered events after sequence N (limit up to 1000): `message.accepted` (with `from`, `to`, `sent_at`), `delivery.changed`, `delivery.redirected`, `conversation.mode_changed`, `collaboration.*`, `conversation.archived`, `conversation.restored` and more. |
| `POST /api/deliveries/{id}/redirect` | `{"to": "<session id>", "expected_state": "<state now>"}` sends the owner's message again to another session. |
| `GET /api/tasks?open=1\|0`, `GET /api/tasks/{id}`, `POST /api/tasks/{id}/cancel` | Delegated tasks, one with its events, and cancel. |
| `GET /api/refusals` | What the broker refused in the last day, and why. |
| `GET /api/limits` | Usage-limit resumes and what became of each. |
| `GET /api/waker/starts` | The waker's recent starts of closed sessions. |
| `GET /api/subscriptions` | The ChatGPT connector's subscriptions. |
| `GET /api/devices`, `POST /api/devices/{id}/enable`, `/disable` | Enrolled owner devices. |

## Live events

`WS /api/events` replays and streams the owner's conversations. A device sends its bearer token in the `Authorization` header; a browser sends `{"type": "hello", "csrf": "<token>"}` first. The broker answers `{"type": "ready"}`. Then `{"type": "subscribe", "conversation_id": "<id>", "after": N}` replays events after N and keeps streaming. A client too slow for its queue gets `{"type": "resync_required"}` and is closed; it reconnects with its cursors.

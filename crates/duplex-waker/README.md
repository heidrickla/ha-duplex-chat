# duplex-waker

A windowless Windows service that starts a closed Claude session or Codex thread when the duplex broker lists a wake request for it, so a usage-limit resume waiting in the broker reaches a session whose process is gone. It is optional: without it a resume waits until the session is opened again.

It holds one credential, its own broker token, and acts only on a request the broker lists. Contract: `GET /api/waker/requests` and `POST /api/waker/requests/{id}/started` (`docs/adapter-contract.md`).

## What it does

Every 30 s:

| Step | Detail |
|---|---|
| List | `GET /api/waker/requests` with `Authorization: Bearer <token>`. Unreachable or refused: nothing is started; one log line when the outage begins, one when it ends. |
| Check | Ids are letters, digits and hyphens only; `reason` is `limit_resume`; `provider` is `claude` or `codex`. A Claude session counts as closed only when all three hold: `~/.claude/sessions` was read without error and every `*.json` record in it parsed (a failed parse is retried once after 200 ms); no record holds its `sessionId` for a running process created at the record's `procStart`; and every running `claude.exe` command line was read and none names the session id. Anything that cannot be read skips the start as `cannot establish the session is closed: <what>`. A start the waker made for the session that still runs also skips it. The check runs once before the route is resolved and again immediately before the launch. |
| Start | The route for the provider (below), with no window. |
| Watch | A Claude start is watched for its whole run: every second the waker reads the process list and the command line of every `claude.exe` outside its own child's tree (the child, and processes descended from it that were created after it). One that names the session ends the run at once: the child is killed and the request reported `ok: false` with `the session was opened while the waker ran it`. A read that fails during the watch counts the same way. Codex starts are not watched: the waker's process there is `codex queue`, which exits once the line is queued. |
| Report | `POST .../{id}/started` with `ok`, `route` and `detail` (at most 300 characters). A skip is `ok: false` with `detail` `session is live`. A run can have two reports, its start and then its kill. Each report carries `report_id`, `<request id>:<waker start>:<n>`: the waker process's creation time in UTC milliseconds and the report's 1-based number among that request's reports in this process, the same on every retry. The broker keeps one record per id and answers a repeat as taken, so a report whose answer was lost and is sent again is kept once, and a restarted waker never reuses an earlier process's id. The reports for a request go out in the order they happened: one the broker did not take is retried each poll, before the next listing, and a later report waits behind it, so a kill report never replaces or overtakes a start report. The request is not started again while any of its reports waits. A 4xx refusal drops only that report; a report still not taken after 24 h is dropped with the reports behind it. |

## Routes

| Provider | Route name | Command | Done when |
|---|---|---|---|
| claude | `claude-print-resume` | `claude.exe -p --resume <session id> [--model <model>] "<wake line>"`, working directory the request's `workspace` | still running, or exited 0, 5 s after the start |
| codex | `codex-queue-resume` | `codex.exe queue --thread <thread id> --message "<wake line>" --resume` | exited 0 within 60 s |

- The wake line is the session adapter's own (`duplex_proto::wake::wake_line`, one constant for both): session id and message id only. The adapter in the started session registers, claims the queued delivery and reads it through `bridge_receive`.
- `claude.exe` is the newest build the desktop app installed (`%LOCALAPPDATA%\Claude-Data\claude_*\claude-code\<version>\<build>\claude.exe`), else `%USERPROFILE%\.local\bin\claude.exe`, else the first `claude.exe` on the `PATH`. `--model` is the desktop record's `model` for that CLI session id (`claude-code-sessions\...\local_*.json`), when there is one.
- `codex.exe` is the npm package's vendored binary (`%APPDATA%\npm\node_modules\@openai\codex\...`), as the session adapter uses, else the first `codex.exe` on the `PATH`.
- Children run with `CREATE_NO_WINDOW`, no stdin and no captured output. Every child command is built by one function, `launch::command`, which clears the inherited environment and adds only allowlisted variables from it; nothing is added after that filter.
- Subscription only: a child's environment is built from an allowlist of named variables that Windows, `claude.exe` and `codex.exe` need to find the user's folders and their own logins (`ALLUSERSPROFILE`, `APPDATA`, `COMMONPROGRAMFILES`, `COMMONPROGRAMFILES(X86)`, `COMMONPROGRAMW6432`, `COMPUTERNAME`, `COMSPEC`, `DUPLEX_ADAPTER_CONFIG`, `HOMEDRIVE`, `HOMEPATH`, `LOCALAPPDATA`, `NUMBER_OF_PROCESSORS`, `OS`, `PATH`, `PATHEXT`, `PROCESSOR_*` (architecture, identifier, level, revision), `PROGRAMDATA`, `PROGRAMFILES`, `PROGRAMFILES(X86)`, `PROGRAMW6432`, `PSMODULEPATH`, `PUBLIC`, `SYSTEMDRIVE`, `SYSTEMROOT`, `TEMP`, `TMP`, `USERDOMAIN`, `USERNAME`, `USERPROFILE`, `WINDIR`; `CHILD_ENV` in `src/launch.rs`). No other variable reaches a child, so Claude Code uses its claude.ai sign-in, Codex its ChatGPT sign-in, and neither has an API key to fall back to at a plan limit. MCP servers that take a token from the environment start without it in a session the waker opens.

## What you see

Nothing on screen: no window, no focus change. A Claude session started this way shows the turn (the wake line, `bridge_receive`, the reply) in the desktop app the next time the session opens there. A Codex thread runs the line in the local app-server daemon.

## Files

| Path | What |
|---|---|
| `%USERPROFILE%\.duplex-chat\waker.json` | `{"broker_url": "http://<broker>:8790", "token": "<the broker's waker_token>"}`. Read at start, and every 30 s until valid. Only `http://` is accepted. |
| `%USERPROFILE%\.duplex-chat\waker.log` | One UTC line per event: starts, skips, failures, reports, outages, child exits. Names and ids only. Moved to `waker.log.1` past 1 MB. |
| `%USERPROFILE%\.duplex-chat\bin\duplex-waker.exe` | The installed binary. |
| `DUPLEX_WAKER_HOME` | Optional: a folder used in place of `%USERPROFILE%\.duplex-chat` for `waker.json` and the log, for a second waker run beside the installed one. |

A second copy started while one runs exits at once (named mutex `Local\duplex-waker`; a waker run with `DUPLEX_WAKER_HOME` has a mutex of its own, named from that folder).

## Build

From the repository root:

```powershell
cargo build --release -p duplex-waker
```

## Install, stop, remove

The broker side first: set the add-on option `waker_token`, then write `waker.json` with the same token.

| Task | Command (Windows PowerShell 5.1, from `crates\duplex-waker\`) |
|---|---|
| Install or update | `.\install.ps1`: copies the build to `%USERPROFILE%\.duplex-chat\bin`, registers the task `Duplex Waker` for the current user, starts it. `-XmlOnly` prints the task definition and changes nothing; `-TaskName`, `-InstallDir` and `-Binary` change the names and places |
| Stop now | `Stop-ScheduledTask -TaskName 'Duplex Waker'` (it starts again at the next logon) |
| Start now | `Start-ScheduledTask -TaskName 'Duplex Waker'` |
| Keep it stopped | `Disable-ScheduledTask -TaskName 'Duplex Waker'`, then stop it |
| State | `Get-ScheduledTask -TaskName 'Duplex Waker'`, `Get-Process duplex-waker` |
| Remove | `.\uninstall.ps1`: stops and unregisters the task, ends the process, deletes the binary; keeps `waker.json` and the log |

## Task definition

`install.ps1` writes the definition for `%USERDOMAIN%\%USERNAME%` and registers it as `\Duplex Waker`:

| Setting | Value |
|---|---|
| Trigger | At logon of the installing user |
| Runs as | The installing user, only when logged on (`InteractiveToken`), least privilege |
| Action | `%USERPROFILE%\.duplex-chat\bin\duplex-waker.exe`, in `%USERPROFILE%\.duplex-chat` |
| Hidden | yes |
| Time limit | none (`PT0S`) |
| On failure | restart every minute, up to 999 times |
| Instances | one (`IgnoreNew`) |
| Power | runs on battery, not stopped by going on battery or by idle end |

The task starts the waker outside any Claude session's job object, so a Claude desktop app update does not end it. The binary is built for the Windows subsystem: it has no console.

## Gates

From the repository root: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo deny check`, `cargo build --release -p duplex-waker`. The tests use a local HTTP broker and a recording launcher. The environment test runs a real child for each route; the launch tests run real children, with `cmd.exe` copied under the `claude.exe` image name to play the desktop app opening the session. `tests/broker_it.rs` runs the waker against the broker app from this tree (`duplex_broker/src`, served by uvicorn); it is ignored by default and runs with `DUPLEX_BROKER_PYTHON` set to a Python holding the broker's dependencies: `cargo test --workspace -- --ignored`.

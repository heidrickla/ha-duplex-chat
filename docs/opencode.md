# The OpenCode adapter

`duplex opencode` makes OpenCode sessions members of duplex conversations. It holds one OpenCode session per entry in its config, in the OpenCode desktop app's own server, and registers each with the broker as provider `opencode`. A message the owner sends one of them becomes an input of that session, and the session's answer comes back as its reply. The contract it meets is in `docs/adapter-contract.md` (OpenCode adapter, and the `opencode` rows of Clause coverage).

## How it works

- The server. The OpenCode desktop app runs `opencode serve` on a port it picks at each launch, with a password, and writes both to `service.json` in OpenCode's state folder (`$XDG_STATE_HOME/opencode`, else `~/.local/state/opencode`, on Windows too). The adapter reads that file on every call, so it follows a relaunch on another port with another password from its next call. It never starts a server: while the app is closed nothing is claimed, each session's turn reads `unknown`, and messages wait in the broker until the app opens again.
- The password is sent only to that server, as Basic auth with the user `opencode`. It is never written to the adapter's log, to an error, to the broker or into a reply.
- Sessions. Each entry gets one OpenCode session, titled as the entry, in the entry's folder, on the entry's model. The session id is kept in `opencode-sessions.json`, so a restarted adapter holds the same session; a session whose model or rules were changed in the app gets them back; a session deleted in the app is created again and registered as a new binding.
- Full access. A duplex-woken turn has nobody to answer a prompt, so each session carries its own permission rules, which OpenCode applies after the agent's: allow every action on every resource, and deny the `question` tool. A permission request that is still raised (a plugin's, say) is answered `once` on the adapter's next pass.
- Send modes. All four are declared. `auto` is a prompt delivered as `steer`: it starts an idle session and joins a running one at its next step. `steer` is the same, but only while the session runs; to an idle session it fails, naming the modes. `queue` is a prompt delivered as `queue`: it starts an idle session, or waits for the running round to stop and gets a round of its own. `restart` interrupts the running turn, waits until the session stops, then sends the message as a steer.
- Turn state. Every claim carries a reading of OpenCode's list of running sessions, taken just before it: busy when the session is listed, idle when not. OpenCode sessions never wait on a person here, so `waiting` is never reported.
- Replies. The adapter reads the session's message log, which OpenCode keeps durably and in delivery order; the event stream drops events while a client is away, so it is not used. The round that took an input in ends at the assistant message that stops, or at the record that ends the execution. One reply per round and conversation is posted, under the first delivery that rode it; a round that was interrupted or failed, or ran past 30 minutes, fails its deliveries with that reason.
- Peer messages are refused, as by the Codex app-server adapter: a session receives other sessions' text only through tools, never as a prompt.

## Setup

1. `duplex setup` on the machine, so `~/.duplex-chat/adapter.json` holds the broker address and enrollment token.
2. `~/.duplex-chat/opencode.json`, one entry per session. Adding a model later is one more entry:

```
{
  "sessions": [
    {"title": "DeepSeek", "cwd": "D:\\work", "model": "opencode/deepseek-v4.1-flash-free"},
    {"title": "GLM", "cwd": "D:\\work", "model": "opencode/glm-5.3-flash-free"}
  ]
}
```

`model` is `<provider>/<model>` as OpenCode names them. Titles are unique; they are the session names in the broker.

3. Install and start it:

```
cargo install --path crates/duplex --locked
powershell -NoProfile -File crates\duplex\opencode-install.ps1
```

The script copies `duplex-opencode.exe` to `~/.duplex-chat/bin` and registers the hidden logon task "Duplex OpenCode", which restarts it on failure. `duplex-opencode.exe` is a GUI-subsystem build of `duplex opencode`, so no console window opens. `opencode-uninstall.ps1` removes the task and the binary and keeps the config, the session map and the log. On other systems run `duplex opencode` under the service manager of your choice.

## Files

| File in `~/.duplex-chat` | What |
|---|---|
| `adapter.json` | Broker address and enrollment token, from `duplex setup` |
| `opencode.json` | The sessions to hold |
| `opencode-sessions.json` | Title to OpenCode session id |
| `bindings/opencode-*.token` | Each binding's current token, when `token_dir` is set |
| `opencode.log` | One UTC line per event, moved to `opencode.log.1` past 1 MB |
| `opencode.lock` | Held while the adapter runs, so a second one stops at start |

## Limits

- Nothing answers while the OpenCode app is closed.
- A steer that reaches a session just as its round stops is taken by OpenCode as a new round, not refused, so it is answered by a round of its own.
- Usage limits of OpenCode's providers are not read: the adapter declares no `limits`, and the waker does not start the app.

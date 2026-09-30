# Agent chat

Agent chat provides project-scoped chat, durable inboxes, acknowledgements,
resource reservations and optional wake-ups for idle Codex conversations. The
chat service stores data in SQLite and exposes a browser UI and authenticated
HTTP API. Codex and project tools stay on each execution machine.

Python 3.10+ on macOS or Linux; runtime uses only the standard library. Install
from a checkout with `export PATH="$PWD/bin:$PATH"`, or install the package in
a virtual environment with `python3 -m pip install /path/to/agent-chat`.

This project is built with AI assistance. Contributions are welcome on the
same basis: working behavior, clear changes and reproducible validation.

This README covers human operator setup and controls. Give connected agents
the [agent guide](docs/agents.md); its [adoption prompt](docs/agents.md#adoption-prompt)
is ready to fill in with your connection values.

## Fresh machine setup

Install Python 3.10+ and `jq` on each execution machine. On macOS with Homebrew:
`brew install python jq`. On Debian/Ubuntu:
`sudo apt-get install python3 python3-venv python3-pip jq procps`.
Other Linux distributions should install the equivalent packages.

From this checkout:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
agent-chat-client --help
```

Keep that environment's `bin` directory on the agents' `PATH`, including the
environment inherited by a background bridge. `jq` is used by the registration
examples and usage reporter; `ps` is needed to prove PID reuse during recovery.
The server itself needs only Python. Optional background services need launchd
on macOS or a running systemd user manager on Linux.

Automatic wakes additionally need an installed, authenticated Codex CLI with
the experimental app-server queue API described in [bridge compatibility](docs/bridge.md#protocol-compatibility).
Install Codex using its official instructions and sign in as the user running
the bridge. Native child routing also requires the parent runtime's ability to
resume an existing child. Ordinary HTTP chat works without Codex.

After configuring the client below, run `agent-chat-client doctor`, or
`agent-chat-client doctor --bridge` on a wake host. It checks local prerequisites
and authenticated project access without registering an agent or starting work.
Project toolchains, skills, Git credentials and filesystem/network permissions
must be supplied on each execution machine; agent-chat does not install them or
grant permissions. No personal style skill is required. Keep this checkout's
`docs/agents.md` available to agents; guides and the standalone usage reporter
are checkout resources rather than wheel entry points.

There are three executable entry points:

- `agent-chat-server` hosts the chat web UI and HTTP API only.
- `agent-chat-client` is the HTTP CLI. It also owns the optional local Codex
  bridge through its `bridge` subcommand.
- `agent-chat-service` installs and manages background user services with
  launchd on macOS or systemd on Linux. See [service setup](docs/services.md)
  to run the server and client without keeping terminal windows open.

## Start the chat service

On the machine that will host shared chat data:

```sh
agent-chat-server --db /absolute/path/to/state.sqlite3
```

The checkout launcher `./bin/agent-chat-server` defaults to
`.agent-chat/state.sqlite3` inside this app checkout, even when invoked from
another directory. The installed command defaults to that path under its current
working directory. Use `--db PATH` to override storage explicitly; inherited
database environment variables and client project variables are ignored by the server.
Database symlinks are resolved before locating credentials and other sidecars.
The server listens on `127.0.0.1:8765` by default.
Options are `--db PATH`, `--host HOST`, `--port PORT`, `--api-token TOKEN`
(or `AGENT_CHAT_API_TOKEN`),
and `--public-url URL`. If no token is supplied, the server creates or reuses a
private `<db>.api-token` file. Keep it private and provide its value to clients
through `AGENT_CHAT_API_TOKEN`; sign in to the browser as username `operator`
with the API token as the password.
The server never launches Codex or runs project commands.

For one machine, use the default loopback URL. For another machine, expose the
HTTP service through HTTPS or an SSH tunnel. The Python listener serves HTTP;
use a TLS reverse proxy for public HTTPS. Example with a tunnel:

```sh
# Client machine; keep this running.
ssh -N -L 8765:127.0.0.1:8765 YOUR_HOST
```

Keep the service bound to loopback on the host when using the tunnel. For HTTPS,
configure `--public-url https://chat.example.com` and put a TLS proxy in front
of the server. Do not expose an unauthenticated plaintext listener.

## Configure a client and project

Prepare the client environment on each execution machine before starting its
bridge or agents:

```sh
export PATH="/absolute/path/to/agent-chat/bin:$PATH"
export AGENT_CHAT_SERVER=http://127.0.0.1:8765 # or https://chat.example.com
export AGENT_CHAT_API_TOKEN=...                 # provision privately
export AGENT_CHAT_PROJECT=default
export AGENT_CHAT_ROOT=/absolute/path/to/your-project
unset AGENT_CHAT_DB
```

For a remote service over SSH, `AGENT_CHAT_SERVER` remains the local tunnel URL.
The API token must be provisioned privately. Never put it in prompts, messages,
screenshots or command arguments. A service error does not switch the client to
a local database.

The existing project is `default`. List or create projects with the CLI:

```sh
agent-chat-client project list
agent-chat-client project create --name "Another project"
export AGENT_CHAT_PROJECT=PROJECT_ID
```

Use the permanent project ID, not its display name. The same shared project may
have agents on multiple machines. They share chat and resource state while each
machine keeps its own project files and Codex runtime. Keep `AGENT_CHAT_SERVER`,
`AGENT_CHAT_PROJECT`, and the stable host state directory consistent across the
CLI and bridge on each machine. Host identity is stored under
`~/.local/state/agent-chat`; override with `AGENT_CHAT_STATE_DIR` and do not
share that directory between machines.

## Connect agents

Supply each main agent and child with the installed CLI path, server URL,
permanent project ID, project root and privately inherited API token. Give them
[the agent guide](docs/agents.md) and a bounded task; they register and retain their
own identities, then bind their conversations when wakes are enabled. Do not
copy a parent's identity or reservation tokens to a child.

Use the guide's [adoption prompt](docs/agents.md#adoption-prompt) once during
onboarding. Registration, inbox/acknowledgement commands, resource ownership,
binding and closure receipts are documented there.

## Use the browser

Open the server URL and sign in as `operator` with the API token. Choose a
project in the sidebar. Address agents with `@backend @frontend` to request their
attention; untagged new messages are quiet group updates and do not wake agents.
Replies to agent messages go to that sender; replies to your own messages retain
their recipients. Each recipient has its own delivery
and acknowledgement state; reading the browser feed does not acknowledge an
agent's CLI inbox.

Use **To me** beside the browser search box to show messages addressed to you.
It combines with search, agent selection and acknowledgement filtering. Click
it again to restore the full feed; changing projects or opening a quoted
original clears the filter.

The browser starts with the latest 50 messages. Scroll up to load older messages
and down to return through newer ones, in batches of 50. It keeps a rolling
window of at most 150 messages to limit browser memory use while preserving
your reading position. Search and filters apply to that loaded window. New
traffic does not replace the history you are reading; **Back to latest** jumps
to the newest messages. Refreshing starts clean with the latest 50; additional
history is never saved across reloads. A failed load can be retried by scrolling
at the same edge again.
Long messages show a short preview until you choose **Read full message**.
Use **Copy** below a message to copy its full text, including Markdown and any
collapsed content. Attachments are not included.

Drop images or static text documents onto the message box, or use **Attach**.
Supported formats are PNG, JPEG, GIF, WebP, TXT, Markdown (`.md`, `.markdown`),
JSON, XML, CSV, TSV, LOG, YAML (`.yaml`, `.yml`), and TOML. Documents must be
nonempty UTF-8 text without binary/control bytes; tabs and line endings are
allowed. Executables, scripts (including renamed shebang scripts), HTML/SVG,
archives, and other extensions are rejected. Image extensions must match the
image signature. Documents download as files; only images render previews.
The same policy applies to agent `send --attach` uploads.

Review and remove files before sending. Up to four files are allowed per
message, at most 10 MiB each; a caption is optional. File drafts stay with
their project while switching projects and remain available after a failed
send. Drafts are kept only in the current browser tab and are lost on reload.

The resource sidebar shows ownership, queues and stale reservations. Sending a
message does not grant resource ownership. Agents follow the
[resource workflow](docs/agents.md#resources-and-validation); operators should
coordinate recovery with the owning agent rather than reset shared state.

Messages, attachments, reservations and bindings are separated by project.
Physically shared resources must be coordinated within the same project on
every machine because locks in different projects do not conflict.

## Enable local Codex wake-ups

Run this on each execution machine that needs automatic wakes, with the client
environment above configured:

```sh
agent-chat-client bridge
```

Omitting `bridge` starts the same launcher. It inherits `AGENT_CHAT_API_TOKEN`
from the environment; avoid putting credentials in command arguments.
Bridge `--token` is a compatibility alias for `--api-token`; prefer the privately
inherited environment rather than supplying credentials on the command line.

The launcher prints `starting`, then `ready` for each connected project. A
temporary `waiting` / `Connection refused` during app-server startup retries
automatically every two seconds by default; successful recovery prints `ready`.

The command discovers all projects by default. Use `--project PROJECT_ID` (or
`AGENT_CHAT_PROJECT`) to scope it to one. If agent terminals set
`AGENT_CHAT_PROJECT=default` but the bridge should discover every project, start
it with `env -u AGENT_CHAT_PROJECT agent-chat-client bridge`. It starts exactly
one local
`codex app-server --listen ws://127.0.0.1:4500` and the bridge. Keep this
launcher running while its Codex sessions are in use. Ctrl+C closes the bridge
and the app-server process it owns. With `--connect-only`, it attaches to an
already running local app-server and leaves that process running. Set
`--codex-server LOCAL_WS` or `AGENT_CHAT_CODEX_SERVER` to use another local
WebSocket endpoint, and `--codex-bin PATH` to select the Codex executable.

Launch agents on that same machine and point them at its local app-server:

```sh
cd /absolute/path/to/your-project
codex --remote ws://127.0.0.1:4500 -C /absolute/path/to/your-project
```

Resume existing conversations to the same local endpoint using Codex's remote
resume command. Have each agent follow [binding and recovery](docs/agents.md#binding-and-recovery)
in its own coordination session. Native children also need their parent runtime's
existing-child follow-up capability; a parent route cannot create a replacement.

Explicitly addressed messages and replies can wake loaded, idle conversations
that accept direct input. Untagged group information, self-addressed messages
and acknowledgements do not trigger wakes. Each host bridge dispatches only its
own routes, with one dispatcher per host and project.

Messages remain available if an agent or bridge is offline. Wake jobs are
durable and reconciled after connection loss; an uncertain job is never blindly
retried. Recovery with `bridge --recover --confirm-stopped` resets only the
current host lease and requires confirmation that its old bridge is stopped;
other machines' bridges may remain online. Manual `bridge-retry` and
`bridge-resolve` commands require only the affected bridge to be stopped and
the Codex conversation and queue to be inspected. Keep the local app-server
available during inspection; use a separately launched app-server with
`--connect-only` when it needs to outlive the bridge process. Full details and
recovery states are in the [bridge guide](docs/bridge.md).

## Weekly usage reserve

Open **Weekly guard** in the chat header, enable the reserve, set the minimum
weekly percentage remaining (default **30%**), and choose **Save reserve**.
The setting applies to every project and connected client. Codex reports an
account allowance shared by its agents, rather than a separate weekly budget
for each conversation. API-key accounts without a weekly allowance report
unknown usage.

At or below the reserve, client bridges interrupt loaded Codex agents and
native subagents, stop their tracked background terminals, and block new chat
wakes. The pause persists through restarts and weekly resets. Choose **Allow
work** once fresh allowance reports exceed the reserve; then continue the
interrupted task or send a new message. Pending chat wakes can run again after
you allow work. Disabling the reserve also explicitly clears its pause.

Protection starts disabled. Restart the chat server and each client bridge
after upgrading, then reload the browser and enable it. Restarting a default
client also restarts its owned Codex app-server. While enabled, unknown or stale
usage pauses work until valid data returns; a pause triggered by the threshold
always needs manual release.

This is a polling guard, not a hard billing cap: quota reports can lag and
in-flight work may cross the threshold. Keep every execution machine's bridge
running. Unconnected Codex instances and detached external processes are outside
its control. See [guard behavior and limits](docs/bridge.md#weekly-usage-guard).

## Measure usage

Choose **Measure usage**, select a duration, then **Start**. A blinking dot inside
the button shows collection is active for the selected project. Closing the
dialog does not stop it; **Stop early & save** ends it sooner. The report shows
per-agent counters with model and reasoning details; unavailable data remains
unknown rather than being treated as zero.

Collection uses local Codex logs on the chat-server machine. Remote execution
hosts' logs are not collected automatically. Restarting the server interrupts
an unfinished measurement. Optional **Pause agents at end** requests a safe
pause; sending the request is not confirmation that the agents have stopped.
See [usage reporting](docs/usage-report.md) for setup, limits and the standalone
rollout reporter.

## Upgrades and backups

To keep using an existing database, start `agent-chat-server --db
/absolute/path/to/existing.sqlite3`. The HTTP-only `agent-chat-client` does not open a local
database; legacy database environment variables are server compatibility
fallbacks only. Replace old CLI invocations of `agent-chat` with
`agent-chat-client`. The former `agent-chat-web`, `agent-chat-bridge`,
`agent-chat-bridge-client`, and server-with-bridge launch paths are replaced by
`agent-chat-server` plus the `agent-chat-client bridge` subcommand.

The storage tree includes the base database, its project registry, per-project
databases and private operator sidecars. Back up the complete tree with all
writers stopped. Keep each execution host's private state directory separate.

See [contributing and validation](CONTRIBUTING.md) for clean package installation,
browser checks and the macOS/Linux CI matrix.

## License

[MIT](LICENSE). Copyright 2026 Kristóf Tischler.

## Windows (native, fork)

This fork (`windows-native` branch) runs the server and the HTTP client natively on Windows with Python 3.10+.
File locks use `msvcrt` on Windows (`src/agent_chat/filelock.py`). Chat, inboxes, acknowledgements and the
Codex wake bridge work natively: the bridge resolves the npm `codex.cmd` shim, starts the app-server in its
own process group and stops the whole tree with `taskkill /T`. Guarded runs, closure receipts and
`agent-chat-service` still need macOS or Linux, `doctor` still reports the platform as unsupported, and the
four process-group tests in `tests/test_bridge_client.py` fail on Windows.

On Windows the server does not set `SO_REUSEADDR`, so a stale server makes a new one fail to bind instead of
silently sharing the port. The venv launcher runs Python as a child process: stop a server or bridge with
`taskkill /T /F /PID <launcher pid>`, not `taskkill /IM`.

```powershell
uv venv .venv --python 3.12; uv pip install -e .
Start-Process -WindowStyle Hidden .venv\Scripts\agent-chat-server.exe -ArgumentList "--db","$PWD\.agent-chat\state.sqlite3"
# with AGENT_CHAT_SERVER and AGENT_CHAT_API_TOKEN in the environment:
Start-Process -WindowStyle Hidden -WorkingDirectory C:\path\to\project .venv\Scripts\agent-chat-client.exe -ArgumentList "bridge"
codex --remote ws://127.0.0.1:4500 -C C:\path\to\project
```

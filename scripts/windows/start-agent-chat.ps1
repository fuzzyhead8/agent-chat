# Starts this checkout's agent-chat server and Codex wake bridge unless they already run.
# Safe to call at every logon (see the Windows section of the README) and by hand.
# -Restart stops both first, for example to load updated code.
param(
    [switch]$Restart,
    [int]$Port = 8765,
    [string]$CodexWorkingDirectory = $env:USERPROFILE
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$state = Join-Path $root '.agent-chat'
$db = Join-Path $state 'state.sqlite3'
$bin = Join-Path $root '.venv\Scripts'
New-Item -ItemType Directory -Force $state | Out-Null

function Test-Listening { [bool](Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) }
function Get-Bridge {
    Get-CimInstance Win32_Process -Filter "Name = 'agent-chat-client.exe'" |
        Where-Object { $_.CommandLine -match '\sbridge(\s|$)' }
}

if ($Restart) {
    # The venv launchers run Python as a child: stop each launcher with its tree.
    $launchers = @(Get-Bridge) + @(Get-CimInstance Win32_Process -Filter "Name = 'agent-chat-server.exe'")
    foreach ($process in $launchers | Where-Object { $_ }) {
        & taskkill.exe /T /F /PID $process.ProcessId | Out-Null
    }
    for ($i = 0; $i -lt 50 -and (Test-Listening); $i++) { Start-Sleep -Milliseconds 200 }
    if (Test-Listening) { throw "port $Port is still in use; stop its owner before restarting" }
}

if (-not (Test-Listening)) {
    Start-Process -WindowStyle Hidden -FilePath (Join-Path $bin 'agent-chat-server.exe') `
        -ArgumentList '--db', "`"$db`"", '--port', $Port `
        -RedirectStandardOutput (Join-Path $state 'server.log') -RedirectStandardError (Join-Path $state 'server.err')
    for ($i = 0; $i -lt 50 -and -not (Test-Listening); $i++) { Start-Sleep -Milliseconds 200 }
    if (-not (Test-Listening)) { throw "agent-chat server did not start; see $state\server.err" }
}

if (-not (Get-Bridge)) {
    $env:AGENT_CHAT_SERVER = "http://127.0.0.1:$Port"
    $env:AGENT_CHAT_API_TOKEN = (Get-Content -Raw "$db.api-token").Trim()
    foreach ($name in 'AGENT_CHAT_PROJECT', 'AGENT_CHAT_SESSION', 'AGENT_CHAT_TOKEN', 'AGENT_CHAT_DB') {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    Start-Process -WindowStyle Hidden -WorkingDirectory $CodexWorkingDirectory `
        -FilePath (Join-Path $bin 'agent-chat-client.exe') -ArgumentList 'bridge' `
        -RedirectStandardOutput (Join-Path $state 'bridge.log') -RedirectStandardError (Join-Path $state 'bridge.err')
}

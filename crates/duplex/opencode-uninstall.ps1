# Removes the duplex OpenCode adapter: stops and unregisters the "Duplex OpenCode" task, ends
# its process and deletes the installed binary. opencode.json, opencode-sessions.json and
# opencode.log are kept, as are the OpenCode sessions themselves.
# Windows PowerShell 5.1.
#
#   -TaskName    the task's name (default "Duplex OpenCode")
#   -InstallDir  where bin\ lives (default %USERPROFILE%\.duplex-chat)
param(
    [string]$TaskName = 'Duplex OpenCode',
    [string]$InstallDir = (Join-Path $env:USERPROFILE '.duplex-chat')
)
$ErrorActionPreference = 'Stop'
$exe = Join-Path $InstallDir 'bin\duplex-opencode.exe'

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TaskName
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
Get-Process -Name 'duplex-opencode' -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -eq $exe } |
    Stop-Process -Force
if (Test-Path $exe) { Remove-Item -Force $exe }
"Removed task '$TaskName' and $exe"

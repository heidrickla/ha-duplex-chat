# Removes the duplex waker: stops and unregisters the "Duplex Waker" task, ends its
# process and deletes the installed binary. waker.json and waker.log are kept.
# Windows PowerShell 5.1.
#
#   -TaskName    the task's name (default "Duplex Waker")
#   -InstallDir  where bin\ lives (default %USERPROFILE%\.duplex-chat)
param(
    [string]$TaskName = 'Duplex Waker',
    [string]$InstallDir = (Join-Path $env:USERPROFILE '.duplex-chat')
)
$ErrorActionPreference = 'Stop'
$exe = Join-Path $InstallDir 'bin\duplex-waker.exe'

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TaskName
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
Get-Process -Name 'duplex-waker' -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -eq $exe } |
    Stop-Process -Force
if (Test-Path $exe) { Remove-Item -Force $exe }
"Removed task '$TaskName' and $exe"

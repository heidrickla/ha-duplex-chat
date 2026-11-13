# Installs the duplex OpenCode adapter for the current user: copies duplex-opencode.exe to
# %USERPROFILE%\.duplex-chat\bin, registers the "Duplex OpenCode" logon task for this user and
# starts it. The task runs hidden; duplex-opencode.exe is a GUI-subsystem build, so no console
# opens. Rerunning it replaces the binary and the task.
# Windows PowerShell 5.1; no elevation needed for a task of the user's own.
#
#   -TaskName    the task's name (default "Duplex OpenCode")
#   -InstallDir  where bin\, adapter.json and opencode.json live (default %USERPROFILE%\.duplex-chat)
#   -Binary      the build to install (default: the one cargo install put in %USERPROFILE%\.cargo\bin,
#                else ..\..\target\release\duplex-opencode.exe)
#   -XmlOnly     print the task definition for this user and change nothing
param(
    [string]$TaskName = 'Duplex OpenCode',
    [string]$InstallDir = (Join-Path $env:USERPROFILE '.duplex-chat'),
    [string]$Binary = '',
    [switch]$XmlOnly
)
$ErrorActionPreference = 'Stop'
$user = "$env:USERDOMAIN\$env:USERNAME"
$bin = Join-Path $InstallDir 'bin'
$installed = Join-Path $bin 'duplex-opencode.exe'
if (-not $Binary) {
    $Binary = Join-Path $env:USERPROFILE '.cargo\bin\duplex-opencode.exe'
    if (-not (Test-Path $Binary)) { $Binary = Join-Path $PSScriptRoot '..\..\target\release\duplex-opencode.exe' }
}

function Get-TaskXml {
    param([string]$Name, [string]$User, [string]$Exe, [string]$WorkDir)
    $e = { param($s) [System.Security.SecurityElement]::Escape($s) }
    @"
<?xml version="1.0"?>
<Task version="1.3" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Duplex OpenCode adapter: holds the OpenCode sessions named in opencode.json for the duplex broker, through the OpenCode desktop app's server.</Description>
    <URI>\$(& $e $Name)</URI>
  </RegistrationInfo>
  <Principals>
    <Principal id="Author">
      <UserId>$(& $e $User)</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>true</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>$(& $e $User)</UserId>
    </LogonTrigger>
  </Triggers>
  <Actions Context="Author">
    <Exec>
      <Command>$(& $e $Exe)</Command>
      <WorkingDirectory>$(& $e $WorkDir)</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@
}

$xml = Get-TaskXml -Name $TaskName -User $user -Exe $installed -WorkDir $InstallDir
if ($XmlOnly) { $xml; return }

if (-not (Test-Path $Binary)) { throw "No build at $Binary; run: cargo install --path crates/duplex --locked" }
foreach ($file in 'adapter.json', 'opencode.json') {
    if (-not (Test-Path (Join-Path $InstallDir $file))) {
        Write-Warning "$InstallDir\$file is missing; the adapter stops at start without it (see docs/opencode.md)."
    }
}

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) { Stop-ScheduledTask -TaskName $TaskName }
# The task's process can outlive Stop-ScheduledTask by a moment; the copy needs it gone.
Get-Process -Name 'duplex-opencode' -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -eq $installed } |
    Stop-Process -Force

New-Item -ItemType Directory -Force -Path $bin | Out-Null
Copy-Item -Force $Binary $installed

Register-ScheduledTask -TaskName $TaskName -Xml $xml -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State

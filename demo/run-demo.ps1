$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Start-Logged([string[]]$ArgumentList, [string]$Name) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "java"
    $psi.WorkingDirectory = $Root
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    $quoted = foreach ($arg in $ArgumentList) {
        if ($arg -match '[\s"]') { '"' + ($arg -replace '"', '\"') + '"' } else { $arg }
    }
    $psi.Arguments = $quoted -join " "
    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    [void]$proc.Start()
    return [pscustomobject]@{
        Name = $Name
        Process = $proc
        OutTask = $proc.StandardOutput.ReadToEndAsync()
        ErrTask = $proc.StandardError.ReadToEndAsync()
    }
}

function Save-Logged($Running) {
    $Running.Process.WaitForExit()
    [IO.File]::WriteAllText((Join-Path $Logs "$($Running.Name).out.log"), $Running.OutTask.Result)
    [IO.File]::WriteAllText((Join-Path $Logs "$($Running.Name).err.log"), $Running.ErrTask.Result)
}

function Stop-Logged($Running) {
    if ($null -eq $Running) { return }
    try {
        if (-not $Running.Process.HasExited) {
            $Running.Process.Kill()
        }
    } catch {
        # The process can exit between the check and Kill.
    }
    Save-Logged $Running
}

Write-Host "starting postgres"
docker compose up -d --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose failed" }

Write-Host "building"
mvn -q package
if ($LASTEXITCODE -ne 0) { throw "mvn package failed" }

$env:LEASE_SECONDS = "6"
$env:HEARTBEAT_MS = "2000"
$env:POLL_INTERVAL_MS = "250"
$env:MAX_PER_ORG = "2"
$env:MAX_INFLIGHT_PER_ORG = "3"
$env:WORKER_THREADS = "4"
$env:RETRY_BACKOFF_SECONDS = "1"

$Jar = Join-Path $Root "target\job-scheduler.jar"
$Logs = Join-Path $Root "demo\logs"
New-Item -ItemType Directory -Force -Path $Logs | Out-Null

function Invoke-Scheduler([string[]]$CommandArgs) {
    & java -jar $Jar @CommandArgs
    if ($LASTEXITCODE -ne 0) { throw "command failed: java -jar job-scheduler.jar $($CommandArgs -join ' ')" }
}

Invoke-Scheduler @("reset")
Invoke-Scheduler @("enqueue", "--org", "northstar", "--type", "slow", "--count", "30")
Invoke-Scheduler @("enqueue", "--org", "bright", "--type", "fast", "--count", "10")
Invoke-Scheduler @("enqueue", "--org", "bright", "--type", "long", "--count", "1")
Invoke-Scheduler @("enqueue", "--org", "bright", "--type", "fail-once", "--count", "1")
Invoke-Scheduler @("enqueue", "--org", "cobalt", "--type", "fast", "--count", "6")
Invoke-Scheduler @("enqueue", "--org", "cobalt", "--type", "fast", "--count", "6", "--delay-seconds", "4")
Invoke-Scheduler @("enqueue", "--org", "cobalt", "--type", "fail-always", "--count", "1", "--max-attempts", "2")

$workers = @()
foreach ($id in @("worker-1", "worker-2", "worker-3")) {
    $workers += Start-Logged @("-jar", $Jar, "worker", "--id", $id) $id
}
$crasher = Start-Logged @("-jar", $Jar, "worker", "--id", "crasher", "--claim-and-exit", "--types", "fast,slow") "crasher"

$exit = 1
try {
    Start-Sleep -Seconds 3
    foreach ($running in $workers) {
        if ($running.Process.HasExited) {
            Save-Logged $running
            Write-Host "worker $($running.Name) exited early with code $($running.Process.ExitCode)"
            Get-ChildItem $Logs -Filter "*.log" | ForEach-Object { Write-Host "---- $($_.Name) ----"; Get-Content $_.FullName -Tail 40 }
            throw "a worker exited during startup"
        }
    }

    if (-not $crasher.Process.WaitForExit(90000)) {
        throw "crasher did not exit"
    }
    Save-Logged $crasher
    Write-Host "---- crasher ----"
    $crasherLog = Get-Content (Join-Path $Logs "crasher.out.log")
    $crasherLog
    if ($crasher.Process.ExitCode -ne 1 -or -not ($crasherLog -match "ABANDONED")) {
        throw "crasher did not abandon a job (exit $($crasher.Process.ExitCode))"
    }

    & java -jar $Jar report --wait --timeout-seconds 180
    $exit = $LASTEXITCODE
    if ($exit -ne 0) {
        foreach ($running in $workers) { Stop-Logged $running }
        Get-ChildItem $Logs -Filter "*.log" | ForEach-Object { Write-Host "---- $($_.Name) ----"; Get-Content $_.FullName -Tail 50 }
    }
} finally {
    foreach ($running in $workers) { Stop-Logged $running }
    Stop-Logged $crasher
}

exit $exit

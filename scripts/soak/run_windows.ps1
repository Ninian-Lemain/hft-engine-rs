param(
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory,
    [ValidatePattern('^[0-9a-f]{16}$')]
    [string]$Seed = '0000000000000001',
    [ValidateRange(1, [long]::MaxValue)]
    [long]$Steps = 100000000,
    [string]$BinaryPath = 'target/release/hft-soak.exe'
)

$ErrorActionPreference = 'Stop'
$repoPath = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$binary = (Resolve-Path -LiteralPath $BinaryPath).Path
if (Test-Path -LiteralPath $OutputDirectory) {
    throw 'Output directory already exists'
}
$output = (New-Item -ItemType Directory -Path $OutputDirectory).FullName
$revision = git -C $repoPath rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Git revision' }
$status = @(git -C $repoPath status --porcelain)
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Git status' }
$sourcePaths = @('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml')
$sourcePaths += @(git -C $repoPath ls-files --cached --others --exclude-standard -- 'crates/*.toml' 'crates/*.rs' 'crates/*.txt')
if ($LASTEXITCODE -ne 0) { throw 'Cannot list source files' }
$sources = @($sourcePaths | Sort-Object -Unique | ForEach-Object {
    [ordered]@{
        path = $_
        sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $repoPath $_)).Hash.ToLowerInvariant()
    }
})
$arguments = @('--profile', 'qualification', '--seed', $Seed, '--steps', $Steps.ToString())
$manifest = [ordered]@{
    schema = 'hft-soak-host/1'
    revision = $revision
    dirty = $status.Count -ne 0
    status = $status
    source_hashes = $sources
    binary_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $binary).Hash.ToLowerInvariant()
    arguments = $arguments
    started_utc = [DateTime]::UtcNow.ToString('o')
    os = [Environment]::OSVersion.VersionString
    cpu = @((Get-CimInstance Win32_Processor).Name)
    logical_processors = [Environment]::ProcessorCount
    rustc = @(rustc -Vv)
    sample_interval_ms = 1000
    measurement = 'External process samples. Working set is resident memory. Private bytes are committed private memory.'
}
$manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $output 'manifest.json') -Encoding utf8
$timer = [Diagnostics.Stopwatch]::StartNew()
$process = Start-Process -FilePath $binary -ArgumentList $arguments -WorkingDirectory $repoPath -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $output 'results.jsonl') -RedirectStandardError (Join-Path $output 'stderr.log')
$samples = 0
$peakResident = 0L
$peakPrivate = 0L
while (-not $process.HasExited) {
    $process.Refresh()
    if ($process.HasExited) { break }
    $resident = $process.WorkingSet64
    $private = $process.PrivateMemorySize64
    $peakResident = [Math]::Max($peakResident, $process.PeakWorkingSet64)
    $peakPrivate = [Math]::Max($peakPrivate, $private)
    [ordered]@{
        elapsed_ms = $timer.ElapsedMilliseconds
        resident_bytes = $resident
        private_bytes = $private
    } | ConvertTo-Json -Compress | Add-Content -LiteralPath (Join-Path $output 'memory.jsonl') -Encoding utf8
    $samples++
    [void]$process.WaitForExit(1000)
}
$process.WaitForExit()
$timer.Stop()
[ordered]@{
    schema = 'hft-soak-host-result/1'
    exit_code = $process.ExitCode
    elapsed_ms = $timer.ElapsedMilliseconds
    samples = $samples
    peak_observed_resident_bytes = if ($samples -gt 0) { $peakResident } else { $null }
    peak_sampled_private_bytes = if ($samples -gt 0) { $peakPrivate } else { $null }
    finished_utc = [DateTime]::UtcNow.ToString('o')
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $output 'host-result.json') -Encoding utf8
if ($process.ExitCode -ne 0) {
    throw "Soak exited with code $($process.ExitCode). Evidence is in $output"
}
$records = @(Get-Content -LiteralPath (Join-Path $output 'results.jsonl'))
if ($records.Count -ne 1) { throw 'Expected one soak result' }
$result = $records[0] | ConvertFrom-Json
if ($result.schema -ne 'hft-soak-results/1' -or $result.status -ne 'passed' -or $result.seed -cne $Seed -or $result.steps -ne $Steps -or $result.completed_steps -ne $Steps) {
    throw 'Soak result does not match the requested run'
}
Write-Output "Completed seed $Seed with $Steps declared steps in $($timer.Elapsed). Evidence is in $output"

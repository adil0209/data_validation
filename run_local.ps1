<#
Sets up the local dev environment (venv activation, JAVA_HOME, HADOOP_HOME)
and runs the validation pipeline, the test suite, or a results inspection.

Usage:
  .\run_local.ps1                                  # run pipeline against local sample data
  .\run_local.ps1 -Env azure -EnvFile .env          # run against Azure
  .\run_local.ps1 -Test                             # run pytest instead
  .\run_local.ps1 -Inspect                          # read back persisted results, no pipeline run
  .\run_local.ps1 -Inspect -LatestOnly              # ...just the most recent run
#>
param(
    [string]$Config = "config\pipeline_config.yaml",
    [string]$Env,
    [string]$EnvFile,
    [switch]$Test,
    [switch]$Inspect,
    [switch]$LatestOnly
)

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
$env:HADOOP_HOME = Join-Path $RepoRoot ".hadoop"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"

& (Join-Path $RepoRoot ".venv\Scripts\Activate.ps1")

if ($Test) {
    pytest -v
    exit $LASTEXITCODE
}

if ($Inspect) {
    $inspectArgs = @("--config", $Config)
    if ($Env) { $inspectArgs += @("--env", $Env) }
    if ($EnvFile) { $inspectArgs += @("--env-file", $EnvFile) }
    if ($LatestOnly) { $inspectArgs += @("--latest-only") }
    python (Join-Path $RepoRoot "scripts\inspect_results.py") @inspectArgs
    exit $LASTEXITCODE
}

$pipelineArgs = @("--config", $Config)
if ($Env) { $pipelineArgs += @("--env", $Env) }
if ($EnvFile) { $pipelineArgs += @("--env-file", $EnvFile) }

run-validation-pipeline @pipelineArgs
exit $LASTEXITCODE

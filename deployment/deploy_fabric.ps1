<#
.SYNOPSIS
    Deploys Data Factory pipelines and Spark notebooks to a Microsoft Fabric workspace.

.DESCRIPTION
    Uses the Fabric REST API to create/update pipelines and notebooks.
    Requires an Azure AD token with Fabric permissions.

.PARAMETER Environment
    Target environment (dev, uat, prod).

.PARAMETER WorkspaceId
    The Fabric workspace GUID.

.PARAMETER DeployPipelines
    Deploy Data Factory pipeline JSON files.

.PARAMETER DeployNotebooks
    Deploy Spark notebook Python files.

.EXAMPLE
    .\deploy_fabric.ps1 -Environment dev -WorkspaceId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" -DeployPipelines -DeployNotebooks
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "uat", "prod")]
    [string]$Environment,

    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [switch]$DeployPipelines,
    [switch]$DeployNotebooks
)

# Import helper functions
. "$PSScriptRoot\fabric_api_helpers.ps1"

# ============================================================
# Configuration
# ============================================================

$ErrorActionPreference = "Stop"
$ScriptRoot = $PSScriptRoot
$ProjectRoot = Split-Path $ScriptRoot -Parent

# Load environment config
$envConfigPath = Join-Path $ProjectRoot "config\environments.json"
$envConfig = Get-Content $envConfigPath | ConvertFrom-Json
$currentEnv = $envConfig.$Environment

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Fabric Deployment — $Environment" -ForegroundColor Cyan
Write-Host "Workspace: $WorkspaceId" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

# ============================================================
# Get Access Token
# ============================================================

Write-Host "`nAcquiring access token..." -ForegroundColor Yellow
$token = Get-FabricAccessToken
if (-not $token) {
    Write-Error "Failed to acquire access token. Ensure you are logged in with az login or a service principal."
    exit 1
}
Write-Host "Token acquired successfully." -ForegroundColor Green

# ============================================================
# Deploy Data Factory Pipelines
# ============================================================

if ($DeployPipelines) {
    Write-Host "`n--- Deploying Data Factory Pipelines ---" -ForegroundColor Cyan

    $pipelineFolder = Join-Path $ProjectRoot "output\adf"
    if (-not (Test-Path $pipelineFolder)) {
        $pipelineFolder = Join-Path $ProjectRoot "templates\data_factory"
    }

    $pipelineFiles = Get-ChildItem -Path $pipelineFolder -Filter "*.json" -Recurse |
        Where-Object { $_.Name -notmatch "linked_services" }

    if ($pipelineFiles.Count -eq 0) {
        Write-Warning "No pipeline JSON files found in $pipelineFolder"
    }
    else {
        foreach ($file in $pipelineFiles) {
            Write-Host "  Deploying pipeline: $($file.Name)..." -NoNewline
            try {
                $pipelineJson = Get-Content $file.FullName -Raw
                $result = Deploy-FabricPipeline -WorkspaceId $WorkspaceId -Token $token -PipelineJson $pipelineJson
                Write-Host " OK" -ForegroundColor Green
            }
            catch {
                Write-Host " FAILED: $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }
}

# ============================================================
# Deploy Spark Notebooks
# ============================================================

if ($DeployNotebooks) {
    Write-Host "`n--- Deploying Spark Notebooks ---" -ForegroundColor Cyan

    $notebookFolder = Join-Path $ProjectRoot "output\spark"
    if (-not (Test-Path $notebookFolder)) {
        $notebookFolder = Join-Path $ProjectRoot "templates\spark"
    }

    $notebookFiles = Get-ChildItem -Path $notebookFolder -Filter "*.py" -Recurse |
        Where-Object { $_.Directory.Name -ne "common" }

    if ($notebookFiles.Count -eq 0) {
        Write-Warning "No notebook files found in $notebookFolder"
    }
    else {
        foreach ($file in $notebookFiles) {
            Write-Host "  Deploying notebook: $($file.Name)..." -NoNewline
            try {
                $notebookContent = Get-Content $file.FullName -Raw
                $notebookName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
                $result = Deploy-FabricNotebook -WorkspaceId $WorkspaceId -Token $token -NotebookName $notebookName -Content $notebookContent
                Write-Host " OK" -ForegroundColor Green
            }
            catch {
                Write-Host " FAILED: $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }
}

# ============================================================
# Summary
# ============================================================

Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host "Deployment complete for $Environment" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Cyan

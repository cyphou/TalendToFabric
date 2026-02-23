<#
.SYNOPSIS
    Helper functions for interacting with the Microsoft Fabric REST API.
#>

$FabricApiBaseUrl = "https://api.fabric.microsoft.com/v1"

function Get-FabricAccessToken {
    <#
    .SYNOPSIS
        Acquire an Azure AD access token for Fabric API.
        Works with az login (interactive) or service principal (CI/CD).
    #>
    try {
        # Try Azure CLI first
        $tokenResponse = az account get-access-token --resource "https://api.fabric.microsoft.com" | ConvertFrom-Json
        return $tokenResponse.accessToken
    }
    catch {
        Write-Warning "az CLI token failed. Trying environment variables for SP auth..."
    }

    # Service principal authentication
    $tenantId = $env:AZURE_TENANT_ID
    $clientId = $env:AZURE_CLIENT_ID
    $clientSecret = $env:AZURE_CLIENT_SECRET

    if ($tenantId -and $clientId -and $clientSecret) {
        $body = @{
            grant_type    = "client_credentials"
            client_id     = $clientId
            client_secret = $clientSecret
            scope         = "https://api.fabric.microsoft.com/.default"
        }

        $tokenUrl = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
        $response = Invoke-RestMethod -Uri $tokenUrl -Method POST -Body $body -ContentType "application/x-www-form-urlencoded"
        return $response.access_token
    }

    throw "No authentication method available. Use 'az login' or set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET."
}

function Get-FabricHeaders {
    param([string]$Token)
    return @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }
}

function Get-FabricWorkspaceItems {
    <#
    .SYNOPSIS
        List all items in a Fabric workspace.
    #>
    param(
        [string]$WorkspaceId,
        [string]$Token
    )

    $url = "$FabricApiBaseUrl/workspaces/$WorkspaceId/items"
    $headers = Get-FabricHeaders -Token $Token
    $response = Invoke-RestMethod -Uri $url -Method GET -Headers $headers
    return $response.value
}

function Deploy-FabricPipeline {
    <#
    .SYNOPSIS
        Create or update a Data Factory pipeline in Fabric.
    #>
    param(
        [string]$WorkspaceId,
        [string]$Token,
        [string]$PipelineJson
    )

    $pipeline = $PipelineJson | ConvertFrom-Json
    $pipelineName = $pipeline.name
    $headers = Get-FabricHeaders -Token $Token

    # Check if pipeline already exists
    $existingItems = Get-FabricWorkspaceItems -WorkspaceId $WorkspaceId -Token $Token
    $existing = $existingItems | Where-Object { $_.displayName -eq $pipelineName -and $_.type -eq "DataPipeline" }

    if ($existing) {
        # Update existing pipeline
        $itemId = $existing.id
        $url = "$FabricApiBaseUrl/workspaces/$WorkspaceId/items/$itemId/updateDefinition"

        $body = @{
            definition = @{
                parts = @(
                    @{
                        path        = "pipeline-content.json"
                        payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($PipelineJson))
                        payloadType = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10

        $response = Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $body
        Write-Verbose "Updated pipeline: $pipelineName"
    }
    else {
        # Create new pipeline
        $url = "$FabricApiBaseUrl/workspaces/$WorkspaceId/items"

        $body = @{
            displayName = $pipelineName
            type        = "DataPipeline"
            definition  = @{
                parts = @(
                    @{
                        path        = "pipeline-content.json"
                        payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($PipelineJson))
                        payloadType = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10

        $response = Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $body
        Write-Verbose "Created pipeline: $pipelineName"
    }

    return $response
}

function Deploy-FabricNotebook {
    <#
    .SYNOPSIS
        Create or update a Spark notebook in Fabric.
    #>
    param(
        [string]$WorkspaceId,
        [string]$Token,
        [string]$NotebookName,
        [string]$Content
    )

    $headers = Get-FabricHeaders -Token $Token

    # Convert Python content to .ipynb format
    $notebookJson = Convert-PyToIpynb -PythonContent $Content

    # Check if notebook already exists
    $existingItems = Get-FabricWorkspaceItems -WorkspaceId $WorkspaceId -Token $Token
    $existing = $existingItems | Where-Object { $_.displayName -eq $NotebookName -and $_.type -eq "Notebook" }

    $encodedContent = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($notebookJson))

    if ($existing) {
        $itemId = $existing.id
        $url = "$FabricApiBaseUrl/workspaces/$WorkspaceId/items/$itemId/updateDefinition"

        $body = @{
            definition = @{
                parts = @(
                    @{
                        path        = "notebook-content.py"
                        payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Content))
                        payloadType = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10

        $response = Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $body
        Write-Verbose "Updated notebook: $NotebookName"
    }
    else {
        $url = "$FabricApiBaseUrl/workspaces/$WorkspaceId/items"

        $body = @{
            displayName = $NotebookName
            type        = "Notebook"
            definition  = @{
                parts = @(
                    @{
                        path        = "notebook-content.py"
                        payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Content))
                        payloadType = "InlineBase64"
                    }
                )
            }
        } | ConvertTo-Json -Depth 10

        $response = Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $body
        Write-Verbose "Created notebook: $NotebookName"
    }

    return $response
}

function Convert-PyToIpynb {
    <#
    .SYNOPSIS
        Convert a .py file with # COMMAND ---------- markers into a basic .ipynb JSON structure.
    #>
    param([string]$PythonContent)

    # Split by Fabric notebook cell markers
    $cells = $PythonContent -split "# COMMAND ----------"

    $notebookCells = @()
    foreach ($cell in $cells) {
        $cell = $cell.Trim()
        if ($cell) {
            $notebookCells += @{
                cell_type = "code"
                source    = @($cell)
                metadata  = @{}
                outputs   = @()
            }
        }
    }

    $notebook = @{
        nbformat       = 4
        nbformat_minor = 5
        metadata       = @{
            kernelspec = @{
                display_name = "Python 3"
                language     = "python"
                name         = "python3"
            }
        }
        cells          = $notebookCells
    }

    return ($notebook | ConvertTo-Json -Depth 10)
}

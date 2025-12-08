<# 
teardown.ps1
Usage:
  Open PowerShell as Administrator in the same folder as your deploy.ps1
  PS> .\teardown.ps1

Removes the Docker stack and cleans up unused containers, networks, volumes, and images.
#>

$ErrorActionPreference = "Stop"

$stackName = "snowflake-stack"

Write-Host "=== Docker Stack Teardown Utility ===`n"

# 1. Check Docker
Write-Host "Checking Docker Engine..."
try {
    docker version | Out-Null
} catch {
    Write-Error "Docker does not seem to be running. Start Docker Desktop and run this script again."
    exit 1
}
Write-Host "Docker is running.`n"

# 2. Remove the stack if it exists
$existing = docker stack ls --format "{{.Name}}" | Where-Object { $_ -eq $stackName }

if ($existing) {
    Write-Host "Removing stack '$stackName'..."
    docker stack rm $stackName | Out-Null

    Write-Host "Waiting for services to stop..."
    Start-Sleep -Seconds 5

    $maxWait = 30
    $elapsed = 0
    while ($elapsed -lt $maxWait) {
        $stillThere = docker stack ls --format "{{.Name}}" | Where-Object { $_ -eq $stackName }
        if (-not $stillThere) {
            break
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }

    if ($elapsed -ge $maxWait) {
        Write-Warning "Stack '$stackName' still appears in the list after timeout."
    } else {
        Write-Host "Stack '$stackName' removed successfully.`n"
    }
} else {
    Write-Host "Stack '$stackName' not found. Nothing to remove.`n"
}

# 3. Optional cleanup
Write-Host "Cleaning up unused Docker resources...`n"

docker system prune -af --volumes

Write-Host "`n✅ Cleanup complete. All unused containers, images, networks, and volumes removed."

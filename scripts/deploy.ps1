<# 
deploy.ps1
Usage:
  Open PowerShell as Administrator in the project folder
  PS> .\deploy.ps1

Requirements:
  - docker desktop / docker engine installed
  - docker-compose.yaml in the same folder
#>

$ErrorActionPreference = "Stop"

Write-Host "=== Docker Swarm Deploy Helper ===`n"

# 1. Check Docker is available
Write-Host "Checking Docker Engine..."
try {
    docker version | Out-Null
} catch {
    Write-Error "Docker does not seem to be running. Start Docker Desktop and run this script again."
    exit 1
}
Write-Host "Docker is running.`n"

# 2. Check Swarm status, init if needed
Write-Host "Checking if this node is part of a swarm..."
$swarmInfo = docker info 2>$null | Select-String "Swarm:"

if (-not $swarmInfo) {
    Write-Warning "Could not read swarm status from 'docker info'."
    Write-Warning "Continuing, but swarm init may fail if Docker is in an unexpected state."
}

if ($swarmInfo -and ($swarmInfo -match "inactive")) {
    Write-Host "Swarm inactive. Initializing swarm on this machine..." -ForegroundColor Yellow
    # Advertise address 127.0.0.1 is fine for single-node dev swarm
    docker swarm init --advertise-addr 127.0.0.1 | Out-Null
    Write-Host "Swarm initialized.`n"
} else {
    Write-Host "Swarm already active.`n"
}

# 3. Stack name + compose file
$stackName = "snowflake-stack"
$composeFile = "docker-compose.yaml"

if (-not (Test-Path $composeFile)) {
    Write-Error "Cannot find $composeFile in the current directory: $(Get-Location)"
    exit 1
}

# 4. Optional: clean previous deploy
Write-Host "Checking if stack '$stackName' already exists..."
$existing = docker stack ls --format "{{.Name}}" | Where-Object { $_ -eq $stackName }

if ($existing) {
    Write-Host "Stack '$stackName' already exists. Removing old stack..." -ForegroundColor Yellow
    docker stack rm $stackName | Out-Null

    Write-Host "Waiting for old services to shut down..."
    Start-Sleep -Seconds 5

    # Poll until services are gone
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
        Write-Warning "Timed out waiting for old stack to fully disappear, continuing anyway."
    } else {
        Write-Host "Old stack fully removed.`n"
    }
} else {
    Write-Host "No existing stack named '$stackName'. Fresh deploy.`n"
}

# 5. Deploy new stack
Write-Host "Deploying stack '$stackName' using $composeFile..."
docker stack deploy -c $composeFile $stackName

Write-Host "`n=== Stack Services ==="
docker stack services $stackName

Write-Host "`n=== Stack Tasks (containers) ==="
docker stack ps $stackName

Write-Host "`nDone."

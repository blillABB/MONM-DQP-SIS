# 🔧 Docker Volume Mount Fix

## Problem
`rulebook_registry.json` is mounted as a **directory** instead of a file in the Docker container. This happened because the file didn't exist when Docker first started, so Docker created it as a directory placeholder.

## Solution
You need to **restart the Docker stack** so it mounts the file correctly.

## Steps to Fix

### Option 1: Quick Restart (Recommended)

Open PowerShell as Administrator in the project directory and run:

```powershell
# Navigate to project directory
cd MONM-MDM-DQP

# Tear down the existing stack
.\scripts\teardown.ps1

# Redeploy with the correct file mounts
.\scripts\deploy.ps1
```

### Option 2: Manual Docker Commands

If the scripts don't work, run these commands manually:

```powershell
# Remove the stack
docker stack rm snowflake-stack

# Wait for it to fully shut down (30 seconds)
Start-Sleep -Seconds 30

# Verify the file exists on host
Get-Item rulebook_registry.json

# Redeploy
docker stack deploy -c docker-compose.yaml snowflake-stack
```

### Option 3: Nuclear Option (If above doesn't work)

This removes ALL Docker volumes and forces a clean start:

```powershell
# Stop the stack
docker stack rm snowflake-stack
Start-Sleep -Seconds 30

# Remove all volumes (WARNING: This deletes all Docker data!)
docker system prune -af --volumes

# Redeploy
docker stack deploy -c docker-compose.yaml snowflake-stack
```

## Verification

After restarting, verify the file is mounted correctly:

```powershell
# Check the container
docker exec -it $(docker ps -q -f name=monm-mdm-dqp) ls -la /app/rulebook_registry.json

# Should show: "-rw-r--r--" (file), NOT "drwxr-xr-x" (directory)
```

## Why This Happened

The docker-compose.yaml file has this volume mount:

```yaml
volumes:
  - ./rulebook_registry.json:/app/rulebook_registry.json
```

When Docker starts and the source file doesn't exist:
- ❌ Docker creates `/app/rulebook_registry.json` as a **directory**
- ✅ After restart with file present, Docker mounts it as a **file**

## Files Fixed in This Branch

All these files are now restored and committed:
- ✅ `core/config.py` - Snowflake configuration
- ✅ `custom_expectations/base.py` - Custom expectation base class
- ✅ `validations/base_validation.py` - Validation suite base class
- ✅ `rulebook_registry.json` - Rule registry (valid JSON with 3 suites)

Branch: `claude/fix-broken-code-01FitcSd4KFCSaereajjjGWp`

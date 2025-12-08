<#
.SYNOPSIS
    Run all tests for MONM-MDM-DQP project.

.DESCRIPTION
    This script runs:
    1. YAML validation suite syntax checks
    2. Python unit tests via pytest

    Run this before pushing changes to verify everything works.

.EXAMPLE
    .\scripts\test.ps1

.EXAMPLE
    .\scripts\test.ps1 -Verbose
#>

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  MONM-MDM-DQP Test Suite" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

$AllPassed = $true

# ---------------------------------------------
# 1. Validate YAML Suites
# ---------------------------------------------
Write-Host "--- Validating YAML Suites ---" -ForegroundColor Yellow
Write-Host ""

$YamlFiles = Get-ChildItem -Path "validation_yaml" -Filter "*.yaml" -ErrorAction SilentlyContinue

if ($YamlFiles.Count -eq 0) {
    Write-Host "  No YAML files found in validation_yaml/" -ForegroundColor Gray
} else {
    try {
        python scripts/validate_yaml.py validation_yaml/*.yaml
        if ($LASTEXITCODE -ne 0) {
            $AllPassed = $false
            Write-Host "  YAML validation FAILED" -ForegroundColor Red
        } else {
            Write-Host "  YAML validation passed" -ForegroundColor Green
        }
    } catch {
        $AllPassed = $false
        Write-Host "  YAML validation error: $_" -ForegroundColor Red
    }
}

Write-Host ""

# ---------------------------------------------
# 2. Run Unit Tests
# ---------------------------------------------
Write-Host "--- Running Unit Tests ---" -ForegroundColor Yellow
Write-Host ""

try {
    python -m pytest tests/ -v --tb=short
    if ($LASTEXITCODE -ne 0) {
        $AllPassed = $false
        Write-Host ""
        Write-Host "  Unit tests FAILED" -ForegroundColor Red
    } else {
        Write-Host ""
        Write-Host "  Unit tests passed" -ForegroundColor Green
    }
} catch {
    $AllPassed = $false
    Write-Host "  Unit test error: $_" -ForegroundColor Red
}

Write-Host ""

# ---------------------------------------------
# Summary
# ---------------------------------------------
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Summary" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

if ($AllPassed) {
    Write-Host "  All checks PASSED" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Safe to push changes." -ForegroundColor Gray
    exit 0
} else {
    Write-Host "  Some checks FAILED" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Please fix issues before pushing." -ForegroundColor Gray
    exit 1
}

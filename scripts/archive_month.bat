@echo off
REM Archive previous month's logs and validation results
REM Schedule this to run on the 1st of each month via Task Scheduler

cd /d "%~dp0.."
python scripts/archive_month.py

if %ERRORLEVEL% EQU 0 (
    echo Archive completed successfully
) else (
    echo Archive failed with error code %ERRORLEVEL%
)

pause

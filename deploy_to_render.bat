@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  RIO PRINT MEDIA — Deploy to Render
::  Render Start Command: uvicorn rio_api:app --host 0.0.0.0 --port $PORT
::  Active file: rio_api.py (root)
::  Place ALL files from this zip into:
::    D:\Rio\Softwares\Merger\Test\Mongo\files\
:: ============================================================

set FILES_DIR=D:\Rio\Softwares\Merger\Test\Mongo\files
set GITHUB_REPO=https://github.com/rioprintmediaa/rio-print-media.git

echo.
echo =====================================================
echo   RIO PRINT MEDIA — Deploy to Render
echo   Active API: rio_api.py (root)
echo =====================================================
echo.

cd /d "%FILES_DIR%"
if %errorlevel% neq 0 (
    echo [ERROR] Folder not found: %FILES_DIR%
    pause & exit /b 1
)

git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Git not installed. Download: https://git-scm.com/download/win
    pause & exit /b 1
)

git status >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Initialising git repo...
    git init
    git remote add origin %GITHUB_REPO%
    git branch -M main
)

:: ── Remove junk files (one-time cleanup) ─────────────────────
echo Cleaning up old dev/temp files...
for %%F in (BUGFIX_ANALYSIS.md BUGFIX_PLAN.md BUGFIX_PYTHON_MONGODB.md BUGFIX_SUMMARY.md FINAL_IMPLEMENTATION_PLAN.md REPORTS_DESIGN_OPTIONS.md SETUP_STATUS.md test_result.md README.md .gitconfig COMPREHENSIVE_FIXES.js fix_all_backend.py cleanup_fy_2026_27.py reports_panel_new.html) do (
    git rm --cached -f "%%F" >nul 2>&1
)
git rm --cached -f backend\rio_api_backup.py >nul 2>&1
git rm --cached -r -f .emergent\ memory\ test_reports\ tests\ frontend\ >nul 2>&1

:: ── Stage production files ────────────────────────────────────
echo Staging files...
git add Rio_Sales_Tracker_ONLINE.html
git add rio_api.py
git add requirements.txt
git add .gitignore
git add DEPLOYMENT_GUIDE.md
git add deploy_to_render.bat
git add backend\server.py
git add backend\Rio_Sales_Tracker_ONLINE.html
git add backend\requirements.txt

:: ── Check for changes ─────────────────────────────────────────
git diff --cached --quiet
if %errorlevel% equ 0 (
    echo.
    echo [INFO] No changes detected. Nothing to deploy.
    pause & exit /b 0
)

:: ── Commit ───────────────────────────────────────────────────
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DT=%%I
set TIMESTAMP=%DT:~0,4%-%DT:~4,2%-%DT:~6,2% %DT:~8,2%:%DT:~10,2%
set COMMIT_MSG=deploy: all fixes + login + user mgmt [%TIMESTAMP%]

echo Committing: %COMMIT_MSG%
git commit -m "%COMMIT_MSG%"

:: ── Push (force to avoid fetch-first rejection) ───────────────
echo Pushing to GitHub...
git push origin main --force

if %errorlevel% equ 0 (
    echo.
    echo =====================================================
    echo   SUCCESS! Pushed to GitHub.
    echo   Render auto-deploys in ~2-3 minutes.
    echo.
    echo   AFTER DEPLOY — run this URL once to fix admin:
    echo   https://rio-print-media.onrender.com/api/auth/reset-admin
    echo.
    echo   Then login: admin / rio@admin
    echo =====================================================
) else (
    echo.
    echo [ERROR] Push failed.
    echo   Try: git push origin main --force
)

echo.
pause
endlocal

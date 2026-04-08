@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  RIO PRINT MEDIA — Clean up repo + Deploy to Render
::  Place ALL files from this zip into:
::    D:\Rio\Softwares\Merger\Test\Mongo\files\
::
::  First run:  does git cleanup + deploys
::  Every run after: just commits changed files + deploys
:: ============================================================

set FILES_DIR=D:\Rio\Softwares\Merger\Test\Mongo\files
set GITHUB_REPO=https://github.com/rioprintmediaa/rio-print-media.git

echo.
echo =====================================================
echo   RIO PRINT MEDIA — Deploy to Render
echo =====================================================
echo.

:: ── Go to repo folder ─────────────────────────────────────────
cd /d "%FILES_DIR%"
if %errorlevel% neq 0 (
    echo [ERROR] Folder not found: %FILES_DIR%
    pause & exit /b 1
)

:: ── Check git is installed ────────────────────────────────────
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Git not installed. Download: https://git-scm.com/download/win
    pause & exit /b 1
)

:: ── Init repo if not already ──────────────────────────────────
git status >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Initialising git repo...
    git init
    git remote add origin %GITHUB_REPO%
    echo.
    echo [ACTION] Edit this script: replace YOUR_USERNAME with your GitHub username.
    echo          Then run again.
    pause & exit /b 1
)

:: ── ONE-TIME CLEANUP — remove junk files from repo ───────────
echo Cleaning up old dev/temp files from repo...

:: Remove dev notes
git rm --cached -f BUGFIX_ANALYSIS.md       >nul 2>&1
git rm --cached -f BUGFIX_PLAN.md           >nul 2>&1
git rm --cached -f BUGFIX_PYTHON_MONGODB.md >nul 2>&1
git rm --cached -f BUGFIX_SUMMARY.md        >nul 2>&1
git rm --cached -f FINAL_IMPLEMENTATION_PLAN.md >nul 2>&1
git rm --cached -f REPORTS_DESIGN_OPTIONS.md    >nul 2>&1
git rm --cached -f SETUP_STATUS.md          >nul 2>&1
git rm --cached -f test_result.md           >nul 2>&1
git rm --cached -f README.md                >nul 2>&1
git rm --cached -f .gitconfig               >nul 2>&1

:: Remove one-time scripts
git rm --cached -f COMPREHENSIVE_FIXES.js   >nul 2>&1
git rm --cached -f fix_all_backend.py       >nul 2>&1
git rm --cached -f cleanup_fy_2026_27.py    >nul 2>&1
git rm --cached -f reports_panel_new.html   >nul 2>&1

:: Remove old backup API
git rm --cached -f backend\rio_api_backup.py >nul 2>&1

:: Remove unused folders
git rm --cached -r -f .emergent\  >nul 2>&1
git rm --cached -r -f memory\     >nul 2>&1
git rm --cached -r -f test_reports\ >nul 2>&1
git rm --cached -r -f tests\      >nul 2>&1
git rm --cached -r -f frontend\   >nul 2>&1

echo Cleanup done.
echo.

:: ── Stage the files we want to keep ──────────────────────────
echo Staging production files...
git add Rio_Sales_Tracker_ONLINE.html
git add rio_api.py
git add requirements.txt
git add .gitignore
git add DEPLOYMENT_GUIDE.md
git add backend\server.py
git add backend\Rio_Sales_Tracker_ONLINE.html
git add backend\requirements.txt
git add deploy_to_render.bat

:: ── Check if anything changed ────────────────────────────────
git diff --cached --quiet
if %errorlevel% equ 0 (
    echo.
    echo [INFO] No changes — already up to date. Nothing to deploy.
    pause & exit /b 0
)

:: ── Commit with timestamp ─────────────────────────────────────
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DT=%%I
set TIMESTAMP=%DT:~0,4%-%DT:~4,2%-%DT:~6,2% %DT:~8,2%:%DT:~10,2%

set COMMIT_MSG=deploy: bugfixes + cleanup [%TIMESTAMP%]
echo Committing: %COMMIT_MSG%
git commit -m "%COMMIT_MSG%"

:: ── Push to GitHub ────────────────────────────────────────────
echo.
echo Pushing to GitHub...
git push origin main

if %errorlevel% equ 0 (
    echo.
    echo =====================================================
    echo   SUCCESS! Pushed to GitHub.
    echo   Render will auto-deploy in ~2-3 minutes.
    echo.
    echo   Monitor at: https://dashboard.render.com
    echo =====================================================
) else (
    echo.
    echo [ERROR] Push failed. Try:
    echo   git push origin HEAD:main
    echo.
    echo Common fixes:
    echo   - Wrong branch: git branch -M main
    echo   - Not logged in: git config credential.helper manager
    echo   - Wrong URL: git remote set-url origin %GITHUB_REPO%
)

echo.
pause
endlocal

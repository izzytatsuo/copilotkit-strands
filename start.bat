@echo off
chcp 65001 > nul
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

echo Starting application...

REM Build if needed
if not exist "_internal\.next" (
    echo Building frontend...
    npm --prefix _internal run build
)

REM Start both servers concurrently
cd /d "%~dp0\_internal"
npx concurrently "npx next start" "cd agent && uv run python main.py" --names ui,agent --prefix-colors blue,green --kill-others

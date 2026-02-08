@echo off
REM Set console to UTF-8 for emoji/unicode support
chcp 65001 > nul

REM Set Python encoding environment variable
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM Navigate to the agent directory
cd /d %~dp0\..\agent

REM Run the agent using uv
uv run python main.py

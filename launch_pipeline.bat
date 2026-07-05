@echo off
setlocal
cd /d "%~dp0"
start "" "http://127.0.0.1:8767/pipeline"
python pipeline\server.py
pause

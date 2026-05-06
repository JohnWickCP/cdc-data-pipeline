@echo off
cd /d "%~dp0"

rem Copy .env nếu chưa có
if not exist .env (
    if exist .env.example (
        copy .env.example .env > nul
        echo [demo] Created .env from .env.example -- edit neu can doi host
    )
)

echo [demo] Installing dependencies...
pip install -r requirements.txt -q

echo [demo] Starting CDC Demo Server...
echo [demo] Open: http://localhost:8888
echo.

python demo_server.py
pause

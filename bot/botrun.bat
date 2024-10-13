@echo off

call %~dp0dis\venv\Scripts\activate

cd %~dp0dis\bot

set TOKEN=<your_token>

python botrun.py

pause
@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

set "PLAYWRIGHT_BRIDGE_ONLY=1"
set "HAS_TEST_TARGET=0"

for %%A in (%*) do (
  set "ARG=%%~A"
  if /I not "!ARG:~0,1!"=="-" (
    set "HAS_TEST_TARGET=1"
    set "NORMALIZED=!ARG:/=\!"
    set "MATCHED=!NORMALIZED:tests\test_playwright_smokes.py=!"
    if /I "!MATCHED!"=="!NORMALIZED!" set "PLAYWRIGHT_BRIDGE_ONLY=0"
  )
)

if defined RUN_PLAYWRIGHT_SMOKES if "%HAS_TEST_TARGET%"=="1" if "%PLAYWRIGHT_BRIDGE_ONLY%"=="1" (
  "%PYTHON_EXE%" -m pytest %*
  exit /b %ERRORLEVEL%
)

"%PYTHON_EXE%" -m pytest --cov=bdw/bit_data_workbench --cov-branch --cov-report=term-missing:skip-covered --cov-report=xml %*
exit /b %ERRORLEVEL%
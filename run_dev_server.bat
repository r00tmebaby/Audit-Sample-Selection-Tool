@echo off
REM Start the REST API with proper environment

echo Starting Audit Sampling REST API...
echo.
echo Working directory: %cd%
echo.

REM Activate virtual environment
call .venv\Scripts\activate.bat || goto :fail

REM Ensure worker is installed in editable mode so restapi can import it
if exist worker\setup.py (
  python -m pip show worker >nul 2>&1
  if errorlevel 1 (
    echo Installing worker package in editable mode...
    python -m pip install -q -e worker || goto :fail
  ) else (
    echo Worker package already installed.
  )
) else (
  echo No setup.py in worker; adding worker to PYTHONPATH.
  set PYTHONPATH=%CD%\worker;%PYTHONPATH%
)

REM Start uvicorn
echo Starting uvicorn on http://127.0.0.1:8888
echo Press Ctrl+C to stop
echo.

python -m uvicorn restapi.src.main:app --host 127.0.0.1 --port 8888 --reload || goto :fail

goto :end

:fail
echo Failed to start REST API.
exit /b 1

:end
exit /b 0

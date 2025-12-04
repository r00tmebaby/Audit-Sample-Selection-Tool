@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Run static checks and tests for worker/src and restapi/src only

REM Ensure venv is active
if not exist .venv\Scripts\activate.bat (
  echo Virtual environment not found. Creating one...
  py -m venv .venv || goto :fail
)
call .venv\Scripts\activate.bat || goto :fail

REM Upgrade pip quietly
python -m pip install -q --upgrade pip || goto :fail

REM Install project dependencies if requirements.txt exists
if exist requirements.txt (
  echo Installing project requirements...
  python -m pip install -q -r requirements.txt || goto :fail
)

REM Install required tooling
python -m pip install -q black==24.10.0 pyflakes==3.2.0 isort==5.13.2 pytest==8.3.3 httpx==0.27.2 || goto :fail

set TARGETS=
if exist "worker\src" set TARGETS=!TARGETS! worker\src
if exist "restapi\src" set TARGETS=!TARGETS! restapi\src

if "!TARGETS!"=="" (
  echo No source folders found. Exiting.
  goto :fail
)

REM Run black with 79 line length
echo Running black on !TARGETS! with line length 79...
python -m black -l 79 !TARGETS! || goto :fail

REM Run isort
echo Running isort on !TARGETS!...
python -m isort !TARGETS! || goto :fail

REM Run pyflakes per target
for %%d in (!TARGETS!) do (
  echo Running pyflakes on %%d...
  python -m pyflakes %%d || goto :fail
)

REM Run pytest over worker and REST API tests
set PYTEST_TARGETS=
if exist "worker\tests" set PYTEST_TARGETS=!PYTEST_TARGETS! worker\tests
if exist "restapi\tests" set PYTEST_TARGETS=!PYTEST_TARGETS! restapi\tests

if "!PYTEST_TARGETS!"=="" (
  echo No pytest targets found.
  goto :fail
)

echo Running pytest on !PYTEST_TARGETS! (verbose)...
python -m pytest -vv !PYTEST_TARGETS! || goto :fail

echo All static checks and tests passed successfully.
exit /b 0

:fail
echo One or more checks/tests failed.
exit /b 1

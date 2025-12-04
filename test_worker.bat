@echo off
setlocal ENABLEDELAYEDEXPANSION

echo Running worker end-to-end smoke tests...

REM Ensure venv exists
if not exist .venv\Scripts\activate.bat (
  echo Creating virtual environment...
  py -m venv .venv || goto :fail
)
call .venv\Scripts\activate.bat || goto :fail

REM Install worker package in editable mode if not already
python -m pip show worker >nul 2>&1
if errorlevel 1 (
  echo Installing worker package in editable mode...
  python -m pip install -q -e worker || goto :fail
) else (
  echo Worker package already installed.
)

set DATA_FILE=data\population_data.csv
if not exist "%DATA_FILE%" (
  echo Sample data file not found at %DATA_FILE%.
  goto :fail
)

set OUTPUT_DIR=worker_test_output
if exist "%OUTPUT_DIR%" (
  echo Cleaning previous output directory...
  rmdir /s /q "%OUTPUT_DIR%" || goto :fail
)
mkdir "%OUTPUT_DIR%" || goto :fail

set COMMON_ARGS=--input "%CD%\%DATA_FILE%" --output-dir "%CD%\%OUTPUT_DIR%" --tolerable 1000 --expected 100 --assurance 2 --seed 42

REM Test 1: default parameters (balance both, exclude zeros)
echo.
echo Test 1: default parameters (balance both, exclude zeros)
python -m worker.src.main %COMMON_ARGS% || goto :fail
call :assert_outputs "Test 1" || goto :fail

REM Test 2: debit-only, include zeros
echo.
echo Test 2: debit-only with zeros included
python -m worker.src.main %COMMON_ARGS% --balance-type debit --include-zeros || goto :fail
call :assert_outputs "Test 2" || goto :fail

REM Test 3: fast streaming mode
echo.
echo Test 3: fast streaming mode
python -m worker.src.main %COMMON_ARGS% --fast || goto :fail
call :assert_outputs "Test 3" || goto :fail

REM All tests passed
echo.
echo Worker smoke tests passed.
exit /b 0

:assert_outputs
set TEST_LABEL=%~1
set OUTPUT_FILE=%OUTPUT_DIR%\sample_selection_output.xlsx
if not exist "%OUTPUT_FILE%" (
  echo %TEST_LABEL% failed: sample_selection_output.xlsx not found.
  exit /b 1
)
set RUNS_DIR=%OUTPUT_DIR%\runs
if not exist "%RUNS_DIR%" (
  echo %TEST_LABEL% failed: runs directory missing.
  exit /b 1
)
for %%f in ("%RUNS_DIR%"\*.json) do (
  set FOUND_RUN=1
  goto :check_params
)
echo %TEST_LABEL% failed: no run summary JSON found.
exit /b 1

:check_params
REM Basic sanity check: ensure JSON contains balance_type
findstr /i "balance_type" "%RUNS_DIR%\*.json" >nul || (
  echo %TEST_LABEL% failed: balance_type missing in run summary.
  exit /b 1
)
exit /b 0

:fail
echo Worker smoke tests failed.
exit /b 1


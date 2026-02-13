@echo off
setlocal EnableExtensions EnableDelayedExpansion

chcp 65001 >nul
cd /d "%~dp0"

set "PROJECT_ROOT=%CD%"
set "MAIN_FILE=%PROJECT_ROOT%\main.py"
set "REQ_FILE=%PROJECT_ROOT%\requirements.txt"
set "VENV_DIR=%PROJECT_ROOT%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "REQ_STAMP=%VENV_DIR%\requirements.installed.txt"

set "RUNTIME_DIR=%PROJECT_ROOT%\runtime"
set "PYTHON_LOCAL=%RUNTIME_DIR%\python\python.exe"
set "PYTHON_INSTALLER_DIR=%RUNTIME_DIR%\python-installer"
set "PYTHON_INSTALLER_FILE="
set "LIBDMTX_DIR=%RUNTIME_DIR%\libdmtx"

set "DEFAULT_OUTPUT=%PROJECT_ROOT%\output\results.csv"
set "DEFAULT_PROGRESS=%PROJECT_ROOT%\output\results.csv.progress.csv"

set "RUN_ARGS=%*"
set "APP_EXIT=0"
set "BASE_PYTHON_EXE="
set "BASE_PYTHON_ARG="

echo ==================================================
echo DMX Grabber - Windows launcher
echo ==================================================
echo Project: %PROJECT_ROOT%
echo.

if not exist "%MAIN_FILE%" (
    echo [ERROR] File not found: main.py
    set "APP_EXIT=1"
    goto :finish
)

if not exist "%REQ_FILE%" (
    echo [ERROR] File not found: requirements.txt
    set "APP_EXIT=1"
    goto :finish
)

if exist "%LIBDMTX_DIR%\bin" set "PATH=%LIBDMTX_DIR%\bin;%PATH%"
if exist "%LIBDMTX_DIR%" set "PATH=%LIBDMTX_DIR%;%PATH%"

call :resolve_python
if errorlevel 1 (
    set "APP_EXIT=1"
    goto :finish
)

call :ensure_venv
if errorlevel 1 (
    set "APP_EXIT=1"
    goto :finish
)

call :ensure_dependencies
if errorlevel 1 (
    set "APP_EXIT=1"
    goto :finish
)

call :check_libdmtx
if errorlevel 1 (
    set "APP_EXIT=1"
    goto :finish
)

call :prepare_run_mode %*
if errorlevel 1 (
    set "APP_EXIT=1"
    goto :finish
)

echo [RUN] python main.py -w 4 %RUN_ARGS%
echo.
"%VENV_PY%" "%MAIN_FILE%" -w 4 %RUN_ARGS%
set "APP_EXIT=%ERRORLEVEL%"

if "%APP_EXIT%"=="0" (
    echo.
    echo [OK] Processing finished.
) else (
    echo.
    echo [ERROR] Processing finished with code %APP_EXIT%.
)
goto :finish

:resolve_python
if exist "%PYTHON_LOCAL%" (
    set "BASE_PYTHON_EXE=%PYTHON_LOCAL%"
    set "BASE_PYTHON_ARG="
    goto :python_found
)

py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "BASE_PYTHON_EXE=py"
    set "BASE_PYTHON_ARG=-3.11"
    goto :python_found
)

py -3.10 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "BASE_PYTHON_EXE=py"
    set "BASE_PYTHON_ARG=-3.10"
    goto :python_found
)

py -3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "BASE_PYTHON_EXE=py"
    set "BASE_PYTHON_ARG=-3"
    goto :python_found
)

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "BASE_PYTHON_EXE=python"
    set "BASE_PYTHON_ARG="
    goto :python_found
)

echo [ERROR] Python 3.10+ was not found.
call :find_python_installer
if defined PYTHON_INSTALLER_FILE (
    echo [INFO] Found bundled installer:
    echo   !PYTHON_INSTALLER_FILE!
    echo.
    choice /C YN /N /M "Start Python installer now? [Y/N]: "
    if errorlevel 2 (
        echo [INFO] Installer was not started.
    ) else (
        start "" "!PYTHON_INSTALLER_FILE!"
        echo [INFO] Complete Python installation, then run this script again.
    )
    echo.
)
echo.
echo Option A: install Python 3.11 from python.org (with pip).
echo Option B: put portable Python at:
echo   runtime\python\python.exe
echo Option C: run bundled installer from:
echo   runtime\python-installer\
echo.
exit /b 1

:python_found
echo [OK] Python source: %BASE_PYTHON_EXE% %BASE_PYTHON_ARG%
exit /b 0

:find_python_installer
set "PYTHON_INSTALLER_FILE="
for /f "delims=" %%F in ('dir /b /a:-d "%PYTHON_INSTALLER_DIR%\python-*.exe" 2^>nul') do (
    set "PYTHON_INSTALLER_FILE=%PYTHON_INSTALLER_DIR%\%%F"
    goto :installer_found
)
:installer_found
exit /b 0

:run_base_python
"%BASE_PYTHON_EXE%" %BASE_PYTHON_ARG% %*
exit /b %ERRORLEVEL%

:ensure_venv
if exist "%VENV_PY%" (
    echo [OK] Virtual environment: .venv
    exit /b 0
)

echo [SETUP] Creating virtual environment...
call :run_base_python -m venv "%VENV_DIR%"
if errorlevel 1 (
    echo [ERROR] Failed to create .venv
    exit /b 1
)

if not exist "%VENV_PY%" (
    echo [ERROR] .venv was created incorrectly, python.exe missing.
    exit /b 1
)

echo [OK] Virtual environment created.
exit /b 0

:ensure_dependencies
set "NEED_INSTALL=0"

if not exist "%REQ_STAMP%" set "NEED_INSTALL=1"
if exist "%REQ_STAMP%" (
    fc /b "%REQ_FILE%" "%REQ_STAMP%" >nul 2>&1
    if errorlevel 1 set "NEED_INSTALL=1"
)

if "!NEED_INSTALL!"=="0" (
    "%VENV_PY%" -c "import fitz,cv2,PIL,pandas,openpyxl,rich,pylibdmtx" >nul 2>&1
    if errorlevel 1 set "NEED_INSTALL=1"
)

if "!NEED_INSTALL!"=="0" (
    echo [OK] Python dependencies already installed.
    exit /b 0
)

echo [SETUP] Installing Python dependencies...
"%VENV_PY%" -m ensurepip --upgrade >nul 2>&1
"%VENV_PY%" -m pip install --upgrade pip setuptools
if errorlevel 1 (
    echo [ERROR] Failed to upgrade pip/setuptools.
    exit /b 1
)

echo [SETUP] Downloading packages from internet...
"%VENV_PY%" -m pip install -r "%REQ_FILE%"
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies from requirements.txt
    exit /b 1
)

copy /y "%REQ_FILE%" "%REQ_STAMP%" >nul
echo [OK] Dependencies installed.
exit /b 0

:check_libdmtx
"%VENV_PY%" -c "from pylibdmtx.pylibdmtx import decode; print('ok')" >nul 2>&1
if not errorlevel 1 (
    echo [OK] DataMatrix runtime library is available.
    exit /b 0
)

echo [ERROR] libdmtx runtime library is not available.
echo.
echo Put DLL files to one of these folders:
echo   runtime\libdmtx\
echo   runtime\libdmtx\bin\
echo.
echo Expected file example: libdmtx-64.dll
echo After that run this script again.
echo.
exit /b 1

:prepare_run_mode
set "HAS_RESUME=0"
set "HAS_CUSTOM_OUTPUT=0"
for %%A in (%*) do (
    if /I "%%~A"=="--resume" set "HAS_RESUME=1"
    if /I "%%~A"=="-o" set "HAS_CUSTOM_OUTPUT=1"
    if /I "%%~A"=="--output" set "HAS_CUSTOM_OUTPUT=1"
)

if "!HAS_RESUME!"=="1" exit /b 0
if "!HAS_CUSTOM_OUTPUT!"=="1" exit /b 0

if exist "%DEFAULT_PROGRESS%" (
    echo.
    echo [INFO] Saved progress was found:
    echo   output\results.csv.progress.csv
    choice /C YN /N /M "Continue from saved progress? [Y/N]: "
    if errorlevel 2 (
        echo [INFO] Starting new run mode.
    ) else (
        set "RUN_ARGS=%RUN_ARGS% --resume"
        echo [OK] Resume mode enabled.
        exit /b 0
    )
)

if exist "%DEFAULT_OUTPUT%" (
    echo.
    echo [WARN] Old file exists: output\results.csv
    choice /C YN /N /M "Delete old result before new run? [Y/N]: "
    if errorlevel 2 (
        echo [INFO] Existing CSV will be appended.
    ) else (
        del /f /q "%DEFAULT_OUTPUT%" >nul 2>&1
        del /f /q "%DEFAULT_PROGRESS%" >nul 2>&1
        echo [OK] Old output files were removed.
    )
)

exit /b 0

:finish
echo.
echo Press any key to close...
pause >nul
exit /b %APP_EXIT%

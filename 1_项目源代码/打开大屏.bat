@echo off
chcp 65001 >nul
setlocal
REM ===========================================================
REM  Project 4 : open the dashboard
REM
REM  The dashboard loads output\*.json through fetch(), so it must
REM  be served over HTTP. Double-clicking dashboard\index.html
REM  directly gives a blank page.
REM
REM  The server itself has no external dependency:
REM    1) JDK 11+  - single-file source launcher (preferred)
REM    2) JDK 8    - compiled once with javac
REM    3) Python 3 - python -m http.server (last resort)
REM  A JDK is always present, because Spark needs one to run.
REM
REM  JAVA_HOME is preferred over PATH: many machines keep an old
REM  JRE on PATH while the real JDK sits behind JAVA_HOME.
REM
REM  Keep this file ASCII-only (see run.bat for the reason).
REM ===========================================================
cd /d "%~dp0"

if not exist "dashboard\index.html" (
    echo [ERROR] dashboard\index.html not found in %CD%
    pause
    exit /b 1
)
if not exist "output\kpi.json" (
    echo [ERROR] output\kpi.json not found in %CD%
    echo         run run.bat first to generate the results
    pause
    exit /b 1
)

REM ---------- pick a free port: 8080 first, fall back to 8081 ----------
set "PORT=8080"
netstat -ano | findstr /C:":8080 " | findstr /C:"LISTENING" >nul 2>nul
if not errorlevel 1 (
    echo [info] port 8080 busy, using 8081 instead
    set "PORT=8081"
)
set "URL=http://127.0.0.1:%PORT%/dashboard/index.html"

echo ===========================================================
echo  dashboard : %URL%
echo  stop      : close this window
echo ===========================================================
echo.

start "" %URL%

REM ---------- locate a JDK: JAVA_HOME first, then PATH ----------
set "JAVAEXE="
set "JAVACEXE="
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "JAVAEXE=%JAVA_HOME%\bin\java.exe"
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\javac.exe" set "JAVACEXE=%JAVA_HOME%\bin\javac.exe"
if not defined JAVAEXE (
    where java >nul 2>nul
    if errorlevel 1 goto TRY_PYTHON
    set "JAVAEXE=java"
)
if not defined JAVACEXE (
    where javac >nul 2>nul
    if not errorlevel 1 set "JAVACEXE=javac"
)

REM ---------- a JRE 8 cannot run a .java file directly ----------
REM  findstr is used instead of for/f on purpose: inside for/f the
REM  quotes around a path containing spaces get stripped, and cmd
REM  then reports "'C:/Program' is not recognized".
set "OLDJAVA="
"%JAVAEXE%" -version 2>&1 | findstr /c:"1.8.0_" >nul 2>nul
if not errorlevel 1 set "OLDJAVA=1"

if defined OLDJAVA goto JAVAC_RUN

REM ---------- JDK 11+ : no build step, just run the source ----------
echo [info] serving dashboard with %JAVAEXE%
"%JAVAEXE%" "dashboard\Server.java" %PORT%
goto SERVER_STOPPED

REM ---------- JDK 8 : compile once, then run ----------
:JAVAC_RUN
if not defined JAVACEXE goto TRY_PYTHON
echo [info] compiling the dashboard server with javac ...
if not exist "dashboard\.build" mkdir "dashboard\.build"
"%JAVACEXE%" -encoding UTF-8 -d "dashboard\.build" "dashboard\Server.java"
if errorlevel 1 goto TRY_PYTHON
"%JAVAEXE%" -cp "dashboard\.build" Server %PORT%
goto SERVER_STOPPED

REM ---------- last resort ----------
:TRY_PYTHON
where python >nul 2>nul
if errorlevel 1 goto NO_SERVER
echo [info] java not usable here, falling back to python http.server
python -m http.server %PORT% --bind 127.0.0.1
goto SERVER_STOPPED

:NO_SERVER
echo.
echo [ERROR] could not find a way to serve the dashboard.
echo         Install a JDK (11 or newer is recommended) or Python 3,
echo         then double-click this file again.
pause
exit /b 1

:SERVER_STOPPED
echo.
echo server stopped.
pause

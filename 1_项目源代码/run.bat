@echo off
chcp 65001 >nul
setlocal
REM ===========================================================
REM  Project 4 : Regional Air Quality Analysis
REM
REM  Usage:
REM    run.bat            run the full pipeline (same as metrics)
REM    run.bat check      environment self-check only
REM    run.bat probe      data profiling        (chapter 1)
REM    run.bat clean      cleaning only         (chapter 2)
REM    run.bat metrics    cleaning + 9 metrics + output (default)
REM
REM  Keep this file ASCII-only. cmd.exe misparses .bat files that
REM  contain multi-byte characters (UTF-8 or GBK) and reports bogus
REM  errors such as "'probe' is not recognized".
REM  Chinese guidance: see the .md readme in this folder.
REM ===========================================================

REM [1/4] switch to the folder holding this script (build.sbt must be here)
cd /d "%~dp0"

REM [2/4] point Hadoop at the bundled native libraries
if exist "%~dp0hadoop\bin\winutils.exe" (
    set "HADOOP_HOME=%~dp0hadoop"
    set "PATH=%~dp0hadoop\bin;%PATH%"
    echo [env] HADOOP_HOME = %~dp0hadoop
) else (
    echo [warn] hadoop\bin\winutils.exe not found, parquet output may fail
)

REM [3/4] keep sbt temp & global dir on an ASCII path (Chinese user name breaks it)
REM   A Chinese Windows user name makes sbt's zinc compiler write its
REM   compiler-bridge under C:\Users\<name>\AppData\Local\Temp and its global
REM   cache under C:\Users\<name>\.sbt; both paths break, failing with
REM   "cannot redefine component ... compiler-bridge". Redirecting both
REM   java.io.tmpdir and sbt.global.base to %PUBLIC% (an ASCII path present on
REM   every Windows box) fixes it. Run the project through this script, or set
REM   the same two -D flags in SBT_OPTS before running sbt by hand.
set "SBT_OPTS=%SBT_OPTS% -Djava.io.tmpdir=%PUBLIC%\tmp -Dsbt.global.base=%PUBLIC%\sbt-global -Dfile.encoding=UTF-8"

if not defined JAVA_HOME (
    where java >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] Java not found. Install JDK 8 / 11 / 17 and set JAVA_HOME.
        pause
        exit /b 1
    )
    echo [env] using java from PATH
) else (
    echo [env] JAVA_HOME   = %JAVA_HOME%
)

where sbt >nul 2>nul
if errorlevel 1 (
    echo [ERROR] sbt not found in PATH. Install sbt 1.9.x, or open this
    echo         project with IntelliJ IDEA and run aqi.Main instead.
    pause
    exit /b 1
)

REM [4/4] dispatch
if "%~1"=="" ( set "STAGE=metrics" ) else ( set "STAGE=%~1" )

echo ===========================================================
echo  folder : %CD%
echo  stage  : %STAGE%
echo ===========================================================
echo.

call sbt -batch "run %STAGE%"

echo.
echo ===========================================================
echo  exit code : %ERRORLEVEL%
echo  results   : %CD%\output
echo  cleaned   : %CD%\cleaned
echo  dashboard : double-click the screen launcher .bat
echo ===========================================================
pause

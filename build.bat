@echo off
echo ========================================================
echo  Building FlashKV (High-Performance In-Memory KV Store)
echo ========================================================

set CXX=g++
set CXXFLAGS=-O3 -std=gnu++14 -Iinclude -D_WIN32_WINNT=0x0601 -Wall -Wextra
set LIBS=-lws2_32

echo [1/4] Compiling flashkv-server.exe...
%CXX% %CXXFLAGS% src\server_main.cpp -o flashkv-server.exe %LIBS%
if %ERRORLEVEL% NEQ 0 (
    echo Error compiling flashkv-server.exe
    exit /b 1
)

echo [2/4] Compiling flashkv-cli.exe...
%CXX% %CXXFLAGS% src\cli_main.cpp -o flashkv-cli.exe %LIBS%
if %ERRORLEVEL% NEQ 0 (
    echo Error compiling flashkv-cli.exe
    exit /b 1
)

echo [3/4] Compiling flashkv-bench.exe...
%CXX% %CXXFLAGS% src\bench_main.cpp -o flashkv-bench.exe %LIBS%
if %ERRORLEVEL% NEQ 0 (
    echo Error compiling flashkv-bench.exe
    exit /b 1
)

echo [4/4] Compiling flashkv-test.exe...
%CXX% %CXXFLAGS% src\test_main.cpp -o flashkv-test.exe %LIBS%
if %ERRORLEVEL% NEQ 0 (
    echo Error compiling flashkv-test.exe
    exit /b 1
)

echo ========================================================
echo  Build Successful! Generated:
echo   - flashkv-server.exe (Core TCP In-Memory Store Server)
echo   - flashkv-cli.exe    (Interactive REPL Client)
echo   - flashkv-bench.exe  (High-Throughput Benchmark Suite)
echo   - flashkv-test.exe   (Unit and Stress Test Suite)
echo ========================================================

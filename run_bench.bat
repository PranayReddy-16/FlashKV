@echo off
echo Running FlashKV Benchmarks against port 6379...
flashkv-bench.exe -p 6379 -n 50000 -c 20 -t ALL
pause

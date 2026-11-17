CXX ?= g++
CXXFLAGS = -O3 -std=gnu++14 -Iinclude -Wall -Wextra
DEFINES = -D_WIN32_WINNT=0x0601

# OS detection
ifeq ($(OS),Windows_NT)
    LIBS = -lws2_32
    EXT = .exe
else
    LIBS = -lpthread
    EXT =
    DEFINES =
endif

TARGET_SERVER = flashkv-server$(EXT)
TARGET_CLI = flashkv-cli$(EXT)
TARGET_BENCH = flashkv-bench$(EXT)
TARGET_TEST = flashkv-test$(EXT)

ALL_TARGETS = $(TARGET_SERVER) $(TARGET_CLI) $(TARGET_BENCH) $(TARGET_TEST)

all: $(ALL_TARGETS)

$(TARGET_SERVER): src/server_main.cpp
	$(CXX) $(CXXFLAGS) $(DEFINES) $< -o $@ $(LIBS)

$(TARGET_CLI): src/cli_main.cpp
	$(CXX) $(CXXFLAGS) $(DEFINES) $< -o $@ $(LIBS)

$(TARGET_BENCH): src/bench_main.cpp
	$(CXX) $(CXXFLAGS) $(DEFINES) $< -o $@ $(LIBS)

$(TARGET_TEST): src/test_main.cpp
	$(CXX) $(CXXFLAGS) $(DEFINES) $< -o $@ $(LIBS)

test: $(TARGET_TEST)
	./$(TARGET_TEST)

clean:
	rm -f $(ALL_TARGETS) *.o *.aof *.rdb *.tmp

.PHONY: all test clean

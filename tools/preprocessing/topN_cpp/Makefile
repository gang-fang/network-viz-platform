CXX ?= g++

SRC_DIR := src
BUILD_DIR := build
BIN_DIR := ../../bin
TARGET := $(BIN_DIR)/topn

CPPFLAGS ?= -I$(SRC_DIR)
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -pthread
LDFLAGS ?=
LDLIBS ?= -pthread

SRCS := io_utils.cpp matrix.cpp speedup.cpp fastp.cpp nw_align.cpp main.cpp
SOURCES := $(addprefix $(SRC_DIR)/,$(SRCS))
OBJECTS := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SOURCES))
DEPS := $(OBJECTS:.o=.d)

.PHONY: all install clean

all: install

install: $(TARGET)

$(TARGET): $(OBJECTS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJECTS) $(LDLIBS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

$(BUILD_DIR) $(BIN_DIR):
	mkdir -p $@

clean:
	rm -rf $(BUILD_DIR)
	rm -f $(TARGET)

-include $(DEPS)

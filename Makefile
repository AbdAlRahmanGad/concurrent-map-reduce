# Compiler and flags
CC=gcc
CFLAGS=-Wall -Werror -pthread -O
ifeq ($(TESTING),1)
	CFLAGS += -DTESTING
endif

# Targets
TARGET=concurrent_map_reduce
SOURCES=main.c mapreduce.c
OBJECTS=$(SOURCES:.c=.o)

# Default target
all: $(TARGET)

# Link the object files into the executable
$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -o $(TARGET)

# Compile each .c file into .o
%.o: %.c
	$(CC) -c $< $(CFLAGS)

# Debug target (with debug symbols)
# witho no optimization
debug: CFLAGS += -g -O0
debug: clean $(TARGET)

# Clean the build files
clean:
	rm -f $(OBJECTS) $(TARGET)

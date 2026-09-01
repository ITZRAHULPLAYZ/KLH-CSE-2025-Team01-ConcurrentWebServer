# ── Compiler & Flags ─────────────────────────────────────────────────
CC      = gcc
CFLAGS  = -Wall -Wextra -pthread -g
TARGET  = server
SRCDIR  = src

# ── Source files ──────────────────────────────────────────────────────
SRCS = $(SRCDIR)/server.c \
       $(SRCDIR)/thread_pool.c \
       $(SRCDIR)/logger.c

# ── Build targets ─────────────────────────────────────────────────────
all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS)
	@echo "Build successful! Run with: ./server [port]"

# ── Clean ─────────────────────────────────────────────────────────────
clean:
	rm -f $(TARGET) server.log

# ── Quick test (requires curl) ────────────────────────────────────────
test:
	@echo "Testing single request..."
	curl -s http://localhost:8080/index.html | grep -o "<title>.*</title>"
	@echo ""
	@echo "Testing 404..."
	curl -s -o /dev/null -w "Status: %{http_code}\n" http://localhost:8080/missing.html
	@echo ""
	@echo "Testing path traversal block..."
	curl -s -o /dev/null -w "Status: %{http_code}\n" "http://localhost:8080/../etc/passwd"
	@echo ""
	@echo "Load test (100 requests, 10 concurrent) -- requires 'ab' (apache bench)"
	ab -n 100 -c 10 http://localhost:8080/index.html 2>/dev/null | grep -E "Requests per|Failed"

.PHONY: all clean test

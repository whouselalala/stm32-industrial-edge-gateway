CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Wpedantic
LDLIBS += -lmodbus -lmosquitto -lsqlite3 -lsystemd -pthread
SOURCES = main.c store.c publisher.c

edge-gateway: $(SOURCES) gateway.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -std=c11 -pthread $(SOURCES) $(LDFLAGS) $(LDLIBS) -o $@

clean:
	$(RM) edge-gateway

.PHONY: clean

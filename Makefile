.PHONY: all clean re test clean-queue

SERVER_DIRPATH := server
CLIENT_DIRPATH := client

GO_SOURCES = $(shell find src -type d -path "src/$(1)" -prune -o -type f -name "*.go" -print)

GO_SERVER_SOURCES := $(call GO_SOURCES,$(CLIENT_DIRPATH)) go.mod
GO_CLIENT_SOURCES := $(call GO_SOURCES,$(SERVER_DIRPATH)) go.mod

all: server client

server: $(GO_SERVER_SOURCES)
	@printf "rebuilt $@ using %s\n" $(filter %.go,$?)
	go build -o $@ ./src/server

client: $(GO_CLIENT_SOURCES)
	@printf "rebuilt $@ using %s\n" $(filter %.go,$?)
	go build -o $@ ./src/client

clean:
	rm -f server client

re: clean all

test:
	go test -race ./...

clean-queue:
	rm -f /dev/mqueue/operator.*
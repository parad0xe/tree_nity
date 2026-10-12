.PHONY: all clean re test clean-queue

all: server client

server:
	go build -o src/server ./server

client:
	go build -o src/client ./client

clean:
	rm -f server client

re: clean all

test:
	go test ./...

clean-queue:
	rm -f /dev/mqueue/operator.*
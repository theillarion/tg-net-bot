APP := tg-net-bot
BUILD_DIR := bin

.PHONY: build run fmt vet clean

build:
	mkdir -p $(BUILD_DIR)
	go build -trimpath -o $(BUILD_DIR)/$(APP) ./cmd/tg-net-bot

run:
	go run ./cmd/tg-net-bot serve

fmt:
	gofmt -w cmd internal

vet:
	go vet ./...

clean:
	rm -rf $(BUILD_DIR)

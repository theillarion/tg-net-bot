APP := tg-net-bot
BUILD_DIR := bin

.PHONY: build run fmt vet clean test-unit test-functional test-integration coverage

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

# Быстрые unit-тесты без внешних сервисов.
test-unit:
	go test ./...

# Функциональные сценарии Telegram-бота с локальным тестовым Bot API.
test-functional:
	go test -tags=functional ./internal/telegram

# Интеграционный сценарий использует временную SQLite-базу.
test-integration:
	go test -tags=integration ./internal/app

# Общий порог покрытия: unit + функциональные + интеграционные тесты.
coverage:
	go test -count=1 -tags=integration,functional -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | awk '/^total:/ { gsub("%", "", $$3); printf "Total coverage: %s%%\n", $$3; if ($$3 + 0 < 95) exit 1; found=1 } END { if (!found) exit 1 }'

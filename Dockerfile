FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tg-net-bot ./cmd/tg-net-bot

FROM alpine:3.21
RUN apk add --no-cache ca-certificates iputils
WORKDIR /app
COPY --from=build /out/tg-net-bot /usr/local/bin/tg-net-bot
VOLUME ["/app/data"]
ENTRYPOINT ["tg-net-bot"]

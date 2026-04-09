FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /redibridge ./cmd/agent

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /redibridge /usr/local/bin/redibridge
COPY config/ /etc/redibridge/

ENTRYPOINT ["redibridge"]
CMD ["--config", "/etc/redibridge/standalone/agent-a.yaml"]

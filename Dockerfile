# Этап сборки
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Копируем сначала только файлы зависимостей для лучшего кэширования
COPY go.mod go.sum ./
RUN go mod download

# Копируем остальные файлы и собираем
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o wb-adverts-stats .

# Этап запуска
FROM alpine:3.18

WORKDIR /app

# Копируем бинарник
COPY --from=builder /app/wb-adverts-stats .

# Устанавливаем минимальные зависимости
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -g '' appuser && \
    mkdir -p /data && \
    chown appuser:appuser /data

# Настройки времени и прав
USER appuser

# Экспонируем порт HTTP-сервера
EXPOSE 8080

# Healthcheck для мониторинга
HEALTHCHECK --interval=30s --timeout=3s \
  CMD wget -q --spider http://localhost:8080/status || exit 1

# Запускаем приложение
CMD ["./wb-adverts-stats"]
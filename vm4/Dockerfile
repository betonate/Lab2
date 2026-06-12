FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum if it exists
COPY go.mod go.sum ./
# Copy proto and src code
COPY proto/ ./proto/
COPY src/ ./src/

# Build component binary
ARG COMPONENT
RUN CGO_ENABLED=0 GOOS=linux go build -o app_bin ./src/${COMPONENT}/main.go

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/app_bin .

# Copy catalog files (needed by producers)
COPY tests_files/ ./tests_files/

# Run the app
ENTRYPOINT ["./app_bin"]

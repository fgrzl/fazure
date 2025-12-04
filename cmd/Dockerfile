# Build stage
FROM golang:1.25 AS builder

WORKDIR /app

# Copy go mod files for better layer caching
COPY ../go.mod ../go.sum ./
RUN go mod download

# Copy source code
COPY ../ .

# Build the binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /fazure ./cmd

# Final stage - minimal runtime image
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

# Copy the binary from builder
COPY --from=builder /fazure /fazure

# Expose port
EXPOSE 10000

# Set environment variables
ENV DATA_DIR=/data \
    PORT=10000

ENTRYPOINT ["/fazure"]

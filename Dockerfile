# Stage 1: Build the Go application
FROM golang:latest AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the webui binary
# CGO_ENABLED=0 is important for a static binary that can run in a scratch image
# -ldflags="-s -w" strips debug information to reduce binary size
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /webui ./cmd/webui

# Stage 2: Create the final, minimal production image
FROM scratch

# Copy the static web assets from the builder stage
COPY --from=builder /app/web/static /web/static

# Copy the compiled binary from the builder stage
COPY --from=builder /webui /webui

# Expose the port the application runs on
EXPOSE 8080

# Set the entrypoint for the container
ENTRYPOINT ["/webui"]

# Step 1: Build the Go Engine
FROM golang:1.22-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy the go modules and install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of your source code
COPY . .

# Build the executable from the correct directory
RUN CGO_ENABLED=0 GOOS=linux go build -o zeno-backend ./cmd/zeno-aoo

# Step 2: Create the lightweight production image
FROM alpine:latest  

WORKDIR /root/

# Copy the compiled binary from the builder stage
COPY --from=builder /app/zeno-backend .

# Expose the WebSocket port
EXPOSE 8080

# Command to run the executable
CMD ["./zeno-backend"]
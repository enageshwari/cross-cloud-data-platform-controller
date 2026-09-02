# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /bin/control-api ./cmd/server

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/control-api /control-api
EXPOSE 8080
ENTRYPOINT ["/control-api"]

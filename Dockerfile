# forgejo-k8s-runner — Native K8s runner for Forgejo Actions
FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -ldflags="-s -w" -o /runner .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /runner /usr/local/bin/forgejo-k8s-runner
ENTRYPOINT ["/usr/local/bin/forgejo-k8s-runner"]
CMD ["daemon"]

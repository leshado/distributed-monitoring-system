FROM golang:1.26-alpine AS builder

WORKDIR /src

ARG SERVICE

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./services/${SERVICE}/cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/service /service
COPY --from=builder /src/migrations /migrations

USER nonroot:nonroot
ENTRYPOINT ["/service"]

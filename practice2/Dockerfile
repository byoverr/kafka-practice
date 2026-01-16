# build
FROM golang:1.25 as builder


RUN apt-get update && apt-get install -y gcc g++ librdkafka-dev

WORKDIR /go/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /go/app/practice2

RUN CGO_ENABLED=1 go build -o /go/app/ ./main.go

# runtime
FROM debian:stable-slim

RUN apt-get update && apt-get install -y librdkafka1 && rm -rf /var/lib/apt/lists/*

WORKDIR /root/
COPY --from=builder /go/app/main .

ENTRYPOINT ["/root/main"]

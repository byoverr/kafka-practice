# build
FROM golang:1.25 as builder


RUN apt-get update && apt-get install -y gcc g++ librdkafka-dev

WORKDIR /go/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /go/app/practice1/single-consumer

RUN CGO_ENABLED=1 go build -o /go/app/single ./main.go

# runtime
FROM debian:stable-slim

RUN apt-get update && apt-get install -y librdkafka1 && rm -rf /var/lib/apt/lists/*

WORKDIR /root/
COPY --from=builder /go/app/single .

ENTRYPOINT ["/root/single"]

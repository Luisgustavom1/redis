# syntax=docker/dockerfile:1

FROM golang:1.22
WORKDIR /app
COPY go.mod go.sum ./
COPY *.go ./

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /service

CMD ["/service"]
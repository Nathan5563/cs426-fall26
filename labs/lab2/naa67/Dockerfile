# syntax=docker/dockerfile:1

FROM golang:1.23 AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /video_rec_service ./video_rec_service/server

FROM alpine:latest

WORKDIR /

COPY --from=build /video_rec_service /video_rec_service

EXPOSE 8080

ENTRYPOINT ["/video_rec_service"]

FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kitsune .

FROM alpine:3.22

RUN adduser -D -H -u 10001 kitsune && mkdir -p /data/kitsune && chown -R kitsune:kitsune /data
COPY --from=build /out/kitsune /usr/local/bin/kitsune
USER kitsune
ENTRYPOINT ["kitsune"]

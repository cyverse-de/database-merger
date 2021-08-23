FROM golang:1.16-alpine

ENV CGO_ENABLED=0

WORKDIR /go/src/github.com/cyverse-de/database-merger
COPY . .
RUN go build .

FROM scratch

WORKDIR /
COPY --from=0 /go/src/github.com/cyverse-de/database-merger/database-merger /bin/database-merger

ENTRYPOINT ["database-merger"]
CMD ["--help"]

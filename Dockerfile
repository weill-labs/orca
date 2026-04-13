FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BUILD_COMMIT=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -X main.BuildCommit=${BUILD_COMMIT}" -o /out/orca-relay ./cmd/orca-relay

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/orca-relay /usr/local/bin/orca-relay

ENV PORT=8080

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/orca-relay"]

# go-shop-lite

A shop to learn microservices fundamentals + gRPC call discipline + GraphQL BFF in one Go monorepo.

This is the second and last repo. `go-realtime-chat` (frozen) covered streaming lifecycle (unary + server streaming, WebSocket hub). This one covers what chat did not: service boundaries, unary call discipline, Saga, events, traces, and BFF aggregation. No streaming lifecycle repeated here.

## Goals

- Split a monolith into services with database per service (one Postgres instance, separate logical DBs, no cross-DB JOIN)
- Practice gRPC unary discipline: deadline propagation, retry + idempotency, error-code mapping, proto evolution, chaos survival
- Run a Saga: CreateOrder -> DeductStock fails -> compensate (cancel order / restore), reproducible in `go test`
- Emit `order.created` via NATS; consumer restart re-receives without loss
- Trace bff -> order -> product with OpenTelemetry in Jaeger; structured logs carry `trace_id`
- Aggregate in one GraphQL round trip what REST needs 3 trips for (`order { items { product } }`)
- Stay solo-runnable: single repo, single `go.mod`, `docker compose up` like a monolith

## Stack

- gRPC: `google.golang.org/grpc` + `protobuf` (code generated with `buf`, Unary only; one server-streaming allowed only if NATS proves insufficient)
- GraphQL: `gqlgen` in `bff` only (aggregation, no business logic; resolvers call gRPC)
- Async: NATS (`order.created` -> log-only consumer is fine)
- DB: one Postgres container, separate databases (`order_db`, `product_db`) + separate users with no cross access
- Traces: OpenTelemetry -> Jaeger
- Time: `int64` Unix milliseconds; errors via gRPC status codes (`InvalidArgument`, `FailedPrecondition`, `Unavailable`, `NotFound`) mapped to GraphQL errors in `bff`
- Tests: `bufconn` + `goleak` (no leaked goroutines, no real ports for `go test ./...`)
- Containers: multi-stage builds on `scratch` images

## Project Layout

```text
.
├── proto/shop/v1/          # contract: order, product, user (numbers frozen)
│   ├── order.proto          # OrderService: CreateOrder / GetOrder
│   ├── product.proto        # ProductService: GetProduct / DeductStock (idempotency_key) / RestoreStock
│   └── user.proto           # UserService: stub auth only
├── proto/gen/               # generated code (committed so services import it directly)
├── internal/tracing/        # OTel setup: propagator always, OTLP export when OTEL_ADDR is set
├── services/order/          # :50051, owns order_db
│   ├── main.go              # bootstrap: listen, product client, events, tracing
│   ├── server.go            # CreateOrder / GetOrder + Saga + bounded retries
│   ├── store.go             # Store interface + memory backend
│   ├── pgstore.go           # Postgres backend (order_db only)
│   ├── events.go            # order.created publisher (NATS JetStream)
│   ├── server_test.go       # bufconn tests (Saga + compensate + retry + trace)
│   ├── store_test.go        # conformance (memory always, PG when DSN set)
│   └── guard_test.go        # cross-DB connection denied (live DB only)
├── services/product/        # :50052, owns product_db
│   ├── main.go              # bootstrap
│   ├── server.go            # error mapping over a Store
│   ├── store.go             # Store interface + memory backend
│   ├── pgstore.go           # Postgres backend (product_db only)
│   └── server_test.go       # bufconn tests (idempotency + evolution)
├── services/notify/         # durable order.created consumer (log-only)
├── services/user/           # :50053, stub auth only
│   └── main.go
├── bff/                     # :8080, GraphQL via gqlgen, gRPC client to the three above
│   ├── main.go              # server + error presenter + trace start
│   ├── gqlgen.yml           # schema in, generated code out
│   └── graph/               # schema.graphqls + resolver.go (hand) + generated.go (tool)
├── scripts/
│   ├── seed/                # go run ./scripts/seed (stocks product_db when SEED_PRODUCT_DSN is set)
│   └── chaos/               # go run ./scripts/chaos -kill=product -for=5s (containers only)
├── docker/postgres/         # init.sql: fenced databases and roles
└── docker-compose.yml       # order + product + user + bff + notify + postgres + nats + jaeger
```

## Flow

Two paths, one rule. Sync calls use gRPC Unary only; async uses NATS. No cross-DB JOIN.

```text
GraphQL --query--> bff :8080 --gRPC--> order :50051 --gRPC--> product :50052
                                    order --NATS--> order.created --> consumer (log-only)
```

- **Order** (unary, Saga): `CreateOrder` -> `DeductStock(idempotency_key)`. On stock failure, compensate (cancel order / restore). Deadline propagates bff -> order -> product with consistent timeouts.
- **Stock** (unary, idempotent): `DeductStock` retries never double-deduct; wrong product id -> `NotFound`, insufficient stock -> `FailedPrecondition`, bad input -> `InvalidArgument`, downstream down -> `Unavailable`.
- **Events** (async): `order.created` via NATS; consumer restart re-receives without loss.
- **BFF** (aggregation only): one GraphQL query fetches `order + items + product`; resolvers hold no business logic.

## Quick Start

Run one stack at a time: local `go run` or compose, never both (ports collide and you will talk to the wrong one). Pick either block below, not both.

```bash
go run ./services/product &
go run ./services/order &
go run ./services/user &
go run ./services/notify &
go run ./bff &
```

```bash
docker compose up --build
```

Then seed and query:

```bash
# Local memory run: verifies the baseline over gRPC.
go run ./scripts/seed

# Against compose Postgres: stock 10000 each first, then verify.
SEED_PRODUCT_DSN="postgres://product_user:product_pw@localhost:5432/product_db?sslmode=disable" go run ./scripts/seed
```

Local runs default to memory stores and silent eventing/tracing: no database,
NATS, or Jaeger needed. Point at live infra with `DATABASE_URL`, `NATS_ADDR`,
and `OTEL_ADDR` when you want it (compose sets all three).

## Demo Script

```bash
# 1. Single query fetching order + items + product in one round trip (REST would need 3)
curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
  -d '{"query":"{ order(id:\"o1\") { id customerId status items { productId qty product { id name } } } }"}'
# -> {"data":{"order":{"id":"o1","customerId":"c1","status":"CREATED","items":[...]}}}

# 2. Chaos: kill product 5s, order retries/degrades correctly, no lost or double orders
go run ./scripts/chaos -kill=product -for=5s
# -> reconcile with the Chaos section below
```

## Manual Test

1. **Aggregate**: seed, create an order, then run the Demo query. One round
   trip returns order + items + product with the same ids end to end.
2. **Compensate**: order more than the stock. The order flips to CANCELLED,
   the deducted items are restored, and the product code (`FailedPrecondition`)
   arrives unchanged inside the GraphQL error extensions.
3. **Restart**: stop the notify consumer mid-load and start it back. Missed
   `order.created` events re-arrive; nothing already logged repeats.
4. **Resilience**: stop product for 5s (`scripts/chaos`). Orders during the
   outage fail clean to CANCELLED; after restart traffic resumes without
   restarting order, and stock reconciles exactly.

## Benchmark (baselines checked in before any optimization)

Tool versions for repro: ghz dev, buf 1.50.0, protoc-gen-go-grpc v1.6.2.

```bash
ghz --insecure --proto proto/shop/v1/order.proto \
  --call shop.v1.OrderService.CreateOrder \
  -d '{"customer_id":"c1","items":[{"product_id":"p1","qty":1}]}' \
  -c 50 -n 5000 localhost:50051

ghz --insecure --proto proto/shop/v1/product.proto \
  --call shop.v1.ProductService.GetProduct \
  -d '{"product_id":"p1"}' \
  -c 50 -n 5000 localhost:50052
```

| Call        | RPS   | p50     | p99     | Notes              |
| ----------- | ----- | ------- | ------- | ------------------ |
| CreateOrder (happy) | 502 | 68ms | 487ms | Postgres-backed, seeded 10000, 5000/5000 OK. In-memory failure-dominated run did 17.4k RPS (10 OK + 4990 FailedPrecondition); its percentiles were withheld over a ghz reporting anomaly |
| GetProduct  | 29.5k | 0.89ms  | 3.65ms  | pure reads, 5000/5000 OK |

## Chaos (kill product 5s mid-load, 2000 orders at c=20)

1133 OK + 867 Unavailable, all reconciled: p1 stock 8867 (= 10000 − 1133,
deducted exactly once each), 867 CANCELLED (= failures, all compensated),
CREATED 6133 (= 5000 earlier + 1133). No lost orders, no double deducts.
Retries cover blips; a 5s outage fails clean and compensates.

## Development

```bash
go list ./...
go vet ./...
go test ./...
gofmt -l .
golangci-lint run
```

Proto regeneration (numbers frozen; adding a field must break no old client, test included).
Run from `proto/` (not the repo root), with the plugin binaries on PATH:

```bash
export PATH="$PATH:/root/go/bin:$(go env GOPATH)/bin"
cd proto && buf generate && cd ..
```

Out of scope for now: chat-repo integration, K8s, service mesh, client-streaming/bidi.

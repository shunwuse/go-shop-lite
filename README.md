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
│   ├── product.proto        # ProductService: GetProduct / DeductStock (idempotency_key)
│   └── user.proto           # UserService: stub auth only
├── proto/gen/               # generated code (committed so services import it directly)
├── services/order/          # :50051, owns order_db
│   ├── main.go              # bootstrap: listen, interceptors, serve
│   ├── server.go            # CreateOrder / GetOrder + Saga + product client
│   └── server_test.go       # bufconn tests (happy + compensate + idempotency)
├── services/product/        # :50052, owns product_db
│   ├── main.go              # bootstrap
│   ├── server.go            # GetProduct / DeductStock (idempotency map)
│   └── server_test.go       # bufconn tests (retry never double-deducts)
├── services/user/           # :50053, stub auth only
│   └── main.go
├── bff/                     # :8080, GraphQL via gqlgen, gRPC client to the three above
│   └── main.go
├── scripts/
│   ├── seed/                # go run ./scripts/seed
│   └── chaos/               # go run ./scripts/chaos -kill=product -for=5s
└── docker-compose.yml       # order + product + user + bff + postgres + nats + jaeger
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
go run ./bff &
```

```bash
docker compose up --build
```

Then seed and query:

```bash
go run ./scripts/seed
```

## Demo Script

Copy-paste the whole block (GraphQL shapes to be frozen in Phase 3):

```bash
# 1. Single query fetching order + items + product in one round trip (REST would need 3)
# POST localhost:8080/query {"query":"{ order(id:\"o1\") { id items { product { id name } } } }"}

# 2. Chaos: kill product 5s, order retries/degrades correctly, no lost or double orders
go run ./scripts/chaos -kill=product -for=5s
```

## Benchmark (baselines checked in before any optimization)

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

| Call        | RPS | p50 | p99 | Notes              |
| ----------- | --- | --- | --- | ------------------ |
| CreateOrder | TBD | TBD | TBD | baseline before optimization |
| GetProduct  | TBD | TBD | TBD | baseline before optimization |

## Development

```bash
go list ./...
go vet ./...
go test ./...
gofmt -l .
```

Proto regeneration (numbers frozen; adding a field must break no old client, test included):

```bash
buf generate
```

Out of scope for now: chat-repo integration, K8s, service mesh, client-streaming/bidi.

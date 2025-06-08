# Flash‑Sale Service

A high‑throughput, two‑phase flash‑sale backend written in Go.  
Every hour sells exactly 10,000 items with per‑user limit of 10.  
Built on **Redis** (atomic counters + Lua scripts) and **PostgreSQL** (durable logs).  
No web frameworks—only Go’s `net/http`—and minimal (only two) external deps:

- `github.com/go-redis/redis/v8`
- `github.com/lib/pq`

---

## 🚀 What it have

- **`POST /checkout?user_id={uid}&id={itemID}`**
  - Issues a one‑time code if inventory and per‑user limits allow.
  - Atomic reservation in Redis via a single Lua script.
- **`POST /purchase?code={code}`**
  - Redeems the code and finalizes the sale, again via one Lua script.
- **Exactly 10,000 items sold per hour**, **maximum 10 per user**.
- Non‑blocking handlers: Redis hot‑path + background Postgres persistence.
- Graceful shutdown and minimal configuration.

---

## 📁 Structure

```

.
├── cmd/
│   └── server/
│       └── main.go             # Bootstraps HTTP server, graceful shutdown
├── internal/  
│   ├── db/  
│   │   ├── redis.go            # Redis client & Lua scripts
│   │   └── postgres.go         # Postgres client & schema init
│   ├── handlers/  
│   │   ├── checkout.go         # /checkout handler
│   │   └── purchase.go         # /purchase handler
│   ├── service/  
│   │   └── sale.go             # Current‑sale epoch logic
│   └── workers/  
│       ├── checkout_worker.go  # Background checkout persistence
│       └── purchase_worker.go  # Background purchase persistence
├── random_checkout.lua         # wrk script for randomized /checkout
├── loadtest.sh                 # automates checkout + purchase load tests
└── docker-compose.yml          # Redis, Postgres, app, wrk

```

---

## ⚙️ Prerequisites

- Docker & Docker Compose
- `bash`, `jq` (for CLI JSON parsing)
- `wrk` (will be run in a Docker container)
- `psql` to run the `loadtest.sh` script. Run `sudo apt install postgresql-client` to isntall it. 
---

## 🛠️ Quick Start

1. **Clone & start services**

   ```bash
   git clone https://github.com/johntad110/not-back-contest.git
   cd not-back-contest
   docker-compose up -d
   ```

2. **Run automated load tests**

   ```bash
   chmod +x loadtest.sh
   ./loadtest.sh
   ```

   This script will:

   1. Run `random_checkout.lua` against `/checkout`
   2. Export a batch of valid codes from Postgres into `codes.txt`
   3. Dynamically generate `purchase_test.lua` and run it against `/purchase`

3. **Tear down**

   ```bash
   docker-compose down
   ```

---

## 📦 Configuration

By default, the service connects to:

- **Redis** at `redis:6379`
- **Postgres** at `postgres://postgres:postgres@postgres/flashsale?sslmode=disable`
- **Port** `8080` for HTTP

Customize by setting these environment variables in the `docker-compose.yml` or your shell:

```yaml
environment:
  - REDIS_ADDR=redis:6379
  - POSTGRES_DSN=postgres://user:pass@host/db?sslmode=disable
  - SERVER_PORT=8080
```

---

## 🛎️ API Reference

### `POST /checkout?user_id={uid}&id={itemID}`

- **Success** `200`

  ```json
  { "code": "32‑hex‑char‑token" }
  ```

- **Errors**

  - `409 Conflict` – sale sold out
  - `429 Too Many Requests` – user limit reached
  - `503 Service Unavailable` – transient failure

### `POST /purchase?code={code}`

- **Success** `200`

  ```json
  {
    "success": true,
    "item_id": "…",
    "item_name": "…",
    "item_image": "…"
  }
  ```

- **Errors**

  - `400 Bad Request` – invalid or expired code
  - `409 Conflict` – sold out or code already used
  - `503 Service Unavailable` – transient failure

---

## 📈 Load Testing

- **Randomized `/checkout`**

  ```bash
  docker-compose run --rm \
  wrk \
  -t8 -c200 -d30s -s .//random_checkout.lua \
  "http://app:8080"
  ```

- **Full flow**
  Just run `./loadtest.sh` (it handles both endpoints in sequence and u need psql run `sudo apt install postgresql-client` to install it).

---

## 🎯 Performance

On a 4 vCPU, 8 GB SSD‑backed VM:

- **> 40,000 RPS** on raw `/checkout` under saturation
- **< 20 ms** median latency
- **“No crashes”** with 500+ concurrent connections

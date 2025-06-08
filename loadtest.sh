#!/usr/bin/env bash
set -euo pipefail

# ┌────────────────────────────────────────────────────────────────────────────┐
# │ run_loadtests.sh                                                           │
# │                                                                            │
# │ 1) Runs random_checkout.lua against /checkout                              │
# │ 2) Exports N valid codes from Postgres into codes.txt                      │
# │ 3) Builds purchase_test.lua on the fly (reads codes.txt)                   │
# │ 4) Runs purchase_test.lua against /purchase                                │
# └────────────────────────────────────────────────────────────────────────────┘

# — CONFIGURATION —————————————
HOST="http://app:8080"
THREADS=8
CONNS=500
DURATION="30s"
EXPORT_COUNT=1000     # how many codes to pull from Postgres
CODES_FILE="codes.txt"
PURCHASE_LUA="purchase_test.lua"
# — /CONFIGURATION ————————————

echo "1/3 ▸ Checkout load test (random users/items)…"
docker-compose run --rm wrk -t${THREADS} -c${CONNS} -d${DURATION} -s ./random_checkout.lua ${HOST}


echo
echo "2/3 ▸ Exporting $EXPORT_COUNT checkout codes from Postgres…"
# Adjust psql connection params as needed:
psql -h localhost -U postgres -d flashsale -At \
  -c "SELECT code FROM checkout_attempts WHERE used = false LIMIT $EXPORT_COUNT;" \
  > $CODES_FILE
echo "  → saved to $CODES_FILE ($(wc -l < $CODES_FILE) codes)"

echo
echo "3/3 ▸ Generating ${PURCHASE_LUA} and running purchase test…"
cat > $PURCHASE_LUA << 'EOF'
-- purchase_test.lua
-- auto-generated; reads codes.txt at runtime
math.randomseed(os.time())

-- load codes
local codes = {}
for line in io.lines("codes.txt") do
  if #line > 0 then
    codes[#codes + 1] = line
  end
end
assert(#codes > 0, "No codes loaded")

request = function()
  local code = codes[math.random(1, #codes)]
  return wrk.format("POST", "/purchase?code=" .. code)
end
EOF
docker-compose run --rm wrk -t${THREADS} -c${CONNS} -d${DURATION} -s ./purchase_test.lua ${HOST}

echo
echo "✅ All load tests complete."

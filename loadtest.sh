#!/bin/bash

# Pre-generate test data
echo "Pre-generating test data..."
for i in {1..1000}; do
  curl -X POST "http://localhost:8080/checkout?user_id=user${i}&id=item${i}"
done

# Run the load test using wrk in Docker
echo "Running load test..."
docker-compose -f docker-compose.yml run --rm wrk -t8  -c500 -d30s -L "http://app:8080/checkout?user_id=bench&id=itembench"
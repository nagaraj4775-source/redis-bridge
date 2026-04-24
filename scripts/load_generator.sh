#!/bin/bash
# Runs as root (invoked via sudo bash), so no sudo needed inside
RA="embedded-bus-redis-a-master1-1"
RB="embedded-bus-redis-b-master1-1"
RC="embedded-bus-redis-c-master1-1"

write_batch() {
  local container=$1 host=$2 prefix=$3 count=$4
  local ts; ts=$(date +%s%3N)
  {
    for j in $(seq 1 "$count"); do
      echo "SET ${prefix}:${ts}:${j} val-${j}"
    done
  } | docker exec -i "$container" redis-cli -c -h "$host" --pipe > /dev/null 2>&1
}

echo "[$(date +%T)] Phase 1: Trickle — 5 keys/site every 100ms for 30s"
for i in $(seq 1 300); do
  write_batch $RA redis-a-master1 "load:trickle:a" 5
  write_batch $RB redis-b-master1 "load:trickle:b" 5
  write_batch $RC redis-c-master1 "load:trickle:c" 5
  sleep 0.1
done

echo "[$(date +%T)] Phase 2: Burst 5000 keys on site-a (creates lag spike)"
{ for i in $(seq 1 5000); do echo "SET load:burst:a:$i burst-$i"; done } \
  | docker exec -i $RA redis-cli -c -h redis-a-master1 --pipe > /dev/null 2>&1

sleep 5

echo "[$(date +%T)] Phase 3: Medium — 20 keys/site every 200ms for 60s"
for i in $(seq 1 300); do
  write_batch $RA redis-a-master1 "load:med:a" 20
  write_batch $RB redis-b-master1 "load:med:b" 20
  sleep 0.2
done

echo "[$(date +%T)] Phase 4: Burst 5000 keys on site-b (second lag spike)"
{ for i in $(seq 1 5000); do echo "SET load:burst:b:$i burst-$i"; done } \
  | docker exec -i $RB redis-cli -c -h redis-b-master1 --pipe > /dev/null 2>&1

sleep 5

echo "[$(date +%T)] Phase 5: Continuous — 30 keys/site every 500ms (runs until killed)"
ROUND=0
while true; do
  ROUND=$((ROUND+1))
  write_batch $RA redis-a-master1 "load:cont:a:${ROUND}" 30
  write_batch $RB redis-b-master1 "load:cont:b:${ROUND}" 30
  write_batch $RC redis-c-master1 "load:cont:c:${ROUND}" 30
  if [ $((ROUND % 20)) -eq 0 ]; then
    A_LEN=$(docker exec $RA redis-cli -c -h redis-a-master1 XLEN repl:stream:ec-site-a 2>/dev/null)
    B_LEN=$(docker exec $RB redis-cli -c -h redis-b-master1 XLEN repl:stream:ec-site-b 2>/dev/null)
    echo "[$(date +%T)] Round $ROUND — streams: site-a=$A_LEN  site-b=$B_LEN"
  fi
  sleep 0.5
done

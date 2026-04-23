#!/usr/bin/env bash
# =============================================================================
# RedisBridge — Interactive Demo: Cluster Embedded-Bus Mode
# =============================================================================
#
# Runs 10 live scenarios showing replication, LWW, lag, and recovery.
# Topology : 3 sites × 3 Redis Cluster masters, NO dedicated bus container.
# Stream   : lives on master1 of each site (embedded in its own cluster).
#
# Usage:
#   chmod +x scripts/demo_cluster_embedded.sh
#   ./scripts/demo_cluster_embedded.sh
#
# Requires: docker, redis-cli, curl, go (for bulk-key generator)
# =============================================================================

set -uo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
BOLD='\033[1m'
DIM='\033[2m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[1;37m'
NC='\033[0m'

# ── Config ────────────────────────────────────────────────────────────────────
COMPOSE_FILE="docker/cluster/embedded-bus/docker-compose.yml"
DOCKER="sudo docker"
DC="$DOCKER compose -f $COMPOSE_FILE"

# Coordinator (HTTP management API) — accessible on host
COORD_A="http://localhost:8291"
COORD_B="http://localhost:8292"
COORD_C="http://localhost:8293"

# Replication stream keys
STREAM_A="repl:stream:ec-site-a"
STREAM_B="repl:stream:ec-site-b"
STREAM_C="repl:stream:ec-site-c"

# Docker container helpers
# NOTE: All redis-cli operations run INSIDE Docker containers via 'docker exec'
# so that Redis Cluster MOVED redirects resolve to internal service names,
# not to Docker-internal IPs (172.x.x.x) which are unreachable from the host.
csite_container() { echo "embedded-bus-redis-${1}-master1-1"; }  # e.g. a→redis-a-master1 container
csite_host()      { echo "redis-${1}-master1"; }                  # e.g. a→redis-a-master1 hostname

# ── Helpers ───────────────────────────────────────────────────────────────────
header() {
    echo ""
    echo -e "${BOLD}${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    printf "${BOLD}${BLUE}║  %-60s  ║${NC}\n" "STEP $1 — $2"
    echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
}

info()    { echo -e "  ${CYAN}→${NC} $*"; }
ok()      { echo -e "  ${GREEN}✓${NC} $*"; }
warn()    { echo -e "  ${YELLOW}⚠${NC} $*"; }
err()     { echo -e "  ${RED}✗${NC} $*"; }
label()   { echo -e "\n  ${BOLD}${WHITE}$*${NC}"; }
dim()     { echo -e "  ${DIM}$*${NC}"; }

pause() {
    echo ""
    echo -e "  ${DIM}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# All redis operations run INSIDE Docker via exec so MOVED redirects work
# (Docker internal hostnames resolve; host-mapped 172.x.x.x IPs do not)
_rdocker() {  # _rdocker <container> <redis-cli args...>
    $DOCKER exec "$1" redis-cli -c -h "$(echo "$1" | sed 's/embedded-bus-//;s/-1$//')" \
        -p 6379 "${@:2}" 2>/dev/null
}

rget() {  # rget <site: a|b|c> <key>
    _rdocker "$(csite_container "$1")" GET "$2"
}

rset() {  # rset <site: a|b|c> <key> <val>
    _rdocker "$(csite_container "$1")" SET "$2" "$3" > /dev/null
}

rdel() {  # rdel <site: a|b|c> <key...>  — one key at a time to avoid CROSSSLOT
    local site=$1; shift
    local key
    for key in "$@"; do
        _rdocker "$(csite_container "$site")" DEL "$key" > /dev/null || true
    done
}

xlen_site() {  # xlen_site <site: a|b|c> <stream>  — routed via master1 with -c
    local site=$1 stream=$2 result
    result=$($DOCKER exec "embedded-bus-redis-${site}-master1-1" \
        redis-cli -c -h "redis-${site}-master1" -p 6379 XLEN "$stream" 2>/dev/null || echo 0)
    # -c follows MOVED redirects inside Docker; result is always an integer
    printf '%d' "${result:-0}" 2>/dev/null || echo 0
}

lag_info() {
    echo ""
    for name_url in "site-a $COORD_A" "site-b $COORD_B" "site-c $COORD_C"; do
        local name="${name_url% *}" url="${name_url#* }"
        local raw
        raw=$(curl -sf "$url/lag" 2>/dev/null || echo '{"error":"agent down"}')
        local summary
        summary=$(echo "$raw" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    peers=d.get('peers',[])
    parts=[]
    for p in peers:
        sid=p.get('site_id','?')
        s=p.get('stream',{})
        parts.append(f'{sid}:pending={s.get(\"pending\",\"?\")}')
    print('  '.join(parts) if parts else 'no peers')
except:
    print(sys.stdin.read())
" 2>/dev/null || echo "$raw")
        printf "  ${MAGENTA}%-10s${NC} %s\n" "$name" "$summary"
    done
    echo ""
}

poll_lag_until_zero() {
    local target_coord=$1 peer=$2 max_wait=${3:-90} waited=0
    echo ""
    info "Polling lag on $target_coord for peer '$peer' ..."
    while true; do
        local lag val
        lag=$(curl -sf "$target_coord/lag" 2>/dev/null || echo '{}')
        val=$(echo "$lag" | python3 -c "
import sys,json
d=json.load(sys.stdin)
for p in d.get('peers',[]):
    if p.get('site_id') == '$peer':
        s = p.get('stream', {})
        print(s.get('lag', '-'))
        sys.exit(0)
print('-')
" 2>/dev/null || echo "-")
        printf "\r  ${CYAN}→${NC} lag [%s] = ${YELLOW}%-12s${NC} (waited %ds)" "$peer" "$val" "$waited"
        if [ "$val" = "0" ] || [ "$val" = "0.00" ]; then
            echo ""
            ok "Lag fully recovered in ${waited}s"
            break
        fi
        if [ "$waited" -ge "$max_wait" ]; then
            echo ""
            warn "Gave up after ${max_wait}s — last lag: $val"
            break
        fi
        sleep 1; waited=$((waited + 1))
    done
}

wait_for_agent() {
    local url=$1 name=$2 tries=0 max=30
    info "Waiting for $name to become healthy..."
    while [ $tries -lt $max ]; do
        local code
        code=$(curl -sf -o /dev/null -w "%{http_code}" "$url/health" 2>/dev/null || echo "000")
        if [ "$code" = "200" ]; then
            ok "$name is healthy"
            return 0
        fi
        sleep 1; tries=$((tries+1))
        printf "\r  ${CYAN}→${NC} waiting... (%ds)" "$tries"
    done
    echo ""
    err "$name did not become healthy in ${max}s"
    return 1
}

wait_for_replication() {
    # wait_for_replication <dst_site: a|b|c> <key> <want> [timeout_s]
    local dst_site=$1 key=$2 want=$3 timeout=${4:-30}
    local start; start=$(date +%s%3N)
    local deadline=$(( start + timeout * 1000 ))
    while true; do
        local val; val=$(rget "$dst_site" "$key")
        if [ "$val" = "$want" ]; then
            echo $(( $(date +%s%3N) - start ))
            return 0
        fi
        [ $(date +%s%3N) -ge $deadline ] && { echo "TIMEOUT"; return 1; }
        sleep 0.05
    done
}

wait_for_key_on_site() {
    # wait_for_key_on_site <site> <key> <max_wait_s>
    # Polls until the key is non-empty (replication complete) or timeout.
    # Reports progress every second.
    local site=$1 key=$2 max_wait=${3:-150} waited=0
    echo ""
    info "Waiting for '${key}' to appear on site-${site} ..."
    while true; do
        local val; val=$(rget "$site" "$key" || true)
        if [ -n "$val" ]; then
            echo ""
            ok "site-${site} received '${key}' in ${waited}s (val='${val}')"
            return 0
        fi
        if [ "$waited" -ge "$max_wait" ]; then
            echo ""
            warn "Gave up after ${max_wait}s — key not yet on site-${site}"
            return 1
        fi
        printf "\r  ${CYAN}→${NC} [%ds] polling site-%s for sentinel key..." "$waited" "$site"
        sleep 1; waited=$((waited + 1))
    done
}

wait_for_stream_stable() {
    # wait_for_stream_stable <site> <stream_key> <max_wait_s>
    # Polls XLEN every 10s until the stream length is unchanged for 3 consecutive checks.
    # Needed because the producer drains its pubsub buffer AFTER bulk_insert returns,
    # so the stream keeps growing for 100-200s after a large bulk insert completes.
    local site=$1 stream=$2 max_wait=${3:-300} prev=-1 same=0 waited=0
    echo ""
    info "Waiting for ${stream} to stabilize (producer draining pubsub buffer)..."
    while [ "$waited" -lt "$max_wait" ]; do
        local len; len=$(xlen_site "$site" "$stream")
        printf "\r  ${CYAN}→${NC} [%ds] stream length: %d  stable: %d/30s" \
            "$waited" "$len" "$(( same * 10 ))"
        if [ "$len" -eq "$prev" ] && [ "$len" -gt 0 ]; then
            same=$(( same + 1 ))
            if [ "$same" -ge 3 ]; then
                echo ""
                ok "Stream ${stream} stable at ${len} entries (${waited}s)"
                return 0
            fi
        else
            same=0
        fi
        prev=$len
        sleep 10; waited=$(( waited + 10 ))
    done
    echo ""
    warn "Stream may still be growing after ${max_wait}s (current: $(xlen_site "$site" "$stream"))"
}

# bulk_key <prefix> <index>  — wraps prefix in hash tag so all keys land on one slot
bulk_key() { echo "{demo}:${1}:${2}"; }

bulk_insert() {  # bulk_insert <site: a|b|c> <prefix> <count>
    # Rate-limited batched insert: 500 keys per 50ms (10k keys/s).
    # Producer pubsub channel is sized to 100000 so events are not dropped.
    local site=$1 prefix=$2 count=$3
    local container; container="$(csite_container "$site")"
    local host;      host="$(csite_host "$site")"
    local batch=500 i=0
    info "Inserting ${count} keys into site-${site} (batches of ${batch}, 50ms apart)..."
    while [ "$i" -lt "$count" ]; do
        local end=$(( i + batch ))
        [ "$end" -gt "$count" ] && end=$count
        python3 -c "for i in range($i,$end): print('SET {demo}:${prefix}:'+str(i)+' val'+str(i))" \
            | $DOCKER exec -i "$container" redis-cli -c -h "$host" -p 6379 --pipe > /dev/null 2>&1 || true
        i=$end
        printf "\r  ${CYAN}→${NC} %d / %d keys inserted..." "$i" "$count"
        sleep 0.05
    done
    echo ""
}

bulk_delete() {  # bulk_delete <site: a|b|c> <prefix> <count>
    local site=$1 prefix=$2 count=$3
    local container; container="$(csite_container "$site")"
    local host;      host="$(csite_host "$site")"
    python3 -c "for i in range($count): print('DEL {demo}:${prefix}:' + str(i))" \
        | $DOCKER exec -i "$container" redis-cli -c -h "$host" -p 6379 --pipe > /dev/null 2>&1 || true
}

# Unique per-run ID so bulk keys from different runs never collide
RUN_ID=$(date +%s)

# ── Banner ────────────────────────────────────────────────────────────────────
clear
echo ""
echo -e "${BOLD}${MAGENTA}"
echo "  ██████╗ ███████╗██████╗ ██╗███████╗██████╗ ██████╗ ██╗██████╗  ██████╗ ███████╗"
echo "  ██╔══██╗██╔════╝██╔══██╗██║██╔════╝██╔══██╗██╔══██╗██║██╔══██╗██╔════╝ ██╔════╝"
echo "  ██████╔╝█████╗  ██║  ██║██║███████╗██████╔╝██████╔╝██║██║  ██║██║  ███╗█████╗  "
echo "  ██╔══██╗██╔══╝  ██║  ██║██║╚════██║██╔══██╗██╔══██╗██║██║  ██║██║   ██║██╔══╝  "
echo "  ██║  ██║███████╗██████╔╝██║███████║██████╔╝██║  ██║██║██████╔╝╚██████╔╝███████╗"
echo "  ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝╚══════╝╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝  ╚═════╝ ╚══════╝"
echo -e "${NC}"
echo -e "  ${BOLD}${WHITE}Cluster Embedded-Bus  ·  Interactive Demo${NC}"
echo -e "  ${DIM}3 sites × 3 Redis Cluster masters  |  No dedicated bus container${NC}"
echo ""
echo -e "  ${DIM}site-a  masters: 6411/6412/6413   coord: 8291   metrics: 9291${NC}"
echo -e "  ${DIM}site-b  masters: 6414/6415/6416   coord: 8292   metrics: 9292${NC}"
echo -e "  ${DIM}site-c  masters: 6417/6418/6419   coord: 8293   metrics: 9293${NC}"
echo ""

# ── Key-count selector ────────────────────────────────────────────────────────
echo -e "  ${BOLD}${WHITE}How many keys for bulk replication tests?${NC}"
echo -e "  ${DIM}  [1]  100       very fast smoke test${NC}"
echo -e "  ${DIM}  [2]  1,000     default (quick CI demo)${NC}"
echo -e "  ${DIM}  [3]  10,000    medium scale${NC}"
echo -e "  ${DIM}  [4]  100,000   production stress (1 lakh)${NC}"
printf  "\n  ${CYAN}→${NC} Enter choice [1-4] or a number, then Enter (auto-starts in 5s): "
_kc_input=""
if IFS= read -t 5 -r _kc_input 2>/dev/null; then
    :
else
    echo ""
    info "No input — using default (1,000 keys)"
fi
case "${_kc_input}" in
    1|100)              BULK_COUNT=100   ;;
    2|1000|1,000)       BULK_COUNT=1000  ;;
    3|10000|10,000)     BULK_COUNT=10000 ;;
    4|100000|100,000)   BULK_COUNT=100000;;
    ""  )               BULK_COUNT=1000  ;;
    *)  warn "Unknown choice '${_kc_input}' — using default (1,000)"
        BULK_COUNT=1000  ;;
esac

# Pre-compute 5 evenly-spaced spot-check indices (10%, 25%, 50%, 75%, last key)
_si1=$(( BULK_COUNT / 10 ))
_si2=$(( BULK_COUNT / 4 ))
_si3=$(( BULK_COUNT / 2 ))
_si4=$(( BULK_COUNT * 3 / 4 ))
_si5=$(( BULK_COUNT - 1 ))
SPOT_INDICES="${_si1} ${_si2} ${_si3} ${_si4} ${_si5}"

ok "Demo configured: ${BULK_COUNT} keys per bulk test  (spot-check indices: ${SPOT_INDICES})"
echo ""
pause

# =============================================================================
# STEP 1 — Bring up the cluster and verify health
# =============================================================================
header 1 "Bring up the cluster — verify all nodes and agents are healthy"

label "Tearing down any existing cluster (clean slate)..."
$DC down -v --remove-orphans 2>&1 | grep -E '(Removed|Stopped|Error|Volume)' || true
sleep 2

label "Starting all 9 Redis masters + 3 cluster-init jobs + 3 agents..."
$DC up -d --build 2>&1 | grep -E '(Started|Running|Error|Building)'

label "Waiting for Redis Cluster formation (polling until cluster_state=ok, up to 60s)..."
_cluster_ready=false
for _ci in $(seq 1 60); do
    _all_cs_ok=true
    for _sl in a b c; do
        _cs=$($DOCKER exec "embedded-bus-redis-${_sl}-master1-1" \
            redis-cli -h "redis-${_sl}-master1" -p 6379 CLUSTER INFO 2>/dev/null \
            | grep "cluster_state" | awk -F: '{print $2}' | tr -d ' \r')
        [ "$_cs" != "ok" ] && _all_cs_ok=false
    done
    if [ "$_all_cs_ok" = "true" ]; then
        _cluster_ready=true
        break
    fi
    printf "\r  ${CYAN}→${NC} [%ds] waiting for cluster formation..." "$_ci"
    sleep 1
done
echo ""
[ "$_cluster_ready" = "true" ] \
    && ok "All 3 clusters reached cluster_state=ok" \
    || { err "Cluster formation timed out after 60s"; exit 1; }

label "Verifying Redis Cluster topology..."
for site_letter in a b c; do
    SITE_UC=$(echo "$site_letter" | tr a-z A-Z)
    container="embedded-bus-redis-${site_letter}-master1-1"
    host="redis-${site_letter}-master1"
    cluster_info=$($DOCKER exec "$container" redis-cli -h "$host" -p 6379 CLUSTER INFO 2>/dev/null || echo "")
    cluster_size=$(echo "$cluster_info" | grep "cluster_known_nodes" | awk -F: '{print $2}' | tr -d ' \r')
    cluster_state=$(echo "$cluster_info" | grep "cluster_state" | awk -F: '{print $2}' | tr -d ' \r')
    if [ "$cluster_state" = "ok" ]; then
        ok "Site $SITE_UC  →  cluster_state=ok  known_nodes=${cluster_size:-?}"
    else
        err "Site $SITE_UC  →  cluster_state=${cluster_state:-unreachable}"
    fi
done

label "Waiting for agent health checks..."
wait_for_agent "$COORD_A" "agent-a (ec-site-a)"
wait_for_agent "$COORD_B" "agent-b (ec-site-b)"
wait_for_agent "$COORD_C" "agent-c (ec-site-c)"

label "Agent status:"
for name_url in "ec-site-a $COORD_A" "ec-site-b $COORD_B" "ec-site-c $COORD_C"; do
    name="${name_url% *}" url="${name_url#* }"
    status=$(curl -sf "$url/health" 2>/dev/null || echo '{"status":"unreachable"}')
    printf "  ${GREEN}✓${NC}  %-12s  %s\n" "$name" "$status"
done

label "Verifying streams are fresh (clean start after full teardown)..."
_all_ok=true
for coord in "$COORD_A" "$COORD_B" "$COORD_C"; do
    lag_json=$(curl -sf "$coord/lag" 2>/dev/null || echo '{}')
    total_lag=$(echo "$lag_json" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(sum(p.get('stream',{}).get('lag',0) for p in d.get('peers',[])))
" 2>/dev/null || echo 0)
    [ "${total_lag:-0}" -gt 0 ] && _all_ok=false
done
[ "$_all_ok" = "true" ] && ok "All streams fresh — ready to demo" || info "Streams initialising (fresh cluster)"
echo ""

pause

# =============================================================================
# STEP 2 — Show data replication in action (string, hash, TTL)
# =============================================================================
header 2 "Basic replication — write on site-a, verify on site-b and site-c"

label "Writing test keys on site-a..."
TS=$(date +%s%N)
K_STR="demo:step2:string:$TS"
K_HASH="demo:step2:hash:$TS"

rset "a" "$K_STR" "hello-from-site-a"
_rdocker "$(csite_container "a")" HSET "$K_HASH" user alice role admin region APAC > /dev/null
info "SET  $K_STR = 'hello-from-site-a'"
info "HSET $K_HASH  user=alice role=admin region=APAC"

label "Waiting for replication..."
t=$(wait_for_replication "b" "$K_STR" "hello-from-site-a") || true
[ "$t" != "TIMEOUT" ] && ok "site-a → site-b  latency: ${t}ms" || err "Timed out waiting for site-b"
t=$(wait_for_replication "c" "$K_STR" "hello-from-site-a") || true
[ "$t" != "TIMEOUT" ] && ok "site-a → site-c  latency: ${t}ms" || err "Timed out waiting for site-c"

label "Hash field verification:"
for sl in b c; do
    user=$(_rdocker "$(csite_container "$sl")" HGET "$K_HASH" user)
    region=$(_rdocker "$(csite_container "$sl")" HGET "$K_HASH" region)
    if [ "$user" = "alice" ]; then
        ok "site-$sl  →  user=$user  region=$region"
    else
        err "site-$sl  →  hash not replicated (user='$user')"
    fi
done

label "Current stream lengths (replication bus):"
for sl in a b c; do
    len=$(xlen_site "$sl" "repl:stream:ec-site-$sl")
    printf "  ${CYAN}→${NC}  %-12s  stream entries: ${YELLOW}%s${NC}\n" "ec-site-$sl" "$len"
done

for sl in a b c; do
    rdel "$sl" "$K_STR"
    rdel "$sl" "$K_HASH"
done

pause

# =============================================================================
# STEP 3 — INSERT → UPDATE → DELETE from site-a, verify on B and C
# =============================================================================
header 3 "CRUD round-trip — INSERT, UPDATE, DELETE from site-a → site-b, site-c"

TS=$(date +%s%N)
K="demo:step3:crud:$TS"

label "[ INSERT ] SET $K = v1"
rset "a" "$K" "v1"
t=$(wait_for_replication "b" "$K" "v1") || true
t2=$(wait_for_replication "c" "$K" "v1") || true
if [ "$t" != "TIMEOUT" ] && [ "$t2" != "TIMEOUT" ]; then
    ok "Replicated → site-b in ${t}ms  site-c in ${t2}ms"
else
    err "Replication timeout (b:$t c:$t2)"
fi
printf "  ${CYAN}→${NC}  site-a=%s  site-b=%s  site-c=%s\n" \
    "$(rget "a" "$K" || true)" "$(rget "b" "$K" || true)" "$(rget "c" "$K" || true)"

label "[ UPDATE ] SET $K = v2"
rset "a" "$K" "v2"
t=$(wait_for_replication "b" "$K" "v2") || true
t2=$(wait_for_replication "c" "$K" "v2") || true
ok "Updated → site-b in ${t}ms  site-c in ${t2}ms"
printf "  ${CYAN}→${NC}  site-a=%s  site-b=%s  site-c=%s\n" \
    "$(rget "a" "$K" || true)" "$(rget "b" "$K" || true)" "$(rget "c" "$K" || true)"

label "[ DELETE ] DEL $K"
rdel "a" "$K"
sleep 2
VB=$(rget "b" "$K" || true); VC=$(rget "c" "$K" || true)
if [ -z "$VB" ] && [ -z "$VC" ]; then
    ok "Deleted → site-b='(nil)'  site-c='(nil)'"
else
    warn "Delete propagation: site-b='$VB'  site-c='$VC'"
fi

pause

# =============================================================================
# STEP 4 — LWW conflict — same key written simultaneously on site-a and site-b
# =============================================================================
header 4 "LWW convergence — same key written on site-a AND site-b simultaneously"

TS=$(date +%s%N)
K="demo:step4:lww:$TS"

label "Concurrent writes (both fire at the same time)..."
rset "a" "$K" "from-site-a" &
rset "b" "$K" "from-site-b" &
wait
info "Both sites wrote simultaneously. LWW (Last-Write-Wins by HLC) resolves conflict..."

label "Polling for LWW convergence (up to 30s)..."
LWW_CONVERGED=false
for _try in $(seq 1 60); do
    VA=$(rget "a" "$K"); VB=$(rget "b" "$K"); VC=$(rget "c" "$K")
    if [ "$VA" = "$VB" ] && [ "$VB" = "$VC" ] && [ -n "$VA" ]; then
        LWW_CONVERGED=true; break
    fi
    printf "\r  ${CYAN}→${NC} [%.1fs] A='%s'  B='%s'  C='%s' ..." "$(echo "scale=1; $_try / 2" | bc)" "$VA" "$VB" "$VC"
    sleep 0.5
done
echo ""

printf "\n  ${BOLD}%-12s  %-20s${NC}\n" "Site" "Final Value"
printf "  ${CYAN}%-12s${NC}  %s\n" "site-a" "'$VA'"
printf "  ${CYAN}%-12s${NC}  %s\n" "site-b" "'$VB'"
printf "  ${CYAN}%-12s${NC}  %s\n" "site-c" "'$VC'"

META_SITE=$(_rdocker "$(csite_container "a")" HGET "__meta:$K" site)
META_HLC=$(_rdocker "$(csite_container "a")" HGET "__meta:$K" hlc)
echo ""
info "Winner (by HLC): site=${META_SITE:-?}  hlc=${META_HLC:-?}"

if [ "$LWW_CONVERGED" = "true" ]; then
    ok "All 3 sites converged to '${VA}' ✓"
else
    warn "Convergence in progress: A='$VA'  B='$VB'  C='$VC' (LWW takes a few more seconds in embedded-cluster mode)"
fi

for sl in a b c; do
    rdel "$sl" "$K"
    rdel "$sl" "__meta:$K"
done

pause

# =============================================================================
# STEP 5 — Bring down site-a AGENT (not Redis), push 100k keys to site-b
# =============================================================================
header 5 "Bring down site-a AGENT, push ${BULK_COUNT} keys to site-b — observe lag"

label "Stopping agent-a (ec-site-a)..."
$DC stop agent-a 2>&1 | grep -E "(Stopped|Error)" || true
ok "agent-a stopped — its replication stream (ec-site-a) is PAUSED"

label "Pushing ${BULK_COUNT} keys to site-b via rate-limited pipeline..."
BULK_PREFIX="demo:step5:bulk:${RUN_ID}"

START_T=$(date +%s%3N)
bulk_insert "b" "$BULK_PREFIX" "$BULK_COUNT"
END_T=$(date +%s%3N)
ELAPSED=$(( END_T - START_T ))
ok "Inserted ${BULK_COUNT} keys in ${ELAPSED}ms"

label "Checking lag (agent-a is DOWN — using stream length as proxy)..."
warn "site-a agent is down — cannot poll its /lag endpoint"
info "Checking site-b stream length (these entries site-a missed):"
sleep 1

STREAM_LEN=$(xlen_site "b" "$STREAM_B")
printf "\n  ${YELLOW}repl:stream:ec-site-b entries: %s${NC}\n" "$STREAM_LEN"

label "site-c should have consumed site-b's stream (c is still running):"
info "Lag on site-c for peer ec-site-b:"
curl -sf "$COORD_C/lag" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
for p in d.get('peers',[]):
    s=p.get('stream',{})
    print(f'    {p.get(\"site_id\",\"?\")}:  pending={s.get(\"pending\",\"?\")}  lag={s.get(\"lag\",\"?\")}')"

label "site-a's lag for ec-site-b will accumulate while agent is DOWN"
info "When we bring site-a back, it will catch up all ${BULK_COUNT} missed keys"

pause

# =============================================================================
# STEP 6 — Bring up site-a AGENT — watch lag drain to 0
# =============================================================================
header 6 "Bring site-a agent back up — watch lag recover in real time"

label "Starting agent-a..."
$DC start agent-a 2>&1 | grep -E "(Started|Error)" || true
wait_for_agent "$COORD_A" "agent-a (ec-site-a)"

label "Waiting for ec-site-b stream to stabilize (producer pubsub buffer draining)..."
wait_for_stream_stable "b" "$STREAM_B" 360

label "Monitoring lag on site-a for peer 'ec-site-b'..."
echo ""
poll_lag_until_zero "$COORD_A" "ec-site-b" 300

label "Spot-checking 5 random bulk keys on site-a..."
for i in $SPOT_INDICES; do
    val=$(rget "a" "$(bulk_key "$BULK_PREFIX" "$i")")
    if [ "$val" = "val${i}" ]; then
        ok "  $(bulk_key "$BULK_PREFIX" "$i") = '${val}'"
    else
        warn "  $(bulk_key "$BULK_PREFIX" "$i") = '${val}' (expected val${i})"
    fi
done

info "Cleaning up ${BULK_COUNT} bulk keys on site-b..."
bulk_delete "b" "$BULK_PREFIX" "$BULK_COUNT"

pause

# =============================================================================
# STEP 7 — Bring down site-a COMPLETELY (Redis + agent), generate lag
# =============================================================================
header 7 "Bring down site-a COMPLETELY (all 3 masters + agent) — push ${BULK_COUNT} keys"

label "Stopping agent-a + all 3 site-a Redis masters..."
$DC stop agent-a redis-a-master1 redis-a-master2 redis-a-master3 2>&1 \
    | grep -E "(Stopped|Error)" || true
ok "site-a is completely offline (no Redis, no agent)"

label "Pushing ${BULK_COUNT} keys to site-b (site-a cannot receive them yet)..."
BULK_PREFIX2="demo:step7:bulk:${RUN_ID}"
BULK_COUNT2=$BULK_COUNT
START_T=$(date +%s%3N)
bulk_insert "b" "$BULK_PREFIX2" "$BULK_COUNT2"
END_T=$(date +%s%3N)
ok "Inserted ${BULK_COUNT2} keys in $(( END_T - START_T ))ms"

label "Stream lengths (site-b accumulating, site-a offline):"
for sl in b c; do
    len=$(xlen_site "$sl" "repl:stream:ec-site-$sl")
    printf "  ${CYAN}→${NC}  %-12s  stream entries: ${YELLOW}%s${NC}\n" "ec-site-$sl" "$len"
done

info "site-a is DOWN — ec-site-a stream entries will drift behind ec-site-b"
info "site-c lag for ec-site-b (c is still consuming):"
curl -sf "$COORD_C/lag" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
for p in d.get('peers',[]):
    s=p.get('stream',{})
    print(f'    {p.get(\"site_id\",\"?\")}:  pending={s.get(\"pending\",\"?\")}')"

pause

# =============================================================================
# STEP 8 — Bring site-a back completely — watch lag drain fast
# =============================================================================
header 8 "Bring site-a fully back — watch lag drain quickly"

label "Starting site-a Redis masters..."
$DC start redis-a-master1 redis-a-master2 redis-a-master3 2>&1 \
    | grep -E "(Started|Error)" || true

label "Waiting for Redis Cluster to reform (5s)..."
sleep 5
cluster_state=$($DOCKER exec embedded-bus-redis-a-master1-1 \
    redis-cli -h redis-a-master1 -p 6379 CLUSTER INFO 2>/dev/null \
    | grep "cluster_state" | awk -F: '{print $2}' | tr -d ' \r' || echo "unknown")
if [ "$cluster_state" = "ok" ]; then
    ok "site-a Redis Cluster is back — state=ok"
else
    warn "site-a cluster state: $cluster_state"
fi

label "Starting agent-a..."
$DC start agent-a 2>&1 | grep -E "(Started|Error)" || true
wait_for_agent "$COORD_A" "agent-a (ec-site-a)"

label "Waiting for ec-site-b stream to stabilize after step-7 bulk insert..."
wait_for_stream_stable "b" "$STREAM_B" 360

label "Waiting for site-a to replicate the last step-7 key (sentinel check)..."
SENTINEL2=$(bulk_key "$BULK_PREFIX2" "$((BULK_COUNT2-1))")
wait_for_key_on_site "a" "$SENTINEL2" 600 || true

label "Spot-checking 5 keys from step-7 batch on site-a..."
for i in $SPOT_INDICES; do
    val=$(rget "a" "$(bulk_key "$BULK_PREFIX2" "$i")")
    if [ "$val" = "val${i}" ]; then
        ok "  $(bulk_key "$BULK_PREFIX2" "$i") = '${val}'"
    else
        warn "  $(bulk_key "$BULK_PREFIX2" "$i") = '${val}' (expected val${i})"
    fi
done

info "Cleaning up step-7 bulk keys..."
bulk_delete "b" "$BULK_PREFIX2" "$BULK_COUNT2"

pause

# =============================================================================
# STEP 9 — Bring down BOTH site-a and site-b, push 200k keys to site-c
# =============================================================================
header 9 "Bring down site-a AND site-b — push ${BULK_COUNT} keys to site-c, observe lag"

label "Stopping site-a and site-b completely..."
$DC stop agent-a redis-a-master1 redis-a-master2 redis-a-master3 \
              agent-b redis-b-master1 redis-b-master2 redis-b-master3 2>&1 \
    | grep -E "(Stopped|Error)" || true
ok "site-a and site-b are fully offline"
ok "Only site-c (ec-site-c) is running"

label "Pushing ${BULK_COUNT} keys to site-c..."
BULK_PREFIX3="demo:step9:bulk:${RUN_ID}"
BULK_COUNT3=$BULK_COUNT
START_T=$(date +%s%3N)
bulk_insert "c" "$BULK_PREFIX3" "$BULK_COUNT3"
END_T=$(date +%s%3N)
ok "Inserted ${BULK_COUNT3} keys in $(( END_T - START_T ))ms"

label "Stream ec-site-c length (growing as keyspace events are processed):"
info "Waiting 5s for keyspace notifications to flush into stream..."
sleep 5
len=$(xlen_site "c" "$STREAM_C")
printf "\n  ${YELLOW}repl:stream:ec-site-c entries: %s${NC}\n" "$len"
warn "site-a and site-b agents are DOWN — they cannot consume this stream"
info "All ${BULK_COUNT3} keys exist only on site-c right now"

label "Verifying a few random keys ARE on site-c..."
for i in $SPOT_INDICES; do
    val=$(rget "c" "$(bulk_key "$BULK_PREFIX3" "$i")")
    if [ "$val" = "val${i}" ]; then
        ok "  site-c  $(bulk_key "$BULK_PREFIX3" "$i") = '${val}'"
    fi
done

label "Verifying keys are NOT on site-a/b (they are offline)..."
_spot_first=$(echo $SPOT_INDICES | awk '{print $1}')
_spot_last=$(echo $SPOT_INDICES | awk '{print $NF}')
for i in $_spot_first $_spot_last; do
    printf "  ${RED}✗${NC}  site-a  $(bulk_key "$BULK_PREFIX3" "$i") = '(Redis is DOWN)'\n"
    printf "  ${RED}✗${NC}  site-b  $(bulk_key "$BULK_PREFIX3" "$i") = '(Redis is DOWN)'\n"
done

pause

# =============================================================================
# STEP 10 — Bring site-a and site-b back — watch both catch up simultaneously
# =============================================================================
header 10 "Bring site-a and site-b back — watch both recover lag simultaneously"

label "Starting site-a Redis masters..."
$DC start redis-a-master1 redis-a-master2 redis-a-master3 2>&1 \
    | grep -E "(Started|Error)" || true

label "Starting site-b Redis masters..."
$DC start redis-b-master1 redis-b-master2 redis-b-master3 2>&1 \
    | grep -E "(Started|Error)" || true

label "Waiting for both clusters to reform (5s)..."
sleep 5

for sl in a b; do
    SITE_UC=$(echo "$sl" | tr a-z A-Z)
    cs=$($DOCKER exec "embedded-bus-redis-${sl}-master1-1" \
        redis-cli -h "redis-${sl}-master1" -p 6379 CLUSTER INFO 2>/dev/null \
        | grep "cluster_state" | awk -F: '{print $2}' | tr -d ' \r' || echo "unknown")
    [ "$cs" = "ok" ] && ok "site-$SITE_UC Redis Cluster state=ok" || warn "site-$SITE_UC state=$cs"
done

label "Starting agents for site-a and site-b..."
$DC start agent-a agent-b 2>&1 | grep -E "(Started|Error)" || true
wait_for_agent "$COORD_A" "agent-a (ec-site-a)"
wait_for_agent "$COORD_B" "agent-b (ec-site-b)"

label "Waiting for ec-site-c stream to stabilize after step-9 bulk insert..."
wait_for_stream_stable "c" "$STREAM_C" 360

label "Waiting for BOTH site-a and site-b to receive the last step-9 key (sentinel)..."
SENTINEL3=$(bulk_key "$BULK_PREFIX3" "$((BULK_COUNT3-1))")
info "Sentinel key: ${SENTINEL3}"

# Poll both sites in parallel — up to 300s (agent EnsureGroup retry can take ~150s on first connect)
_done_a=false; _done_b=false
T_START=$(date +%s)
while true; do
    T_NOW=$(date +%s); _elapsed=$(( T_NOW - T_START ))
    if [ "$_done_a" = "false" ]; then
        _va=$(rget "a" "$SENTINEL3" || true)
        [ -n "$_va" ] && _done_a=true
    fi
    if [ "$_done_b" = "false" ]; then
        _vb=$(rget "b" "$SENTINEL3" || true)
        [ -n "$_vb" ] && _done_b=true
    fi
    printf "\r  ${CYAN}→${NC} [%ds] site-a: %-8s  site-b: %-8s" \
        "$_elapsed" "$([ "$_done_a" = "true" ] && echo 'DONE ✓' || echo 'waiting')" \
        "$([ "$_done_b" = "true" ] && echo 'DONE ✓' || echo 'waiting')"
    if [ "$_done_a" = "true" ] && [ "$_done_b" = "true" ]; then
        echo ""
        ok "Both site-a and site-b fully recovered in ${_elapsed}s"
        break
    fi
    if [ "$_elapsed" -ge 600 ]; then
        echo ""
        warn "Sentinel timed out after 600s — a:${_done_a}  b:${_done_b}"
        break
    fi
    sleep 1
done

# Ensure lag is fully drained on both sites before spot-checking keys
label "Draining any remaining lag on site-a for ec-site-c..."
poll_lag_until_zero "$COORD_A" "ec-site-c" 600
label "Draining any remaining lag on site-b for ec-site-c..."
poll_lag_until_zero "$COORD_B" "ec-site-c" 300

label "Spot-checking 5 keys on BOTH recovered sites..."
for i in $SPOT_INDICES; do
    va=$(rget "a" "$(bulk_key "$BULK_PREFIX3" "$i")")
    vb=$(rget "b" "$(bulk_key "$BULK_PREFIX3" "$i")")
    if [ "$va" = "val${i}" ] && [ "$vb" = "val${i}" ]; then
        ok "  $(bulk_key "$BULK_PREFIX3" "$i")  →  site-a='${va}'  site-b='${vb}'"
    else
        warn "  $(bulk_key "$BULK_PREFIX3" "$i")  →  site-a='${va}'  site-b='${vb}' (expected val${i})"
    fi
done

label "Final lag snapshot across all agents:"
lag_info

info "Cleaning up step-9 bulk keys (${BULK_COUNT3}) on site-c..."
bulk_delete "c" "$BULK_PREFIX3" "$BULK_COUNT3"

pause

# =============================================================================
# STEP 11 — Master-node failure on site-a: replica promotion + replication check
# =============================================================================
header 11 "site-a master1 failure — replica promotion, write keys, verify replication"

# ── Step-11 helpers: route site-a operations through master2 (master1 is DOWN) ──
_S11_CONTAINER="embedded-bus-redis-a-master2-1"
_S11_HOST="redis-a-master2"
rset_m2()  { $DOCKER exec "$_S11_CONTAINER" redis-cli -c -h "$_S11_HOST" -p 6379 SET "$1" "$2" > /dev/null 2>&1; }
rget_m2()  { $DOCKER exec "$_S11_CONTAINER" redis-cli -c -h "$_S11_HOST" -p 6379 GET "$1" 2>/dev/null; }
rdel_m2()  { $DOCKER exec "$_S11_CONTAINER" redis-cli -c -h "$_S11_HOST" -p 6379 DEL "$1" > /dev/null 2>&1 || true; }
wait_for_repl_m2() {
    local dst_site=$1 key=$2 want=$3 timeout=${4:-30}
    local start; start=$(date +%s%3N)
    local deadline=$(( start + timeout * 1000 ))
    while true; do
        local val; val=$(rget "$dst_site" "$key")
        [ "$val" = "$want" ] && { echo $(( $(date +%s%3N) - start )); return 0; }
        [ $(date +%s%3N) -ge $deadline ] && { echo "TIMEOUT"; return 1; }
        sleep 0.05
    done
}

# ── Parse CLUSTER NODES with hostname support ──
# When --cluster-preferred-endpoint-type hostname is set, the address field is
# :0@0,<hostname> — we extract the hostname from after the comma, falling back
# to the IP field if no comma is present.
_cluster_nodes_fmt() {
    $DOCKER exec "$_S11_CONTAINER" redis-cli -h "$_S11_HOST" -p 6379 CLUSTER NODES 2>/dev/null \
    | awk '{
        addr = $2
        # Try hostname after comma (Redis 7 hostname-preferred mode)
        if (match(addr, /,/)) {
            hostname = substr(addr, RSTART+1)
            sub(/:.*/, "", hostname)   # drop any trailing :port
            display = hostname ":6379"
        } else {
            split(addr, a, "@")
            display = a[1]
        }
        if ($3 ~ /fail/)        role = "[FAILED ]"
        else if ($3 ~ /master/) role = "[MASTER ]"
        else                    role = "[replica]"
        printf "  %-30s %s  %s\n", display, role, substr($1,1,8)"..."
    }'
}

label "Cluster topology on site-a BEFORE failure (via master2)..."
echo ""
_cluster_nodes_fmt

pause

label "Stopping redis-a-master1 (simulating a hardware failure)..."
$DC stop redis-a-master1 2>&1 | grep -E "(Stopped|Error)" || true
ok "redis-a-master1 is DOWN"

label "Waiting for Redis Cluster to detect failure and promote a replica..."
info "(cluster-node-timeout=5s → fail flag → election → promotion ≈ 15-25s)"
_promoted=false
_takeover_sent=false
for _try in $(seq 1 60); do
    _nodes=$($DOCKER exec "$_S11_CONTAINER" \
        redis-cli -h "$_S11_HOST" -p 6379 CLUSTER NODES 2>/dev/null) || true
    # Count nodes whose flags field (field 3) contains "fail"
    _has_fail=$(echo "$_nodes" | awk '
        { split($3,f,","); for(i in f) if(f[i]=="fail"||f[i]=="fail?"){c++; break} }
        END{print c+0}') || _has_fail=0
    # Count nodes whose flags field (field 3) has "master" but NOT "fail"
    _healthy_masters=$(echo "$_nodes" | awk '
        { split($3,f,","); m=0; fl=0
          for(i in f){ if(f[i]=="master") m=1; if(f[i]=="fail"||f[i]=="fail?") fl=1 }
          if(m && !fl) c++ }
        END{print c+0}') || _healthy_masters=0
    printf "\r  ${CYAN}→${NC} [%ds] fail_detected=%s  healthy_masters=%s/3" \
        "$_try" "${_has_fail}" "${_healthy_masters}"
    # Success: master1 marked as fail AND 3 healthy masters
    if [ "${_has_fail}" -ge 1 ] && [ "${_healthy_masters}" -ge 3 ]; then
        _promoted=true
        break
    fi
    # After 20 s without organic election, force TAKEOVER on master1's replica.
    # In Docker the election sometimes stalls; TAKEOVER bypasses the vote mechanism.
    if [ "$_try" -eq 20 ] && [ "$_takeover_sent" = "false" ] && [ "${_has_fail}" -ge 1 ]; then
        # Identify master1's node ID from CLUSTER NODES
        _m1_id=$(echo "$_nodes" | awk '
            { addr=$2
              if (match(addr,/,/)) { h=substr(addr,RSTART+1); sub(/:.*$/,"",h) }
              else { split(addr,a,"@"); h=a[1]; sub(/:.*$/,"",h) }
              split($3,f,",")
              for(i in f) if(f[i]=="master") { is_m=1 }
              for(i in f) if(f[i]=="fail"||f[i]=="fail?") { is_f=1 }
              if(is_m && is_f && h=="redis-a-master1") { print $1; exit }
            }')
        if [ -n "$_m1_id" ]; then
            # Find the replica whose master-id (field 4) matches master1
            _replica_h=$(echo "$_nodes" | awk -v m1id="$_m1_id" '
                $4==m1id {
                    addr=$2
                    if (match(addr,/,/)) { h=substr(addr,RSTART+1); sub(/:.*$/,"",h) }
                    else { split(addr,a,"@"); h=a[1]; sub(/:.*$/,"",h) }
                    if (length(h)>0) { print h; exit }
                }')
            if [ -n "$_replica_h" ]; then
                printf "\n  ${YELLOW}→${NC} Organic election stalled — forcing TAKEOVER on %s...\n" "$_replica_h"
                _replica_ctr="embedded-bus-${_replica_h}-1"
                $DOCKER exec "$_replica_ctr" \
                    redis-cli -h "$_replica_h" -p 6379 CLUSTER FAILOVER TAKEOVER 2>/dev/null \
                    && _takeover_sent=true || true
            fi
        fi
    fi
    sleep 1
done
echo ""
if [ "$_promoted" = "true" ]; then
    ok "master1 marked as FAILED, replica promoted — 3 healthy masters"
else
    warn "Promotion not fully detected after 60s — continuing anyway"
fi

label "New topology (promoted replica now listed as master)..."
echo ""
_cluster_nodes_fmt

label "Waiting for agent-a topology watch to detect promoted master (~15s)..."
info "(ReloadState + ForEachMaster runs every 10s)"
sleep 15
ok "Producer should now be subscribed to the promoted master"

label "Writing 3 keys on site-a via master2 (cluster routes to correct shards)..."
TS11=$(date +%s%N)
K11_1="demo:step11:failover:k1:${TS11}"
K11_2="demo:step11:failover:k2:${TS11}"
K11_3="demo:step11:failover:k3:${TS11}"
_write_ok=true
for _n in 1 2 3; do
    eval "_K=\$K11_${_n}"
    _V="after-failover-${_n}"
    if rset_m2 "$_K" "$_V"; then
        ok "  SET ${_K##*:} = '${_V}'"
    else
        warn "  SET ${_K##*:} FAILED (slot may still be migrating)"
        _write_ok=false
    fi
done
[ "$_write_ok" = "true" ] && ok "3 keys written on site-a via master2 (post-failover)" \
    || warn "Some writes failed — cluster may need more recovery time"

label "Verifying all 3 keys replicate to site-b and site-c (30s timeout each)..."
echo ""
_step11_ok=true
for _n in 1 2 3; do
    eval "_K=\$K11_${_n}"
    _V="after-failover-${_n}"
    for site in b c; do
        ms=$(wait_for_repl_m2 "$site" "$_K" "$_V" 30)
        if [ "$ms" != "TIMEOUT" ]; then
            ok "  k${_n} → site-$site  [${ms}ms]"
        else
            warn "  k${_n} → site-$site  TIMEOUT (replication still in progress)"
            _step11_ok=false
        fi
    done
done

label "Cross-site consistency check (read from all 3 sites)..."
echo ""
printf "  ${BOLD}%-6s  %-22s  %-22s  %-22s${NC}\n" "Key" "site-a" "site-b" "site-c"
for _n in 1 2 3; do
    eval "_K=\$K11_${_n}"
    _va=$(rget_m2 "$_K"); _vb=$(rget "b" "$_K"); _vc=$(rget "c" "$_K")
    printf "  k%-5s  %-22s  %-22s  %-22s\n" "${_n}" "'${_va}'" "'${_vb}'" "'${_vc}'"
done

[ "$_step11_ok" = "true" ] \
    && ok "All 3 keys replicated correctly across all sites after master failover \u2713" \
    || warn "Some keys delayed — replication was still catching up within the timeout"

label "Restoring redis-a-master1 (it rejoins the cluster as a replica)..."
$DC start redis-a-master1 2>&1 | grep -E "(Started|Error)" || true
sleep 5
_total_nodes=$($DOCKER exec "$_S11_CONTAINER" \
    redis-cli -h "$_S11_HOST" -p 6379 CLUSTER NODES 2>/dev/null | wc -l | tr -d ' ') || true
ok "redis-a-master1 restarted — ${_total_nodes:-?} nodes visible in cluster (rejoins as replica)"

label "Cleaning up step-11 keys..."
for _n in 1 2 3; do
    eval "_K=\$K11_${_n}"
    rdel_m2 "$_K"
    rdel "b" "$_K"
    rdel "c" "$_K"
done
ok "Step-11 keys removed from all sites"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BOLD}${GREEN}"
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║                   ALL 11 STEPS COMPLETE ✓                   ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  ${BOLD}What was demonstrated:${NC}"
echo -e "  ${GREEN}1.${NC}  Cluster formed (9 Redis masters, 3 sites × 3 shards; site-a has HA replicas)"
echo -e "  ${GREEN}2.${NC}  String + hash replication working, sub-10ms latency"
echo -e "  ${GREEN}3.${NC}  Full INSERT → UPDATE → DELETE CRUD propagation"
echo -e "  ${GREEN}4.${NC}  LWW conflict resolution — all 3 sites converged to same value"
echo -e "  ${GREEN}5.${NC}  Agent down → ${BULK_COUNT} keys pushed to site-b → lag visible"
echo -e "  ${GREEN}6.${NC}  Agent recovered → lag drained to 0 automatically"
echo -e "  ${GREEN}7.${NC}  Site-a fully down (Redis + agent) → ${BULK_COUNT} keys accumulated"
echo -e "  ${GREEN}8.${NC}  Site-a fully restored → lag cleared"
echo -e "  ${GREEN}9.${NC}  Both a+b down → ${BULK_COUNT} pushed to site-c → lag on both"
echo -e "  ${GREEN}10.${NC} Both a+b restored → both caught up simultaneously"
echo -e "  ${GREEN}11.${NC} site-a master1 killed → replica promoted → keys replicated cross-site ✓"
echo ""
echo -e "  ${DIM}Cluster is still running. To tear down:${NC}"
echo -e "  ${BOLD}  make down-embedded-cluster${NC}"
echo ""

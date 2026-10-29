#!/usr/bin/env bash
# Shared helpers for demo / chaos / load scripts.  Requires: bash, curl, docker (with the compose stack up).
set -euo pipefail

BASE="${BASE:-http://localhost:8081}"
COMPOSE="${COMPOSE:-docker compose}"
cd "$(dirname "${BASH_SOURCE[0]}")/.."

token() {  # token <user> <password>
  curl -s -X POST "$BASE/auth/token" -H 'Content-Type: application/json' \
    -d "{\"username\":\"$1\",\"password\":\"$2\"}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}

OPS_TOKEN="${OPS_TOKEN:-$(token operator operator-pass)}"

api() {  # api <METHOD> <path> [json-body-file-or-string]
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" && -f "$body" ]]; then
    curl -s -X "$method" "$BASE$path" -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' --data-binary "@$body"
  elif [[ -n "$body" ]]; then
    curl -s -X "$method" "$BASE$path" -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' -d "$body"
  else
    curl -s -X "$method" "$BASE$path" -H "Authorization: Bearer $OPS_TOKEN"
  fi
}

sql() {  # sql "<query>"  -> unaligned, tuples only
  $COMPOSE exec -T postgres psql -U scheduler -d scheduler -At -c "$1"
}

metric() {  # metric <port> <name-regex>   -> summed value of matching prometheus lines on that node
  curl -s "http://localhost:$1/actuator/prometheus" -H "Authorization: Bearer $OPS_TOKEN" \
    | grep -E "^$2" | awk '{s+=$NF} END {printf "%d\n", s}'
}

wait_for() {  # wait_for <description> <timeout-seconds> <sql-returning-t-or-f>
  local what="$1" timeout="$2" query="$3" waited=0
  until [[ "$(sql "$query")" == "t" ]]; do
    sleep 1; waited=$((waited + 1))
    if (( waited >= timeout )); then echo "TIMEOUT waiting for: $what" >&2; return 1; fi
  done
}

make_bulk_body() {  # make_bulk_body <file> <count> <type> <extra-payload-json-fields>
  local file="$1" count="$2" type="$3" extra="$4" i
  { printf '['
    for ((i = 1; i <= count; i++)); do
      (( i > 1 )) && printf ','
      printf '{"name":"job-%d","type":"%s","payload":{%s},"maxRunSeconds":120}' "$i" "$type" "$extra"
    done
    printf ']'; } > "$file"
}

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

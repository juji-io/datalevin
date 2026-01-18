#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run-queries.sh [options]

Runs Neo4j queries from queries.cypher and writes raw results + timings.

Options:
  --address ADDR     Neo4j bolt address (default: bolt://localhost:7687)
  --password PASS    Neo4j password (default: neo4jtest)
  --results FILE     Results file (default: benchmarks/LDBC-SNB-bench/neo4j/results/results.csv)
  --perf FILE        Perf file (default: benchmarks/LDBC-SNB-bench/neo4j/results/perf.csv)
  --out FILE         Alias for --results
  -h, --help         Show this help
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATALEVIN_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

NEO4J_ADDRESS="${NEO4J_ADDRESS:-bolt://localhost:7687}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4jtest}"
RESULTS_FILE="${RESULTS_FILE:-${DATALEVIN_ROOT}/benchmarks/LDBC-SNB-bench/neo4j/results/results.csv}"
PERF_FILE="${PERF_FILE:-${DATALEVIN_ROOT}/benchmarks/LDBC-SNB-bench/neo4j/results/perf.csv}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --address)
      NEO4J_ADDRESS="${2:-}"
      shift 2
      ;;
    --password)
      NEO4J_PASSWORD="${2:-}"
      shift 2
      ;;
    --results|--out)
      RESULTS_FILE="${2:-}"
      shift 2
      ;;
    --perf)
      PERF_FILE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v cypher-shell >/dev/null 2>&1; then
  echo "ERROR: cypher-shell is not available in PATH." >&2
  echo "Please install Neo4j or add cypher-shell to your PATH." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required for timing output." >&2
  exit 1
fi

mkdir -p "$(dirname "$RESULTS_FILE")"
mkdir -p "$(dirname "$PERF_FILE")"
: > "$RESULTS_FILE"
echo "Query,Total Time (ms),Result Count,Error" > "$PERF_FILE"

# Extract :param declarations from the beginning of the file
PARAMS=""
while IFS= read -r line; do
  if [[ $line =~ ^:param ]]; then
    PARAMS+="$line"$'\n'
  elif [[ $line =~ ^//[[:space:]]*(IC|IS) ]]; then
    break
  fi
done < "${SCRIPT_DIR}/queries.cypher"

now_ms() {
  python3 - <<'PY'
import time
print(f"{time.time() * 1000:.3f}")
PY
}

run_query() {
  local name="$1"
  local query="$2"
  local start end duration_ms output status row_count error_msg

  start=$(now_ms)
  set +e
  output=$(cypher-shell \
    -a "$NEO4J_ADDRESS" -u neo4j -p "$NEO4J_PASSWORD" --format plain <<EOF
${PARAMS}
${query}
EOF
  )
  status=$?
  set -e
  end=$(now_ms)
  duration_ms=$(awk -v s="$start" -v e="$end" 'BEGIN {printf "%.3f", (e - s)}')

  if [[ $status -ne 0 ]]; then
    error_msg=$(printf "%s" "$output" | tr '\n' ' ' | sed 's/,/;/g')
    printf "%s,,,%s\n" "$name" "$error_msg" >> "$PERF_FILE"
    return
  fi

  if [[ -n "$output" ]]; then
    printf "%s\n" "$output" >> "$RESULTS_FILE"
  fi

  row_count=$(printf "%s\n" "$output" | awk '/^"?((IC|IS)[0-9]+)"?,/ {count++} END {print count+0}')
  printf "%s,%s,%s,\n" "$name" "$duration_ms" "$row_count" >> "$PERF_FILE"
}

current=""
query=""
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ $line =~ ^//[[:space:]]+((IC|IS)[0-9]+) ]]; then
    if [[ -n "$current" ]]; then
      run_query "$current" "$query"
    fi
    current="${BASH_REMATCH[1]}"
    query=""
    continue
  fi
  # Skip :param lines (already extracted)
  if [[ $line =~ ^:param ]]; then
    continue
  fi
  query+="$line"$'\n'
done < "${SCRIPT_DIR}/queries.cypher"

if [[ -n "$current" ]]; then
  run_query "$current" "$query"
fi

echo "Neo4j query results written to: $RESULTS_FILE"
echo "Neo4j perf written to: $PERF_FILE"

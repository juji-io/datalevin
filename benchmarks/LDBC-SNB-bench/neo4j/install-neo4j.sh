#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: install-neo4j.sh [options]

Installs and configures Neo4j for LDBC SNB benchmarks (macOS via Homebrew).

Options:
  --password PASS    Initial Neo4j password (default: neo4jtest)
  --heap SIZE        Heap memory size (default: 2g)
  --pagecache SIZE   Page cache size (default: 1g)
  --start            Start Neo4j after installation
  -h, --help         Show this help
EOF
}

NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4jtest}"
HEAP_SIZE="2g"
PAGECACHE_SIZE="1g"
START_NEO4J="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --password)
      NEO4J_PASSWORD="${2:-}"
      shift 2
      ;;
    --heap)
      HEAP_SIZE="${2:-}"
      shift 2
      ;;
    --pagecache)
      PAGECACHE_SIZE="${2:-}"
      shift 2
      ;;
    --start)
      START_NEO4J="1"
      shift
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

if [[ "$(uname)" != "Darwin" ]]; then
  echo "This script is for macOS. For other platforms, install Neo4j manually."
  echo "See: https://neo4j.com/docs/operations-manual/current/installation/"
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "ERROR: Homebrew is required. Install from https://brew.sh"
  exit 1
fi

echo "Installing Neo4j via Homebrew..."
if brew list neo4j &>/dev/null; then
  echo "Neo4j is already installed."
else
  brew install neo4j
fi

NEO4J_HOME="$(brew --prefix neo4j)/libexec"
NEO4J_CONF="$NEO4J_HOME/conf/neo4j.conf"

echo "Configuring Neo4j..."
echo "  NEO4J_HOME: $NEO4J_HOME"

# Stop Neo4j if running
if pgrep -f "neo4j" >/dev/null 2>&1; then
  echo "Stopping Neo4j..."
  neo4j stop 2>/dev/null || true
  sleep 2
fi

# Configure memory settings
if [[ -f "$NEO4J_CONF" ]]; then
  # Update heap settings
  if grep -q "^server.memory.heap" "$NEO4J_CONF"; then
    sed -i '' "s/^server.memory.heap.initial_size=.*/server.memory.heap.initial_size=$HEAP_SIZE/" "$NEO4J_CONF"
    sed -i '' "s/^server.memory.heap.max_size=.*/server.memory.heap.max_size=$HEAP_SIZE/" "$NEO4J_CONF"
  else
    echo "server.memory.heap.initial_size=$HEAP_SIZE" >> "$NEO4J_CONF"
    echo "server.memory.heap.max_size=$HEAP_SIZE" >> "$NEO4J_CONF"
  fi

  # Update page cache
  if grep -q "^server.memory.pagecache.size" "$NEO4J_CONF"; then
    sed -i '' "s/^server.memory.pagecache.size=.*/server.memory.pagecache.size=$PAGECACHE_SIZE/" "$NEO4J_CONF"
  else
    echo "server.memory.pagecache.size=$PAGECACHE_SIZE" >> "$NEO4J_CONF"
  fi

  echo "  Heap: $HEAP_SIZE, Page cache: $PAGECACHE_SIZE"
fi

# Set initial password
echo "Setting initial password..."
neo4j-admin dbms set-initial-password "$NEO4J_PASSWORD" 2>/dev/null || true

echo ""
echo "Neo4j installation complete!"
echo ""

if [[ "$START_NEO4J" == "1" ]]; then
  echo "Starting Neo4j..."
  neo4j start
  echo ""
  echo "Waiting for Neo4j to become ready..."
  for i in {1..30}; do
    if cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1;" >/dev/null 2>&1; then
      echo "Neo4j is ready!"
      exit 0
    fi
    sleep 2
  done
  echo "Neo4j may still be starting. Check with 'neo4j status'."
else
  echo "To start Neo4j:"
  echo "  neo4j start"
  echo ""
  echo "To import data:"
  echo "  ./bulk-import-native.sh --start"
fi

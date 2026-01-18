#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: generate-data-docker.sh [options] [-- <extra datagen args>]

Generates LDBC SNB data via the Spark Datagen Docker image.

Options:
  --scale-factor N     Scale factor (default: 1)
  --parallelism N      Spark parallelism (default: 4)
  --memory SIZE        Driver memory (default: 8g)
  --cores N            Number of local cores (default: unset)
  --output-dir PATH    Output directory (default: benchmarks/LDBC-SNB-bench/data)
  --datagen-repo PATH  Datagen repo path (default: ../ldbc_snb_datagen_spark)
  --image TAG          Docker image tag (default: ldbc/datagen-standalone:local)
  --build              Force rebuild of the Docker image
  --no-build           Do not build; require the image to already exist
  -h, --help           Show this help

Example:
  ./generate-data-docker.sh --scale-factor 1 --parallelism 4 --memory 8g
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATALEVIN_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SCALE_FACTOR="1"
PARALLELISM="4"
MEMORY="8g"
CORES=""
OUTPUT_DIR="${DATALEVIN_ROOT}/benchmarks/LDBC-SNB-bench/data"
DATAGEN_REPO="${DATALEVIN_ROOT}/../ldbc_snb_datagen_spark"
IMAGE_TAG="ldbc/datagen-standalone:local"
FORCE_BUILD="0"
NO_BUILD="0"
EXTRA_GEN_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale-factor)
      SCALE_FACTOR="${2:-}"
      shift 2
      ;;
    --parallelism)
      PARALLELISM="${2:-}"
      shift 2
      ;;
    --memory)
      MEMORY="${2:-}"
      shift 2
      ;;
    --cores)
      CORES="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --datagen-repo)
      DATAGEN_REPO="${2:-}"
      shift 2
      ;;
    --image)
      IMAGE_TAG="${2:-}"
      shift 2
      ;;
    --build)
      FORCE_BUILD="1"
      shift
      ;;
    --no-build)
      NO_BUILD="1"
      shift
      ;;
    --)
      shift
      EXTRA_GEN_ARGS=("$@")
      break
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

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not available in PATH." >&2
  exit 1
fi

if [[ ! -d "$DATAGEN_REPO" ]]; then
  echo "ERROR: Datagen repo not found at: $DATAGEN_REPO" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

if [[ "$FORCE_BUILD" == "1" ]]; then
  if [[ "$NO_BUILD" == "1" ]]; then
    echo "ERROR: --build and --no-build cannot be used together." >&2
    exit 1
  fi
  DOCKER_BUILDKIT=1 docker build --target=standalone -t "$IMAGE_TAG" "$DATAGEN_REPO"
else
  if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
    if [[ "$NO_BUILD" == "1" ]]; then
      echo "ERROR: Docker image not found and --no-build was set: $IMAGE_TAG" >&2
      exit 1
    fi
    DOCKER_BUILDKIT=1 docker build --target=standalone -t "$IMAGE_TAG" "$DATAGEN_REPO"
  fi
fi

RUN_ARGS=(--parallelism "$PARALLELISM")
if [[ -n "$MEMORY" ]]; then
  RUN_ARGS+=(--memory "$MEMORY")
fi
if [[ -n "$CORES" ]]; then
  RUN_ARGS+=(--cores "$CORES")
fi

CONF_DIR="${DATAGEN_REPO}/conf"
MOUNT_CONF=()
ENV_CONF=()
if [[ -d "$CONF_DIR" ]]; then
  MOUNT_CONF=(--mount "type=bind,source=${CONF_DIR},target=/conf,readonly")
  ENV_CONF=(-e "SPARK_CONF_DIR=/conf")
fi

echo "Generating LDBC SNB data..."
echo "  Datagen repo:   $DATAGEN_REPO"
echo "  Output dir:     $OUTPUT_DIR"
echo "  Scale factor:   $SCALE_FACTOR"
echo "  Parallelism:    $PARALLELISM"
echo "  Memory:         $MEMORY"
echo "  Image:          $IMAGE_TAG"
echo

DOCKER_CMD=(docker run --rm)
DOCKER_CMD+=(--mount "type=bind,source=${OUTPUT_DIR},target=/out")
if [[ ${#MOUNT_CONF[@]} -gt 0 ]]; then
  DOCKER_CMD+=("${MOUNT_CONF[@]}")
fi
if [[ ${#ENV_CONF[@]} -gt 0 ]]; then
  DOCKER_CMD+=("${ENV_CONF[@]}")
fi
DOCKER_CMD+=("$IMAGE_TAG")
DOCKER_CMD+=("${RUN_ARGS[@]}")
DOCKER_CMD+=(--)
DOCKER_CMD+=(--format csv)
DOCKER_CMD+=(--scale-factor "$SCALE_FACTOR")
DOCKER_CMD+=(--mode raw)
DOCKER_CMD+=(--output-dir /out)
if [[ ${#EXTRA_GEN_ARGS[@]} -gt 0 ]]; then
  DOCKER_CMD+=("${EXTRA_GEN_ARGS[@]}")
fi

"${DOCKER_CMD[@]}"

echo
echo "Done. Data is under:"
echo "  ${OUTPUT_DIR}/graphs/csv/raw/composite-merged-fk"

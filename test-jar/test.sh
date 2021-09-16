#!/bin/bash

set -eou pipefail

cd "$(dirname "$0")"

clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED -J--illegal-access=permit -X test-jar.core/run

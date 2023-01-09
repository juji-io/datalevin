#!/bin/bash

set -eou pipefail

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )

echo "Java version $jvm_version"

cd "$(dirname "$0")"

if [[ "$jvm_version" -gt "8" ]]; then
  clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED -J--illegal-access=permit -X test-jar.core/run
else
  clojure -X test-jar.core/run
fi

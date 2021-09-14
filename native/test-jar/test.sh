#!/bin/bash

set -eou pipefail

cd "$(dirname "$0")"

echo "testing jar in JVM"

clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED -J--illegal-access=permit -X test-jar.core/run

echo "testing uberjar in GraalVM native image"

uberdeps/package.sh

if [ -z "$GRAALVM_HOME" ]; then
    echo "Please set GRAALVM_HOME"
    exit 1
fi

export JAVA_HOME=$GRAALVM_HOME
export PATH=$GRAALVM_HOME/bin:$PATH

"$GRAALVM_HOME/bin/native-image" -jar target/project.jar jar-test

.jar-test

#!/bin/bash

set -eou pipefail

cd "$(dirname "$0")"

echo "testing jar in JVM"

clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED -J--illegal-access=permit -X test-jar.core/run

echo "testing uberjar in GraalVM native image"

if [ -z "$GRAALVM_HOME" ]; then
    echo "Please set GRAALVM_HOME"
    exit 1
fi

export JAVA_HOME=$GRAALVM_HOME
export PATH=$GRAALVM_HOME/bin:$PATH

clojure -X:uberjar :jar target/test-jar.jar :main-class test-jar.core

"$GRAALVM_HOME/bin/native-image" -jar target/test-jar.jar jar-test

# ./jar-test

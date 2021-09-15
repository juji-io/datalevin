#!/bin/bash

set -eou pipefail

if [ -z "$GRAALVM_HOME" ]; then
    echo "Please set GRAALVM_HOME"
    exit 1
fi

export JAVA_HOME=$GRAALVM_HOME
export PATH=$GRAALVM_HOME/bin:$PATH

lein clean
lein uberjar
#clojure -X:uberjar :jar target/test-jar-0.5.10-standalone.jar


"$GRAALVM_HOME/bin/native-image" \
    --initialize-at-build-time=test-jar \
    -jar target/test-jar-0.5.11-standalone.jar \
    jar-test

# ./jar-test

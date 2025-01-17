#!/bin/bash

set -eou pipefail

cd "$(dirname "$0")"

if [ -z "$GRAALVM_HOME" ]; then
    echo "Please set GRAALVM_HOME"
    exit 1
fi

export JAVA_HOME=$GRAALVM_HOME
export PATH=$GRAALVM_HOME/bin:$PATH

clj -T:build clean
clj -T:build uber

"$GRAALVM_HOME/bin/native-image" \
    --verbose \
    --features=clj_easy.graal_build_time.InitClojureClasses \
    -jar target/test-jar.jar \
    test-jar

./test-jar

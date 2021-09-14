#!/bin/bash -e
cd "$( dirname "${BASH_SOURCE[0]}" )"
clojure -M -m uberdeps.uberjar --deps-file ../deps.edn --target ../target/project.jar

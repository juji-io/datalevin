#!/bin/bash

set -eou pipefail

jvm_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1 )

echo $jvm_version

cd "$(dirname "$0")"

if [[ "$jvm_version" -gt "8" ]]; then

    java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --illegal-access=permit \
         -jar ../target/datalevin-0.6.19-standalone.jar exec << EOF
(def conn (get-conn "/tmp/test-db"))
(transact! conn [{:name "world"}])
(q '[:find ?g :where [_ :name ?g]] @conn)
(close conn)
EOF

else

    java -jar ../target/datalevin-0.6.19-standalone.jar exec << EOF
(def conn (get-conn "/tmp/test-db"))
(transact! conn [{:name "world"}])
(q '[:find ?g :where [_ :name ?g]] @conn)
(close conn)
EOF

fi

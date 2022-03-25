(def version "0.6.2")

(defproject test-jar version
  :description "Test jar for Datalevin GraalVM native image compile"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojars.huahaiy/datalevin-native "0.6.2"]]
  :jvm-opts ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
             "--illegal-access=permit"
             "-Dclojure.compiler.direct-linking=true"]
  :main test-jar.core
  :profiles {:uberjar {:main test-jar.core
                       :aot  :all}})

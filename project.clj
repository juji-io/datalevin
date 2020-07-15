(def version "0.1.1")

(defproject datalevin version
  :description "A port of Datascript to LMDB"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url "https://www.eclipse.org/legal/epl-1.0/"}
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [persistent-sorted-set "0.1.2"]
                 [org.lmdbjava/lmdbjava "0.8.1"]
                 [com.taoensso/nippy "2.14.0"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.0.0"]
                                  [criterium "0.4.6"]
                                  [com.taoensso/timbre "4.10.0"]]}}
  :jvm-opts ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"]
  :global-vars {*warn-on-reflection*   true
                *print-namespace-maps* false
                ;; *unchecked-math* :warn-on-boxed
                })

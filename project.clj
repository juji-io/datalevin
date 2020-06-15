(defproject datalightning "0.1.0-SNAPSHOT"
  :description "A port of Datascript to LMDB"
  :url "https://github.com/juji-io/datalightning"
  :license {:name "EPL-1.0"
            :url "https://www.eclipse.org/legal/epl-1.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.lmdbjava/lmdbjava "0.8.1"]
                 [com.taoensso/nippy "2.15.0-RC1"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.0.0"]
                                  [criterium "0.4.5"]
                                  [com.taoensso/timbre "4.10.0"]]}}
  :warn-on-reflection true)

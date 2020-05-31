(defproject datalightning "0.1.0-SNAPSHOT"
  :description "Datascript on lmdb"
  :url "https://github.com/juji-io/DataLightning"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [datascript "0.18.13"]
                 [org.lmdbjava/lmdbjava "0.7.0"]
                 ;; [lmdbjava/lmdbjava "6e64befa0aed84500dc1b08f3009d0a0475fc329"]
                 [com.taoensso/nippy "2.15.0-RC1"]]
  :repositories [["public-github" {:url "git://github.com"}]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.0.0"]
                                  [criterium "0.4.5"]
                                  [com.taoensso/timbre "4.10.0"]]}}
  :warn-on-reflection true
  :repl-options {:init-ns datalogrocks.core})

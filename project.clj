(defproject datalightning "0.1.0-SNAPSHOT"
  :description "Datascript on lmdb"
  :url "https://github.com/juji-io/DataLightning"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [datascript "0.18.13"]
                 [org.lmdbjava/lmdbjava "0.8.1"]
                 ;[org.lmdbjava/lmdbjava "0.8.0-SNAPSHOT"]
                 [com.taoensso/nippy "2.15.0-RC1"]]
  :repositories [["public-github" {:url "git://github.com"}]
                 ["sonatype-oss-public" {:url "https://oss.sonatype.org/content/groups/public/"}]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.0.0"]
                                  [criterium "0.4.5"]
                                  [com.taoensso/timbre "4.10.0"]]}}
  :warn-on-reflection true
  :repl-options {:init-ns datalogrocks.core})

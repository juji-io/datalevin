(defproject bulk-load "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.json "2.4.0"]
                 [medley "1.4.0"]
                 [datalevin "0.7.12"]]
  :profiles {:dev {:jvm-opts
                   ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
                    "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"]}}
  :repl-options {:init-ns bulk-load.core})

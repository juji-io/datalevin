(def version "0.3.7")

(defproject datalevin version
  :description "A simple, fast and durable Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [persistent-sorted-set "0.1.2"]
                 [com.taoensso/nippy "2.15.0"]
                 ;; [com.alipay.sofa/jraft-core "1.3.4"]
                 [org.lmdbjava/lmdbjava "0.8.1"
                  ;; uncomment when run lein codox
                  ;; :exclusions
                  ;; [org.ow2.asm/asm-analysis
                  ;;  org.ow2.asm/asm-commons
                  ;;  org.ow2.asm/asm-tree
                  ;;  org.ow2.asm/asm-util]
                  ]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.0"]
                                  [com.taoensso/timbre "4.10.0"]]}}
  :jvm-opts ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"]

  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]]
  :plugins [[lein-codox "0.10.7"]]
  :codox {:output-path "codox"
          :metadata    {:doc/format :markdown}
          :source-uri
          {#"target/classes" "https://github.com/juji-io/datalevin/blob/master/src/{classpath}x#L{line}"
           #".*"             "https://github.com/juji-io/datalevin/blob/master/{filepath}#L{line}"}}
  :global-vars {*print-namespace-maps* false
                ;; *unchecked-math*       :warn-on-boxed
                ;; *warn-on-reflection*   true
                })

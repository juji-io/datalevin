(def version "0.4.40")

(defproject datalevin version
  :description "A simple, fast and durable Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies [[org.clojure/clojure "1.10.3"]
                         [org.clojure/tools.cli "1.0.206"]
                         [org.clojure/test.check "1.1.0"]
                         [babashka/babashka.pods "0.0.1"]
                         [com.cognitect/transit-clj "1.0.324"
                          :exclusions [com.fasterxml.jackson.core/jackson-core]]
                         [nrepl/bencode "1.1.0"]
                         [org.graalvm.sdk/graal-sdk "21.1.0"]
                         [org.graalvm.nativeimage/svm "21.1.0"]
                         [borkdude/sci "0.2.5"]
                         [com.taoensso/nippy "3.1.1"]
                         [persistent-sorted-set "0.1.2"]
                         [org.lmdbjava/lmdbjava "0.8.1"
                          ;; uncomment when run lein codox
                          ;; :exclusions
                          ;; [org.ow2.asm/asm-analysis
                          ;;  org.ow2.asm/asm-commons
                          ;;  org.ow2.asm/asm-tree
                          ;;  org.ow2.asm/asm-util]
                          ]]
  :dependencies [[org.clojure/clojure :scope "provided"]
                 [com.taoensso/nippy]
                 [persistent-sorted-set]
                 [org.lmdbjava/lmdbjava]]
  :source-paths ["src"]
  :profiles {:uberjar      {:main         datalevin.main
                            :aot          [#"^datalevin.*"
                                           pod.huahaiy.datalevin],
                            :uberjar-name "main.uberjar.jar"}
             :test-uberjar {:main         datalevin.test
                            :aot          [#"^datalevin.*"]
                            :uberjar-name "test.uberjar.jar"}
             :dev          {:source-paths      ["src" "native/src/clj" "test"]
                            :java-source-paths ["native/src/java"]
                            :dependencies
                            [[org.clojure/test.check]
                             [babashka/babashka.pods]
                             [org.graalvm.nativeimage/svm]
                             [org.clojure/tools.cli]
                             [borkdude/sci]
                             [com.cognitect/transit-clj]
                             [nrepl/bencode]]}}
  :jar-exclusions [#"graal"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :jvm-opts ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
             "--illegal-access=permit"
             "-Dclojure.compiler.direct-linking=true"]
  :javac-options ["--release" "11"]

  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]]
  :plugins [[lein-codox "0.10.7"]]
  :codox {:output-path "codox"
          :namespaces  [datalevin.core datalevin.main]
          :metadata    {:doc/format :markdown}
          :source-uri
          {#"target/classes" "https://github.com/juji-io/datalevin/blob/master/src/{classpath}x#L{line}"
           #".*"             "https://github.com/juji-io/datalevin/blob/master/{filepath}#L{line}"}}
  :global-vars {*print-namespace-maps* false
                *unchecked-math*       :warn-on-boxed
                *warn-on-reflection*   true})

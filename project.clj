(def version "0.6.11")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies [
                         [babashka/babashka.pods "0.1.0"]
                         [com.cognitect/transit-clj "1.0.329"]
                         [com.fasterxml.jackson.core/jackson-core "2.13.2"]
                         [com.github.clj-easy/graal-build-time "0.1.4"]
                         [com.taoensso/nippy "3.1.1"]
                         [com.taoensso/timbre "5.2.1"]
                         [me.lemire.integercompression/JavaFastPFOR "0.1.12"]
                         [nrepl/bencode "1.1.0"]
                         [org.babashka/sci "0.3.5"]
                         [org.bouncycastle/bcprov-jdk15on "1.70"]
                         [org.clojure/clojure "1.11.1"]
                         [org.clojure/tools.cli "1.0.206"]
                         [org.clojure/test.check "1.1.1"]
                         [org.clojars.huahaiy/dtlvnative-macos-amd64 "0.4.2"]
                         [org.clojars.huahaiy/dtlvnative-windows-amd64 "0.4.2"]
                         [org.clojars.huahaiy/dtlvnative-linux-amd64 "0.4.2"]
                         [org.eclipse.collections/eclipse-collections "11.0.0"]
                         [org.graalvm.sdk/graal-sdk "21.3.0"]
                         [org.graalvm.nativeimage/svm "21.3.0"]
                         [org.lmdbjava/lmdbjava "0.8.2"
                          ;; uncomment when run lein codox
                          ;; :exclusions
                          ;; [org.ow2.asm/asm-analysis
                          ;;  org.ow2.asm/asm-commons
                          ;;  org.ow2.asm/asm-tree
                          ;;  org.ow2.asm/asm-util]
                          ]
                         [org.roaringbitmap/RoaringBitmap "0.9.26"]
                         [persistent-sorted-set "0.1.4"]
                         ]
  :dependencies [[org.clojure/clojure :scope "provided"]
                 [org.lmdbjava/lmdbjava]
                 [com.taoensso/nippy]
                 [org.babashka/sci]
                 [com.fasterxml.jackson.core/jackson-core]
                 [org.roaringbitmap/RoaringBitmap]
                 [org.eclipse.collections/eclipse-collections]
                 [me.lemire.integercompression/JavaFastPFOR]
                 [com.cognitect/transit-clj]
                 [org.roaringbitmap/RoaringBitmap]
                 [persistent-sorted-set]]
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :profiles {:uberjar        {:main           datalevin.main
                              :aot            [datalevin.main]
                              :jar-inclusions [#"graal"]
                              :dependencies
                              [[nrepl/bencode]
                               [org.clojure/tools.cli]
                               [org.bouncycastle/bcprov-jdk15on]
                               [com.taoensso/timbre]]}
             :native-uberjar {:aot          [pod.huahaiy.datalevin],
                              :uberjar-name "main.uberjar.jar"}
             :test-uberjar   {:main         datalevin.test
                              :uberjar-name "test.uberjar.jar"}
             :dev            {:source-paths      ["src" "test"]
                              :java-source-paths ["native/src/java"]
                              ;; uncomment on java 11 and above
                              :jvm-opts
                              ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
                               "--add-opens" "java.base/java.lang=ALL-UNNAMED"
                               "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
                               "--add-opens" "java.base/jdk.internal.ref=ALL-UNNAMED"
                               "-Djdk.attach.allowAttachSelf"]
                              :dependencies
                              [[org.clojure/test.check]
                               [org.clojure/tools.cli]
                               [org.bouncycastle/bcprov-jdk15on]
                               [com.taoensso/timbre]
                               [nrepl/bencode]
                               [babashka/babashka.pods]
                               [org.graalvm.nativeimage/svm]]
                              :global-vars
                              {*print-namespace-maps* false
                               *unchecked-math*       :warn-on-boxed
                               *warn-on-reflection*   true}}}
  :jar-exclusions [#"graal"]
  :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]]
  :plugins [[lein-codox "0.10.8"]]
  :codox {:source-paths ["src"]
          :output-path  "codox"
          :namespaces   [datalevin.core datalevin.client datalevin.interpret
                         datalevin.search-utils]
          :metadata     {:doc/format :markdown}
          :source-uri
          {#"target/classes"
           "https://github.com/juji-io/datalevin/blob/master/src/{classpath}x#L{line}"
           #".*"
           "https://github.com/juji-io/datalevin/blob/master/{filepath}#L{line}"}}
  )

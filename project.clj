(def version "0.7.2")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies
  [[babashka/babashka.pods "0.1.0"]
   [com.cognitect/transit-clj "1.0.329"]
   [com.fasterxml.jackson.core/jackson-core "2.14.1"]
   [com.github.clj-easy/graal-build-time "0.1.4"]
   [com.github.jnr/jnr-ffi "2.2.13"]
   [com.taoensso/encore "3.43.0"]
   [com.taoensso/nippy "3.2.0"]
   [com.taoensso/timbre "6.0.4"]
   [joda-time/joda-time "2.12.2"]
   [me.lemire.integercompression/JavaFastPFOR "0.1.12"]
   [nrepl/bencode "1.1.0"]
   [org.babashka/sci "0.5.36"]
   [org.bouncycastle/bcprov-jdk15on "1.70"]
   [org.clojure/clojure "1.11.1"]
   [org.clojure/tools.cli "1.0.214"]
   [org.clojure/test.check "1.1.1"]
   [org.clojars.huahaiy/dtlvnative-windows-amd64 "0.6.5"]
   [org.clojars.huahaiy/dtlvnative-linux-amd64 "0.6.5"]
   [org.clojars.huahaiy/dtlvnative-macos-amd64 "0.6.5"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64 "0.6.5"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared "0.6.5"]
   [org.eclipse.collections/eclipse-collections "11.1.0"]
   [org.graalvm.sdk/graal-sdk "21.3.0"]
   [org.graalvm.nativeimage/svm "21.3.0"]
   [org.lmdbjava/lmdbjava "0.8.2"]
   [org.roaringbitmap/RoaringBitmap "0.9.35"]
   [persistent-sorted-set "0.2.3"]]
  :dependencies
  [[org.clojure/clojure :scope "provided"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared]
   [com.github.jnr/jnr-ffi]
   [org.lmdbjava/lmdbjava]
   [com.taoensso/encore]
   [com.taoensso/nippy]
   [org.babashka/sci]
   [com.fasterxml.jackson.core/jackson-core]
   [org.roaringbitmap/RoaringBitmap]
   [org.eclipse.collections/eclipse-collections]
   [me.lemire.integercompression/JavaFastPFOR]
   [com.cognitect/transit-clj]
   [persistent-sorted-set]]
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :profiles
  {:uberjar        {:main           datalevin.main
                    :aot            [datalevin.main]
                    :jar-inclusions [#"graal" #"test"]
                    :dependencies
                    [[nrepl/bencode]
                     [org.clojure/tools.cli]
                     [org.bouncycastle/bcprov-jdk15on]
                     [com.taoensso/timbre]]}
   :native-uberjar {:aot            [pod.huahaiy.datalevin],
                    :jar-inclusions [#"test"]
                    :uberjar-name   "main.uberjar.jar"}
   :test0-uberjar  {:main         datalevin.test0
                    :uberjar-name "test0.uberjar.jar"}
   :test1-uberjar  {:main         datalevin.test1
                    :uberjar-name "test1.uberjar.jar"}
   :test2-uberjar  {:main         datalevin.test2
                    :uberjar-name "test2.uberjar.jar"}
   :dev            {:main              datalevin.test0
                    :source-paths      ["src" "test"]
                    :java-source-paths ["native/src/java"]
                    :jvm-opts
                    ["-XX:+IgnoreUnrecognizedVMOptions"
                     "--add-opens=java.base/java.nio=ALL-UNNAMED"
                     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                     "--add-opens=java.base/java.lang=ALL-UNNAMED"
                     "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"]
                    :dependencies
                    [[org.clojure/test.check]
                     [org.clojure/tools.cli]
                     [org.bouncycastle/bcprov-jdk15on]
                     [com.taoensso/timbre]
                     [nrepl/bencode]
                     [joda-time/joda-time]
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
                                    :sign-releases false}]])

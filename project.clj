(def version "0.8.16")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies
  [[babashka/babashka.pods "0.2.0"]
   [cheshire "5.11.0"]
   [com.clojure-goes-fast/clj-memory-meter "0.2.3"]
   [com.cognitect/transit-clj "1.0.333"]
   [com.fasterxml.jackson.core/jackson-core "2.15.0"]
   [com.fasterxml.jackson.core/jackson-databind "2.15.0"]
   [com.github.clj-easy/graal-build-time "0.1.4"]
   [com.github.jnr/jnr-ffi "2.2.13"]
   ;; [com.github.luben/zstd-jni "1.5.5-1"]
   [com.github.seancorfield/next.jdbc "1.3.874"]
   [com.taoensso/encore "3.59.0"]
   [com.taoensso/nippy "3.2.0"]
   [com.taoensso/timbre "6.1.0"]
   [joda-time/joda-time "2.12.5"]
   [me.lemire.integercompression/JavaFastPFOR "0.1.12"]
   [nrepl/bencode "1.1.0"]
   [org.babashka/sci "0.7.39"]
   [org.bouncycastle/bcprov-jdk15on "1.70"]
   [org.clojure/clojure "1.10.3"]
   [org.clojure/tools.cli "1.0.219"]
   [org.clojure/test.check "1.1.1"]
   [org.clojure/data.csv "1.0.1"]
   [org.clojars.huahaiy/dtlvnative-windows-amd64 "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-linux-amd64 "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-macos-amd64 "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64 "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-windows-amd64-shared "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-linux-amd64-shared "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-macos-amd64-shared "0.8.8"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared "0.8.8"]
   [org.eclipse.collections/eclipse-collections "12.0.0.M1"]
   [org.graalvm.sdk/graal-sdk "22.3.1"]
   [org.graalvm.nativeimage/svm "22.3.1"]
   [org.graalvm.nativeimage/library-support "22.3.1"]
   [org.lmdbjava/lmdbjava "0.8.3"]
   [org.roaringbitmap/RoaringBitmap "0.9.44"]]
  :dependencies
  [[org.clojure/clojure :scope "provided"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared]
   [org.clojars.huahaiy/dtlvnative-windows-amd64-shared]
   [org.clojars.huahaiy/dtlvnative-linux-amd64-shared]
   [org.clojars.huahaiy/dtlvnative-macos-amd64-shared]
   ;; [com.github.luben/zstd-jni]
   [com.github.jnr/jnr-ffi]
   [com.taoensso/timbre]
   [org.lmdbjava/lmdbjava]
   [com.taoensso/encore]
   [com.taoensso/nippy]
   [org.babashka/sci]
   [nrepl/bencode]
   [org.clojure/tools.cli]
   [org.clojure/data.csv]
   [org.bouncycastle/bcprov-jdk15on]
   [cheshire]
   [babashka/babashka.pods]
   [com.fasterxml.jackson.core/jackson-core]
   [org.roaringbitmap/RoaringBitmap]
   [org.eclipse.collections/eclipse-collections]
   [me.lemire.integercompression/JavaFastPFOR]
   [com.github.seancorfield/next.jdbc]
   [com.cognitect/transit-clj]]
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :profiles
  {:uberjar        {:main           datalevin.main
                    :aot            [datalevin.main]
                    :jar-inclusions [#"graal" #"test"]}
   :native-uberjar {:aot            [pod.huahaiy.datalevin],
                    :jar-inclusions [#"test"]
                    :uberjar-name   "main.uberjar.jar"}
   :test0-uberjar  {:main         datalevin.test0
                    :uberjar-name "test0.uberjar.jar"}
   :test1-uberjar  {:main         datalevin.test1
                    :uberjar-name "test1.uberjar.jar"}
   :dev            {:main              datalevin.test0
                    :source-paths      ["src" "test"]
                    :java-source-paths ["native/src/java"]
                    :jvm-opts
                    ["--add-opens=java.base/java.nio=ALL-UNNAMED"
                     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                     "--add-opens=java.base/java.lang=ALL-UNNAMED"
                     "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
                     "-Djdk.attach.allowAttachSelf"]
                    :dependencies
                    [[org.clojure/test.check]
                     [joda-time/joda-time]
                     [com.clojure-goes-fast/clj-memory-meter]
                     [org.graalvm.nativeimage/svm]]
                    :global-vars
                    {*print-namespace-maps* false
                     *unchecked-math*       :warn-on-boxed
                     *warn-on-reflection*   true}}}
  :jar-exclusions [#"graal" #"datalevin.ni"]
  :jvm-opts ["-XX:+IgnoreUnrecognizedVMOptions"
             "-Dclojure.compiler.direct-linking=true"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])

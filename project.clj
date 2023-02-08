(def version "0.8.4")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies
  [[babashka/babashka.pods "0.2.0"]
   [cheshire "5.11.0"]
   [com.cognitect/transit-clj "1.0.329"]
   [com.fasterxml.jackson.core/jackson-core "2.14.2"]
   [com.fasterxml.jackson.core/jackson-databind "2.14.2"]
   [com.github.clj-easy/graal-build-time "0.1.4"]
   [com.github.jnr/jnr-ffi "2.2.13"]
   [com.taoensso/encore "3.49.0"]
   [com.taoensso/nippy "3.2.0"]
   [com.taoensso/timbre "6.0.4"]
   [joda-time/joda-time "2.12.2"]
   [me.lemire.integercompression/JavaFastPFOR "0.1.12"]
   [nrepl/bencode "1.1.0"]
   [org.babashka/sci "0.7.38"]
   [org.bouncycastle/bcprov-jdk15on "1.70"]
   [org.clojure/clojure "1.11.1"]
   [org.clojure/tools.cli "1.0.214"]
   [org.clojure/test.check "1.1.1"]
   [org.clojure/data.csv "1.0.1"]
   [org.clojars.huahaiy/dtlvnative-windows-amd64 "0.7.7"]
   [org.clojars.huahaiy/dtlvnative-linux-amd64 "0.7.7"]
   [org.clojars.huahaiy/dtlvnative-macos-amd64 "0.7.7"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64 "0.7.7"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared "0.7.7"]
   [org.eclipse.collections/eclipse-collections "12.0.0.M1"]
   [org.graalvm.sdk/graal-sdk "22.3.1"]
   [org.graalvm.nativeimage/svm "22.3.1"]
   [org.graalvm.nativeimage/library-support "22.3.1"]
   [org.lmdbjava/lmdbjava "0.8.3"]
   [org.roaringbitmap/RoaringBitmap "0.9.39"]]
  :dependencies
  [[org.clojure/clojure :scope "provided"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared]
   [com.github.jnr/jnr-ffi]
   [org.lmdbjava/lmdbjava]
   [com.taoensso/encore]
   [com.taoensso/nippy]
   [org.babashka/sci]
   [nrepl/bencode]
   [org.clojure/tools.cli]
   [org.clojure/data.csv]
   [org.bouncycastle/bcprov-jdk15on]
   [cheshire]
   [com.taoensso/timbre]
   [com.fasterxml.jackson.core/jackson-core]
   [org.roaringbitmap/RoaringBitmap]
   [org.eclipse.collections/eclipse-collections]
   [me.lemire.integercompression/JavaFastPFOR]
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
                     "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"]
                    :dependencies
                    [[org.clojure/test.check]
                     [org.clojure/data.csv]
                     [cheshire]
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
  :jar-exclusions [#"graal" #"datalevin.ni"]
  :jvm-opts ["-XX:+IgnoreUnrecognizedVMOptions"
             "-Dclojure.compiler.direct-linking=true"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])

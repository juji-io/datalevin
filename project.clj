(def version "0.9.11")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies
  [[babashka/babashka.pods "0.2.0"]
   [com.cognitect/transit-clj "1.0.333"]
   [com.github.clj-easy/graal-build-time "0.1.4"]
   [com.github.jnr/jnr-ffi "2.2.16"]
   [com.taoensso/nippy "3.4.2"]
   [com.taoensso/timbre "6.5.0"]
   [joda-time/joda-time "2.13.0"]
   [me.lemire.integercompression/JavaFastPFOR "0.1.12"]
   [metosin/jsonista "0.3.11"]
   [nrepl/bencode "1.2.0"]
   [org.babashka/sci "0.8.43"]
   [org.bouncycastle/bcprov-jdk15on "1.70"]
   [org.clojure/clojure "1.12.0"]
   [org.clojure/tools.cli "1.1.230"]
   [org.clojure/test.check "1.1.1"]
   [org.clojars.huahaiy/dtlvnative-windows-amd64 "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-linux-amd64 "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-linux-aarch64 "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-macos-amd64 "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64 "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-x86_64-windows-gnu "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-linux-amd64-shared "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-linux-aarch64-shared "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-macos-amd64-shared "0.9.8"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared "0.9.8"]
   [org.eclipse.collections/eclipse-collections "11.1.0"]
   [org.graalvm.sdk/graal-sdk "22.3.1"]
   [org.graalvm.nativeimage/svm "22.3.1"]
   [org.graalvm.nativeimage/library-support "22.3.1"]
   [org.lmdbjava/lmdbjava "0.9.0"]
   [org.roaringbitmap/RoaringBitmap "1.3.0"]]
  :dependencies
  [[org.clojure/clojure :scope "provided"]
   [org.clojars.huahaiy/dtlvnative-macos-aarch64-shared]
   [org.clojars.huahaiy/dtlvnative-macos-amd64-shared]
   [org.clojars.huahaiy/dtlvnative-linux-aarch64-shared]
   [org.clojars.huahaiy/dtlvnative-linux-aarch64]
   [org.clojars.huahaiy/dtlvnative-linux-amd64-shared]
   [org.clojars.huahaiy/dtlvnative-x86_64-windows-gnu]
   [com.github.jnr/jnr-ffi]
   [com.taoensso/nippy]
   [com.taoensso/timbre]
   [org.lmdbjava/lmdbjava]
   [org.babashka/sci]
   [nrepl/bencode]
   [org.clojure/tools.cli]
   [org.bouncycastle/bcprov-jdk15on]
   [metosin/jsonista]
   [babashka/babashka.pods]
   [org.roaringbitmap/RoaringBitmap]
   [org.eclipse.collections/eclipse-collections]
   [me.lemire.integercompression/JavaFastPFOR]
   [com.cognitect/transit-clj]]
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :profiles
  {:uberjar        {:main           datalevin.main
                    :aot            [datalevin.main]
                    :jar-inclusions [#"graal"]
                    }
   :native-uberjar {:aot            [pod.huahaiy.datalevin],
                    :jar-inclusions [#"graal"]
                    :uberjar-name   "main.uberjar.jar"}
   :test0-uberjar  {:main           datalevin.test0
                    :jar-inclusions [#"graal" #"test"]
                    :uberjar-name   "test0.uberjar.jar"}
   :test1-uberjar  {:main           datalevin.test1
                    :jar-inclusions [#"graal" #"test"]
                    :uberjar-name   "test1.uberjar.jar"}
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
                     [org.graalvm.nativeimage/svm]]
                    :global-vars
                    {*print-namespace-maps* false
                     *unchecked-math*       :warn-on-boxed
                     *warn-on-reflection*   true}}}
  :jar-exclusions [#"graal" #"datalevin.ni"]
  :jvm-opts ["-XX:+IgnoreUnrecognizedVMOptions"
             "-Dclojure.compiler.direct-linking=true"]
  :javac-options ["-target" "1.8" "-source" "1.8"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])

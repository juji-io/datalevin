(def version "0.9.14")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/juji-io/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies
  [
   [babashka/babashka.pods "0.2.0"]
   [com.cognitect/transit-clj "1.0.333"]
   [com.github.clj-easy/graal-build-time "1.0.5"]
   [com.taoensso/nippy "3.4.2"]
   [com.taoensso/timbre "6.5.0"]
   [joda-time/joda-time "2.13.0"]
   [me.lemire.integercompression/JavaFastPFOR "0.1.12"]
   [metosin/jsonista "0.3.13"]
   [nrepl/bencode "1.2.0"]
   [org.babashka/sci "0.9.44"]
   [org.bouncycastle/bcprov-jdk15on "1.70"]
   [org.clojure/clojure "1.12.0"]
   [org.clojure/tools.cli "1.1.230"]
   [org.clojure/test.check "1.1.1"]
   [org.eclipse.collections/eclipse-collections "11.1.0"]
   [org.clojars.huahaiy/dtlvnative-macosx-arm64 "0.11.4"]
   [org.clojars.huahaiy/dtlvnative-macosx-x86_64 "0.11.4"]
   [org.clojars.huahaiy/dtlvnative-linux-arm64 "0.11.4"]
   [org.clojars.huahaiy/dtlvnative-linux-x86_64 "0.11.4"]
   [org.clojars.huahaiy/dtlvnative-windows-x86_64 "0.11.4"]
   [org.roaringbitmap/RoaringBitmap "1.3.0"]]
  :dependencies
  [[org.clojure/clojure :scope "provided"]
   [org.clojars.huahaiy/dtlvnative-macosx-arm64]
   [org.clojars.huahaiy/dtlvnative-macosx-x86_64]
   [org.clojars.huahaiy/dtlvnative-linux-arm64]
   [org.clojars.huahaiy/dtlvnative-linux-x86_64]
   [org.clojars.huahaiy/dtlvnative-windows-x86_64]
   [com.github.clj-easy/graal-build-time]
   [com.taoensso/nippy]
   [com.taoensso/timbre]
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
  :source-paths ["src" "test"]
  :java-source-paths ["src/java"]
  :profiles
  {:uberjar        {:main datalevin.main
                    :aot  [datalevin.main]}
   :native-uberjar {:aot          [datalevin.main],
                    :uberjar-name "main.uberjar.jar"}
   :test0-uberjar  {:main           datalevin.test0
                    :aot            [datalevin.test0],
                    :dependencies
                    [[org.clojure/test.check]
                     [joda-time/joda-time]]
                    :jar-inclusions [#"test"]
                    :uberjar-name   "test0.uberjar.jar"}
   :test1-uberjar  {:main           datalevin.test1
                    :aot            [datalevin.test1],
                    :dependencies
                    [[org.clojure/test.check]
                     [joda-time/joda-time]]
                    :jar-inclusions [#"test"]
                    :uberjar-name   "test1.uberjar.jar"}
   :dev            {:main datalevin.test0
                    :dependencies
                    [[org.clojure/test.check]
                     [joda-time/joda-time]]}}
  :global-vars {*print-namespace-maps* false
                *unchecked-math*       :warn-on-boxed
                *warn-on-reflection*   true}
  :jvm-opts ["-XX:+IgnoreUnrecognizedVMOptions"
             "-Xlint:all"
             "-Dclojure.compiler.direct-linking=true"
             "--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
  :javac-options ["-target" "1.8" "-source" "1.8"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])

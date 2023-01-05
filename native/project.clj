(def version "0.7.8")

(defproject org.clojars.huahaiy/datalevin-native version
  :description "Datalevin GraalVM native image and command line tool"
  :parent-project {:path    "../project.clj"
                   :inherit [:managed-dependencies :profiles
                             :deploy-repositories :global-vars
                             :uberjar-exclusions]}
  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.cli]
                 [org.babashka/sci]
                 [com.cognitect/transit-clj]
                 [org.clojars.huahaiy/dtlvnative-macos-amd64]
                 [org.clojars.huahaiy/dtlvnative-windows-amd64]
                 [org.clojars.huahaiy/dtlvnative-linux-amd64]
                 [nrepl/bencode]
                 [com.taoensso/nippy]
                 [org.roaringbitmap/RoaringBitmap]
                 [org.eclipse.collections/eclipse-collections]
                 [me.lemire.integercompression/JavaFastPFOR]
                 [com.taoensso/timbre]
                 [persistent-sorted-set]
                 [org.graalvm.sdk/graal-sdk]
                 [org.graalvm.nativeimage/svm]
                 [org.lmdbjava/lmdbjava]
                 [org.clojure/test.check]
                 [org.bouncycastle/bcprov-jdk15on]
                 [com.github.clj-easy/graal-build-time]
                 [joda-time/joda-time]
                 [babashka/babashka.pods]]
  :jvm-opts ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
             "--add-opens" "java.base/jdk.internal.ref=ALL-UNNAMED"
             "--illegal-access=permit"
             "-Djdk.attach.allowAttachSelf"
             "-Dclojure.compiler.direct-linking=true"]
  :javac-options ["--release" "11"]
  :aot [#"^datalevin.*"]
  :source-paths ["../src" "../test"]
  :java-source-paths ["src/java" "../src/java"]
  :test-paths ["../test"]
  :plugins [[lein-parent "0.3.8"]]
  )

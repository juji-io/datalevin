(def version "0.5.10")

(defproject org.clojars.huahaiy/datalevin-native version
  :description "Datalevin GraalVM native image and command line tool"
  :parent-project {:path    "../project.clj"
                   :inherit [:managed-dependencies :profiles :jvm-opts
                             :deploy-repositories :global-vars
                             :uberjar-exclusions]}
  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.cli]
                 [borkdude/sci]
                 [com.cognitect/transit-clj]
                 [org.clojars.huahaiy/dtlvnative-macos-amd64]
                 [org.clojars.huahaiy/dtlvnative-windows-amd64]
                 [org.clojars.huahaiy/dtlvnative-linux-amd64]
                 [nrepl/bencode]
                 [com.taoensso/nippy]
                 [com.taoensso/timbre]
                 [persistent-sorted-set]
                 [org.graalvm.sdk/graal-sdk]
                 [org.graalvm.nativeimage/svm]
                 [org.lmdbjava/lmdbjava]
                 [org.clojure/test.check]
                 [org.bouncycastle/bcprov-jdk15on]
                 [babashka/babashka.pods]]
  :javac-options ["--release" "11"]
  :aot [#"^datalevin.*"]
  :source-paths ["../src" "../test"]
  :java-source-paths ["src/java" "../src/java"]
  :test-paths ["../test"]
  :plugins [[lein-parent "0.3.8"]]
  )

(def version "0.4.18")

(defproject datalevin-native version
  :description "Datalevin running in GraalVM native image, and it can also run as a command line tool for Datalevin"
  :parent-project {:path    "../project.clj"
                   :inherit [:managed-dependencies :profiles :jvm-opts
                             :deploy-repositories :global-vars]}
  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.cli]
                 [persistent-sorted-set]
                 [borkdude/sci]
                 [com.taoensso/nippy]
                 [org.graalvm.sdk/graal-sdk]
                 [org.graalvm.nativeimage/svm]
                 [org.lmdbjava/lmdbjava]
                 [org.clojure/test.check]]
  :source-paths ["src/clj" "../src" "../test"]
  :java-source-paths ["src/java"]
  :test-paths ["../test"]
  :plugins [[lein-parent "0.3.8"]]
  )

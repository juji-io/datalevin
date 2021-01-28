(def version "0.3.17")

(defproject datalevin-native version
  :description "Datalevin running in GraalVM native image, and it can also run as a command line tool for Datalevin"
  :parent-project {:path    "../project.clj"
                   :inherit [:managed-dependencies :profiles :jvm-opts
                             :deploy-repositories :global-vars]}
  :dependencies [[org.clojure/clojure :scope "provided"]
                 [persistent-sorted-set]
                 [com.taoensso/nippy]
                 [org.graalvm.sdk/graal-sdk]
                 [org.graalvm.nativeimage/svm]
                 [org.lmdbjava/lmdbjava]
                 ]
  :source-paths ["src/clj" "../src"]
  :java-source-paths ["src/java"]
  :plugins [[lein-parent "0.3.8"]]
  )

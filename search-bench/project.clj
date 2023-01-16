(def version "0.7.12")

(defproject org.clojars.huahaiy/search-bench version
  :description "Datalevin search benchmark"
  :parent-project {:path    "../project.clj"
                   :inherit [:managed-dependencies :profiles
                             :global-vars
                             :uberjar-exclusions]}
  :dependencies [[org.clojure/clojure]
                 [com.github.jnr/jnr-ffi]
                 [com.taoensso/encore]
                 [com.taoensso/nippy]
                 [com.fasterxml.jackson.core/jackson-core]
                 [com.fasterxml.jackson.core/jackson-databind]
                 [org.roaringbitmap/RoaringBitmap]
                 [org.eclipse.collections/eclipse-collections]
                 [me.lemire.integercompression/JavaFastPFOR]
                 [com.cognitect/transit-clj]
                 [org.lmdbjava/lmdbjava]]
  :jvm-opts ["--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
             "-Djdk.attach.allowAttachSelf"
             "-Dclojure.compiler.direct-linking=true"]
  :source-paths ["../src" "src"]
  :java-source-paths ["../src/java"]
  :plugins [[lein-parent "0.3.8"]]
  :profiles {:datalevin {:main datalevin.bench}
             :lucene    {:main lucene.bench}})

(def version "0.3.17")

(defproject datalevin-native version
  :description "Native version of Datalevin running in GraalVM native image, and it can also run as a command line tool for Datalevin"
  :parent-project {:path    "../project.clj"
                   :inherit [:managed-dependencies :profiles :jvm-opts :deploy-repositories :global-vars]}
  :dependencies [[org.clojure/clojure :scope "provided"]
                 [persistent-sorted-set]
                 [com.taoensso/nippy]
                 ]
  :plugins [[lein-parent "0.3.8"]]
  )

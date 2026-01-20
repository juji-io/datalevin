(ns build
  (:require [clojure.tools.build.api :as b]))

(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-java [_]
  (b/javac {:src-dirs   ["src/java"]
            :class-dir  class-dir
            :basis      basis
            :javac-opts ["--release" "21"]}))

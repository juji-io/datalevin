;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.migrate
  "Helpers to migrate databases by shelling out to released uberjars."
  (:require
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [datalevin.constants :as c]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.io File]
   [java.net URL]
   [java.nio.file AtomicMoveNotSupportedException Files Paths
    StandardCopyOption]
   [java.util UUID]))

(def ^:private java-opts
  ["--add-opens=java.base/java.nio=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"])

(defn- jar-url
  [version]
  (format
    "https://github.com/juji-io/datalevin/releases/download/%s/datalevin-%s-standalone.jar"
    version version))

(defn- ensure-jar
  [major minor patch]
  (let [version (str major "." minor "." patch)
        jar-dir (io/file (u/tmp-dir "datalevin-migrate") "jars" version)
        jar     (io/file jar-dir (str "datalevin-" version "-standalone.jar"))]
    (u/create-dirs (.getPath jar-dir))
    (when-not (.exists jar)
      (try
        (with-open [in  (.openStream (URL. ^String (jar-url version)))
                    out (io/output-stream jar)]
          (io/copy in out))
        (catch Exception e
          (raise "Failed to download Datalevin uberjar: " (.getMessage e)
                 {:version version}))))
    (.getAbsolutePath jar)))

(defn- run-cmd
  [cmd]
  (let [{:keys [exit err out]} (apply shell/sh cmd)]
    (when (pos? exit)
      (raise "Failed to run command: " (or (not-empty err) (not-empty out))
             {:exit exit :cmd cmd}))))

(defn perform-migration
  [dir major minor patch]
  ())

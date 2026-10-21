(ns datalevin.release-cljdoc
  "Run the release gate from the sibling cljdoc checkout."
  (:require [cljdoc.config :as config]
            [cljdoc.server.api :as api]
            [cljdoc.server.build-log :as build-log]
            [cljdoc.server.system :as system]
            [integrant.core :as ig]))

(defn check!
  "Analyze local release artifacts and fail if API or Git import fails."
  [coords]
  (let [cfg (config/config)
        sys (ig/init
              (select-keys (system/system-config cfg)
                           [:cljdoc/db-spec
                            :cljdoc/db-restore
                            :cljdoc/db-init
                            :cljdoc/storage
                            :cljdoc/build-tracker
                            :cljdoc/analysis-service]))]
    (try
      (let [tracker (:cljdoc/build-tracker sys)
            {:keys [build-id future]}
            (api/kick-off-build!
              {:storage (:cljdoc/storage sys)
               :build-tracker tracker
               :analysis-service (:cljdoc/analysis-service sys)
               :maven-repos (config/get-in cfg [:maven-repos])}
              coords)]
        @future
        (let [result (build-log/get-build tracker build-id)]
          (when-not (and (build-log/api-import-successful? result)
                         (build-log/git-import-successful? result)
                         (:import_completed_ts result))
            (throw (ex-info "cljdoc release analysis failed"
                            {:project (:project coords)
                             :version (:version coords)
                             :build result})))
          (println "cljdoc release analysis passed for"
                   (:project coords) (:version coords))
          result))
      (finally
        (ig/halt! sys)
        (shutdown-agents)))))

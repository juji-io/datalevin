(ns datalevin.release-cljdoc
  "Require a successful local cljdoc build before publishing a release."
  (:require [cljdoc.config :as config]
            [cljdoc.server.api :as api]
            [cljdoc.server.build-log :as build-log]
            [cljdoc.server.system :as system]
            [integrant.core :as ig]))

(defn assert-successful-build!
  [build]
  (when-not (and (:import_completed_ts build)
                 (build-log/api-import-successful? build)
                 (build-log/git-import-successful? build))
    (throw (ex-info "Cljdoc analysis or import failed"
                    (select-keys build [:id :error :git_problem
                                       :api_imported_ts :import_completed_ts]))))
  build)

(defn check!
  [coords]
  (let [cfg (config/config)
        sys (ig/init (select-keys (system/system-config cfg)
                                 [:cljdoc/db-spec
                                  :cljdoc/db-restore
                                  :cljdoc/db-init
                                  :cljdoc/storage
                                  :cljdoc/build-tracker
                                  :cljdoc/analysis-service]))]
    (try
      (let [tracker (:cljdoc/build-tracker sys)
            services {:storage (:cljdoc/storage sys)
                      :build-tracker tracker
                      :analysis-service (:cljdoc/analysis-service sys)
                      :maven-repos (config/get-in cfg [:maven-repos])}
            {:keys [build-id future]} (api/kick-off-build! services coords)]
        @future
        ;; The worker records analyzer failures instead of throwing them.
        ;; Inspect this exact build so an earlier success cannot pass the gate.
        (assert-successful-build! (build-log/get-build tracker build-id))
        (println "Cljdoc build" build-id "completed successfully."))
      (finally
        (ig/halt! sys)
        (shutdown-agents)))))

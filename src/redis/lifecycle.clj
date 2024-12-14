(ns redis.lifecycle
  (:require
   [aleph.tcp :as tcp]
   [integrant.core :as ig]
   [taoensso.timbre :as log]

   [redis.config :as config]
   [redis.handlers :as handlers]
   [redis.rdb.deserialize :as deserialize]
   [redis.runtime :as runtime]
   [redis.storage :as storage])
  (:import
   [org.apache.commons.lang3.exception ExceptionUtils]))

;; state
(def cli-opts (atom {}))

;; ---------------------------------------------------------------------------- State Handlers
;; -------------------------------------------------------- Aleph
(defmethod ig/init-key :adapter/aleph [_ {:keys [handlers port]
                                          :as   opts}]
  (log/info "Starting Aleph TCP socket server on port:" opts)
  (let [connection-handler (-> handlers :connection)
        port (or (-> @cli-opts :options :port) port)]
    (tcp/start-server
     connection-handler
     {:port port})))

(defmethod ig/halt-key! :adapter/aleph [_ server]
  (log/info ::aleph :stopping) 
  (.close server))

;; -------------------------------------------------------- Environment (prod, nonprod, etc.)

(defmethod ig/init-key :env/environment
  [_ {:keys [env]}]
  (log/trace ::init-key :env/environment env)
  (let [environ (keyword env)
        profile (config/set-env environ)]

    (log/info ::init-key {:key :env/environment
                          :profile profile})))

;; -------------------------------------------------------- Handle Connection

(defmethod ig/init-key :handler/handle-connection [_ opts]
  (log/trace ::init-key :handler/handle-connection opts)
  (fn [socket info]
    (handlers/handle-connection socket info)))


;; -------------------------------------------------------- Persistence
(defmethod ig/init-key :redis/config [_ opts]
  (log/trace ::init-key :redis/config opts)
  (let [service-config (-> @cli-opts :options)
        config-db      (merge opts service-config)]
    (log/trace ::init-key :redis/config {:opts       opts
                                         :cli-config service-config})
    (doseq [kv config-db]
      (config/write [:redis/config (key kv)] (val kv)))
    
    (if (seq config-db)
      (let [{:keys [dir dbfilename]} config-db
            path (str dir "/" dbfilename)
            database       (deserialize/rdb-file->database path)]
        (storage/add-db (:id database) database))
      
      ;; create an empty database at id 0 if no config passed.
      (storage/add-db 0 (deserialize/empty-db 0)))
    
    config-db))


;; ---------------------------------------------------------------------------- Run Server

(defn run 
  "Start the system, with an optional set of arguments that override configuration."
  [options]
  (log/trace ::run options)
  (when (seq options)
    (reset! cli-opts options))

  (let [system (ig/init (config/get-configuration))]
    ;; if we have any cli options, merge them in

    (println "Service started...")

    (runtime/set-default-uncaught-exception-handler!
     (fn [thread e]
       (.printStackTrace (ExceptionUtils/getRootCause e) System/err)
       (log/error :message "Handling uncaught exception."
                  :exception e
                  :thread thread)

       (ig/halt! system)))
    (runtime/add-shutdown-hook!
     ::stop #(do
               (log/info :message "System exiting, running shutdown hooks.")
               (ig/halt! system)
               (shutdown-agents)))))

;; ------------------------------------------------------------------------------------------- REPL
(comment

  (def system (atom (ig/init (config/get-configuration))))

  (defn restart [system]
    (ig/halt! @system)
    (config/refresh-configuration)
    (let [system (reset! system (ig/init (config/get-configuration)))]
      system))

  (defn halt []
    (ig/halt! @system)
    (reset! system nil))
  
  (storage/find-keys "*")
  
  (str (config/get-value [:redis/config :dir]) "/" (config/get-value [:redis/config :dbfilename]))

  (restart system)
  "Leave this here.")

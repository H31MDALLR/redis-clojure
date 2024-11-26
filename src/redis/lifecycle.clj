(ns redis.lifecycle
  (:require
   [aleph.tcp :as tcp]
   [integrant.core :as ig]
   [redis.config :as config]
   [redis.handlers :as handlers]
   [redis.runtime :as runtime]
   [taoensso.timbre :as log])
  (:import
   [org.apache.commons.lang3.exception ExceptionUtils]))

;; ------------------------------------------------------------------------------------------- State Handlers
;; -------------------------------------------------------- Aleph
(defmethod ig/init-key :adapter/aleph [_ {:keys [handlers port rdb]
                                          :as   opts}]
  (log/info "Starting Aleph TCP socket server on port:" opts)
  (let [connection-handler (-> handlers :connection)]
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
(defmethod ig/init-key :persistence/rdb [_ opts]
  (log/trace ::init-key :persistence/rdb opts)
  opts)

;; ------------------------------------------------------------------------------------------- Run Server

(defn run []
  (let [system (ig/init (config/get-configuration))]
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

  (restart system)
  "Leave this here.")

(ns redis.metrics.clients
  (:require [redis.metrics.state :as state]))

 
(defn record-client-connection! [client-info]
  (state/update-metric! [:clients :connected-clients] inc)
  (state/update-metric! [:clients :client-infos (str (:id client-info))] merge client-info))

(defn record-client-disconnection! [client-id]
  (state/update-metric! [:clients :connected-clients] dec)
  (state/update-metric! [:clients :client-infos] dissoc (str client-id)))

(defn record-client-buffer! [buffer-size direction]
  (let [buffer-key (case direction
                    :input :client-recent-max-input-buffer
                    :output :client-recent-max-output-buffer)]
    (state/update-metric! [:clients buffer-key] max buffer-size)))

(defn record-client-blocked! [client-id command]
  (state/update-metric! [:clients :blocked-clients] assoc client-id command))

(defn record-client-unblocked! [client-id]
  (state/update-metric! [:clients :blocked-clients] dissoc client-id))

(defn record-replication-role! [role]
  (state/update-metric! [:replication] merge {:role role}))

(defn record-replica-of! [{:keys [host port]}]
  (state/update-metric! [:replication] merge {:host host :port port}))

(defn get-client-metrics []
  (let [{:keys [connected-clients 
                client-info 
                blocked-clients 
                tracking-clients
                pubsub-clients 
                watching-clients 
                clients-in-timeout-table]} (state/get-metric [:clients])]
    {:connected-clients connected-clients
     :client-recent-max-input-buffer (state/get-metric [:clients :client-recent-max-input-buffer])
     :client-recent-max-output-buffer (state/get-metric [:clients :client-recent-max-output-buffer])
     :blocked-clients (count blocked-clients)
     :tracking-clients (count tracking-clients)
     :clients-in-timeout-table (count clients-in-timeout-table)
     :clients-by-flags (->> client-info
                           vals
                           (mapcat :flags)
                           frequencies)}))

(comment 
  ;; client fields we track.
  :clients      {:connected-clients               0
                 :cluster-connections             0
                 :maxclients                      0
                 :client-recent-max-input-buffer  0
                 :client-recent-max-output-buffer 0
                 :blocked-clients                 0
                 :tracking-clients                0
                 :pubsub-clients                  0
                 :watching-clients                0
                 :clients-in-timeout-table        0
                 :total-watched-keys              0
                 :total-blocking-keys             0
                 :total-blocking-keys-on-nokey    0}
  
  (get-client-metrics)

  (state/update-metric! [:replication] merge {:role (keyword "slave")})
  (record-replication-role! "slave")
  (record-replica-of! {:host "localhost" :port 6389})
  (record-client-connection! {:id "123" :flags #{"N"} :age 10 :idle 10 :events "r" :cmd nil})
  ::leave-this-here)
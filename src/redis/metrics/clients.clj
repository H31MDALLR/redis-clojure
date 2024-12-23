(ns redis.metrics.clients
  (:require [redis.metrics.state :as state]))

 
(defn record-client-connection! [client_info]
  (state/update-metric! [:clients :connected_clients] inc)
  (state/update-metric! [:clients :client_infos (str (:id client_info))] merge client_info))

(defn record-client-disconnection! [client-id]
  (state/update-metric! [:clients :connected_clients] dec)
  (state/update-metric! [:clients :client_infos] dissoc (str client-id)))

(defn record-client-buffer! [buffer-size direction]
  (let [buffer-key (case direction
                    :input :client_recent_max_input_buffer
                    :output :client_recent_max_output_buffer)]
    (state/update-metric! [:clients buffer-key] max buffer-size)))

(defn record-client-blocked! [client-id command]
  (state/update-metric! [:clients :blocked_clients] assoc client-id command))

(defn record-client-unblocked! [client-id]
  (state/update-metric! [:clients :blocked_clients] dissoc client-id))

(defn get-client-metrics []
  (let [{:keys [connected_clients 
                client_info 
                blocked_clients 
                tracking_clients
                pubsub-clients 
                watching-clients 
                clients_in_timeout_table]} (state/get-metric [:clients])]
    {:connected_clients connected_clients
     :client_recent_max_input_buffer (state/get-metric [:clients :client_recent_max_input_buffer])
     :client_recent_max_output_buffer (state/get-metric [:clients :client_recent_max_output_buffer])
     :blocked_clients (count blocked_clients)
     :tracking_clients (count tracking_clients)
     :clients_in_timeout_table (count clients_in_timeout_table)
     :clients_by_flags (->> client_info
                           vals
                           (mapcat :flags)
                           frequencies)}))

(comment 
  ;; client fields we track.
  :clients      {:connected_clients               0
                 :cluster-connections             0
                 :maxclients                      0
                 :client_recent_max_input_buffer  0
                 :client_recent_max_output_buffer 0
                 :blocked_clients                 0
                 :tracking_clients                0
                 :pubsub-clients                  0
                 :watching-clients                0
                 :clients_in_timeout_table        0
                 :total-watched-keys              0
                 :total-blocking-keys             0
                 :total-blocking-keys-on-nokey    0}
  
  (get-client-metrics)

  (state/update-metric! [:replication] merge {:role (keyword "slave")})
  (record-client-connection! {:id "123" :flags #{"N"} :age 10 :idle 10 :events "r" :cmd nil})
  ::leave-this-here)
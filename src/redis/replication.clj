(ns redis.replication
  (:require
   [aleph.tcp :as tcp]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [redis.encoding.commands :as encoding]
   [redis.metrics.replication :as replication-metrics]
   [redis.parsing.resp2 :as parser]
   [redis.snowflake :as snowflake]
   [taoensso.timbre :as log]))

(defn replica-handshake [{:keys [host port]}]
  (log/info "Initiating handshake with master server at" host ":" port)
  (try
    (let [socket   @(tcp/client {:host host
                                 :port port})
          ping-cmd (encoding/ping)]
      (log/info ::replica-handshake :initiating-handshake)

      (s/put! socket ping-cmd)

      (->  (s/take! socket)
           (d/chain
            
            (fn [response]
              (log/info ::replica-handshake :response response)
              (String. response "UTF-8"))
            
            (fn [string-response]
              (log/info ::replica-handshake {:string-response string-response})
              (parser/parse-resp {:message string-response}))
            
            (fn [ctx]
              (let [expected '([:SimpleString "PONG"])]
                (if (= (:parse-result ctx) expected)
                  (log/info ::replica-handshake :handshake-completed ctx)
                  (log/error ::replica-handshake :handshake-failed {:expected expected :actual (:parse-result ctx)})))))
           (d/catch Exception  #(log/error ::replica-handshake :handshake-failed %)))) 
    
    (catch Exception e
      (log/error "Failed to connect to master server:" (.getMessage e))
      (throw (ex-info "Failed to establish connection with master" 
                      {:host  host 
                       :port  port 
                       :error (.getMessage e)})))))

(defn set-replication-data
  "Set the replication data for the server."
  [role {:keys [host port]}]
    (replication-metrics/update-connected-slaves! 0)
    (replication-metrics/update-master-id! (snowflake/generate-snowflake))
    (replication-metrics/update-role! (name role))
    (replication-metrics/update-master-host! host)
    (replication-metrics/update-master-port! port))

(defn initialize-replication! [{:keys [host] :as replica-info}]
    (let [role (if (seq host) :slave :master)]
      (set-replication-data role replica-info)
      (case role
        :slave (replica-handshake replica-info)
        :master (log/info ::initialize-replication! :master-initialized))))

(comment
  (initialize-replication! {:host "127.0.0.1" :port 6379}) 
  ::leave-this-here)

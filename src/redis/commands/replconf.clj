(ns redis.commands.replconf
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.metrics.replication :as replication-metrics]
   [redis.metrics.state :as state]
   [redis.replication :as replication]
   [redis.session :as session]
   [taoensso.timbre :as log]))

(defmulti impl-replconf (fn [ctx] (-> ctx :command-info :defaults first)))

(defmethod impl-replconf :listeningport
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-replconf {:command-info command-info
                             :session-id   session-id})
  (let [port (-> command-info :args first)]

  ;; store the listening port in the session
    (.add-item! session/sm session-id [:listening-port] port)
    (resp2/ok)))

(defmethod impl-replconf :capa
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-replconf {:command-info command-info
                             :session-id   session-id})
  (let [arg          (-> command-info :args first)
        replica-port (.get-item session/sm session-id [:listening-port])
        fingerprint  (.get-item session/sm session-id [:fingerprint])
        replica-info (state/get-metric [:clients :client_infos fingerprint])]
    (if (contains? #{"psync2"} arg)
      ;; setup streaming data to the replica in another thread
      (let [stream       (replication/replicate-to replica-port replica-info)
            replica-info (assoc replica-info :stream stream)]
        (replication-metrics/update-connected-slaves! inc)
        (replication-metrics/add-replica replica-info)
        (resp2/ok))
      (resp2/error "ERR unknown option"))))

(defmethod impl-replconf :default
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-replconf {:command-info command-info
                             :session-id session-id})
  (resp2/error "ERR unknown option"))

;; ------------------------------------------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :replconf
  [ctx]
  (impl-replconf ctx))
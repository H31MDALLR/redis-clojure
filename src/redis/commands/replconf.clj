(ns redis.commands.replconf
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.metrics.replication :as replication-metrics]
   [redis.metrics.state :as state]
   [redis.replication :as replication]
   [redis.session :as session]
   [redis.utils :as utils]
   [taoensso.timbre :as log]))

(defmulti impl-replconf (fn [ctx] (-> ctx :command-info :defaults first utils/keywordize)))

(defmethod impl-replconf :listening-port
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-replconf {:command-info command-info
                             :session-id   session-id})
  (let [[_ port] (-> command-info :defaults)]

  ;; store the listening port in the session
    (.add-item! session/sm session-id [:listening-port] port)
    (assoc ctx :response (resp2/ok))))

(defmethod impl-replconf :capa
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-replconf {:command-info command-info
                             :session-id   session-id})
  (let [[_ psync-arg]          (-> command-info :defaults)
        replica-port (.get-item session/sm session-id [:listening-port])
        fingerprint  (.get-item session/sm session-id [:fingerprint])
        replica-info (state/get-metric [:clients :client_infos fingerprint])]
    (if (contains? #{"psync2"} psync-arg)
      ;; setup streaming data to the replica in another thread
      (let [stream       (replication/replicate-to replica-port replica-info)
            replica-info (assoc replica-info :stream stream)]
        (replication-metrics/update-connected-slaves! inc)
        (replication-metrics/add-replica replica-info)
        (assoc ctx :response (resp2/ok)))
      (assoc ctx :response (resp2/error "ERR unknown option")))))

 (defmethod impl-replconf :default
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-replconf {:command-info command-info
                             :session-id session-id})
  (assoc ctx :response (resp2/error "ERR unknown option")))

;; ------------------------------------------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :replconf
  [ctx]
  (impl-replconf ctx))


;; ------------------------------------------------------------------------------------------- REPLCONF

(comment
  (def impl-replconf nil)
  (-> {:command-info {:command :replconf,
                       :defaults ["capa" "psync2"],
                       :options {}}
       :session-id   :8637727d-f641-4032-807a-e945a3e9b0e8
       :parse-result ["REPLCONF" "listening-port" "6379"]}
      :command-info :defaults first utils/keywordize)
  
  (impl-replconf {:command-info {:defaults ["listening-port" "6379"]}
                  :session-id   :8637727d-f641-4032-807a-e945a3e9b0e8
                  :parse-result ["REPLCONF" "listening-port" "6379"]})
  (impl-replconf {:command-info {:command :replconf, 
                                 :defaults ["capa" "psync2"], 
                                 :options {}}, 
                  :session-id :8637727d-f641-4032-807a-e945a3e9b0e8})

  
  ::leave-this-here)

(ns redis.commands.psync 
  (:require
   [clojure.string :as str]
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.metrics.state :as state]
   [redis.utils :as utils]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Implementation
(defmulti impl-psync (fn [ctx] (-> ctx :command-info :defaults first utils/keywordize)))

(defmethod impl-psync :? 
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-psync {:command-info command-info
                          :session-id   session-id})
  (let [master-replica-id (state/get-metric [:replication :master_replid])]
    (assoc ctx :response (resp2/simple-string (str/join \space ["+FULLRESYNC" master-replica-id "0"])))))

 (defmethod impl-psync :default
  [{:keys [command-info session-id]
    :as   ctx}]
  (log/info ::impl-psync {:command-info command-info
                          :session-id   session-id})
  (assoc ctx :response (resp2/error "ERR unknown PSYNC option")))

;; ------------------------------------------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :psync
  [ctx]
  (impl-psync ctx))

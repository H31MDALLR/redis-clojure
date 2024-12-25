(ns redis.commands.getex
  (:require
   [taoensso.timbre :as log]
   
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.session :as session]
   [redis.storage :as storage]))

;; TBD - support the  -EX part of the command
(defmethod dispatch/command-dispatch :get
  [{:keys [command-info session-id] :as ctx}]
  (let [{:keys [defaults options]} command-info
        db (.get-item session/sm session-id [:db])
        _ (log/trace ::command-dispatch :getex {:db db
                                                :defaults defaults
                                                :options options})
        v (storage/retrieve db (first defaults))]
    (log/trace ::command-dispatch :get {:value v})
    (assoc ctx :response (resp2/bulk-string v))))


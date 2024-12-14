(ns redis.commands.get
  (:require
   [taoensso.timbre :as log]
   
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.session :as session]
   [redis.storage :as storage]))

(defmethod dispatch/command-dispatch :get
  [{:keys [command-info session-id] :as ctx}]
  (let [{:keys [defaults]} command-info
        db (.get-item! session/sm session-id [:db])
        _ (log/trace ::command-dispatch :get {:db db
                                              :defaults defaults})
        v (storage/retrieve db (first defaults))]
    (log/trace ::command-dispatch :get {:value v})
    (assoc ctx :response (resp2/bulk-string v))))

(comment 
  (storage/retrieve 0 "expires_ms_precision")
  (storage/retrieve 0 "blueberry")
  (storage/retrieve 0 "mango")

  (.get-item! session/sm :625cab57-1b8a-4163-8de3-1d3c4e5c2932 [:db])
  ::leave-this-here)

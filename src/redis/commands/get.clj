(ns redis.commands.get
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.session :as session]
   [redis.storage :as storage]
   [taoensso.timbre :as log]))

(defmethod dispatch/command-dispatch :get
  [{:keys [command-info session-id] :as ctx}]
  (let [{:keys [defaults]} command-info
        db (session/get-item session-id [:db])
        _ (log/trace ::command-dispatch :get {:db db
                                              :defaults defaults})
        v (storage/retrieve db (first defaults))]
    (log/trace ::command-dispatch :get {:value v})
    (assoc ctx :response (resp2/bulk-string v))))

(comment 
  (session/get-item :7f32ba48-04e0-488d-8c87-c11cac0d20db [:db])
  ::leave-this-here)

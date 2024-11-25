(ns redis.commands.get
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.storage :as storage]))

(defmethod dispatch/command-dispatch :get
  [{:keys [defaults]}]
  (let [v (storage/retrieve (first defaults))]
    (resp2/bulk-string v)))

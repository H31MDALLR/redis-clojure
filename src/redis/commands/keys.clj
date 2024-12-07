(ns redis.commands.keys
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.storage :as storage]))

(defmethod dispatch/command-dispatch :keys
  [{:keys [defaults]}]
  (let [v (storage/retrieve (first defaults))]
    (resp2/bulk-string v)))

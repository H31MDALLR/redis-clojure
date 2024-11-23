(ns redis.commands.get
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoder :as encoder]
   [redis.storage :as storage]))

(defmethod dispatch/command-dispatch :get
  [{:keys [k]}]
  (let [v (get @storage/store k)]
    (encoder/encode-resp {:bulk-string v})))

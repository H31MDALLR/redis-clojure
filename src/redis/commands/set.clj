(ns redis.commands.set 
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoder :as encoder]
   [redis.storage :as storage]))

(defmethod dispatch/command-dispatch :set
  [{:keys [k v]}]
  (swap! storage/store assoc k v)
  (encoder/encode-resp {:simple-string "OK"}))
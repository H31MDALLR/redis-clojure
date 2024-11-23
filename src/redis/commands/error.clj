(ns redis.commands.error 
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoder :as encoder]))

(defmethod dispatch/command-dispatch :error
  [{:keys [exception]}]
  (encoder/encode-resp {:error exception}))
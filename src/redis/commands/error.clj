(ns redis.commands.error 
  (:require
   [redis.commands.command :as command]
   [redis.encoder :as encoder]))

(defmethod command/command :error
  [{:keys [exception]}]
  (encoder/encode-resp {:error exception}))
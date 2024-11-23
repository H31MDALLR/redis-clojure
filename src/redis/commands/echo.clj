(ns redis.commands.echo
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoder :as encoder]))

(defmethod dispatch/command-dispatch :echo
  [{:keys [message]}]
  (encoder/encode-resp {:bulk-string message}))
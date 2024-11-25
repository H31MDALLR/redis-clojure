(ns redis.commands.echo
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]))

(defmethod dispatch/command-dispatch :echo
  [{:keys [message]}]
  (resp2/bulk-string message))
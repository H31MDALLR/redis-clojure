(ns redis.commands.echo
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [taoensso.timbre :as log]))

(defmethod dispatch/command-dispatch :echo
  [{:keys [defaults]}]
  (log/trace ::echo defaults)
  (resp2/bulk-string (first defaults)))
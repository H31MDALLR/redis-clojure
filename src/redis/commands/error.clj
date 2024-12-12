(ns redis.commands.error 
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]))

(defmethod dispatch/command-dispatch :error
  [{:keys [exception] :as ctx}]
  (assoc ctx :response (resp2/error exception)))
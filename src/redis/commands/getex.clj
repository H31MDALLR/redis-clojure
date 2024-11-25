(ns redis.commands.getex
    (:require
    [redis.commands.dispatch :as dispatch]
    [redis.encoding.resp2 :as resp2]
    [redis.storage :as storage]))


  (defmethod dispatch/command-dispatch :get
    [{:keys [k]}]
    (let [v (get @storage/store k)]
      (resp2/bulk-string v)))


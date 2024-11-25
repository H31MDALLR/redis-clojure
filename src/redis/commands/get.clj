(ns redis.commands.get
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.commands.impl.get :as impl]))

(defmethod dispatch/command-dispatch :get
  [{:keys [k]}]
  (let [v (impl/key->value k)]
    (resp2/bulk-string v)))

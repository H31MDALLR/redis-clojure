(ns redis.commands.impl.get
  (:require
   [redis.storage :as storage]
   [redis.time :as time]))


(defn key->value [k]
  (let [{:keys [expiry]
         :as   value} (get k @storage/store)
        value (if expiry value (time/expired? value))]
    (assoc value :last-accessed (time/now))))
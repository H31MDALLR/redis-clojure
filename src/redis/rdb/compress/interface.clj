(ns redis.rdb.compress.interface 
  (:require
   [redis.rdb.compress.core :as core]))

(defn compress [data]
  (let [compressed-data (core/compress data)]
    compressed-data))


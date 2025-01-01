(ns redis.rdb.decoding.list
  (:require [redis.rdb.decoding.string :as string]))

(defn decode-list [bytes]
  (mapv string/decode-string bytes))

(defn list->value [data]
  data) 
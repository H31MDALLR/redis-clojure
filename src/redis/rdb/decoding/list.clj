(ns redis.rdb.decoding.list
  (:require [redis.rdb.decoding.core :refer [decode-storage]]))

(defn decode-list [bytes]
  (mapv #(decode-storage % :bytes) bytes))

(defn list->value [data]
  data) 
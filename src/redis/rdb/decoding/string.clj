(ns redis.rdb.decoding.string
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]))

(defmethod decode-storage ::bytes [value _]
  {:type :string
   :data (String. value "UTF-8")})

(defmethod decode-type :string [{:keys [data]} _]
  data) 
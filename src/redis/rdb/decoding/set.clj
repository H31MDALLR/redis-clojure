(ns redis.rdb.decoding.set
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]))

(defmethod decode-type :set [{:keys [encoding data]} _]
  (case encoding
    :string (into #{} (map #(decode-storage % :bytes) data))
    :listpack data  ; listpack already decoded to set format
    :intset data    ; intset already decoded to set format
    (throw (ex-info "Unsupported storage type for set"
                   {:storage-type encoding}))))

(defn set->value [data]
  data) 
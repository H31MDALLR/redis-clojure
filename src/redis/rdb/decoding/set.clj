(ns redis.rdb.decoding.set
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]
            [redis.rdb.decoding.string :as string]))

(defmethod decode-type :set [{:keys [type data]} _]
  (case type
    :string (into #{} (map string/decode-string data))
    :listpack data  ; listpack already decoded to set format
    :intset data    ; intset already decoded to set format
    (throw (ex-info "Unsupported storage type for set"
                   {:storage-type type}))))

(defn set->value [data]
  data) 
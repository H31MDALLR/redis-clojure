(ns redis.rdb.decoding.hash
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]
            [redis.rdb.decoding.string :as string]))

(defmethod decode-type :hash [{:keys [type data]} _]
  (case type
    :string (into {} (map (fn [[k v]] 
                           [(string/decode-string k) (string/decode-string v)])
                         (partition 2 data)))
    :listpack data  ; listpack already decoded to hash format
    :ziplist data   ; ziplist already decoded to hash format
    (throw (ex-info "Unsupported storage type for hash" 
                   {:storage-type type}))))

(defn hash->value [data]
  data) 
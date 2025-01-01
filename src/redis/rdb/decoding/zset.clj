(ns redis.rdb.decoding.zset
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]
            [redis.rdb.decoding.string :as string]))

(defn- parse-float [s]
  (try 
    (Double/parseDouble s)
    (catch NumberFormatException _
      (case s
        "nan" Double/NaN
        "inf" Double/POSITIVE_INFINITY
        "-inf" Double/NEGATIVE_INFINITY
        (throw (ex-info "Invalid float value" {:value s}))))))

(defmethod decode-type :zset [{:keys [type data]} _]
  (case type
    :string (into (sorted-map)
                 (map (fn [[member score]]
                        [(string/decode-string member) 
                         (if (map? score)
                           (case (:type score)
                             :float (parse-float (:value score))
                             :nan Double/NaN
                             :pos-infinity Double/POSITIVE_INFINITY
                             :neg-infinity Double/NEGATIVE_INFINITY)
                           (parse-float (string/decode-string score)))])
                      (partition 2 data)))
    :listpack data  ; listpack already decoded to zset format
    :ziplist data   ; ziplist already decoded to zset format
    (throw (ex-info "Unsupported storage type for zset"
                   {:storage-type type}))))

(defn zset->value [data]
  data) 
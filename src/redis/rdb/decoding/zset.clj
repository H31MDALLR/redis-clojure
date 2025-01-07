(ns redis.rdb.decoding.zset
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]))

(defn- parse-float [s]
  (try 
    (Double/parseDouble s)
    (catch NumberFormatException _
      (case s
        "nan" Double/NaN
        "inf" Double/POSITIVE_INFINITY
        "-inf" Double/NEGATIVE_INFINITY
        (throw (ex-info "Invalid float value" {:value s}))))))

(defmethod decode-type :zset [{:keys [encoding data]} _]
  (case encoding
    :string (into (sorted-map)
                 (map (fn [[member score]]
                        [(decode-storage member :bytes) 
                         (if (map? score)
                           (case (:type score)
                             :float (parse-float (:value score))
                             :nan Double/NaN
                             :pos-infinity Double/POSITIVE_INFINITY
                             :neg-infinity Double/NEGATIVE_INFINITY)
                           (parse-float (decode-storage score :bytes)))])
                      (partition 2 data)))
    :listpack data  ; listpack already decoded to zset format
    :ziplist data   ; ziplist already decoded to zset format
    (throw (ex-info "Unsupported storage type for zset"
                   {:storage-type encoding}))))

(defmethod decode-type :zset-listpack [{:keys [elements]} _]
  (->> elements
       flatten
       (partition 2)
       (reduce (fn [acc [member score]]
                 (assoc acc 
                        (-> member :value)
                        (-> score :value float)))
               {})))

(defn zset->value [data]
  data) 

(comment 
  (def test-data  {:header {:total-bytes 70, :num-elements 6},
                   :elements
                   [[{:kind :string/tiny, :value "station:1", :backlen [10]}
                     {:kind :number/uint64, :value 1367952638197536N, :backlen [9]}
                     {:kind :string/tiny, :value "station:2", :backlen [10]}
                     {:kind :number/uint64, :value 1367952641196278N, :backlen [9]}
                     {:kind :string/tiny, :value "station:3", :backlen [10]}
                     {:kind :number/uint64, :value 1367953014079341N, :backlen [9]}]]})

  (decode-type test-data :zset-listpack)
  (->> test-data
      :elements
      flatten
      (partition 2)
       (reduce (fn [acc [member score]]
                 (assoc acc (-> member :value) (-> score :value float)))
               {}))
  
  ::leave-this-here)

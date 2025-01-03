(ns redis.rdb.reverse-transform
  (:require [taoensso.timbre :as log]))

(defmulti reverse-transform-value 
  (fn [data]
    (if (map? data)
      (:type data)
      :primitive)))

(defmethod reverse-transform-value :primitive
  [data]
  [])  ;; Skip primitive values or handle specifically if needed

(defmethod reverse-transform-value :string
  [{:keys [data encoding]}]
  (case encoding
    :int {:type :int-string 
          :kind 3 
          :special 0 
          :data [data]}
    :raw {:type :string
          :kind 0
          :special nil
          :size (count data)
          :data (mapv int data)}))

(defmethod reverse-transform-value :set
  [{:keys [items]}]
  {:type :set
   :items (mapv (fn [{:keys [data]}]
                  {:type :string
                   :kind 1
                   :special nil
                   :size (count data)
                   :data (mapv int data)})
                items)})

(defmethod reverse-transform-value :zset
  [{:keys [data encoding]}]
  (when (= encoding :listpack)
    {:type :lzh-string
     :kind 0
     :size (:compressed-length data)
     :special nil
     :uncompressed-length {:kind 1 
                          :size (:uncompressed-length data)}
     :data (:compressed-data data)}))

(defmethod reverse-transform-value :stream
  [{:keys [metadata content encoding]}]
  (when (= encoding :listpack-v3)
    {:metadata-size (:size metadata)
     :first-stream-len (:first-stream-len metadata)
     :last-id (:last-id metadata)
     :first-id (:first-id metadata)
     :padding (:padding metadata)
     :stream-id (:stream-id metadata)
     :unknown (:unknown metadata)
     :content {:type :lzh-string
               :kind 1
               :size (:compressed-length content)
               :special nil
               :uncompressed-length {:kind 1 
                                   :size (:uncompressed-length content)}
               :data (:compressed-data content)}}))

(defn string->bytes [s]
  {:type :string
   :kind 0
   :special nil
   :size (count s)
   :data (mapv int s)})

(defmulti reverse-transform :type)

(defmethod reverse-transform :aux
  [{:keys [aux]}]
  (for [[k v] aux]
    {:type :aux
     :kind :RDB_OPCODE_AUX
     :k (string->bytes k)
     :v (reverse-transform-value (:value v))}))

(defmethod reverse-transform :database
  [{:keys [database]}]
  (for [[k v] database]
    {:type :key-value
     :expiry (when-let [exp (:expiry v)]
               {:kind :RDB_OPCODE_EXPIRETIME_MS
                :timestamp exp
                :unit :milliseconds})
     :kind (:kind v)
     :k (string->bytes k)
     :v (reverse-transform-value (:value v))}))

(defmethod reverse-transform :resizdb-info
  [{:keys [resizdb-info]}]
  [{:type :resizdb-info
    :kind :RDB_OPCODE_RESIZEDB
    :db-hash-table-size {:kind 0 
                        :size (:db-hash-table-size resizdb-info)}
    :expiry-hash-table-size {:kind 0 
                            :size (:expiry-hash-table-size resizdb-info)}}])

(defmethod reverse-transform :selectdb
  [{:keys [id]}]
  [{:type :selectdb
    :kind :RDB_OPCODE_SELECTDB
    :db-number {:kind 0 
                :size id}}])

(defn reverse-transform-data
  "Transform the in-memory format back to RDB format"
  [data]
  (let [[signature & transforms] data
        signature [{:signature (:signature signature)
                    :version (:version signature)}]
        transforms (->> transforms
                        (mapcat (fn [[k v]]
                                  (log/trace ::reverse-transform-data {:k k :v v})
                                  (if (map? v)
                                    (reverse-transform (assoc v :type k))
                                    (reverse-transform v)))))]
    (vec (concat signature transforms))))

;; REPL examples
(comment
  (do
    (require '[clojure.edn :as edn]
             '[clojure.java.io :as io]
             '[java-time.api :as jt]
             '[redis.rdb.transform :as transform]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def input-data (-> "test/db/deserialized.edn"
                        io/resource
                        slurp
                        edn/read-string)))
  
  (log/set-level! :trace)

  (-> input-data
    second
    transform/transform-data)
  
  (-> input-data
      second
      transform/transform-data
      (reverse-transform-data))


  (reverse-transform-data sample-db)
  
  ::leave-this-here
  )
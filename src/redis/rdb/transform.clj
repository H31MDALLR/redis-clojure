(ns redis.rdb.transform
  (:require
   [clojure.edn :as edn]
   [java-time.api :as jt]
   [redis.rdb.schema.streams :as streams]
   [redis.rdb.schema.util :as util]
   [taoensso.timbre :as log]))

; ----------------------------------------------------------------------------- Defs

; ----------------------------------------------------------------------------- Layer 0
;; no deps inside this file
(defn bytes->string
  "Convert a sequence of bytes (integers) into a UTF-8 string."
  [byte-seq]
  (log/trace ::bytes->string {:byte-seq byte-seq})
  (when byte-seq
    (cond
      (map? byte-seq) (bytes->string (:data byte-seq))
      (sequential? byte-seq) (String. (byte-array byte-seq) "UTF-8")
      :else (str byte-seq))))


; --------------------------------------------------------- Value parsing

(defmulti parse-value (fn [kind _] kind))
(defmethod parse-value :aux
  [_ {:keys [data]}]
  (condp = (count data) 
    1 (get data 0)
    (util/parse-stringized-value data)))

(defmethod parse-value :RDB_OPCODE_EXPIRETIME
  [_ v]
  (when (number? v) (jt/instant v)))

(defmethod parse-value :RDB_OPCODE_EXPIRETIME_MS
  [_ v]
  (when (number? v) (jt/instant v)))

(defmethod parse-value :RDB_TYPE_STRING
  [_ {:keys [data]
      :as   value}]
  (log/trace ::parse-value :RDB_TYPE_STRING {:data data}
             :value value)
  (if (= (:type value) :int-string)
    (first data)
    (util/parse-stringized-value data)))

(defmethod parse-value :RDB_TYPE_LIST
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_ZSET
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_SET
  [_ v]
  (when-let [items (get-in v [:items])]
    {:type :set
     :encoding :string
     :items (into #{} 
                  (map #(edn/read-string (bytes->string (:data %))))
                  items)}))

(defmethod parse-value :RDB_TYPE_HASH
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_ZSET_2
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_MODULE_PRE_GA
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_MODULE_2
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_ZIPMAP
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_LIST_ZIPLIST
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_SET_INTSET
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_ZSET_ZIPLIST
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_ZIPLIST
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_LIST_QUICKLIST
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_STREAM_LISTPACKS
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_LISTPACK
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_ZSET_LISTPACK
  [_ v]
  (when (map? v)
    {:type :zset
     :encoding :listpack
     :data {:compressed-length (:size v)
            :uncompressed-length (get-in v [:uncompressed-length :size])
            :compressed-data (:data v)}}))

(defmethod parse-value :RDB_TYPE_LIST_QUICKLIST_2
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_STREAM_LISTPACKS_2
  [_ {:keys [data]
      :as   v}]
  (log/trace ::parse-value 
             :RDB_TYPE_STREAM_LISTPACKS_2 {:data data}
             :v v)
  (for [{:keys [stream-id listpack]} data]
    {:type     :stream
     :encoding :listpack-v2
     :metadata (select-keys v [:element-count
                               :first-stream-id
                               :last-stream-id 
                               :max-tombstone-id 
                               :offset 
                               :groups])
     :stream   {:id   (streams/bytes->stream-id (:data stream-id))
                :data (:data listpack)}}))


(defmethod parse-value :RDB_TYPE_STREAM_LISTPACKS_3
  [_ v]
  ;; for now, just use the same parsing logic as the previous version
  (parse-value :RDB_TYPE_STREAM_LISTPACKS_2 v))

(defmethod parse-value :RDB_TYPE_SET_LISTPACK
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_METADATA_PRE_GA
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_METADATA
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_HASH_LISTPACK_EX
  [_ v]
  v)

(defmethod parse-value :default
  [_ _]
  (log/warn ::parse-value :default {:anomaly :anomalies/incorrect})
  nil)

; ----------------------------------------------------------------------------- Layer 2
;; only depends on layer 1

(defn calc-expiry [exp-map]
  (let [{:keys [kind timestamp]} exp-map
        expiry (if (every? some? [kind timestamp])
                 (parse-value kind timestamp)
                 nil)]
    expiry))
; ----------------------------------------------------------------------------- Layer 3
;; only depends on layer 2
; --------------------------------------------------------- Transform Interface

(defmulti transform :type)
(defmethod transform :aux 
  [{:keys [type kind k v]}]
  {:aux
   {(bytes->string (:data k))
    {:type type
     :kind kind
     :value (parse-value :aux v)}}})

(defmethod transform :key-value 
  [{:keys [expiry kind k v]}]
  (log/trace ::transform :key-value {:expiry expiry})
  {:database
   {(bytes->string (:data k))
    {:expiry (calc-expiry expiry)
     :kind kind
     :value (parse-value kind v)}}})

(defmethod transform :resizedb-info 
  [{:keys[kind :db-hash-table-size :expiry-hash-table-size] :as info}]
  {:resizdb-info info})

(defmethod transform :selectdb 
  [db-info]
  (let [{:keys [kind size]} db-info]
    {:selectdb {:kind kind
                :id size}}))

(defn transform-data
  "Transform the entire vector of maps into the desired map."
  [data]
  (reduce  (fn [acc curr]
             (log/trace ::transform-data {:transforming curr})
             (when (seq curr)
               (merge-with into acc (transform curr))))
           {}
           data))


; ----------------------------------------------------------------------------- REPL
(comment
  (ns-unalias *ns* 'parse-value)

  (def orange {:type   :key-value
               :expiry {:kind      :RDB_OPCODE_EXPIRETIME_MS
                        :timestamp 1956528000000N
                        :unit      :milliseconds}
               :kind   :RDB_TYPE_STRING
               :k      "orange"
               :v      [114 97 115 112 98 101 114 114 121]})
  (def blueberry {:type   :key-value
                  :expiry {:kind      :RDB_OPCODE_EXPIRETIME_MS
                           :timestamp 1640995200000N
                           :unit      :milliseconds}
                  :kind   :RDB_TYPE_STRING
                  :k      "blueberry"
                  :v      [97 112 112 108 101]})
  (transform blueberry)
  (transform orange)
  ; --------------------------------------------------------- In memory format
  
  (def db-edn-format
    {0 {:id          0
        :aux         {"redis-ver"  "7.4.2"
                      "redis-bits" 64}
        :resize-info {:db-hash-table-size     15
                      :expiry-hash-table-size 0}
        :data        {"bike:1:stats"   {:kind :RDB_TYPE_STRING
                                        :v    "some string"}
                      "bikes:rentable" {:kind :RDB_TYPE_ZSET_LISTPACK
                                        :v    {:type                :lzh-string,
                                               :compressed-length   54,
                                               :uncompressed-length {:size 70},
                                               :compressed-data     [26 70 0 0 0 6 0 -119 115 116 97 116 105 111 110
                                                                     58 49 10 -12 32 -65 17 75 37 -36 4 0 9 -32 0 20
                                                                     5 50 10 -12 -10 -128 63 -32 6 20 6 51 10 -12 109
                                                                     63 121 97 64 41 1 9 -1]}}
                      "718:boxster"    {:kind :RDB_TYPE_HASH_LISTPACK
                                        :v    ["imagine a byte array"]}
                      "race:france"    {:content          {:type                :lzh-string,
                                                           :compressed-length   132,
                                                           :uncompressed-length {:size 137}
                                                           :flag                -127
                                                           :v                   ["imagine lzh string here"]},
                                        :current-elements {:size 3},
                                        :first-stream-len {:size 0},
                                        :metadata         4097
                                        :last-id          1732739449600N,
                                        :first-id         1732739436148N,
                                        :padding          0,
                                        :stream-id        {:ms  1732739436148N
                                                           :seq 0N},
                                        :type             :RDB_TYPE_STREAM_LISTPACKS_3,
                                        :unknown          50331648}}}})

; --------------------------------------------------------- Transform Debugging
  
  ;; Example usage
  (do
    (log/set-level! :trace)
    (require '[clojure.edn :as edn]
             '[clojure.java.io :as io]
             '[java-time.api :as jt]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def input-data (-> "test/db/deserialized.edn"
                        io/resource
                        slurp
                        edn/read-string))) 
  
  (filter #(= (:kind %) :RDB_OPCODE_AUX) (second input-data))

  (parse-value 
   :RDB_TYPE_STRING 
   {:encoding :any
    :kind     0
    :special  nil
    :size     17
    :data     [49 48 46 53 51 52 53 49 52 51 57 53 56 54 49 57 48]})
  
  (parse-value 
   :RDB_TYPE_STRING
   {:encoding :any
    :kind     0
    :special  nil
    :size     13
    :data     [50 44 49 52 55 44 52 56 51 44 54 52 55]})
  
  (parse-value :RDB_TYPE_STRING
               {:type    :int-string
                :kind    3
                :special 1
                :data    [1000]})
  
  (parse-value :RDB_TYPE_STREAM_LISTPACKS_3 
               {:data             [{:stream-id {:encoding :any
                                                :kind     0
                                                :special  nil
                                                :size     16
                                                :data     [0 0 1 -109 111 81 -114 116 0 0 0 0 0 0 0 0]}, 
                                    :listpack  {:encoding            :lzh-string, 
                                                :kind                1, 
                                                :size                132, 
                                                :special             nil,
                                                :uncompressed-length {:kind 1
                                                                      :size 137},
                                                :data                [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1]}}], 
                :element-count    {:kind 0
                                   :size 3},
                :last-stream-id   {:ms  1732739449600N
                                   :seq 0}, 
                :first-stream-id  {:ms  1732739436148N
                                   :seq 0},
                :max-tombstone-id {:ms  0
                                   :seq 0}, 
                :offset           {:kind 0
                                   :size 3}, 
                :groups           nil})

  (def transformed-data
    (transform-data (second input-data)))

  (transform  {:type   :key-value,
               :expiry {},
               :kind   :RDB_TYPE_STRING,
               :k      {:type    :string
                        :kind    0
                        :special nil
                        :size    8
                        :data    [105 110 116 118 97 108 117 101]},
               :v      {:type    :string
                        :kind    0
                        :special nil
                        :size    13
                        :data    [50 44 49 52 55 44 52 56 51 44 54 52 55]}})

;; Print the transformed data
  (map  transform (input-data))

  (def aux-values [{:type :aux,
                    :kind :RDB_OPCODE_AUX,
                    :k    {:type    :string
                           :kind    0
                           :special nil
                           :size    9
                           :data    [114 101 100 105 115 45 118 101 114]},
                    :v    {:type    :string
                           :kind    0
                           :special nil
                           :size    5
                           :data    [55 46 50 46 54]}}
                   {:type :aux,
                    :kind :RDB_OPCODE_AUX,
                    :k    {:type    :string
                           :kind    0
                           :special nil
                           :size    10
                           :data    [114 101 100 105 115 45 98 105 116 115]},
                    :v    {:type    :int-string
                           :kind    3
                           :special 0
                           :data    [64]}}
                   {:type :aux,
                    :kind :RDB_OPCODE_AUX,
                    :k    {:type    :string
                           :kind    0
                           :special nil
                           :size    5
                           :data    [99 116 105 109 101]},
                    :v    {:type    :int-string
                           :kind    3
                           :special 2
                           :data    [1732739669]}}
                   {:type :aux,
                    :kind :RDB_OPCODE_AUX,
                    :k    {:type    :string
                           :kind    0
                           :special nil
                           :size    8
                           :data    [117 115 101 100 45 109 101 109]},
                    :v    {:type    :int-string
                           :kind    3
                           :special 2
                           :data    [1520096]}}
                   {:type :aux,
                    :kind :RDB_OPCODE_AUX,
                    :k    {:type    :string
                           :kind    0
                           :special nil
                           :size    8
                           :data    [97 111 102 45 98 97 115 101]},
                    :v    {:type    :int-string
                           :kind    3
                           :special 0
                           :data    [0]}}])

  
  ;; ------------------------------------------------------ Debug transfomation for expiry-db
  (do
    (require '[java-time.api :as jt]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def sample-expiry-db [{:type :aux
                            :k    [114 101 100 105 115 45 118 101 114]
                            :v    [55 46 50 46 48]}
                           {:type :aux
                            :k    [114 101 100 105 115 45 98 105 116 115]
                            :v    [64]}
                           {:type      :selectdb
                            :db-number {:size 0}}
                           {:type                   :resizdb-info
                            :db-hash-table-size     {:size 3}
                            :expiry-hash-table-size {:size 3}}
                           {:type   :key-value
                            :expiry {:expiry 44172959069306880N
                                     :type   :expiry-ms
                                     :unit   :milliseconds}
                            :kind   :RDB_TYPE_STRING
                            :k      [109 97 110 103 111]
                            :v      [97 112 112 108 101]}
                           {:type   :key-value
                            :expiry {:expiry 3422276229857280N
                                     :type   :expiry-ms
                                     :unit   :milliseconds}
                            :kind   :RDB_TYPE_STRING
                            :k      [98 108 117 101 98 101 114 114 121]
                            :v      [112 101 97 114]}
                           {:type   :key-value
                            :expiry {:expiry 3422276229857280N
                                     :type   :expiry-ms
                                     :unit   :milliseconds}
                            :kind   :RDB_TYPE_STRING
                            :k      [112 101 97 114]
                            :v      [111 114 97 110 103 101]}])
    (-> sample-expiry-db
        transform/transform-data
        (utils/apply-f-to-key :k utils/binary-array->string)
        ;time/expired?
        ))

  
  (reduce  (fn [acc curr]
             (merge-with into acc (transform curr)))
           {}
           input-data)

  ::leave-this-here
  )

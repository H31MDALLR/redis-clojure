(ns redis.rdb.transform
   (:require [java-time.api :as jt]
             [taoensso.timbre :as log]))

; ----------------------------------------------------------------------------- Defs

; ----------------------------------------------------------------------------- Load
(defn bytes->string
  "Convert a sequence of bytes (integers) into a UTF-8 string."
  [byte-seq]
  (String. (byte-array byte-seq) "UTF-8"))

; ----------------------------------------------------------------------------- Value parsing

(defmulti parse-value (fn [kind _] kind))
(defmethod parse-value :aux
  [_ v]
  (if (> (count v) 1)
    (bytes->string v)
    (get v 0)))

(defmethod parse-value :RDB_OPCODE_EXPIRETIME
  [_ v]
  (when (number? v) (jt/instant v)))

(defmethod parse-value :RDB_OPCODE_EXPIRETIME_MS
  [_ v]
  (when (number? v) (jt/instant v)))

(defmethod parse-value :RDB_TYPE_STRING
  [_ v]
  (bytes->string v))

(defmethod parse-value :RDB_TYPE_LIST
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_ZSET
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_SET
  [_ v]
  v)

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
  v)

(defmethod parse-value :RDB_TYPE_LIST_QUICKLIST_2
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_STREAM_LISTPACKS_2
  [_ v]
  v)

(defmethod parse-value :RDB_TYPE_STREAM_LISTPACKS_3
  [_ v]
  v)

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

; ----------------------------------------------------------------------------- Transform Interface

(defmulti transform :type)
(defmethod transform :aux [{:keys [type k v]}]
  {:aux 
   {k {:type type
       :value (parse-value :aux v)}}})

(defmethod transform :key-value [{:keys [expiry kind k v]}]
  (let [{:keys [kind timestamp]} expiry
        expiry (if (every? some? [kind timestamp]) 
                 (parse-value kind timestamp) 
                   nil)])
  {:database
   {k {:expiry expiry
       :kind kind
       :value (parse-value kind v)}}})

(defmethod transform :resizdb-info [info]
  {:resizdb-info 
   {:db-hash-table-size (-> info :db-hash-table-size :size)
    :expiry-hash-table-size (-> info :expiry-hash-table-size :size)}})

(defmethod transform :selectdb [{:keys [db-number]}]
  {:id (:size db-number)})

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

  ; --------------------------------------------------------- In memory format

  (def db-edn-format
    {0 {:id          0
        :aux         {"redis-ver"  "7.4.2"
                      "redis-bits" 64}
        :resize-info {:db-hash-table-size     15
                      :expiry-hash-table-size 0}
        :data        {"bike:1:stats"      {:kind :RDB_TYPE_STRING
                                           :v    "some string"}
                      "bikes:rentable"    {:kind :RDB_TYPE_ZSET_LISTPACK
                                           :v    {:type                :lzh-string,
                                                  :compressed-length   54,
                                                  :uncompressed-length {:size 70},
                                                  :compressed-data     [26 70 0 0 0 6 0 -119 115 116 97 116 105 111 110
                                                                        58 49 10 -12 32 -65 17 75 37 -36 4 0 9 -32 0 20
                                                                        5 50 10 -12 -10 -128 63 -32 6 20 6 51 10 -12 109
                                                                        63 121 97 64 41 1 9 -1]}}
                      "718:boxster"       {:kind :RDB_TYPE_HASH_LISTPACK
                                           :v    ["imagine a byte array"]}
                      "race:france"       {:content {:type :lzh-string,
                                                     :compressed-length 132,
                                                     :uncompressed-length {:size 137}
                                                     :flag -127
                                                     :v ["imagine lzh string here"]},
                                           :current-elements {:size 3},
                                           :first-stream-len {:size 0},
                                           :metadata 4097
                                           :last-id 1732739449600N,
                                           :first-id 1732739436148N,
                                           :padding 0,
                                           :stream-id {:ms  1732739436148N
                                                       :seq 0N},
                                           :type :RDB_TYPE_STREAM_LISTPACKS_3,
                                           :unknown 50331648}}}})

; --------------------------------------------------------- Transform Debugging

  ;; Example usage
  (def input-data
    [{:type :aux
      :k    "redis-ver"
      :v    [55 46 50 46 54]}
     {:type :aux
      :k    "redis-bits"
      :v    {:int-8bit 64}}
     {:type :aux
      :k    "ctime"
      :v    {:int-32bit 1732739669}}
     {:type :aux
      :k    "used-mem"
      :v    {:int-32bit 1520096}}
     {:type :aux
      :k    "aof-base"
      :v    {:int-8bit 0}}
     {:type      :selectdb
      :db-number {:size 0}}
     {:type                   :resizdb-info
      :db-hash-table-size     {:size 15}
      :expiry-hash-table-size {:size 0}}
     {:type   :key-value
      :expiry {}
      :kind   :RDB_TYPE_STRING
      :k      "bike:1:stats"
      :v      [0 0 3 -74 0 0 0 1]}
     {:type   :key-value
      :expiry {}
      :kind   :RDB_TYPE_STRING
      :k      "intvalue"
      :v      [50 44 49 52 55 44 52 56 51 44 54 52 55]}])

  (def transformed-data
    (transform-data input-data))

  (transform {:type   :key-value, 
              :expiry {}, 
              :kind   :RDB_TYPE_STRING, 
              :k      "bike:1:stats", 
              :v      [0 0 3 -74 0 0 0 1]})

;; Print the transformed data
  (map  transform input-data)

  (def aux-values [{:type :aux
                    :k    "redis-ver"
                    :v    [55 46 50 46 54]}
                   {:type :aux
                    :k    "redis-bits"
                    :v    [64]}
                   {:type :aux
                    :k    "ctime"
                    :v    [1732739669]}
                   {:type :aux
                    :k    "used-mem"
                    :v    [1520096]}
                   {:type :aux
                    :k    "aof-base"
                    :v    [0]}]) 
  
  (reduce  (fn [acc curr] 
             (merge-with into acc (transform curr))) 
           {}
           input-data)

  ::leave-this-here)

; --------------------------------------------------------- Sample RDB AST

; [{:type :aux, :k "redis-ver", :v [55 46 50 46 54]}
;  {:type :aux, :k "redis-bits", :v {:int-8bit 64}}
;  {:type :aux, :k "ctime", :v {:int-32bit 1732739669}}
;  {:type :aux, :k "used-mem", :v {:int-32bit 1520096}}
;  {:type :aux, :k "aof-base", :v {:int-8bit 0}}
;  {:type :selectdb, :db-number {:size 0}}
;  {:type :resizdb-info,
;   :db-hash-table-size {:size 15},
;   :expiry-hash-table-size {:size 0}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "bike:1:stats",
;   :v [0 0 3 -74 0 0 0 1]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "intvalue",
;   :v [50 44 49 52 55 44 52 56 51 44 54 52 55]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_ZSET_LISTPACK,
;   :k "bikes:rentable",
;   :v
;   {:type :lzh-string,
;    :compressed-length 54,
;    :uncompressed-length {:size 70},
;    :compressed-data
;    [26
;     70
;     0
;     0
;     0
;     6
;     0
;     -119
;     115
;     116
;     97
;     116
;     105
;     111
;     110
;     58
;     49
;     10
;     -12
;     32
;     -65
;     17
;     75
;     37
;     -36
;     4
;     0
;     9
;     -32
;     0
;     20
;     5
;     50
;     10
;     -12
;     -10
;     -128
;     63
;     -32
;     6
;     20
;     6
;     51
;     10
;     -12
;     109
;     63
;     121
;     97
;     64
;     41
;     1
;     9
;     -1]}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_HASH_LISTPACK,
;   :k "718:boxster",
;   :v
;   [75
;    0
;    0
;    0
;    10
;    0
;    -123
;    98
;    114
;    97
;    110
;    100
;    6
;    -121
;    80
;    111
;    114
;    115
;    99
;    104
;    101
;    8
;    -124
;    116
;    114
;    105
;    109
;    5
;    -125
;    71
;    84
;    83
;    4
;    -124
;    121
;    101
;    97
;    114
;    5
;    -57
;    -29
;    2
;    -122
;    101
;    110
;    103
;    105
;    110
;    101
;    7
;    -113
;    50
;    46
;    52
;    76
;    32
;    84
;    117
;    114
;    98
;    111
;    32
;    52
;    99
;    121
;    108
;    16
;    -126
;    104
;    112
;    3
;    -63
;    109
;    2
;    -1]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STREAM_LISTPACKS_3,
;   :k "race:france",
;   :v
;   {:metadata-size 4097,
;    :first-stream-len {:size 0},
;    :unknown 50331648,
;    :content
;    {:compressed-data
;     [31
;      -119
;      0
;      0
;      0
;      32
;      0
;      3
;      1
;      0
;      1
;      4
;      1
;      -123
;      114
;      105
;      100
;      101
;      114
;      6
;      -123
;      115
;      112
;      101
;      101
;      100
;      6
;      -120
;      112
;      111
;      115
;      105
;      116
;      8
;      105
;      111
;      110
;      9
;      -117
;      108
;      111
;      99
;      97
;      64
;      9
;      6
;      95
;      105
;      100
;      12
;      0
;      1
;      2
;      32
;      44
;      18
;      0
;      1
;      -120
;      67
;      97
;      115
;      116
;      105
;      108
;      108
;      97
;      9
;      -124
;      51
;      48
;      46
;      50
;      5
;      1
;      32
;      0
;      0
;      7
;      32
;      27
;      19
;      -15
;      99
;      28
;      3
;      0
;      1
;      -123
;      78
;      111
;      114
;      101
;      109
;      6
;      -124
;      50
;      56
;      46
;      56
;      5
;      3
;      -64
;      26
;      1
;      -116
;      52
;      32
;      26
;      16
;      -120
;      80
;      114
;      105
;      99
;      107
;      101
;      116
;      116
;      9
;      -124
;      50
;      57
;      46
;      55
;      5
;      2
;      64
;      29
;      1
;      1
;      -1],
;     :compressed-length 132,
;     :type :lzh-string,
;     :uncompressed-length {:size 137}},
;    :flag -127,
;    :type :RDB_TYPE_STREAM_LISTPACKS_3,
;    :current-elements {:size 3},
;    :stream-id {:ms 1732739436148N, :seq 0N},
;    :first-id 1732739436148N,
;    :padding 0,
;    :last-id 1732739449600N}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "bike:2",
;   :v [65 114 101 115]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "total_crashes",
;   :v {:int-8bit 0}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "bike:1",
;   :v [68 101 105 109 111 115]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_ZSET_LISTPACK,
;   :k "racer_scores",
;   :v
;   [71
;    0
;    0
;    0
;    12
;    0
;    -124
;    70
;    111
;    114
;    100
;    5
;    6
;    1
;    -118
;    83
;    97
;    109
;    45
;    66
;    111
;    100
;    100
;    101
;    110
;    11
;    8
;    1
;    -123
;    78
;    111
;    114
;    101
;    109
;    6
;    10
;    1
;    -123
;    82
;    111
;    121
;    99
;    101
;    6
;    10
;    1
;    -120
;    67
;    97
;    115
;    116
;    105
;    108
;    108
;    97
;    9
;    12
;    1
;    -120
;    80
;    114
;    105
;    99
;    107
;    101
;    116
;    116
;    9
;    14
;    1
;    -1]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "integerstring",
;   :v {:int-16bit 1000}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_SET,
;   :k "cars:sportscar",
;   :v
;   {:type :set,
;    :items
;    [[123
;      58
;      99
;      111
;      117
;      110
;      116
;      114
;      121
;      32
;      58
;      103
;      101
;      114
;      109
;      97
;      110
;      121
;      32
;      58
;      109
;      97
;      110
;      117
;      102
;      97
;      99
;      116
;      117
;      114
;      101
;      114
;      32
;      58
;      112
;      111
;      114
;      115
;      99
;      104
;      101
;      32
;      58
;      109
;      111
;      100
;      101
;      108
;      32
;      58
;      55
;      49
;      56
;      32
;      58
;      115
;      101
;      114
;      105
;      101
;      115
;      32
;      58
;      98
;      111
;      120
;      115
;      116
;      101
;      114
;      125]
;     [123
;      58
;      99
;      111
;      117
;      110
;      116
;      114
;      121
;      32
;      58
;      103
;      101
;      114
;      109
;      97
;      110
;      121
;      32
;      58
;      109
;      97
;      110
;      117
;      102
;      97
;      99
;      116
;      117
;      114
;      101
;      114
;      32
;      58
;      112
;      111
;      114
;      115
;      99
;      104
;      101
;      32
;      58
;      109
;      111
;      100
;      101
;      108
;      32
;      58
;      55
;      49
;      56
;      32
;      58
;      115
;      101
;      114
;      105
;      101
;      115
;      32
;      58
;      99
;      97
;      121
;      109
;      97
;      110
;      125]]}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "pings:2024-01-01-00:00",
;   :v [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 16]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_LIST_QUICKLIST_2,
;   :k "bikes:repairs",
;   :v
;   {:type :RDB_TYPE_LIST_QUICKLIST_2,
;    :unknown 72645931543035906N,
;    :content
;    [0 -122 98 105 107 101 58 50 7 -122 98 105 107 101 58 49 7]}}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "doublevalue",
;   :v [49 48 46 53 51 52 53 49 52 51 57 53 56 54 49 57 48]}
;  {:type :key-value,
;   :expiry {},
;   :kind :RDB_TYPE_STRING,
;   :k "bike:3",
;   :v [86 97 110 116 104]}]
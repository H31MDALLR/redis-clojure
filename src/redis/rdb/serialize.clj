(ns redis.rdb.serialize
  (:require [clojure.java.io :as io]
            [gloss.io :as gio]
            [taoensso.timbre :as log]
            [redis.rdb.schema :as schema]))

;; ----------------------------------------------------------------------------------- Layer 0
;; Helper functions

(defn write-bytes!
  "Write a sequence of bytes to an output stream"
  [^java.io.OutputStream out bytes]
  (.write out (byte-array bytes)))

(defn string->bytes
  "Convert a string to a byte array using UTF-8 encoding"
  [s]
  (.getBytes s "UTF-8"))

;; ----------------------------------------------------------------------------------- Layer 1
;; Basic RDB structure

(defn write-magic-number!
  "Write the REDIS magic number"
  [out]
  (write-bytes! out (string->bytes "REDIS")))

(defn write-version!
  "Write the RDB version"
  [out version]
  (write-bytes! out (string->bytes version)))

(defn write-aux-field!
  "Write an auxiliary field (key-value pair)"
  [out k v]
  (write-bytes! out [0xFA])  ;; AUX opcode
  (gio/encode schema/parse-section-selector 
              {:type :aux
               :k (string->bytes k)
               :v (if (number? v)
                    [v]
                    (string->bytes v))}
              out))

(defn write-database-header!
  "Write the database selector and resize info"
  [out {:keys [id resizdb-info]}]
  ;; Write database selector
  (write-bytes! out [0xFE])  ;; SELECTDB opcode
  (gio/encode schema/parse-section-selector
              {:type :selectdb
               :db-number {:size id}}
              out)
  
  ;; Write resize info
  (write-bytes! out [0xFB])  ;; RESIZEDB opcode
  (gio/encode schema/parse-section-selector
              {:type :resizdb-info
               :db-hash-table-size {:size (:db-hash-table-size resizdb-info)}
               :expiry-hash-table-size {:size (:expiry-hash-table-size resizdb-info)}}))

(defn write-eof!
  "Write the EOF marker"
  [out]
  (write-bytes! out [0xFF]))  ;; EOF opcode

;; ----------------------------------------------------------------------------------- Layer 2
;; Database writing

(defn write-rdb!
  "Write a minimal empty database to an RDB file"
  [db path]
  (with-open [out (io/output-stream path)]
    ;; Write header
    (write-magic-number! out)
    (write-version! out "0009")
    
    ;; Write aux fields
    (doseq [[k v] (:aux db)]
      (write-aux-field! out k v))
    
    ;; Write database header
    (write-database-header! out db)
    
    ;; Write EOF
    (write-eof! out)))

;; ----------------------------------------------------------------------------------- REPL

(comment
  (def empty-db
    {:aux          {"redis-ver"  "7.2.0"
                    "redis-bits" 64}
     :id           0
     :resizdb-info {:db-hash-table-size     0
                    :expiry-hash-table-size 0}
     :database     {}})
  (write-rdb! empty-db "empty.rdb")
  

  (do 
    (require '[redis.rdb.deserialize :as deserialize]) 
    
    (defn parse-db [file]
      (deserialize/parse-rdb-file-ex file))

    (defn serialize-all [parser file]
      (as-> file $
        (parse-db $)
        (second $)
        (gloss.io/encode-all (parser) $)))
    
    (defn test-serialization [parser file index]
      (as-> file $
        (parse-db $)
        (second $)
        (get-in $ [index])
        (gloss.io/encode (parser) $)))
    
    (defn test-value-serialization [parser file index]
      (as-> file $
        (deserialize/parse-rdb-file-ex $)
        (second $)
        (get-in $ [index :v])
        (gloss.io/encode (parser) $))))
  
  (-> "resources/test/rdb/dump.rdb"
      parse-db
      second
      (get 17))
  #_{:type :aux,
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
  #_{:type :aux,
     :kind :RDB_OPCODE_AUX,
     :k    {:type    :string
            :kind    0
            :special nil
            :size    10
            :data    [114 101 100 105 115 45 98 105 116 115]},
     :v    [64]}
  
  #_{:type :key-value,
     :expiry {},
     :kind :RDB_TYPE_ZSET_LISTPACK,
     :k {:type :string, :kind 0, :special nil, :size 14, :data [98 105 107 101 115 58 114 101 110 116 97 98 108 101]},
     :v
     {:type :lzh-string,
      :kind 0,
      :compressed-length 54,
      :special nil,
      :uncompressed-length {:kind 1, :size 70},
      :compressed-data []}}
  
  #_{:type :key-value,
     :expiry {},
     :kind :RDB_TYPE_STREAM_LISTPACKS_3,
     :k {:type :string, :kind 0, :special nil, :size 11, :data [114 97 99 101 58 102 114 97 110 99 101]},
     :v
     {:metadata-size 4097,
      :first-stream-len {:kind 0, :size 0},
      :unknown 50331648,
      :content
      {:data
       [31 ...],
       :kind 1,
       :size 132,
       :special nil,
       :type :lzh-string,
       :uncompressed-length {:kind 1, :size 137}},
      :flag -127,
      :type :RDB_TYPE_STREAM_LISTPACKS_3,
      :current-elements {:kind 0, :size 3},
      :stream-id {:ms 1732739436148N, :seq 0N},
      :first-id 1732739436148N,
      :padding 0,
      :last-id 1732739449600N}}
  
  #_{:type :key-value,
     :expiry {},
     :kind :RDB_TYPE_SET,
     :k {:type :string, :kind 0, :special nil, :size 14, :data [99 97 114 115 58 115 112 111 114 116 115 99 97 114]},
     :v
     {:type :set,
      :items
      [{:type :string,
        :kind 1,
        :special nil,
        :size 71,
        :data
        [123  ...]}
       {:type :string,
        :kind 1,
        :special nil,
        :size 70,
        :data
        [123 ...]}]}}
  
  (test-serialization schema/parse-auxiliary-field "resources/test/rdb/dump.rdb" 1)
  (test-serialization schema/parse-key-value "resources/test/rdb/dump.rdb" 9)
  (test-value-serialization schema/parse-lzf-string "resources/test/rdb/dump.rdb" 9)

  (test-serialization #(schema/parse-key-value :RDB_TYPE_ZSET_LISTPACK) "resources/test/rdb/dump.rdb" 9)
  (test-serialization #(schema/parse-key-value :RDB_TYPE_STREAM_LISTPACKS_3) "resources/test/rdb/dump.rdb" 11)
  (test-serialization #(schema/parse-key-value :RDB_TYPE_SET) "resources/test/rdb/dump.rdb" 17)
  (test-serialization schema/parse-section-selector "resources/test/rdb/dump.rdb" 9)

  (serialize-all schema/parse-section-selector "resources/test/rdb/dump.rdb" ) 
  )

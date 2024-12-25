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
  (gio/encode schema/section-selector 
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
  (gio/encode schema/section-selector
              {:type :selectdb
               :db-number {:size id}}
              out)
  
  ;; Write resize info
  (write-bytes! out [0xFB])  ;; RESIZEDB opcode
  (gio/encode schema/section-selector
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
    {:aux {"redis-ver" "7.2.0"
           "redis-bits" 64}
     :id 0
     :resizdb-info {:db-hash-table-size 0
                    :expiry-hash-table-size 0}
     :database {}})
  
  (write-rdb! empty-db "empty.rdb")
  
  ::leave-this-here)

(ns redis.rdb.schema.opcodes 
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Layer 0
;; Depends only on things outside of this namespace

(defn data->opcode
  "Convert a data structure back to its RDB opcode for serialization"
  [data]
  (log/trace ::data->opcode {:data data})
  (case (:type data)
    :aux :RDB_OPCODE_AUX
    :selectdb :RDB_OPCODE_SELECTDB
    :resizdb-info :RDB_OPCODE_RESIZEDB
    :key-value (or (:expiry-type data) (:kind data))
    (throw (ex-info "Unknown data type for serialization" {:data data}))))


(defn parse-rdb-header
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :kind :RDB_OPCODE_RDB_HEADER
    :signature (gloss/string :ascii :length 5)
    :version   (gloss/string :ascii :length 4))))

(defn parse-auxiliary-field
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :kind :RDB_OPCODE_AUX
    :k (string/parse-string-encoded-value)
    :v (string/parse-string-encoded-value))))

(defn parse-resizedb-info
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :kind :RDB_OPCODE_RESIZEDB
    :db-hash-table-size (primitives/parse-length)
    :expiry-hash-table-size (primitives/parse-length))))

(defn parse-selectdb
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :kind :RDB_OPCODE_SELECTDB
    :db-number (primitives/parse-length))))

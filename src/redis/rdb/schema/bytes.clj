(ns redis.rdb.schema.bytes 
  (:require
   [clj-commons.byte-streams :as bs]
   [clojure.java.io :as io]
   [gloss.core :as gloss]
   [gloss.io :as gio]
   [manifold.stream :as ms]))

(defn bytes->stream [b]
  (let [s (-> b
              byte-array
              (io/input-stream)
              (bs/convert (bs/stream-of bytes)))]
    s))

(defn get-byte-stream-parser [bytes frame]
  (let [byte-stream (bytes->stream bytes)
        reader (gio/decode-stream byte-stream frame)]
    reader))

;; ---------------------------------------------------------------------------- REPL
(comment
  (do 
  (require '[redis.rdb.schema.streams :as streams]
           '[redis.rdb.schema.primitives :as primitives]) 

    @(-> [0 0 1 -109 111 81 -114 116 0 0 0 0 0 0 0 0]
         (get-byte-stream-parser (gloss/compile-frame
                                  (gloss/ordered-map :ms :uint64-be ;; High 64 bits of stream ID
                                                     :seq :uint64-be)))
         ms/take!))
  )
(ns redis.rdb.schema.primitives 
  (:require
   [gloss.core :as gloss]
   [gloss.core.codecs :as codecs]
   [taoensso.timbre :as log]))


;; ------------------------------------------------------------------------------------------- Layer 0
;; Depends only on things outside of this namespace

(def rdb-types
  {:RDB_TYPE_STRING                  0
   :RDB_TYPE_LIST                    1
   :RDB_TYPE_SET                     2
   :RDB_TYPE_ZSET                    3
   :RDB_TYPE_HASH                    4
   :RDB_TYPE_ZSET_2                  5  ;; ZSET version 2 with doubles stored in binary.
   :RDB_TYPE_MODULE_PRE_GA           6  ;; Used in 4.0 release candidates
   :RDB_TYPE_MODULE_2                7  ;; Module value with annotations for parsing without
                                                ;; the generating module being loaded.
   :RDB_TYPE_HASH_ZIPMAP             9
   :RDB_TYPE_LIST_ZIPLIST            10
   :RDB_TYPE_SET_INTSET              11
   :RDB_TYPE_ZSET_ZIPLIST            12
   :RDB_TYPE_HASH_ZIPLIST            13
   :RDB_TYPE_LIST_QUICKLIST          14
   :RDB_TYPE_STREAM_LISTPACKS        15
   :RDB_TYPE_HASH_LISTPACK           16
   :RDB_TYPE_ZSET_LISTPACK           17
   :RDB_TYPE_LIST_QUICKLIST_2        18
   :RDB_TYPE_STREAM_LISTPACKS_2      19
   :RDB_TYPE_SET_LISTPACK            20
   :RDB_TYPE_STREAM_LISTPACKS_3      21
   :RDB_TYPE_HASH_METADATA_PRE_GA    22   ;; Hash with HFEs. Doesn't attach min TTL at start (7.4 RC)
   :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA 23   ;; Hash LP with HFEs. Doesn't attach min TTL at start (7.4 RC)
   :RDB_TYPE_HASH_METADATA           24   ;; Hash with HFEs. Attach min TTL at start
   :RDB_TYPE_HASH_LISTPACK_EX        25   ;; Hash LP with HFEs. Attach min TTL at start

  ;; Special RDB opcodes
   :RDB_OPCODE_SLOT_INFO             244   ;; Individual slot info, such as slot id and size (cluster mode only).
   :RDB_OPCODE_FUNCTION2             245   ;; function library data
   :RDB_OPCODE_FUNCTION_PRE_GA       246   ;; old function library data for 7.0 rc1 and rc2
   :RDB_OPCODE_MODULE_AUX            247   ;; Module auxiliary data.
   :RDB_OPCODE_IDLE                  248   ;; LRU idle time.
   :RDB_OPCODE_FREQ                  249   ;; LFU frequency.
   :RDB_OPCODE_AUX                   250   ;; RDB aux field.
   :RDB_OPCODE_RESIZEDB              251   ;; Hash table resize hint.
   :RDB_OPCODE_EXPIRETIME_MS         252   ;; Expire time in milliseconds.
   :RDB_OPCODE_EXPIRETIME            253   ;; Old expire time in seconds.
   :RDB_OPCODE_SELECTDB              254   ;; DB number of the following keys.
   :RDB_OPCODE_EOF                   255   ;; End of the RDB file.
   })

(defn encode-length
  "Encode the length as a byte value given a length header"
  [data]
  (let [{:keys [kind size special remaining]} data]
    (case kind
      0 (bit-or (bit-shift-left 0 6)  ; 6 bit length
                (bit-and size 0x3F))
      1 (bit-or (bit-shift-left 1 6)  ; 14 bit length
                (bit-and (bit-shift-right size 8) 0x3F))
      2 (bit-or (bit-shift-left 2 6)  ; 32/64 bit length
                remaining)
      3 (bit-or (bit-shift-left 3 6)  ; Special encoding
                (bit-and special 0x3F)))))

(defn parse-14bit-length
  [kind remaining]
  (gloss/ordered-map
   :kind kind
   :size (gloss/compile-frame
          :ubyte
          identity
          (fn [v]
            (log/trace ::parse-length {:kind      :14bit
                                       :remaining remaining})
            (+ (bit-shift-left remaining 8) v)))))

(defn parse-32or64bit-length
  [kind remaining]
  (gloss/ordered-map :kind kind
               :remaining remaining
               :size (if (= remaining 0x01)
                       :uint64-be
                       :uint32-be)))

(defn parse-eof
  []
  gloss/nil-frame)


(defn parse-expiry [kind]
  (gloss/compile-frame
   (condp = kind
     :RDB_OPCODE_EXPIRETIME (gloss/ordered-map :kind :RDB_OPCODE_EXPIRETIME
                                               :encoding :seconds
                                               :timestamp :uint32-le)
     :RDB_OPCODE_EXPIRETIME_MS (gloss/ordered-map
                                :kind :RDB_OPCODE_EXPIRETIME_MS
                                :encoding :milliseconds
                                :timestamp :uint64-le)
     {})))

(defn repeat-parser
  "Repeat a parser n times. Returns a compiled frame."
  [n parser]
  (gloss/compile-frame (into [] (repeat n parser))))

(defn value-parser [v]
  (gloss/compile-frame [] identity (constantly v)))

;; ------------------------------------------------------------------------------------------- Layer 1
;; Depends only on Layer 0

(defn parse-byte-code [] (codecs/enum :ubyte rdb-types))


(defn parse-length
  []
  (log/trace ::parse-length :enter)
  (gloss/compile-frame
   (gloss/header
    :ubyte
    (fn [header]
      (let [kind      (bit-shift-right header 6)
            remaining (bit-and header 0x3F)]
        (log/trace ::parse-length {:kind      kind
                                   :remaining remaining})
        (case kind
          ; RDB_6BITLEN: 6 bits length
          0  (gloss/ordered-map :kind kind
                          :size remaining)

          ; RDB_14BITLEN: 14 bits length (6 + 8)
          1 (parse-14bit-length kind remaining)

          ; RDB_32BITLEN/RDB_64BITLEN
          2  (parse-32or64bit-length kind remaining)

          ; RDB_ENCVAL: Special encoding
          3  (gloss/ordered-map :kind kind
                          :special remaining))))
    encode-length)))
;; ------------------------------------------------------------------------------------------- Layer 2
;; Depends only on Layer 1

(defn parse-header-byte []
  (let [opcode (parse-byte-code)]
    (println ::parse-expiry-or-value-type {:opcode opcode})
    (gloss/compile-frame  opcode)))


(defn parse-length-prefix []
  (gloss/prefix (parse-length) :size encode-length))
;; ------------------------------------------------------------------------------------------- REPL

(comment 
  (parse-header-byte)
  (parse-expiry :RDB_OPCODE_EXPIRETIME)
  (parse-expiry :RDB_OPCODE_EXPIRETIME_MS)
  ::leave-this-here)

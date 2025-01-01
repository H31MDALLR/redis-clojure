(ns redis.rdb.schema.core 
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.kv :as kv]
   [redis.rdb.schema.opcodes :as opcodes]
   [redis.rdb.schema.primitives :as primitives]
   [taoensso.timbre :as log]
   [redis.rdb.schema.util :as util]
   [redis.rdb.schema.streams :as streams]))

(defn parse-rdb-header
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :signature (gloss/string :ascii :length 5)
    :version   (gloss/string :ascii :length 4))))

(defn parse-section-selector
  []
  (gloss/compile-frame
   (gloss/header
    (primitives/parse-header-byte)
    (fn [header]
      (log/trace ::section-selector {:header header})
      (case header
        :RDB_OPCODE_AUX (opcodes/parse-auxiliary-field)
        :RDB_OPCODE_EXPIRETIME (kv/parse-key-value-with-expiry header)
        :RDB_OPCODE_EXPIRETIME_MS (kv/parse-key-value-with-expiry header)
        :RDB_OPCODE_RESIZEDB (opcodes/parse-resizedb-info)
        :RDB_OPCODE_SELECTDB (opcodes/parse-selectdb)
        :RDB_OPCODE_EOF (primitives/parse-eof)
        (kv/parse-key-value header)))
    opcodes/data->opcode)))

; ----------------------------------------------------------------------------------- REPL

(comment
  (ns-unalias *ns* 'header)

; --------------------------------------------------------- Parser Debugging
  
  (do
    (log/set-min-level! :trace)

    (require  '[clj-commons.byte-streams :as bs]
              '[clojure.java.io :as io]
              '[clojure.pprint :as pp]
              '[clojure.walk :as walk]
              '[manifold.deferred :as d]
              '[manifold.stream :as ms]
              '[redis.rdb.schema.streams :as streams]
              '[redis.rdb.schema.string :as string]
              '[redis.utils :as utils])

    ;; However Redis is storing these things differently than the struct in their stream.h
;; Some compression of the data at the end is going on (i.e., 0x00 when no groups exist)
;; as the stream id is only represented by the high 64 bits in the end metadata data.
    (defn parse-stream-listpack-og
      []
      (gloss/ordered-map :encoding :RDB_TYPE_STREAM_LISTPACKS_3
                         :metadata (gloss/compile-frame [:byte :byte])
                         :stream-id (streams/parse-stream-id)
                         :content (string/parse-string-encoded-value)
                         :elements-count (primitives/parse-length-prefix)
                         :last-ms (primitives/parse-length)
                         :unknown (primitives/parse-length-prefix)
                         :first-ms (primitives/parse-length)
                         :unknown [:byte :byte :byte :byte :byte]))


    (gloss/defcodec sections
      (gloss/repeated parse-section-selector {:prefix     :none
                                              :delimiters [-1]}))

    (gloss/defcodec sections-debug
      (gloss/repeated (gloss/header parse-section-selector
                                    (fn [frame]
                                      (println ::sections-frame-parsed frame)
                                      frame)
                                    identity)
                      :delimiters [-1]))

    (def test-file (-> (io/resource "test/rdb/dump.rdb")
                       io/input-stream
                       (bs/convert (bs/stream-of bytes))

                       #_bs/stream-of))
    
    (def stream-listpack-v3-parsers [(primitives/parse-header-byte)
                                     (string/parse-string-encoded-value)
                                     (gloss/compile-frame [:byte :byte])
                                     (streams/parse-stream-id)
                                     :listpacks (string/parse-string-encoded-value)
                                     :elements-count (primitives/parse-length-prefix) 
                                     :last-ms (primitives/parse-length) 
                                     :unknown (primitives/parse-length-prefix)
                                     :first-ms (primitives/parse-length)
                                     [:byte :byte :byte :byte :byte]
                                     (parse-section-selector)
                                     ])
    
    (def quicklist-v2-parsers [(primitives/parse-header-byte)
                               {:k (string/parse-string-encoded-value)}
                               :fake-node-count (primitives/parse-length-prefix)
                               :list-count (primitives/parse-length-prefix)
                               :items (string/parse-string-encoded-value)])

    (def codecs-to-use (gloss/compile-frame (into [(opcodes/parse-rdb-header)]
                                                  cat 
                                                   [(repeat 19 (parse-section-selector))
                                                    quicklist-v2-parsers
                                                    [(parse-section-selector)]])))

    (gloss/defcodec test codecs-to-use)

    (try
      (def decoder-ring-magic-header (gloss.io/decode-stream-headers test-file codecs-to-use))
      (catch Exception e
        (ex-message e))))
  

  (let [results @(-> decoder-ring-magic-header
                     ms/take!)]
    (->> results 
         rseq
         (take (+ 2 (count quicklist-v2-parsers)))
         (util/stringize-keys :k [:data])
         ;; (take (count stream-listpack-v3-parsers))
         vec
         rseq
         ))
  
  (util/bytes->string (byte-array [123
                                   58
                                   99
                                   111
                                   117
                                   110
                                   116
                                   114
                                   121
                                   32
                                   58
                                   103
                                   101
                                   114
                                   109
                                   97
                                   110
                                   121
                                   32
                                   58
                                   109
                                   97
                                   110
                                   117
                                   102
                                   97
                                   99
                                   116
                                   117
                                   114
                                   101
                                   114
                                   32
                                   58
                                   112
                                   111
                                   114
                                   115
                                   99
                                   104
                                   101
                                   32
                                   58
                                   109
                                   111
                                   100
                                   101
                                   108
                                   32
                                   58]))

  (count codecs-to-use)

  (require '[java-time.api :as jt])
  (map #(-> %
            :ms 
            jt/instant)[ {:ms 14069390648618748160N, :seq 35184422486017N}
                          {:ms 288658577569375602N, :seq 469908662704759814N}
                          {:ms 9831480528030926953N, :seq 8029365680072516449N}])
  (jt/instant 1732739436148N)


;; 68 70 63 C1 6D 02 FF 15 0B 72 61 63 65 3A 66 72
;; 61 6E 63 65 01 10 00 00 01 93 6F 51 8E 74 00 00
;; 00 00 00 00 C3 40 84 40 89 1F 89 00 00 00 00 00
;; 20 00 03 01 00 01 04 01 85 72 69 64 65 72 06 85
;; 73 70 65 65 64 06 88 70 6F 73 69 74 08 69 6F 6E
;; 09 8B 6C 6F 63 61 40 09 06 5F 69 64 0C 00 01 02
;; 20 2c 12 00 01 88 43 61 73 74 69 6C 6C 61 09 84
;; 33 30 2E 32 05 00 01 20 00 00 07 1b 13 F1 63 1C
;; 03 00 01 85 4e 6f 72 65 6D 06 84 32 38 2E 38 05
;; 03 C0 1A 01 8C 34 20 1A 10 88 50 72 69 63 6B 65
;; 74 74 08 84 32 39 2E 37 05 02 40 1D 01 FF

  [0x00 0x00 0x01 0x93 0x6F 0x51 0x8E 0x74  0x00 0x00 0x00 0x00 0x00 0x00]
  (def listpack-bytes
    [[0x01 0x10]
     [0x00 0x00 0x01 0x93 0x6F 0x51 0x8E 0x74 0x00 0x00 0x00 0x00 0x00 0x00] ;; stream id
     [0xC3 0x40 0x84 0x40 0x89 0x1F 0x89 0x00 0x00 0x00 0x00 0x00
      0x20 0x00 0x03 0x01 0x00 0x01 0x04 0x01 0x85 0x72 0x69 0x64 0x65 0x72 0x06 0x85
      0x73 0x70 0x65 0x65 0x64 0x06 0x88 0x70 0x6F 0x73 0x69 0x74 0x08 0x69 0x6F 0x6E
      0x09 0x8B 0x6C 0x6F 0x63 0x61 0x40 0x09 0x06 0x5F 0x69 0x64 0x0C 0x00 0x01 0x02
      0x20 0x2c 0x12 0x00 0x01 0x88 0x43 0x61 0x73 0x74 0x69 0x6C 0x6C 0x61 0x09 0x84
      0x33 0x30 0x2E 0x32 0x05 0x00 0x01 0x20 0x00 0x00 0x07 0x1b 0x13 0xF1 0x63 0x1C
      0x03 0x00 0x01 0x85 0x4e 0x6f 0x72 0x65 0x6D 0x06 0x84 0x32 0x38 0x2E 0x38 0x05
      0x03 0xC0 0x1A 0x01 0x8C 0x34 0x20 0x1A 0x10 0x88 0x50 0x72 0x69 0x63 0x6B 0x65
      0x74 0x74 0x08 0x84 0x32 0x39 0x2E 0x37 0x05 0x02 0x40 0x1D 0x01 0xFF] ;; lzh compressed listpack
     [0x03 0x81 0x00 0x00 0x01 0x93 0x6F 0x51 0xC3 0x00 ;; 2 bytes unknown + (:ms stream-id)
      0x00 0x81 0x00 0x00 0x01 0x93 0x6F 0x51 0x8E 0x74 ;; 2 bytes unknown + (:ms stream-id)
      0x00 0x00 0x00 0x3 0x00] ;; 5 unknown bytes
     ])

  (-> listpack-bytes (get 3) count)
  (-> "ZV" (String.) (.getBytes))


  ::leave-this-here)
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
              '[manifold.stream :as ms]
              '[redis.rdb.schema.streams :as streams]
              '[redis.rdb.schema.string :as string]
              '[redis.utils :as utils])

    (def test-file (-> (io/resource "test/rdb/dump.rdb")
                       io/input-stream
                       (bs/convert (bs/stream-of bytes))

                       #_bs/stream-of))
    
    (def quicklist-v2-parsers [(primitives/parse-header-byte)
                               {:k (string/parse-string-encoded-value)}
                               :fake-node-count (primitives/parse-length-prefix)
                               :list-count (primitives/parse-length-prefix)
                               :items (string/parse-string-encoded-value)])
    
    (def kv-debug-parsers [(primitives/parse-header-byte)
                         {:k (string/parse-string-encoded-value)}])

    (defn compose-codecs [prev current after]
      (gloss/compile-frame (into [(opcodes/parse-rdb-header)]
                                 cat
                                 [prev current after])))
    
    ;; change prev repeat number to the number *before* you have trouble
    ;; add the parsers one by one to the debug-parsers
    ;; then when you think it is OK add (parse-section-selector) to after
    (def codecs-to-use 
      (compose-codecs
       (repeat 12 (parse-section-selector))
       [kv-debug-parsers]
       []))

    (try
      (def decoder-ring-magic-header (gloss.io/decode-stream-headers test-file codecs-to-use))
      (catch Exception e
        (ex-message e))))
  
  ;; retrieve our deserialized data
  (let [results @(-> decoder-ring-magic-header
                     ms/take!) 
         codec-count (count codecs-to-use)]
    (->> results 
         rseq
         (take (+ 2 codec-count)) ;; window -2 before current debug codecs to ensure they are ok.
         (util/stringize-keys :k [:data])
         vec
         rseq
         ))
  
  ;; if you need to check string data 
  (util/bytes->string (byte-array []))

  ;; Check stream timestamps here.
  (require '[java-time.api :as jt])
  (map #(-> %
            :ms 
            jt/instant)[ {:ms 14069390648618748160N, :seq 35184422486017N}
                          {:ms 288658577569375602N, :seq 469908662704759814N}
                          {:ms 9831480528030926953N, :seq 8029365680072516449N}])
  
  ;; check normal timestamps
  (jt/instant 1732739436148N)

  ::leave-this-here)
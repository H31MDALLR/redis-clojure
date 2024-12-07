(ns redis.rdb.schema
  (:require
   [clj-commons.byte-streams :as bs]
   [clojure.java.io :as io]
   [gloss.core :as gloss :refer [defcodec finite-frame header repeated]]
   [gloss.core.codecs :refer [enum ordered-map]]
   [gloss.core.structure :refer [compile-frame]]
   [gloss.data.bytes.bits :refer [bit-seq]]
   [manifold.deferred :as d]
   [manifold.stream :as ms]
   [taoensso.timbre :as log]))


(defn hex->int
  "Takes an expected hex value as key and returns the integer mapping"
  [hex-key]
  (let [hexmap {:0xFA 250
                :0xFB 251
                :0xFC 252
                :0xFD 253
                :0xFE 254
                :0xFF 255}]
    (get hexmap hex-key)))

;; Length Encoding
(defcodec encoding-type (enum :byte {:short 0 :medium 1 :long 2 :special 3}))


;; ------------------------------------------------------------------------------------------- Layer 0

(defn bytes->integer [& bytes]
  (reduce (fn [acc byte]
            (+ (bit-shift-left acc 8) byte))
          0
          bytes))

(defn value-parser [v]
  (compile-frame [] identity (constantly v)))

;; ------------------------------------------------------------------------------------------- Parsers

;; (defcodec length-encoding
;;   (header
;;    (bit-seq 2 6)
;;    (fn [header]
;;      (let [[kind remaining] header]
;;        (println ::length-encoding {:kind      kind
;;                                    :remaining remaining})

;;        (condp = kind
;;          0 (compile-frame {:size remaining})
;;          1 (compile-frame {:highbits remaining
;;                            :lowbits :byte})
;;          2 (compile-frame {:size :int32})
;;          3 (special-string remaining))))
;;    identity))


(defn parse-length
  []
  (log/trace ::parse-length :enter)
  (compile-frame
   (header
    (bit-seq 2 6)
    (fn [[kind remaining]]
      (log/trace ::parse-length {:kind kind
                                 :remaining remaining})
      (case kind
        0 (ordered-map :size remaining)
        1 (ordered-map :size
                       (compile-frame :ubyte
                                      identity
                                      (fn [v]
                                        (log/trace ::parse-length {:kind      :14bit
                                                                   :remaining remaining})
                                        (bytes->integer remaining v))))
        2 (ordered-map :size :uint32)
        3 (ordered-map :special remaining)))
    identity)))


(defn parse-lzf-string []
  (header
   (parse-length)
   (fn [{:keys [size]}]
     (log/trace ::parse-lzf-string :compressed-length size)
     (compile-frame (ordered-map
                     :type :lzh-string
                     :compressed-length size
                     :uncompressed-length (parse-length)
                     :compressed-data  (compile-frame (repeat size :byte)))))
   identity))


(defn parse-string
  []
  (compile-frame
   (header
    (parse-length)
    (fn [{:keys [size special]
          :as   header}]
      (log/trace ::parse-string header)
      (if special
          ;; Special encoding
        (condp = special
          0 (ordered-map :int-8bit :byte)
          1 (ordered-map :int-16bit :int16-le)
          2 (ordered-map :int-32bit :int32-le)
          3 (parse-lzf-string)
          (throw (Exception. (str "Unknown special encoding: " special))))
          ;; Regular string
        (compile-frame (repeat size :byte))))
    identity)))

(defn binary-array->string
  [arr]
  (String. (byte-array arr)  java.nio.charset.StandardCharsets/UTF_8))

(binary-array->string [98 105 107 101 58 49 58 115 116 97 116 115])

#_(defcodec byte-codes
    (enum :ubyte {;; Opcodes
                  :AUX                (hex->int :0xFA)
                  :RESIZEDB           (hex->int :0xFB)
                  :EXPIRETIMEMS       (hex->int :0xFC)
                  :EXPIRETIME         (hex->int :0xFD)
                  :SELECTDB           (hex->int :0xFE)
                  :EOF                (hex->int :0xFF)

                  ;; values
                  :string             0
                  :list               1
                  :set                2
                  :sorted-set         3
                  :hashmap            4
                  :zset2              5
                  :zipmap             9
                  :ziplist            10
                  :intset             11
                  :ziplist-sorted-set 12
                  :ziplist-hashmap    13
                  :quicklist          14
                  :stream-listpacks   15

                  ;; expirytime
                  :seconds            (hex->int :0xFD)
                  :milliseconds       (hex->int :0xFC)}))


(defcodec byte-codes
  (enum :ubyte
        {:RDB_TYPE_STRING                  0
         :RDB_TYPE_LIST                    1
         :RDB_TYPE_SET                     2
         :RDB_TYPE_ZSET                    3
         :RDB_TYPE_HASH                    4
         :RDB_TYPE_ZSET_2                  5 ;; ZSET version 2 with doubles stored in binary. */
         :RDB_TYPE_MODULE_PRE_GA           6 ;; Used in 4.0 release candidates */
         :RDB_TYPE_MODULE_2                7 ;; Module value with annotations for parsing without
                    ;; the generating module being loaded. */
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
         :RDB_TYPE_HASH_METADATA_PRE_GA    22   ;; Hash with HFEs. Doesn't attach min TTL at start (7.4 RC) */
         :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA 23   ;; Hash LP with HFEs. Doesn't attach min TTL at start (7.4 RC) */
         :RDB_TYPE_HASH_METADATA           24             ;; Hash with HFEs. Attach min TTL at start */
         :RDB_TYPE_HASH_LISTPACK_EX        25          ;; Hash LP with HFEs. Attach min TTL at start */
;; NOTE: WHEN ADDING NEW RDB TYPE, UPDATE rdbIsObjectType(), and rdb_type_string[] */

;; Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
         :RDB_OPCODE_SLOT_INFO             244   ;; Individual slot info, such as slot id and size (cluster mode only). */
         :RDB_OPCODE_FUNCTION2             245   ;; function library data */
         :RDB_OPCODE_FUNCTION_PRE_GA       246   ;; old function library data for 7.0 rc1 and rc2 */
         :RDB_OPCODE_MODULE_AUX            247   ;; Module auxiliary data. */
         :RDB_OPCODE_IDLE                  248   ;; LRU idle time. */
         :RDB_OPCODE_FREQ                  249   ;; LFU frequency. */
         :RDB_OPCODE_AUX                   250   ;; RDB aux field. */
         :RDB_OPCODE_RESIZEDB              251   ;; Hash table resize hint. */
         :RDB_OPCODE_EXPIRETIME_MS         252    ;; Expire time in milliseconds. */
         :RDB_OPCODE_EXPIRETIME            253       ;; Old expire time in seconds. */
         :RDB_OPCODE_SELECTDB              254   ;; DB number of the following keys. */
         :RDB_OPCODE_EOF                   255   ;; End of the RDB file. */
         }))

(defn parse-header-byte []
  (let [opcode byte-codes]
    (println ::parse-expiry-or-value-type {:opcode opcode})
    (compile-frame  opcode)))

(defcodec list-encoding
  (finite-frame
   (parse-length)
   (repeated (parse-string) :prefix :none)))

(defn parse-scored-value
  []
  (log/trace ::parse-scored-value-encoding :enter)
  (header
   :byte
   (fn [header]
     (log/trace ::scored-value-encoding {:header header})
     (condp = header
       (hex->int :0xFD) (value-parser :nan)
       (hex->int :0xFE) (value-parser :pos-infinity)
       (hex->int :0xFF) (value-parser :neg-infinity)
       (gloss/string-float :ascii :length header)))
   identity))


(def parse-stream-id
  (ordered-map
   :ms :uint64
   :seq :uint64))

(defn parse-list []
  (log/trace ::parse-list :enter)
  (ordered-map
   :type :list
   :items (gloss/repeated (parse-length) (parse-string))))


(defn parse-hash []
  (log/trace ::parse-hash :enter)
  (ordered-map
   :type :hash
   :entries (gloss/repeated
             (parse-length)
             (ordered-map
              :k (parse-string)
              :v (parse-string)))))


(defn parse-set []
  (log/trace ::parse-set :enter)
  (header
   (parse-length)
   (fn [{:keys [size]}]
     (log/trace ::parse-set {:elements size})
     (ordered-map
      :type :set
      :items (repeat size (parse-string))))
   identity))

(defn parse-zset []
  (log/trace ::parse-zset :enter)
  (header
   (parse-length)
   (fn [{:keys [size]}]
     (log/trace ::parse-zset {:elements size})
     (ordered-map
      :type :sorted-set
      :entries (repeat
                size
                (ordered-map
                 :v (parse-string)
                 :score (compile-frame :int64)))))
   identity))


(defn parse-zset2 []
  (ordered-map
   :type :sorted-set
   :entries (repeated
             (-> (parse-length) :size)
             (ordered-map
              :member (parse-string)
              :score (parse-scored-value)))))


(defn parse-zipmap
  []
  (compile-frame (parse-string)))

(defn parse-ziplist
  []
  (compile-frame (parse-string)))

(defn parse-intset-ziplist
  []
  (compile-frame (parse-string)))

(defn parse-zset-ziplist
  []
  (compile-frame (parse-string)))

(defn parse-hash-ziplist
  []
  (compile-frame (parse-string)))

(defn parse-quicklist
  []
  (compile-frame (parse-string)))

;; Excellent treatise on listpacks
;; https://github.com/zpoint/Redis-Internals/blob/5.0/Object/listpack/listpack.md

(defn parse-stream-id
  []
  (compile-frame
   (ordered-map :ms :uint64-be ;; High 64 bits of stream ID
                :seq :uint64-be ;; Low 64 bits of stream ID
                )))

;; However Redis is storing these things differently than the struct in their stream.h
;; Some compression of the data at the end is going on (i.e., 0x00 when no groups exist)
;; as the stream id is only represented by the high 64 bits in the end metadata data.
(defn parse-stream-listpack
  []
  (log/trace ::parse-stream-listpack3 :begin)
  (header
   (ordered-map :metadata :int16-le    ;; Metadata size, 2 bytes
                :stream-id (parse-stream-id)
                :content (parse-string))
   (fn [{:keys [content metadata stream-id listpack-size elements-count]}]
     (log/trace ::parse-stream-listpack3 {:metadata  metadata ;; unknown at this point
                                          :stream-id stream-id
                                          :content   content})
     ;; Continue with listpack parsing
     (ordered-map :type :RDB_TYPE_STREAM_LISTPACKS_3
                  :metadata-size metadata
                  :stream-id stream-id
                  :content content
                  :current-elements (parse-length)
                  :flag :byte
                  :last-id :uint64-be
                  :first-stream-len (parse-length)
                  :unknown :int32-le
                  :groups :byte))
   identity))

(defn parse-stream-listpacks2
  []
  (log/trace ::parse-stream-listpack3 :begin)
  (header
   (ordered-map :metadata :int16-le    ;; Metadata size, 2 bytes
                :stream-id (parse-stream-id)
                :content (parse-string))
   (fn [{:keys [content metadata stream-id listpack-size elements-count]}]
     (log/trace ::parse-stream-listpack3 {:metadata  metadata ;; unknown at this point
                                          :stream-id      stream-id
                                          :content  content})
     ;; Continue with listpack parsing
     (ordered-map :type :RDB_TYPE_STREAM_LISTPACKS_3
                  :metadata-size metadata
                  :stream-id stream-id
                  :content content
                  :current-elements (parse-length)
                  :flag :byte
                  :last-id :uint64-be
                  :first-stream-len (parse-length)
                  :flag :byte
                  :first-id :uint64-be
                  :unknown :int32-le
                  :padding :byte))
   identity))


(defn parse-stream-listpack-3
  []
  (log/trace ::parse-stream-listpack3 :begin)
  (header
   (ordered-map :metadata :int16-le    ;; Metadata size, 2 bytes
                :stream-id (parse-stream-id)
                :content (parse-string))
   (fn [{:keys [content metadata stream-id]}]
       ;; solely to log parsing in this foresaken structure lol.
     (log/trace ::parse-stream-listpack3 {:metadata  metadata ;; unknown at this point
                                          :stream-id      stream-id
                                          :content  content})
     ;; Continue with listpack parsing
     (ordered-map :type :RDB_TYPE_STREAM_LISTPACKS_3
                  :metadata-size metadata
                  :stream-id stream-id
                  :content content
                  :current-elements (parse-length)
                  :flag :byte
                  :last-id :uint64-be
                  :first-stream-len (parse-length)
                  :flag :byte
                  :first-id :uint64-be
                  :unknown :int32-le
                  :padding :byte))
   identity))

(defn parse-hash-listpacks
  []
  (compile-frame (parse-string)))

(defn parse-hash-listpacks-ex
  []
  (compile-frame [(parse-string)]))

(defn parse-set-listpacks
  []
  (compile-frame (parse-string)))

(defn parse-quicklist2
  []
  (header
   (ordered-map :unknown :uint64
                :content (gloss/repeated :byte :delimiters [-1]))
   (fn [{:keys [content unknown]}]
          ;; solely to log parsing in this foresaken structure lol.
     (log/trace ::parse-stream-listpack3 {:unknown unknown
                                          :content  content})
     (ordered-map :type :RDB_TYPE_LIST_QUICKLIST_2
                  :unknown unknown
                  :content content))
   identity))

(defn parse-zset-listpacks
  []
  (compile-frame (parse-string)))


(defn parse-zipmap-length
  []
  (compile-frame
   (header
    :byte
    (fn [first-byte]
      (if (< first-byte 254)
        (compile-frame first-byte)
          ;; Read next 4 bytes as length
        (gloss/finite-frame 4 :uint32-be)))
    identity)))

(defn value-kind->value
  [kind]
  (println ::value-kind->value kind)
  (compile-frame
   (case kind
     :RDB_TYPE_STRING                  (parse-string)
     :RDB_TYPE_LIST                    (parse-list)
     :RDB_TYPE_SET                     (parse-set)
     :RDB_TYPE_ZSET                    (parse-zset)
     :RDB_TYPE_HASH                    (parse-hash)
     :RDB_TYPE_ZSET_2                  (parse-zset2) ;; ZSET version 2 with doubles stored in binary. */
     :RDB_TYPE_HASH_ZIPMAP             (parse-zipmap)
     :RDB_TYPE_LIST_ZIPLIST            (parse-ziplist)
     :RDB_TYPE_SET_INTSET              (parse-intset-ziplist)
     :RDB_TYPE_ZSET_ZIPLIST            (parse-zset-ziplist)
     :RDB_TYPE_HASH_ZIPLIST            (parse-hash-ziplist)
     :RDB_TYPE_LIST_QUICKLIST          (parse-quicklist)
     :RDB_TYPE_STREAM_LISTPACKS        (parse-stream-listpack)
     :RDB_TYPE_HASH_LISTPACK           (parse-hash-listpacks)
     :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA (parse-hash-listpacks)
     :RDB_TYPE_ZSET_LISTPACK           (parse-zset-listpacks)
     :RDB_TYPE_LIST_QUICKLIST_2        (parse-quicklist2)
     :RDB_TYPE_STREAM_LISTPACKS_2      (parse-stream-listpacks2)
     :RDB_TYPE_SET_LISTPACK            (parse-set-listpacks)
     :RDB_TYPE_STREAM_LISTPACKS_3      (parse-stream-listpack-3)
     (throw (ex-info "Unknown kind" {:kind kind})))))


(defn is-expiry? [bits]
  (condp = header
    (hex->int :0xFC) true
    (hex->int :0xFD) true
    false))

(defn get-expiry [kind]
  (condp = kind
    (hex->int :0xFC) (ordered-map :type :expiry
                                  :unit :seconds
                                  :expiry (gloss/string :utf8 :length 8))
    (hex->int :0xFD) (ordered-map
                      :type :expiry-ms
                      :unit :milliseconds
                      :expiry (gloss/string :utf8 :length 8))
    {}))

(defcodec rdb-header
  (ordered-map
   :signature (gloss/string :ascii :length 5)
   :version   (gloss/string :ascii :length 4)))

(defcodec auxiliary-field
  (ordered-map :type :aux
               :k (parse-string)
               :v (parse-string)))

(defcodec expiretime
  (ordered-map :type :expiretime-ms
               :expiry :int64
               :next-entry ()))

(defcodec eof
  gloss/nil-frame)

(defcodec resizedb-info
  (ordered-map :type :resizdb-info
               :db-hash-table-size (parse-length)
               :expiry-hash-table-size (parse-length)))

(defcodec selectdb
  (ordered-map :type :selectdb
               :db-number (parse-length)))

(defn parse-key-value
  [expiry-or-kind]
  (let [expiry (get-expiry expiry-or-kind)
        kind   (if (empty? expiry) expiry-or-kind (parse-header-byte))]
    (log/trace ::key-value {:expiry      expiry
                            :kind        kind
                            :expiry-or-kind expiry-or-kind})
    (compile-frame (ordered-map
                    :type :key-value
                    :expiry expiry
                    :kind kind
                    :k  (parse-string)
                    :v (value-kind->value kind)))))

(defcodec section-selector
  (header
   (parse-header-byte)
   (fn [header]
     (log/trace ::section-selector {:header header})
     (case header
       :RDB_OPCODE_AUX auxiliary-field
       :RDB_OPCODE_RESIZEDB resizedb-info
       :RDB_OPCODE_SELECTDB selectdb
       :RDB_OPCODE_EOF gloss/nil-frame
       (parse-key-value header)))
   (fn [body]
     (log/trace ::section-selector {:frame body})
     body)))


(defcodec sections
  (repeated section-selector :prefix :none :delimiters [-1]))

(defcodec sections-debug
  (repeated (header section-selector
                    (fn [frame]
                      (println ::sections-frame-parsed frame)
                      frame)
                    identity)
            :delimiters [-1]))

(defcodec rdb-file
  [rdb-header sections])



;; ------------------------------------------------------------------------------------------- REPL


(comment
  (ns-unalias *ns* 'header)
  (do
    (log/set-min-level! :trace)

    (require  '[clj-commons.byte-streams :as bs]
              '[clojure.java.io :as io]
              '[clojure.pprint :as pp]
              '[clojure.walk :as walk]
              '[manifold.deferred :as d]
              '[manifold.stream :as ms])

    (defn apply-f-to-key [m k f]
      (walk/postwalk
       (fn [x]
         (if (and (map? x) (contains? x k))
           (update x k f)
           x))
       m))

    (def test-file (-> (io/resource "test/rdb/dump.rdb")
                       io/input-stream
                       (bs/convert (bs/stream-of bytes))

                       #_bs/stream-of))

    (defcodec rdb-zset-parsing
      (ordered-map
       :kind     (parse-header-byte)
       :elements (parse-length)
       :value    [:size (parse-length)
                  :string-len (parse-length)]))

    (defn parse-stream-id
      []
      (compile-frame
       (ordered-map :ms :uint64-be ;; High 64 bits of stream ID
                    :seq :uint64-be ;; Low 64 bits of stream ID
                    )))

    (defn parse-stream-listpack-3
      []
      (log/trace ::parse-stream-listpack3 :begin)
      (header
       (ordered-map :metadata :int16-le    ;; Metadata size, 2 bytes
                    :stream-id (parse-stream-id)
                    :content (parse-string))
       (fn [{:keys [content metadata stream-id listpack-size elements-count]}]
         (log/trace ::parse-stream-listpack3 {:metadata  metadata ;; unknown at this point
                                              :stream-id stream-id
                                              :content   content})
     ;; Continue with listpack parsing
         (ordered-map :type :RDB_TYPE_STREAM_LISTPACKS_3
                      :metadata-size metadata
                      :stream-id stream-id
                      :content content
                      :current-elements (parse-length)
                      :flag :byte
                      :last-id :uint64-be
                      :first-stream-len (parse-length)
                      :flag :byte
                      :first-id :uint64-be
                      :unknown :int32-le
                      :padding :byte))
       identity))



    (defcodec test [rdb-header

                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    section-selector
                    ;sections



                    #_(repeated section-selector :prefix :none :delimiters [-1])])

    (defcodec db [:header rdb-header
                  sections])
    (try
      (def decoder-ring-magic-header (gloss.io/decode-stream-headers test-file db))
      (catch Exception e
        (ex-message e))))

  (let [results @(-> decoder-ring-magic-header
                     ms/take!)
        results (apply-f-to-key results :k binary-array->string)]
    (pp/pprint results))

  (do
    (require  '[clj-commons.byte-streams :as bs]
              '[clojure.java.io :as io]
              '[clojure.pprint :as pp]
              '[clojure.walk :as walk]
              '[manifold.deferred :as d]
              '[manifold.stream :as ms])
    
    (def test-file (-> (io/resource "test/rdb/dump.rdb")
                       io/input-stream
                       (bs/convert (bs/stream-of bytes))
                       #_bs/stream-of))
    (def decoder-ring-magic-header (gloss.io/decode-stream-headers test-file rdb-header)) 

    #_@(ms/take! section-reader)
    (defn apply-f-to-key [m k f]
      (walk/postwalk
       (fn [x]
         (if (and (map? x) (contains? x k))
           (update x k f)
           x))
       m))
    
    (defn deserialize [source]
      (loop [source source
             result []]
        (let [parsed (first source)]
          (log/trace ::deserialize {:parsed parsed
                                    :result result})
          (if-not (seq parsed)
            result
            (recur (rest source) (conj result parsed))))))

    (defn parse-database [db]
      (let [header @(ms/take! decoder-ring-magic-header)
            buffer @(ms/take! decoder-ring-magic-header)
            section-reader (gloss.io/lazy-decode-all section-selector buffer)
            results        (deserialize section-reader)
            results        (apply-f-to-key results :k binary-array->string)]
        (pp/pprint results))))
  (parse-database test-file)

  
  (def decoder-ring (gloss.io/decode-stream test-file section-selector))
  (ms/take! decoder-ring)
  (binary-array->string (byte-array [105 110 116 101 103 101 114 115 116 114 105 110 103]))

  (binary-array->string (byte-array [75
                                     0
                                     0
                                     0
                                     10
                                     0
                                     -123
                                     98
                                     114
                                     97
                                     110
                                     100
                                     6
                                     -121
                                     80
                                     111
                                     114
                                     115
                                     99
                                     104
                                     101
                                     8
                                     -124
                                     116
                                     114
                                     105
                                     109
                                     5
                                     -125
                                     71
                                     84
                                     83
                                     4
                                     -124
                                     121
                                     101
                                     97
                                     114
                                     5
                                     -57
                                     -29
                                     2
                                     -122
                                     101
                                     110
                                     103
                                     105
                                     110
                                     101
                                     7
                                     -113
                                     50
                                     46
                                     52
                                     76
                                     32
                                     84
                                     117
                                     114
                                     98
                                     111
                                     32
                                     52
                                     99
                                     121
                                     108
                                     16
                                     -126
                                     104
                                     112
                                     3
                                     -63
                                     109
                                     2
                                     -1]))

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

  (binary-array->string (byte-array [114 97 99 101 58 102 114 97 110 99 101]))

  (-> listpack-bytes (get 3) count)



                    ;;  :(parse-header-byte) 
                    ;;  :redis-ver auxiliary-field
                    ;; :(parse-header-byte) (parse-header-byte)
                    ;; :redis-bits auxiliary-field
                    ;; :(parse-header-byte) (parse-header-byte)
                    ;; :ctime auxiliary-field
                    ;; :(parse-header-byte) (parse-header-byte)
                    ;; :used-mem auxiliary-field
                    ;; :(parse-header-byte) (parse-header-byte)
                    ;; :aof-base auxiliary-field
                    ;; :(parse-header-byte) (parse-header-byte)
                    ;; :dbselector selectdb
                    ;; :(parse-header-byte) (parse-header-byte)
                    ;; :resizedb resizedb-info
                    ;; :kv-pair-header kv-pair-header
                    ;; :ziplist length-encoding
                    ;; :key (parse-string)
                    ;; :value (parse-string)
                    ;; :next (expiry-or-value-type)
                     ;; :key (parse-string)
                    ;; :value (parse-string)
                    ;; :next (expiry-or-value-type)
                    ;; :key (parse-string)
                    ;; :value (parse-string)
                    ;; :intset kv-pair-header
                    ;; :intset-value intset-encoding
                    ;; :next (expiry-or-value-type)


  ::leave-this-here)

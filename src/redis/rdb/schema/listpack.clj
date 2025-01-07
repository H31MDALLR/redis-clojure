(ns redis.rdb.schema.listpack
  (:require
   [gloss.core :as g]
   [redis.rdb.schema.util :as util]
   [redis.rdb.schema.bytes :as bytes]
   [taoensso.timbre :as log]
   [redis.rdb.schema.primitives :as primitives]))

;; ------------------------------------------------------------------------------------------------- Layer 0
;; Depends only on things outside this namespace

;; the following are from the Redis Listpack implementation
;; https://github.com/antirez/listpack/blob/master/listpack.c#L93

(defn equivalent? [a b mask]
  (== (bit-and a mask) b))

(defn is-eof? [byte]
  (equivalent? byte 2r11111111 2r11111111))

(def lp-encoding-7bit-uint 0)
(def lp-encoding-7bit-uint-mask 0x80)
(defn is-7bit-uint-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-7bit-uint
               lp-encoding-7bit-uint-mask))

(def lp-encoding-6bit-str 0x80)
(def lp-encoding-6bit-str-mask 0xC0)
(defn is-6bit-str-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-6bit-str
               lp-encoding-6bit-str-mask))

(def lp-encoding-13bit-int 0xC0)
(def lp-encoding-13bit-int-mask 0xE0)
(defn is-13bit-int-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-13bit-int
               lp-encoding-13bit-int-mask))

(def lp-encoding-12bit-str 0xE0)
(def lp-encoding-12bit-str-mask 0xF0)
(defn is-12bit-str-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-12bit-str
               lp-encoding-12bit-str-mask))

(def lp-encoding-16bit-int 0xF1)
(def lp-encoding-16bit-int-mask 0xFF)
(defn is-16bit-int-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-16bit-int
               lp-encoding-16bit-int-mask))

(def lp-encoding-24bit-int 0xF2)
(def lp-encoding-24bit-int-mask 0xFF)
(defn is-24bit-int-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-24bit-int
               lp-encoding-24bit-int-mask))

(def lp-encoding-32bit-int 0xF3)
(def lp-encoding-32bit-int-mask 0xFF)
(defn is-32bit-int-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-32bit-int
               lp-encoding-32bit-int-mask))

(def lp-encoding-64bit-int 0xF4)
(def lp-encoding-64bit-int-mask 0xFF)
(defn is-64bit-int-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-64bit-int
               lp-encoding-64bit-int-mask))

(def lp-encoding-32bit-str 0xF0)
(def lp-encoding-32bit-str-mask 0xFF)
(defn is-32bit-str-encoding?
  [byte]
  (equivalent? byte
               lp-encoding-32bit-str
               lp-encoding-32bit-str-mask))

(defn backlen-size
  "Calculate size of backlen field based on encoded length"
  [encoded-len]
  (cond
    (<= encoded-len 127) 1
    (<= encoded-len 16383) 2
    (<= encoded-len 2097151) 3
    (<= encoded-len 268435455) 4
    :else 5))

(defn parse-listpack-header
  []
  (g/ordered-map
   :total-bytes :uint32-le
   :num-elements :uint16-le))

;; TBD - this is wrong, figure out how to get the total bytes and element count
;; once we have the transform to/from
(defn- listpack-body->header
  "Transform the listpack body into a header"
  [m]
  (let [stringized-data (-> m :value pr-str)]
    {:total-bytes  (-> stringized-data .getBytes count)
     :num-elements 1}))

;; -------------------------------------------------------------------------------------- Layer 1
;; Depends only on Layer 0

(defn get-encoding-type
  [byte]
  (condp apply [byte]
    is-7bit-uint-encoding? :number/small
    is-6bit-str-encoding? :string/tiny
    is-13bit-int-encoding? :number/thirteen-bit
    is-12bit-str-encoding? :string/four-k
    is-32bit-str-encoding? :string/large
    is-16bit-int-encoding? :number/uint16
    is-24bit-int-encoding? :number/uint24
    is-32bit-int-encoding? :number/uint32
    is-64bit-int-encoding? :number/uint64
    is-eof? :EOF))

(defn encoded-size 
  "Calculate the size of the current encoded element"
  [byte]
  (condp apply [byte]
    is-7bit-uint-encoding? 1
    is-6bit-str-encoding? 1
    is-13bit-int-encoding? 1
    is-12bit-str-encoding? 2
    is-16bit-int-encoding? 1
    is-24bit-int-encoding? 1
    is-32bit-int-encoding? 1
    is-64bit-int-encoding? 1
    is-32bit-str-encoding? 5
    is-eof? 1
    :else (throw (ex-info "Unknown encoding" {:byte byte}))))


;; -------------------------------------------------------- Parsing

(defn parse-backlen 
  "Parses backlen bytes as bytes, does not parse the value inside them."
  [size]
  (g/compile-frame
   (primitives/repeat-parser size :byte)))

;; -------------------------------------------------------------------------------------- Layer 2
;; Depends only on Layer 1

;; -------------------------------------------------------- Listpack Type Parsers
(defn parse-small-number 
  "Parses a small number from the first byte"
  [first-byte]
  (let [value (bit-and first-byte 2r01111111)]
    (log/debug ::parse-small-number {:humon "Parsing small number"
                                     :value value})
    (g/ordered-map
     :kind :number/small
     :value (primitives/value-parser value)
     :backlen (parse-backlen (encoded-size first-byte)))))

(defn parse-12bit-string [first-byte]
  (g/header
   :byte
   (fn [next-byte]
     (let [length (bit-and 2r0001111111111111 (util/bytes-to-int [first-byte next-byte]))]
       (g/ordered-map
        :kind :string/four-k
        :value (g/finite-frame
                (g/prefix length identity count)
                :byte)
        :backlen (parse-backlen (encoded-size first-byte)))))
   identity))

(defn parse-13bit-integer [first-byte]
  (g/header
   :byte
   (fn [next-byte]
     (g/ordered-map
      :kind :number/thirteen-bit
      :value (bit-and 2r0000111111111111 (util/bytes-to-int [first-byte next-byte]))
      :backlen (parse-backlen (encoded-size first-byte))))
   identity))

(defn parse-tiny-string
  "Parser for strings up to 63 bytes (10xxxxxx)"
  [first-byte]
  (let [length (bit-and first-byte 2r00111111)]
    (log/debug ::parse-tiny-string
               {:humon "Parsing tiny string"
                :encoding-byte (util/integer->binary-string first-byte)
                :encoding-pattern (format "%s" (util/integer->binary-string (bit-and first-byte 0xC0)))
                :length length})
    (g/ordered-map
     :kind :string/tiny
     :value  (g/string :utf-8 :length length)
     :backlen (parse-backlen (encoded-size first-byte)))))

(defn parse-32bit-string [first-byte]
  (g/header
   :uint32-le
   (fn four-byte-len->large-string 
     [length]
     (g/ordered-map 
      :kind :string/large
      :value (g/finite-frame length :byte)
      :backlen (parse-backlen (encoded-size first-byte))))
   (fn string->header
     [data]
     (-> data .getBytes count))))

(defn parse-uint16 [first-byte]
  (g/ordered-map 
   :kind :number/uint16
   :value :uint16-le
   :backlen (parse-backlen (encoded-size first-byte))))

(defn parse-uint24 [first-byte]
  (g/ordered-map 
   :kind :number/uint24
   :value :uint24-le
   :backlen (parse-backlen (encoded-size first-byte))))

(defn parse-uint32 [first-byte]
  (g/ordered-map
   :kind :number/uint32
   :value :uint32-le
   :backlen (parse-backlen (encoded-size first-byte))))

(defn parse-uint64 [first-byte]
  (g/ordered-map 
   :kind :number/uint64
   :value :uint64-le
   :backlen (parse-backlen (encoded-size first-byte))))

(defn parse-64bit-float [first-byte]
  (g/ordered-map 
   :kind :number/float64
   :value :float64-le
   :backlen (parse-backlen (encoded-size first-byte))))

;; -------------------------------------------------------------------------------------- Layer 3
;; Depends only on Layer 2

(defn parse-listpack-element
  []
  (g/header
   :byte
   (fn [first-byte]
     (log/debug ::parse-listpack-element 
                {:byte first-byte
                 :binary (util/integer->binary-string first-byte)
                 :is-7bit? (is-7bit-uint-encoding? first-byte)})
     (let [encoding-type (get-encoding-type first-byte)]
       (condp = encoding-type
     
           ;; 0xxxxxxx - small number
       :number/small (parse-small-number first-byte)
     
           ;; 10xxxxxx - tiny string
       :string/tiny (parse-tiny-string first-byte)
     
           ;; 110xxxxx - 13 bit signed integer
       :number/thirteen-bit (parse-13bit-integer first-byte)
     
           ;; 1110xxxx - string up to 4095
       :string/four-k (parse-12bit-string first-byte)
     
           ;; Match 1111 0000 - String with 4-byte length
       :string/large (parse-32bit-string first-byte)
     
           ;; Match 1111 0001 - uint16
       :number/uint16 (parse-uint16 first-byte)
     
           ;; Match 1111 0010 - uint24
       :number/uint24 (parse-uint24 first-byte)
     
           ;; Match 1111 0011 - uint32
       :number/uint32 (parse-uint32 first-byte)
     
           ;; Match 1111 0100 - uint64
       :number/uint64 (parse-uint64 first-byte)

           ;; Special case for EOF
       :EOF (primitives/value-parser :EOF)
     
       (do
         (log/error ::parse-encoding
                    {:humon   "Unknown multibyte encoding"
                     :byte    (util/integer->binary-string first-byte)})
         (throw (ex-info "Unknown multibyte encoding"
                         {:byte    (util/integer->binary-string first-byte)}))))))
   identity))

(defn parse-listpack []
  (g/repeated 
   (parse-listpack-element) 
   :prefix (g/prefix (parse-listpack-header) 
                     :num-elements 
                     listpack-body->header)))
 

;; ------------------------------------------------------------------------------------------------- REPL
(comment
  ;; Listpack specification from Redis 2017
  ;; https://github.com/antirez/listpack/blob/master/listpack.md
  
  (do
    (require '[manifold.stream :as s]) 
    (require '[redis.rdb.decompress.interface :as decompress])

    (log/set-min-level! :trace)

    (def stream-listpack-bytes [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1])

    (def zset-listpack {:encoding :lzh-string, :kind 0, :size 54, :special nil, :uncompressed-length {:kind 1, :size 70}, :data [26 70 0 0 0 6 0 -119 115 116 97 116 105 111 110 58 49 10 -12 32 -65 17 75 37 -36 4 0 9 -32 0 20 5 50 10 -12 -10 -128 63 -32 6 20 6 51 10 -12 109 63 121 97 64 41 1 9 -1]})
    (defn run-top-level-parser 
      [listpack]
      ;; the lp above has 137 decompressed bytes and 32 elements. Manually encode this for the LZH decompression
      (let [listpack (decompress/inflate listpack)]
          ;; Print first few bytes after header
        (println "First 10 bytes after header:" (take 10 (drop 6 listpack)))
        
        (let [stream (bytes/get-byte-stream-parser listpack (parse-listpack))]
          @(s/take! stream))))
    
    (defn run-test [listpack num-elements]
      ;; the lp above has 137 decompressed bytes and 32 elements. Manually encode this for the LZH decompression
      (let [listpack (decompress/inflate listpack)]
        (println "decompressed listpack: " listpack)

        ;; Print first few bytes after header
        (println "First 10 bytes after header:" (take 10 (drop 6 listpack)))

        (let [parser (g/compile-frame
                      (g/ordered-map
                       :header (parse-listpack-header)
                       :elements (g/compile-frame [(primitives/repeat-parser num-elements (parse-listpack-element))])))

            ;; Update the parser for debugging to the above, currently using the top level parser.
              stream (bytes/get-byte-stream-parser listpack parser)]
          @(s/take! stream)))))
  
  (run-test zset-listpack 6)
  (-> zset-listpack decompress/inflate util/binary-array->ascii-string)

  (run-top-level-parser {:encoding :lzh-string
                         :uncompressed-length {:size 137}
                         :data stream-listpack-bytes})
#_[{:kind :number/small, :value 3, :backlen [1]}
   {:kind :number/small, :value 0, :backlen [1]}
   {:kind :number/small, :value 4, :backlen [1]}
   {:kind :string/tiny, :value "rider", :backlen [6]}
   {:kind :string/tiny, :value "speed", :backlen [6]}
   {:kind :string/tiny, :value "position", :backlen [9]}
   {:kind :string/tiny, :value "location_id", :backlen [12]}
   {:kind :number/small, :value 0, :backlen [1]}
   {:kind :number/small, :value 2, :backlen [1]}
   {:kind :number/small, :value 0, :backlen [1]}
   {:kind :number/small, :value 0, :backlen [1]}
   {:kind :string/tiny, :value "Castilla", :backlen [9]}
   {:kind :string/tiny, :value "30.2", :backlen [5]}
   {:kind :number/small, :value 1, :backlen [1]}
   {:kind :number/small, :value 1, :backlen [1]}
   {:kind :number/small, :value 7, :backlen [1]}
   {:kind :number/small, :value 2, :backlen [1]}
   {:kind :number/uint16, :value 796, :backlen [0 1 -123]}
   {:kind :number/small, :value 78, :backlen [111]}
   {:kind :number/small, :value 114, :backlen [101]}
   {:kind :number/small, :value 109, :backlen [6]}
   {:kind :string/tiny, :value "28.8", :backlen [5]}
   {:kind :number/small, :value 3, :backlen [1]}
   {:kind :number/small, :value 1, :backlen [1]}
   {:kind :number/small, :value 7, :backlen [1]}
   {:kind :number/small, :value 2, :backlen [1]}
   {:kind :number/uint16, :value 820, :backlen [0 1 -120]}
   {:kind :number/small, :value 80, :backlen [114]}
   {:kind :number/small, :value 105, :backlen [99]}
   {:kind :number/small, :value 107, :backlen [101]}
   {:kind :number/small, :value 116, :backlen [116]}
   {:kind :number/small, :value 9, :backlen [-124]}] 
  
  (->>
   (run-test {:encoding :lzh-string
              :uncompressed-length {:size 137}
              :data stream-listpack-bytes} 30) ;; run this if you just want to see the elements
   :elements
   first
   (sort-by :kind)
   (partition-by :kind))
  #_(({:kind :number/small, :value 3, :backlen [1]}
      {:kind :number/small, :value 0, :backlen [1]}
      {:kind :number/small, :value 4, :backlen [1]}
      {:kind :number/small, :value 0, :backlen [1]}
      {:kind :number/small, :value 2, :backlen [1]}
      {:kind :number/small, :value 0, :backlen [1]}
      {:kind :number/small, :value 0, :backlen [1]}
      {:kind :number/small, :value 1, :backlen [1]}
      {:kind :number/small, :value 1, :backlen [1]}
      {:kind :number/small, :value 7, :backlen [1]}
      {:kind :number/small, :value 2, :backlen [1]}
      {:kind :number/small, :value 78, :backlen [111]}
      {:kind :number/small, :value 114, :backlen [101]}
      {:kind :number/small, :value 109, :backlen [6]}
      {:kind :number/small, :value 3, :backlen [1]}
      {:kind :number/small, :value 1, :backlen [1]}
      {:kind :number/small, :value 7, :backlen [1]}
      {:kind :number/small, :value 2, :backlen [1]}
      {:kind :number/small, :value 80, :backlen [114]}
      {:kind :number/small, :value 105, :backlen [99]}
      {:kind :number/small, :value 107, :backlen [101]}
      {:kind :number/small, :value 116, :backlen [116]}
      {:kind :number/small, :value 9, :backlen [-124]})
     ({:kind :number/uint16, :value 796, :backlen [0 1 -123]} {:kind :number/uint16, :value 820, :backlen [0 1 -120]})
     ({:kind :string/tiny, :value "rider", :backlen [6]}
      {:kind :string/tiny, :value "speed", :backlen [6]}
      {:kind :string/tiny, :value "position", :backlen [9]}
      {:kind :string/tiny, :value "location_id", :backlen [12]}
      {:kind :string/tiny, :value "Castilla", :backlen [9]}
      {:kind :string/tiny, :value "30.2", :backlen [5]}
      {:kind :string/tiny, :value "28.8", :backlen [5]}))
  
  ;; place to check bit anding
  (bit-and 2r11110100 2r00001111)
  (bit-and 2r00011111 2r00011111) 
  (= 0xFF 2r11111111)


  ::leave-this-here
  )
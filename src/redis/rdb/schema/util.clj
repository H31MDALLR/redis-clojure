(ns redis.rdb.schema.util
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [taoensso.timbre :as log])
  (:import
   [com.ning.compress.lzf LZFDecoder LZFEncoder]
   [com.ning.compress.lzf.util ChunkDecoderFactory]
   [java.io ByteArrayOutputStream]))

(defn apply-f-to-key
  [f k path m]
  (walk/postwalk
   (fn [x]
     (if (and (map? x) (contains? x k))
       (update-in x (into [k] path) f)
       x))
   m))


;; -------------------------------------------------------- LZH compression/decompression
(defn decompress-lzh-string->bytes
  "Decompress an LZH compressed string from Redis (which omits headers)
     compressed-bytes: the raw compressed bytes
     uncompressed-length: the expected length after decompression"
  [compressed-bytes uncompressed-length]
  (let [decoder (LZFDecoder/safeDecoder)
        input (byte-array compressed-bytes)
        output (byte-array uncompressed-length)]
    (.decodeChunk decoder 
                  input
                  0                    ; input offset
                  output              ; output buffer
                  0                   ; output offset
                  uncompressed-length ; expected length
                  )
    output))

  (defn lzf-string->string
    "Decompress an LZH compressed string from Redis (which omits headers)
   compressed-bytes: the raw compressed bytes
   uncompressed-length: the expected length after decompression"
    [compressed-bytes uncompressed-length]
    (let [bytes (decompress-lzh-string->bytes compressed-bytes uncompressed-length)]
      (String. bytes "UTF-8")))

(defn lzh-string->string 
  "Decompress an LZF string from a Redis RDB file
   Takes either a byte array or a map containing compression metadata"
  [input]
  (if (map? input)
    (let [{:keys [data uncompressed-length]} input
          len (get-in uncompressed-length [:size])]
      (lzf-string->string data len))
    ;; Assume raw bytes if not a map
    (String. input "UTF-8")))


;; -------------------------------------------------------- String / byte conversions

        (defn binary-array->string
          [arr]
          (String. (byte-array arr)  java.nio.charset.StandardCharsets/UTF_8))


        (defn bytes->string
          [bytes]
          (log/trace ::bytes->string {:bytes bytes})
          (String. bytes))


        (defn string->bytes
          [string]
          (log/trace ::string->bytes {:string string})
          (.getBytes string))

        (def kewordize-keys (partial apply-f-to-key (comp keyword binary-array->string) :k [:data]))
        (def stringize-keys (partial apply-f-to-key binary-array->string))

;; -------------------------------------------------------- Numeric strings

        (defn parse-stringized-value
          [bytes]
          (let [s (binary-array->string bytes)
                maybe-number (str/replace s "," "")
                v (or (parse-long maybe-number) (parse-double maybe-number) s)]
            v))

        (defn numeric-string?
          "Returns true if string represents a valid number"
          [s]
          (boolean (or (parse-long s)
                       (parse-double s))))

;; ---------------------------------------------------------------------------- REPL
        (comment
          (parse-stringized-value [49 48 46 53 51 52 53 49 52 51 57 53 56 54 49 57 48])
          (parse-stringized-value [50 44 49 52 55 44 52 56 51 44 54 52 55])
          (parse-long "2147483647")

          (decompress-lzh-string->bytes  [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1] 137)
          (lzf-string->string  [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1] 137)
          ::leave-this-here)

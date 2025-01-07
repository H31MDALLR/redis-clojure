(ns redis.rdb.decompress.core 
  (:require
   [taoensso.timbre :as log]) 
  (:import
   [com.ning.compress.lzf LZFDecoder]))

;; ---------------------------------------------------------------------------- Layer 0
;; Depends only on things outside this namespace.

;; -------------------------------------------------------- LZH
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
  "Decompress an LZH compressed string from Redis (which omits headers).
   Takes either a byte array or a map containing compression metadata\n
   compressed-bytes | the raw compressed bytes\n
   uncompressed-length | the expected length after decompression"
  [compressed-bytes uncompressed-length]
  (let [bytes (decompress-lzh-string->bytes compressed-bytes uncompressed-length)]
    (String. bytes "UTF-8")))

(defn lzh-string->string
  "Decompress an LZF string from a Redis RDB file"
   
  [input]
  (if (map? input)
    (let [{:keys [data uncompressed-length]} input
          len (get-in uncompressed-length [:size])]
      (lzf-string->string data len))
    ;; Assume raw bytes if not a map
    (String. input "UTF-8")))

;; ---------------------------------------------------------------------------- Layer 1
;; Depends only on Layer 0

;; -------------------------------------------------------- Exposed via interface

(defmulti inflate :encoding)

(defmethod inflate :string
  [value-map]
  (-> value-map :data))

(defmethod inflate :lzh-string
  [{:keys [data uncompressed-length] :as v}]
  (log/trace ::inflate {:inflating v})
  (let [uncompressed-len (->  uncompressed-length :size)
        decompressed     (decompress-lzh-string->bytes data uncompressed-len)]
    decompressed))


;; ---------------------------------------------------------------------------- REPL

(comment 
  
  (decompress-lzh-string->bytes  [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1] 137)
  (lzf-string->string  [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1] 137)

  ::leave-this-here
  )
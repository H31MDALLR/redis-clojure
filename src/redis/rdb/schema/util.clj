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

;; ---------------------------------------------------------------------------- Bitwise operations
(defmulti bytes-to-int (fn [arr] (type arr)))

(defmethod bytes-to-int byte/1
  [^bytes arr]
  (reduce (fn [acc b]
            (bit-or (bit-shift-left acc 8)
                    (bit-and b 0xFF)))
          0
          arr))

(defmethod bytes-to-int clojure.lang.PersistentVector
  [^bytes arr]
  (bytes-to-int (byte-array arr)))

(defmethod bytes-to-int :default
  [arr]
  (throw (ex-info "Unsupported type" {:type (type arr)})))

;; ---------------------------------------------------------------------------- Key transformations

(defn apply-f-to-key
  "Apply a function to a key in a map"
  [f k path m]
  (walk/postwalk
   (fn [x]
     (if (and (map? x) (contains? x k))
       (update-in x (into [k] path) f)
       x))
   m))



;; -------------------------------------------------------- String / byte conversions

(defn integer->binary-string [i]
  (Integer/toBinaryString (bit-and i 0xFF)))

(defn binary-array->string
  [arr]
  (log/trace ::binary-array->string {:arr arr})
  (String. (byte-array arr)  java.nio.charset.StandardCharsets/UTF_8))

(defn binary-array->ascii-string
  [arr]
  (log/trace ::binary-array->string {:arr arr})
  (String. (byte-array arr)  java.nio.charset.StandardCharsets/US_ASCII))

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

  (let [first  ( = 13315 (bytes-to-int [52 3]))
        second ( = 13315 (bytes-to-int (byte-array [52 3])))
        third  ( = (bytes-to-int (byte-array [52 3])) (bytes-to-int [52 3]))]
    (and first second third))

  ::leave-this-here)

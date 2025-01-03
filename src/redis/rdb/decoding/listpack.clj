(ns redis.rdb.decoding.listpack
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]
            [clojure.java.io :as io])
  (:import [java.util.zip Inflater]))


;; Excellent treatise on listpacks
;; https://github.com/zpoint/Redis-Internals/blob/5.0/Object/listpack/listpack.md

;; -------------------------------------------------------------------------- defs

(def listpack-header-size 6)  ; Total size (4 bytes) + Number of elements (2 bytes)
(def listpack-entry-size 4)   ; Entry header size

;; -------------------------------------------------------------------------- decode impl

(defn- bytes->int 
  "Convert bytes to int, assuming little-endian byte order"
  [bytes]
  (reduce (fn [acc b] 
            (+ (bit-shift-left acc 8) 
               (bit-and b 0xFF)))
          0
          (reverse bytes)))

(defn- decode-listpack-header [data]
  (let [total-size (bytes->int (subvec (:data data) 0 4))
        num-elements (bytes->int (subvec (:data data) 4 6))]
    {:total-size total-size
     :num-elements num-elements}))

(defn- decompress-lzf
  "The format has two types of blocks:
   Literal runs (ctrl < 32): Just copy the next N bytes directly
   Back references (ctrl >= 32): Copy previously seen bytes from a given offset
   

   For literal runs:
   Length is (ctrl + 1)
   Use System/arraycopy for efficient copying

   For back references:
Length is (len + 2) where len is (ctrl >> 5)
If len is 7, read an additional byte for the length
Reference offset is calculated from the lower 5 bits of ctrl and the next byte
Copy bytes one at a time to handle overlapping references correctly

   The implementation:

   1. Takes the input bytes and expected output length
Uses byte arrays for efficient access and modification
Maintains input (ip) and output (op) pointers
Processes each control byte to either copy literals or handle back references
Returns the decompressed data as a Clojure vector"
  [data]
  (let [in-data  (byte-array (:data data))
        out-len  (get-in data [:uncompressed-length :size])
        out-data (byte-array out-len)
        in-len   (alength in-data)]
    (loop [ip 0  ; input pointer
           op 0] ; output pointer
      (if (>= ip in-len)
        (vec out-data)
        (let [ctrl (bit-and (aget in-data ip) 0xFF)]
          (if (< ctrl 32)  ; literal run
            (let [run-len (inc ctrl)]
              (when (and (< (+ ip run-len) in-len)
                         (< (+ op run-len) out-len))
                (System/arraycopy in-data (inc ip) out-data op run-len)
                (recur (+ ip (inc run-len)) (+ op run-len))))
            (let [len        (bit-shift-right ctrl 5)
                  ref-high   (bit-and ctrl 0x1f)
                  ref-low    (bit-and (aget in-data (inc ip)) 0xFF)
                  ref-offset (+ (* ref-high 256) ref-low 1)
                  final-len  (if (= len 7)
                               (+ len (bit-and (aget in-data (+ ip 2)) 0xFF))
                               len)
                  total-len  (+ final-len 2)]
              (when (and (>= op ref-offset)
                         (< (+ op total-len) out-len))
                (loop [i 0]
                  (when (< i total-len)
                    (aset out-data (+ op i)
                          (aget out-data (- (+ op i) ref-offset)))
                    (recur (inc i))))
                (recur (+ ip (if (= len 7) 3 2))
                       (+ op total-len))))))))))

(defn- decode-compressed-entries [data]
  (decompress-lzf data))

(defn- decode-entries [data compressed?]
  (if compressed?
    (decode-compressed-entries data)
    (vec (:data data))))

(defn- decode-stream-entries [data]
  (let [header (decode-listpack-header data)
        entries-data (assoc data :data (subvec (:data data) listpack-header-size))
        entries (decode-entries entries-data true)]
    {:header header
     :entries entries}))

;; -------------------------------------------------------- Storage Format Decoders

(defmethod decode-storage ::listpack [value _]
  {:type :listpack
   :data (decode-stream-entries value)})

(defmethod decode-storage ::listpack-v2 [value _]
  (let [entries (decode-stream-entries value)]
    {:type :listpack
     :data (assoc entries :version 2)}))

(defmethod decode-storage ::listpack-v3 [value _]
  (let [entries (decode-stream-entries value)
        flags (bit-and (first (:data value)) 0xFF)]
    {:type :listpack
     :data (assoc entries
                  :version 3
                  :metadata {:flags flags
                             :encoding (:type value)
                             :uncompressed-length (:uncompressed-length value)})}))

(defmethod decode-storage ::listpack-collection [value _]
  {:type :listpack-collection
   :data (mapv decode-stream-entries value)})

(defmethod decode-storage ::listpack-v2-collection [value _]
  {:type :listpack-collection
   :data (mapv #(assoc (decode-stream-entries %) :version 2) value)})

(defmethod decode-storage ::listpack-v3-collection [value _]
  {:type :listpack-collection
   :data (mapv #(assoc (decode-stream-entries %)
                       :version 3
                       :metadata {:flags (bit-and (first (:data %)) 0xFF)
                                  :encoding (:type %)
                                  :uncompressed-length (:uncompressed-length %)})
               value)})

;; -------------------------------------------------------------------------- REPL

(comment
  
;; The structure of the listpack is:
;; <tot-bytes> <num-elements> <element-1> ... <element-N> <listpack-end-byte>
;; Where each element is of the structure:
;; <encoding-type><element-data><element-tot-len>.
;; Reference: https://github.com/antirez/listpack/blob/master/listpack.md


  
  (do
    (require '[clojure.edn :as edn]
             '[clojure.java.io :as io]
             '[java-time.api :as jt]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def input-data (-> "test/db/deserialized.edn"
                        io/resource
                        slurp
                        edn/read-string)))
  (get (second input-data) 9)

  (decode-storage (-> (second input-data) (get 9) :v) ::listpack)

  ::leave-this-here)

(defn- string->long
  "Attempts to parse string as long, returns nil if not possible"
  [s]
  (try 
    (Long/parseLong s)
    (catch NumberFormatException _
      nil)))

(defn- string->double
  "Attempts to parse string as double, returns nil if not possible"
  [s]
  (try 
    (Double/parseDouble s)
    (catch NumberFormatException _
      nil)))

(defn- numeric-string?
  "Returns true if string represents a valid number"
  [s]
  (boolean (or (string->long s)
               (string->double s))))
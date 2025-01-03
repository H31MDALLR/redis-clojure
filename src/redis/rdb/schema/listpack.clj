(ns redis.rdb.schema.listpack
  (:require
   [gloss.core :as g]
   [gloss.core.formats :refer [to-byte-buffer]]
   [gloss.io :as gio]
   [redis.rdb.schema.util :as util]))

(defn parse-small-number []
  "Parser for 7-bit unsigned integers (0xxxxxxx)"
  (g/compile-frame
   (g/ordered-map
    :type :string
    :value (g/compile-frame 
            :byte
            #(str (bit-and % 0x7F))  ; Convert to string representation
            #(Integer/parseInt %)))))

(defn parse-tiny-string []
  "Parser for strings up to 63 bytes (10xxxxxx)"
  (g/header
   :byte
   (fn [first-byte]
     (let [length (bit-and first-byte 0x3F)]
       (g/ordered-map
        :type :string
        :value (g/string :utf-8 :length length))))
   identity))

(defn parse-13bit-integer []
  "Parser for 13-bit signed integers (110xxxxx yyyyyyyy)"
  (g/compile-frame
   (g/ordered-map
    :type :string
    :value (g/compile-frame
            [:byte :byte]
            (fn [[b1 b2]]
              (let [val (+ (bit-shift-left (bit-and b1 0x1F) 8) b2)
                    ;; Handle sign bit (bit 12)
                    signed-val (if (bit-test val 12)
                               (- val 0x2000)
                               val)]
                (str signed-val)))
            #(Integer/parseInt %)))))

(defn parse-4095-string []
  "Parser for strings up to 4095 bytes (1110xxxx yyyyyyyy)"
  (g/header
   [:byte :byte]
   (fn [[b1 b2]]
     (let [length (+ (bit-shift-left (bit-and b1 0x0F) 8) b2)]
       (g/ordered-map
        :type :string
        :value (g/string :utf-8 :length length))))
   identity))

(defn parse-large-string []
  "Parser for strings with 4-byte length (11110000 <len> <string>)"
  (g/header
   [:uint32-le]
   (fn [length]
     (g/ordered-map
      :type :string
      :value (g/string :utf-8 :length length)))
   identity))

(defn parse-multi-byte-integer [size]
  "Parser for 16/24/32/64 bit integers"
  (g/compile-frame
   (g/ordered-map
    :type :string
    :value (g/compile-frame
            (case size
              16 :int16-le
              24 [:byte :byte :byte]
              32 :int32-le
              64 :int64-le)
            str
            #(Long/parseLong %)))))

(defn parse-listpack-element []
  "Parser for a single listpack element"
  (g/header
   :byte
   (fn [first-byte]
     (let [first-bit  (bit-test first-byte 7)
           second-bit (bit-test first-byte 6)]
       (cond
         ;; 0xxxxxxx - Small number
         (not first-bit)
         (parse-small-number)
         
         ;; 10xxxxxx - Tiny string
         (and first-bit (not second-bit))
         (parse-tiny-string)
         
         ;; 11xxxxxx - Multi byte encodings
         (and first-bit second-bit)
         (let [sub-encoding (bit-and first-byte 0x3F)]
           (cond
             ;; 110xxxxx - 13 bit integer
             (< sub-encoding 0x30)
             (parse-13bit-integer)
             
             ;; 1110xxxx - String up to 4095 bytes
             (< sub-encoding 0x38)
             (parse-4095-string)
             
             ;; 1111xxxx - Special encodings
             (= (bit-and first-byte 0xF0) 0xF0)
             (case (bit-and first-byte 0x0F)
               0x00 (parse-large-string)
               0x01 (parse-multi-byte-integer 16)
               0x02 (parse-multi-byte-integer 24)
               0x03 (parse-multi-byte-integer 32)
               0x04 (parse-multi-byte-integer 64)
               0x0F nil  ; End of listpack marker
               (throw (Exception. (str "Unknown special encoding: " (bit-and first-byte 0x0F)))))
             
             :else
             (throw (Exception. (str "Invalid listpack encoding: " (format "0x%02X" first-byte)))))))))
   identity))

(def listpack-header
  (g/ordered-map
   :total-bytes :uint32-le
   :num-elements :uint16-le))

(defn parse-listpack []
  (g/ordered-map
   :header listpack-header
   :elements (g/repeated (parse-listpack-element)
                        :prefix false
                        :terminator nil?)))

(defn decode-listpack [bytes]
  (let [parsed (gio/decode (parse-listpack) (to-byte-buffer bytes))]
    {:total-bytes (get-in parsed [:header :total-bytes])
     :num-elements (get-in parsed [:header :num-elements])
     :elements (->> parsed
                   :elements
                   (remove nil?)  ; Remove end marker
                   (mapv :value))}))


;; ------------------------------------------------------------------------------------------------- REPL
(comment
  ;; Listpack specification from Redis 2017
  ;; https://github.com/antirez/listpack/blob/master/listpack.md
  
  (let [bytes (byte-array [31 -119 0 0 0 32 0 3 1 0 1 4 1 -123 114 105 100 101 114 6 -123 115 112 101 101 100 6 -120 112 111 115 105 116 8 105 111 110 9 -117 108 111 99 97 64 9 6 95 105 100 12 0 1 2 32 44 18 0 1 -120 67 97 115 116 105 108 108 97 9 -124 51 48 46 50 5 1 32 0 0 7 32 27 19 -15 99 28 3 0 1 -123 78 111 114 101 109 6 -124 50 56 46 56 5 3 -64 26 1 -116 52 32 26 16 -120 80 114 105 99 107 101 116 116 9 -124 50 57 46 55 5 2 64 29 1 1 -1])
        listpack  (util/lzf-string->string bytes 137)] 
    (decode-listpack listpack))
  
  ::leave-this-here)
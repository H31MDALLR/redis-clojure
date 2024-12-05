 (ns redis.rdb.parser
   (:import [java.io DataInputStream BufferedInputStream FileInputStream]))
 
 ;; Declare functions to handle forward references
 (declare parse-string-object 
          parse-encoded-string 
          parse-hash-listpack
          parse-object 
          parse-lzf-string
          parse-length
          parse-int 
          parse-signed-short 
          parse-byte 
          parse-signed-byte 
          parse-bytes
          parse-string 
          parse-entry 
          parse-list
          parse-list-quicklist
          parse-list-quicklist-2
          parse-set 
          parse-stream-listpacks
          parse-zset
          parse-zset-2
          parse-hash 
          parse-zset-listpack)
 
 ;; Define constants as vars for use in comparisons
 (def RDB_OPCODE_AUX           0xFA)
 (def RDB_OPCODE_RESIZEDB      0xFB)
 (def RDB_OPCODE_EXPIRETIME_MS 0xFC)
 (def RDB_OPCODE_EXPIRETIME    0xFD)
 (def RDB_OPCODE_SELECTDB      0xFE)
 (def RDB_OPCODE_EOF           0xFF)
 
 (def RDB_TYPE_STRING          0x00)
 (def RDB_TYPE_LIST            0x01)
 (def RDB_TYPE_SET             0x02)
 (def RDB_TYPE_ZSET            0x03)
 (def RDB_TYPE_HASH            0x04)
 (def RDB_TYPE_ZSET_2          0x05)
 (def RDB_TYPE_MODULE          0x06)
 (def RDB_TYPE_MODULE_2        0x07)
 
 ;; Object types for encoded objects
 (def RDB_TYPE_HASH_ZIPMAP      0x09)
 (def RDB_TYPE_LIST_ZIPLIST     0x0A)
 (def RDB_TYPE_SET_INTSET       0x0B)
 (def RDB_TYPE_ZSET_ZIPLIST     0x0C)
 (def RDB_TYPE_HASH_ZIPLIST     0x0D)
 (def RDB_TYPE_LIST_QUICKLIST   0x0E)
 (def RDB_TYPE_STREAM_LISTPACKS 0x0F)
 (def RDB_TYPE_HASH_LISTPACK    0x10)
 (def RDB_TYPE_ZSET_LISTPACK    0x11)
 (def RDB_TYPE_LIST_QUICKLIST_2 0x12)
 
 (def SPECIAL_ENCODING_INT8    0x00)
 (def SPECIAL_ENCODING_INT16   0x01)
 (def SPECIAL_ENCODING_INT32   0x02)
 (def SPECIAL_ENCODING_LZF     0x03)
 
 ;; Adjusted object types to match the user's error (17)
 ;; Note: Since Clojure uses decimal numbers by default, 0x11 is 17 in decimal.
 
 ;; Basic parsing functions
 (defn parse-byte [^DataInputStream in]
   (.readUnsignedByte in))
 
 (defn parse-signed-byte [^DataInputStream in]
   (.readByte in))
 
 (defn parse-signed-short [^DataInputStream in]
   (.readShort in))
 
 (defn parse-int [^DataInputStream in]
   (.readInt in))
 
 (defn parse-long [^DataInputStream in]
   (.readLong in))
 
 (defn parse-bytes [^DataInputStream in n]
   (let [bytes (byte-array n)]
     (.readFully in bytes)
     bytes))
 
 (defn parse-string [^DataInputStream in n]
   (String. (parse-bytes in n) "UTF-8"))
 
 ;; Function to parse length-encoded integers
 (defn parse-length [^DataInputStream in]
   (let [first-byte (parse-byte in)
         enc-type   (bit-shift-right first-byte 6)] ; Extract the first 2 bits
     (condp = enc-type
       0 ; 00: 6-bit length
       (bit-and first-byte 0x3F)
 
       1 ; 01: 14-bit length
       (let [second-byte (parse-byte in)]
         (bit-or (bit-shift-left (bit-and first-byte 0x3F) 8)
                 second-byte))
 
       2 ; 10: 32-bit length
       (.readInt in)
 
       3 ; 11: Special encoding
       {:special-encoding (bit-and first-byte 0x3F)})))
 
 ;; Function to parse strings (handles special encodings)
 (defn parse-string-object [^DataInputStream in]
   (let [length (parse-length in)]
     (if (map? length) ; Check for special encoding
       (parse-encoded-string in (:special-encoding length))
       (parse-string in length))))
 
 ;; Function to parse specially encoded strings
 (defn parse-encoded-string [^DataInputStream in encoding-type]
   (condp = encoding-type
     SPECIAL_ENCODING_INT8  ; 8-bit signed integer
     (str (parse-signed-byte in))
 
     SPECIAL_ENCODING_INT16 ; 16-bit signed integer
     (str (parse-signed-short in))
 
     SPECIAL_ENCODING_INT32 ; 32-bit signed integer
     (str (parse-int in))
 
     SPECIAL_ENCODING_LZF   ; LZF-compressed string
     (parse-lzf-string in)
 
     (throw (Exception. (str "Unknown string encoding: " encoding-type)))))
 
 ;; Placeholder function for LZF decompression
 (defn lzf-decompress [compressed-bytes uncompressed-length]
   ;; Implement LZF decompression here or use a library
   (throw (Exception. "LZF decompression not implemented")))
 
 ;; Function to parse LZF-compressed strings
 (defn parse-lzf-string [^DataInputStream in]
   (let [compressed-length   (parse-length in)
         uncompressed-length (parse-length in)
         compressed-bytes    (parse-bytes in compressed-length)
         uncompressed-bytes  (lzf-decompress compressed-bytes uncompressed-length)]
     (String. uncompressed-bytes "UTF-8")))
 
 ;; Function to parse different object types
 (defn parse-object [^DataInputStream in object-type]
   (condp = object-type
     RDB_TYPE_STRING          (parse-string-object in)
     RDB_TYPE_LIST            (parse-list in)
     RDB_TYPE_SET             (parse-set in)
     RDB_TYPE_ZSET            (parse-zset in)
     RDB_TYPE_ZSET_2          (parse-zset-2 in)
     RDB_TYPE_ZSET_LISTPACK   (parse-zset-listpack in)
     RDB_TYPE_HASH            (parse-hash in)
     RDB_TYPE_HASH_LISTPACK   (parse-hash-listpack in)
     RDB_TYPE_LIST_QUICKLIST  (parse-list-quicklist in)
     RDB_TYPE_LIST_QUICKLIST_2 (parse-list-quicklist-2 in)
     RDB_TYPE_STREAM_LISTPACKS (parse-stream-listpacks in)
     ;; Implement parsing for other types as needed
 
     (throw (Exception. (str "Unknown object type: " object-type)))))
 
 ;; Placeholder functions for other data types
 (defn parse-list [^DataInputStream in]
   ;; Implement list parsing
   (throw (Exception. "List parsing not implemented")))
 
 (defn parse-set [^DataInputStream in]
   ;; Implement set parsing
   (throw (Exception. "Set parsing not implemented")))
 
 (defn parse-zset [^DataInputStream in]
   ;; Implement zset parsing
   (throw (Exception. "ZSet parsing not implemented")))
 
 (defn parse-zset-2 [^DataInputStream in]
   ;; Implement zset version 2 parsing
   (throw (Exception. "ZSet version 2 parsing not implemented")))
 
 (defn parse-zset-listpack [^DataInputStream in]
   ;; Implement zset listpack parsing
   (throw (Exception. "ZSet Listpack parsing not implemented")))
 
 (defn parse-hash [^DataInputStream in]
   ;; Implement hash parsing
   (throw (Exception. "Hash parsing not implemented")))
 
 (defn parse-hash-listpack [^DataInputStream in]
   ;; Implement hash listpack parsing
   (throw (Exception. "Hash Listpack parsing not implemented")))
 
 (defn parse-list-quicklist [^DataInputStream in]
   ;; Implement list quicklist parsing
   (throw (Exception. "List QuickList parsing not implemented")))
 
 (defn parse-list-quicklist-2 [^DataInputStream in]
   ;; Implement list quicklist version 2 parsing
   (throw (Exception. "List QuickList version 2 parsing not implemented")))
 
 (defn parse-stream-listpacks [^DataInputStream in]
   ;; Implement stream listpacks parsing
   (throw (Exception. "Stream Listpacks parsing not implemented")))
 
 ;; Function to parse individual entries in the RDB file
 (defn parse-entry [^DataInputStream in]
   (let [opcode (parse-byte in)]
     (cond
       (= opcode RDB_OPCODE_EOF)
       {:type :eof}
 
       (= opcode RDB_OPCODE_SELECTDB)
       (let [db-number (parse-length in)]
         {:type :selectdb
          :db-number db-number})
 
       (= opcode RDB_OPCODE_AUX)
       (let [aux-key   (parse-string-object in)
             aux-value (parse-string-object in)]
         {:type  :aux
          :key   aux-key
          :value aux-value})
 
       (= opcode RDB_OPCODE_RESIZEDB)
       (let [db-size      (parse-length in)
             expires-size (parse-length in)]
         {:type         :resizedb
          :db-size      db-size
          :expires-size expires-size})
 
       (= opcode RDB_OPCODE_EXPIRETIME_MS)
       (let [expiry (.readLong in)
             object-type (parse-byte in)
             key         (parse-string-object in)
             value       (parse-object in object-type)]
         {:type   :key-value
          :key    key
          :value  value
          :expiry expiry})
 
       (= opcode RDB_OPCODE_EXPIRETIME)
       (let [expiry (.readInt in)
             object-type (parse-byte in)
             key         (parse-string-object in)
             value       (parse-object in object-type)]
         {:type   :key-value
          :key    key
          :value  value
          :expiry expiry})
 
       :else
       ;; Default case: assume opcode is object-type
       (let [object-type opcode
             key         (parse-string-object in)
             value       (parse-object in object-type)]
         {:type   :key-value
          :key    key
          :value  value
          :expiry nil}))))
 
 ;; Function to parse the RDB file header
 (defn parse-header [^DataInputStream in]
   (let [signature (parse-string in 5)
         version   (Integer/parseInt (parse-string in 4))]
     (when-not (= signature "REDIS")
       (throw (Exception. "Invalid RDB file signature")))
     {:signature signature
      :version   version}))
 
 ;; Main function to parse the entire RDB file
 (defn parse-rdb [filename]
   (with-open [in (DataInputStream. (BufferedInputStream. (FileInputStream. filename)))]
     (let [header (parse-header in)]
       (loop [entries []]
         (let [entry (parse-entry in)]
           (if (= (:type entry) :eof)
             {:header  header
              :entries entries}
             (recur (conj entries entry))))))))



;; ------------------------------------------------------------------------------------------- REPL
(comment 
  
(def rdb-data (parse-rdb "resources/test/rdb/dump.rdb"))
(println (:header rdb-data))
(println (count (:entries rdb-data)))
 
 (let [object-type 0]
   (condp = object-type
     RDB_TYPE_STRING "parsing string"
     
     RDB_TYPE_LIST  "list"
     
     RDB_TYPE_SET   "set"
     
     RDB_TYPE_ZSET  "zset"
     
     RDB_TYPE_HASH   "hash"
     (println "Did not parse: " object-type)))
  
  (= 0x00 0)
  
  ::leave-this-here)